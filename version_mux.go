package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

const defaultVersionRemoveTimeout = 10 * time.Minute

// A versionMux handles routing requests to the various versions available for
// a db. It handles two specific problems that crop up:
//
// First, it has the concept of "prepared" versions - versions that can serve
// local requests, but for which the cluster doesn't forward requests to by
// default. To make any version upgrade monotonic, we need to be able to (for a
// very short time) handle requests for version N-1 to outside clients, while
// responding to requests for version N if they are proxied from other nodes who
// have upgraded before us - and vice versa for clients who upgrade after us.
//
// Second, it has a scheme for making sure that all requests are finished to
// a version before closing it out. This is implemented with a timer and a
// reference count. The timer starts when we mark a version "ready to delete"
// and is reset every time a request comes in, so that a version still serving
// frequent requests will never be closed. Even once we remove a version from
// the mux, in-flight requests might have a pointer to the version, so we need
// to increment a reference count to the version when we pass one out and
// decrement it after the request is done.
type versionMux struct {
	versions             map[string]versionReferenceCount
	currentVersion       versionReferenceCount
	lock                 sync.RWMutex
	versionRemoveTimeout time.Duration
}

type versionReferenceCount struct {
	*version
	count      *sync.WaitGroup
	closeTimer *time.Timer
	removing   bool
}

func newVersionMux(overrideVersionRemoveTimeout time.Duration) *versionMux {
	mux := &versionMux{
		versions: make(map[string]versionReferenceCount),
	}

	// This is overridden by a secret config property, and then only
	// in tests.
	if overrideVersionRemoveTimeout != 0 {
		mux.versionRemoveTimeout = overrideVersionRemoveTimeout
	} else {
		mux.versionRemoveTimeout = defaultVersionRemoveTimeout
	}

	return mux
}

// serveKey is the entrypoint for HTTP requests.
func (mux *versionMux) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	proxyVersion := r.URL.Query().Get("proxy")
	var vs *version

	if proxyVersion != "" {
		vs = mux.getVersion(proxyVersion)

		// If this is a proxy request, but we don't have the version the peer is
		// asking for, just return what we have. This should never happen unless
		// sharding is broken, since peers should only route requests for versions
		// that we expressly make available.
		if vs == nil {
			vs = mux.getCurrent()

			// If we don't have *any* version, we need to indicate that, rather than
			// returning a 404, which might indicate that we do have the dataset but
			// that key doesn't exist. We use http 501 for this.
			if vs == nil {
				w.WriteHeader(http.StatusNotImplemented)
				return
			}
		}
	} else {
		vs = mux.getCurrent()
		if vs == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}

	vs.serveKey(w, r, key)
	mux.release(vs)
}

// getCurrent returns the current version and increments the reference count
// for it. It returns nil if there is no prepared version.
func (mux *versionMux) getCurrent() *version {
	mux.lock.RLock()
	defer mux.lock.RUnlock()

	vs := mux.currentVersion
	if vs.version != nil {
		vs.count.Add(1)
		if vs.closeTimer != nil {
			vs.closeTimer.Reset(mux.versionRemoveTimeout)
		}
	}

	return vs.version
}

// getVersion returns the version that matches the given name, and increments
// the reference count for it. It returns nil if there is no version matching
// that name.
func (mux *versionMux) getVersion(name string) *version {
	mux.lock.RLock()
	defer mux.lock.RUnlock()

	vs := mux.versions[name]
	if vs.version != nil {
		vs.count.Add(1)
		if vs.closeTimer != nil {
			vs.closeTimer.Reset(mux.versionRemoveTimeout)
		}
	}

	return vs.version
}

// getAll returns a snapshot of all known versions.
func (mux *versionMux) getAll() []*version {
	mux.lock.RLock()
	defer mux.lock.RUnlock()

	versions := make([]*version, 0, len(mux.versions))
	for _, vs := range mux.versions {
		versions = append(versions, vs.version)
	}

	return versions
}

// release signifies that a request is done with a version, decrementing the
// reference count.
func (mux *versionMux) release(version *version) {
	if version == nil {
		return
	}

	mux.lock.RLock()
	defer mux.lock.RUnlock()

	if vs, ok := mux.versions[version.name]; ok {
		vs.count.Done()
	}
}

// prepare puts a new version in the wings, allowing requests to be routed to it
// but not setting it as the default. If the passed version is already prepared,
// this method will panic.
func (mux *versionMux) prepare(version *version) {
	mux.lock.Lock()
	defer mux.lock.Unlock()

	if _, ok := mux.versions[version.name]; ok {
		panic(fmt.Sprintf("version already prepared: %s", version.name))
	}

	mux.versions[version.name] = versionReferenceCount{
		version: version,
		count:   new(sync.WaitGroup),
	}
}

// upgrade switches the given version to the current default. If the given
// version hasn't been prepared, or if it currently has a different version
// by the same name, this method will panic.
func (mux *versionMux) upgrade(version *version) {
	mux.lock.Lock()
	defer mux.lock.Unlock()

	vs := mux.mustGet(version)
	mux.currentVersion = vs
}

// remove starts a timer that will remove a version from the mux. Any time a
// proxied request comes in for the version, we reset the timer, so it must
// be completely unused for the full period of time before it will get removed
// from the mux (if shouldWait is false, this step is skipped). Once we remove
// it from the mux, we make extra sure that nothing is using it by waiting for
// the reference count to drop to zero. Finally, remove returns the version
// that was removed.
//
// It's idempotent to call remove for the same version multiple times. If
// the version is already being removed, remove will return nil.
func (mux *versionMux) remove(version *version, shouldWait bool) *version {
	if version == nil {
		return nil
	}

	mux.lock.Lock()
	vs, ok := mux.versions[version.name]
	alreadyRemoving := vs.removing
	vs.removing = true
	mux.lock.Unlock()

	// The version has already been removed, or is already in the process
	// of being removed
	if !ok || alreadyRemoving {
		return nil
	}

	// Set the timer, then wait for it. Any request from here on will reset the
	// timer.
	if shouldWait {
		mux.lock.Lock()
		timer := time.NewTimer(mux.versionRemoveTimeout)
		vs.closeTimer = timer
		mux.versions[version.name] = vs
		mux.lock.Unlock()

		// Wait for the timer, which is reset on every request.
		<-timer.C
	}

	mux.lock.Lock()
	delete(mux.versions, version.name)
	mux.lock.Unlock()

	// Wait for the reference count to drop to zero.
	vs.count.Wait()
	return version
}

func (mux *versionMux) mustGet(version *version) versionReferenceCount {
	vs, ok := mux.versions[version.name]
	if !ok {
		panic(fmt.Sprintf("version doesn't exist: %s", version.name))
	} else if vs.version != version {
		panic(fmt.Sprintf("somehow got another reference to the same version: %s", version.name))
	}

	return vs
}
