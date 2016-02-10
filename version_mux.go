package main

import (
	"log"
	"net/http"
	"sync"
)

// A versionMux handles routing requests to the various versions we have
// available. It handles two specific problems that crop up while routing
// requests.
//
// The first is that when we switch to a version, we need to make sure that no
// active HTTP requests are holding on to a reference to the old version
// (having not yet actually fetched data from it) before closing it out. To
// handle that, we introduce a simple reference counting scheme.
//
// Secondly, it has the concept of "prepared" versions - versions that can
// serve local requests, but for which the cluster isn't ready to serve requests
// generally. To make any version upgrade monotonic, we need to be able to (for
// a very short time) handle requests for version N-1 to outside clients, while
// responding to requests for version N if they are proxied from other nodes who
// have upgraded before us.
type versionMux struct {
	preparedVersion *version
	currentVersion  *version
	refcounts       map[*version]*sync.WaitGroup
	lock            sync.RWMutex
}

func newVersionMux() *versionMux {
	return &versionMux{
		refcounts: make(map[*version]*sync.WaitGroup),
	}
}

// ServeHTTP implements http.Handler.
func (mux *versionMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proxyVersion := r.URL.Query().Get("proxy")
	var vs *version

	if proxyVersion != "" {
		vs = mux.getVersion(proxyVersion)
		if vs == nil {
			log.Println("Got proxied request for unavailable version:", proxyVersion)
			w.WriteHeader(500)
			return
		}
	} else {
		vs = mux.getCurrent()
	}

	vs.ServeHTTP(w, r)
	mux.release(vs)
}

// getCurrent returns the current version and increments the reference count
// for it.
func (mux *versionMux) getCurrent() *version {
	mux.lock.RLock()
	defer mux.lock.RUnlock()

	vs := mux.currentVersion
	if vs != nil {
		mux.refcounts[vs].Add(1)
	}

	return vs
}

// getPrepared returns the prepared version and increments the reference count
// for it.
func (mux *versionMux) getPrepared() *version {
	mux.lock.RLock()
	defer mux.lock.RUnlock()

	vs := mux.preparedVersion
	if vs != nil {
		mux.refcounts[vs].Add(1)
	}

	return vs
}

// getVersion returns either the prepared or current version - whichever one
// matches the given version.
func (mux *versionMux) getVersion(name string) *version {
	mux.lock.RLock()
	defer mux.lock.RUnlock()

	var vs *version
	if mux.preparedVersion != nil && mux.preparedVersion.name == name {
		vs = mux.preparedVersion
	} else if mux.currentVersion != nil && mux.currentVersion.name == name {
		vs = mux.currentVersion
	}

	if vs != nil {
		mux.refcounts[vs].Add(1)
	}

	return vs
}

// release signifies that a request is done with a version, decrementing the
// reference count.
func (mux *versionMux) release(vs *version) {
	if vs != nil {
		mux.refcounts[vs].Done()
	}
}

// prepareVersion puts a new version in the wings. If there is a different
// version currently prepared, it is unceremoniously discarded (but that should
// never happen).
func (mux *versionMux) prepare(version *version) {
	mux.lock.Lock()

	mux.preparedVersion = version
	mux.refcounts[version] = &sync.WaitGroup{}

	mux.lock.Unlock()
}

// upgrade switches the prepared version to the available version, and returns
// the available version once all requests for it are finished (ie, once the
// reference count drops to zero).
func (mux *versionMux) upgrade() *version {
	mux.lock.Lock()

	oldVersion := mux.currentVersion
	mux.currentVersion = mux.preparedVersion
	mux.preparedVersion = nil

	mux.lock.Unlock()

	if oldVersion != nil {
		mux.refcounts[oldVersion].Wait()
		delete(mux.refcounts, oldVersion)
	}

	return oldVersion
}
