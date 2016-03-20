package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var errNoVersions = errors.New("no versions available")

type db struct {
	sequins *sequins

	name        string
	mux         *versionMux
	refreshLock sync.Mutex
	newVersions chan *version

	versionStatus     map[string]versionStatus
	versionStatusLock sync.RWMutex

	cleanupLock sync.Mutex
}

type dbStatus struct {
	CurrentVersion string                   `json:"current_version"`
	Versions       map[string]versionStatus `json:"versions",omitempty`
}

type versionStatus struct {
	Path    string       `json:"path"`
	Created int64        `json:"created"`
	State   versionState `json:"state"`
}

type versionState string

const (
	versionAvailable versionState = "AVAILABLE"
	versionRemoving               = "REMOVING"
	versionBuilding               = "BUILDING"
)

type trackedVersionState struct {
	versionState
	time.Time
}

func newDB(sequins *sequins, name string) *db {
	db := &db{
		sequins:       sequins,
		name:          name,
		mux:           newVersionMux(),
		versionStatus: make(map[string]versionStatus),
		newVersions:   make(chan *version),
	}

	go db.takeNewVersions()
	return db
}

// backfillVersions is called at startup, and tries to grab any versions that
// are either downloaded locally or available entirely at peers. This allows a
// node to join a cluster with an existing version all set to go, and start up
// serving that version (but exclusively proxy it). It also allows it to start
// up with stale data, even if there's newer data available.
func (db *db) backfillVersions() error {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	versions, err := db.sequins.backend.ListVersions(db.name, "", db.sequins.config.RequireSuccessFile)
	if err != nil {
		return err
	} else if len(versions) == 0 {
		return nil
	}

	// Only look at the last 3 versions, to keep this next part quick.
	if len(versions) > 3 {
		versions = versions[len(versions)-3:]
	}

	// Iterate through all the versions we know about, and track the remote and
	// local partitions for it. We don't download anything we don't have, but if
	// one is ready - because we have all the partitions locally, or because our
	// peers already do - we can switch to it immediately. Even if none are
	// available immediately, we can still start watching out for peers on old
	// versions for which we have data locally, in case they start to appear (as
	// would happen if a bunch of nodes with stale data started up together).
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		files, err := db.sequins.backend.ListFiles(db.name, v)
		if err != nil {
			return err
		}

		version := newVersion(db.sequins, db.localPath(v), db.name, v, len(files))
		if version.ready() {
			// The version is complete, most likely because our peers have it. We
			// can switch to it right away, and build any (possibly underreplicated)
			// partitions in the background.
			// TODO: In the case that we *do* have some data locally, this will cause
			// us to advertise that before we're actually listening over HTTP.
			log.Println("Starting with pre-loaded version", v, "of", db.name)

			db.mux.prepare(version)
			db.upgrade(version)
			db.trackVersion(version, versionAvailable)
			go func() {
				version.build(files)
				version.advertiseAndWait()
			}()

			break
		} else if version.getBlockStore() != nil {
			// The version isn't complete, but we have partitions locally and can
			// start waiting on peers. This happens if, for example, a complete
			// cluster with stored data comes up all at once.
			db.switchVersion(version)
		} else {
			version.close()
		}
	}

	go db.cleanupStore()
	return nil
}

// refresh finds the latest version in S3 and then triggers an upgrade.
func (db *db) refresh() error {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	after := ""
	currentVersion := db.mux.getCurrent()
	db.mux.release(currentVersion)
	if currentVersion != nil {
		after = currentVersion.name
	}

	versions, err := db.sequins.backend.ListVersions(db.name, after, db.sequins.config.RequireSuccessFile)
	if err != nil {
		return err
	} else if len(versions) == 0 {
		if after == "" {
			return errNoVersions
		} else {
			return nil
		}
	}

	latestVersion := versions[len(versions)-1]
	existingVersion := db.mux.getVersion(latestVersion)
	db.mux.release(existingVersion)
	if existingVersion != nil {
		return nil
	}

	files, err := db.sequins.backend.ListFiles(db.name, latestVersion)
	if err != nil {
		return err
	}

	vs := newVersion(db.sequins, db.localPath(latestVersion), db.name, latestVersion, len(files))
	db.trackVersion(vs, versionBuilding)
	err = vs.build(files)
	if err != nil {
		return err
	}

	db.switchVersion(vs)
	return nil
}

// switchVersion goes through the upgrade process, making sure that we switch
// versions in step with our peers.
func (db *db) switchVersion(version *version) {
	// Prepare the version, so that during the switching period we can respond
	// to requests for it.
	db.mux.prepare(version)
	db.trackVersion(version, versionAvailable)

	if version.ready() {
		version.advertiseAndWait()
		db.newVersions <- version
	} else {
		go func() {
			// Wait for all our peers to be ready. All peers should all see that
			// everything is ready at roughly the same time. If they switch before us,
			// that's fine; the new version has been 'prepared' and we can serve it to
			// peers (but not clients). If they switch after us, that's also fine,
			// since we'll keep the old version around for a bit before deleting it.
			success := version.advertiseAndWait()
			if success {
				db.newVersions <- version
			}
		}()
	}
}

// takeNewVersions continually takes new versions over the channel and makes
// them the default. This allows us to be waiting for peers on multiple
// versions, and then switch to them as they finish. If it gets a version that
// is older than the current one, it ignores it, ensuring that it always
// rolls forward.
func (db *db) takeNewVersions() {
	for version := range db.newVersions {
		// This is just to make functional tests easier to write.
		delay := db.sequins.config.Test.UpgradeDelay.Duration
		if delay != 0 {
			time.Sleep(delay)
		}

		db.upgrade(version)
	}
}

func (db *db) upgrade(version *version) {
	// Make sure we always roll forward.
	current := db.mux.getCurrent()
	db.mux.release(current)
	if current != nil && version.name < current.name {
		go db.removeVersion(version, false)
		return
	} else if version == current {
		return
	}

	log.Printf("Switching to version %s of %s!", version.name, db.name)
	db.mux.upgrade(version)

	// Close the current version, and any older versions that were
	// also being prepared (effectively preempting them).
	for _, old := range db.mux.getAll() {
		if old == current {
			go db.removeVersion(old, true)
		} else if old.name < version.name {
			go db.removeVersion(old, false)
		}
	}
}

// removeVersion removes a version, blocking until it is no longer being
// requested by peers.
func (db *db) removeVersion(old *version, shouldWait bool) {
	db.cleanupLock.Lock()
	defer db.cleanupLock.Unlock()

	db.trackVersion(old, versionRemoving)

	// If we don't have any peers, we never need to wait until the versions
	// aren't being used.
	if db.sequins.peers == nil {
		shouldWait = false
	}

	// This will block until the version is no longer being used.
	if removed := db.mux.remove(old, shouldWait); removed != nil {
		log.Println("Removing and clearing version", removed.name, "of", db.name)
		removed.close()
		err := removed.delete()
		if err != nil {
			log.Println("Error cleaning up version %s of %s: %s", removed.name, db.name, err)
		}

		db.untrackVersion(removed)
	}
}

func (db *db) cleanupStore() {
	db.cleanupLock.Lock()
	defer db.cleanupLock.Unlock()

	dirs, err := ioutil.ReadDir(db.localPath(""))
	if os.IsNotExist(err) {
		return
	} else if err != nil {
		log.Println("Error listing local dir:", err)
		return
	}

	for _, info := range dirs {
		if !info.IsDir() {
			continue
		}

		v := info.Name()
		version := db.mux.getVersion(v)
		if version != nil {
			continue
		}

		log.Println("Clearing defunct version", v, "of", db.name)
		os.RemoveAll(db.localPath(v))
	}
}

func (db *db) localPath(version string) string {
	return filepath.Join(db.sequins.config.LocalStore, "data", db.name, version)
}

func (db *db) status() dbStatus {
	status := dbStatus{Versions: make(map[string]versionStatus)}

	db.versionStatusLock.RLock()
	defer db.versionStatusLock.RUnlock()

	for name, versionStatus := range db.versionStatus {
		status.Versions[name] = versionStatus
	}

	current := db.mux.getCurrent()
	db.mux.release(current)
	if current != nil {
		status.CurrentVersion = current.name
	}

	return status
}

func (db *db) trackVersion(version *version, state versionState) {
	db.versionStatusLock.Lock()
	defer db.versionStatusLock.Unlock()

	st := db.versionStatus[version.name]
	if st.State == "" {
		st = versionStatus{
			Path:    db.sequins.backend.DisplayPath(db.name, version.name),
			Created: version.created.Unix(),
			State:   state,
		}
	} else {
		st.State = state
	}

	db.versionStatus[version.name] = st
}

func (db *db) untrackVersion(version *version) {
	db.versionStatusLock.Lock()
	defer db.versionStatusLock.Unlock()

	delete(db.versionStatus, version.name)
}

func (db *db) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	if key == "" {
		jsonBytes, err := json.Marshal(db.status())
		if err != nil {
			log.Println("Error serving status:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header()["Content-Type"] = []string{"application/json"}
		w.Write(jsonBytes)
		return
	}

	db.mux.serveKey(w, r, key)
}

func (db *db) close() {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	for _, vs := range db.mux.getAll() {
		vs.close()
	}
}

func (db *db) delete() {
	for _, vs := range db.mux.getAll() {
		vs.delete()
	}
}
