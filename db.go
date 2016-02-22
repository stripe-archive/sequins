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
	}

	db.newVersions = make(chan *version)
	go db.takeNewVersions()
	return db
}

// loadLatestLocalVersion loads the latest version we have downloaded locally
// with enough partitions. This allows us to start up fast with local data even
// if there are newer versions available.
func (db *db) loadLatestLocalVersion() error {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	versions, err := db.sequins.backend.ListVersions(db.name, db.sequins.config.RequireSuccessFile)
	if err != nil {
		return err
	} else if len(versions) == 0 {
		return errNoVersions
	}

	res, err := ioutil.ReadDir(db.localPath(""))
	if err != nil {
		return err
	}

	localVersions := make(map[string]bool)
	for _, localVersion := range res {
		localVersions[localVersion.Name()] = true
	}

	// Cycle through the versions, starting with the newest one, and load
	// whichever one we have locally.
	var picked *version
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if !localVersions[v] {
			continue
		}

		path := db.localPath(v)
		_, err = os.Stat(filepath.Join(path, ".manifest"))
		if err == nil {
			builder := newVersion(db.sequins, db.name, v)
			version, err := builder.build(path, true)
			if err == nil {
				picked = version
				break
			}
		}
	}

	if picked == nil {
		return errNoVersions
	}

	log.Println("Loading stored version", picked.name, "of", db.name)
	db.switchVersion(picked)
	return nil
}

// refresh loads the latest version on S3 and then triggers an upgrade.
func (db *db) refresh() error {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	versions, err := db.sequins.backend.ListVersions(db.name, db.sequins.config.RequireSuccessFile)
	if err != nil {
		return err
	} else if len(versions) == 0 {
		return errNoVersions
	}

	latestVersion := versions[len(versions)-1]
	existingVersion := db.mux.getVersion(latestVersion)
	db.mux.release(existingVersion)
	if existingVersion != nil {
		return nil
	}

	builder := newVersion(db.sequins, db.name, latestVersion)
	db.trackVersionBuilding(builder)
	vs, err := builder.build(db.localPath(latestVersion), false)
	if err != nil {
		return err
	}

	db.switchVersion(vs)
	return nil
}

// Switch version goes through the upgrade process, making sure that we switch
// versions in step with our peers.
func (db *db) switchVersion(version *version) {
	// Prepare the version, so that during the switching period we can respond
	// to requests for it.
	db.mux.prepare(version)
	db.trackVersion(version, versionAvailable)

	go func() {
		// Wait for all our peers to be ready. All peers should all see that
		// everything is ready at roughly the same time. If they switch before us,
		// that's fine; the new version has been 'prepared' and we can serve it to
		// peers (but not clients). If they switch after us, that's also fine,
		// since we'll keep the old version around for a bit before deleting it.
		success := version.waitForPeers()
		if success {
			db.newVersions <- version
		}
	}()
}

// takeNewVersions continually takes new versions over the channel and makes
// them the default. This allows us to be waiting for peers on multiple
// versions, and then switch to them as they finish. If it gets a version that
// is older than the current one, it ignores it, ensuring that it always
// rolls forward.
func (db *db) takeNewVersions() {
	for version := range db.newVersions {
		current := db.mux.getCurrent()
		db.mux.release(current)
		if current != nil && version.name <= current.name {
			continue
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
}

// removeVersion removes a version, blocking until it is no longer being
// requested by peers.
func (db *db) removeVersion(old *version, shouldWait bool) {
	db.trackVersion(old, versionRemoving)

	// If we don't have any peers, we never need to wait until the versions
	// aren't being used.
	if db.sequins.peers == nil {
		shouldWait = false
	}

	// This will block until the version is no longer being used.
	log.Println("Removing and clearing version", old.name, "of", db.name)
	if removed := db.mux.remove(old, shouldWait); removed != nil {
		removed.close()
		err := removed.delete()
		if err != nil {
			log.Println("Error cleaning up version %s of %s: %s", removed.name, db.name, err)
		}

		db.untrackVersion(removed)
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

func (db *db) trackVersionBuilding(builder *versionBuilder) {
	db.versionStatusLock.Lock()
	defer db.versionStatusLock.Unlock()

	db.versionStatus[builder.name] = versionStatus{
		Path:    db.sequins.backend.DisplayPath(db.name, builder.name),
		Created: builder.created.Unix(),
		State:   versionBuilding,
	}
}

func (db *db) trackVersion(version *version, state versionState) {
	db.versionStatusLock.Lock()
	defer db.versionStatusLock.Unlock()

	st := db.versionStatus[version.name]
	st.State = state

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
