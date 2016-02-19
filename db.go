package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"
)

var errNoVersions = errors.New("no versions available")

type db struct {
	sequins *sequins

	name        string
	mux         *versionMux
	refreshLock sync.Mutex

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
	return &db{
		sequins:       sequins,
		name:          name,
		mux:           newVersionMux(),
		versionStatus: make(map[string]versionStatus),
	}
}

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
	currentVersion := db.mux.getCurrent()
	db.mux.release(currentVersion)
	if currentVersion != nil && latestVersion == currentVersion.name {
		return nil
	}

	files, err := db.sequins.backend.ListFiles(db.name, latestVersion)
	if err != nil {
		return err
	}

	builder := newVersion(db.sequins, db.name, latestVersion, len(files))
	db.trackVersionBuilding(builder)
	vs, err := builder.build()
	if err != nil {
		return err
	}

	// Prepare the version, so that during the switching period we can respond
	// to requests for it.
	db.mux.prepare(vs)
	db.trackVersion(vs, versionAvailable)

	// Then, wait for all our peers to be ready. All peers should all see that
	// everything is ready at roughly the same time. If they switch before us,
	// that's fine; we can serve the new version we've 'prepared' when peers
	// ask for it. If we're not part of a cluster, this returns immediately.
	vs.waitForPeers()

	// Now we can start serving requests for the new version to clients, as well.
	log.Printf("Switching to version %s of %s!", vs.name, db.name)
	db.mux.upgrade(vs)

	// Finally, clean up the old version after some amount of time and after
	// no peers are asking for it anymore. Again, this is immediate if there are
	// no peers to consider.
	if currentVersion != nil {
		go func() {
			db.trackVersion(currentVersion, versionRemoving)
			shouldWait := (db.sequins.peers != nil)
			db.mux.remove(currentVersion, shouldWait)
			log.Println("Removing and clearing version", currentVersion.name, "of", db.name)

			// TODO: this doesn't actually delete the data
			currentVersion.close()
			db.untrackVersion(currentVersion)
		}()
	}

	return nil
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

func (db *db) close() error {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	var err error
	for _, vs := range db.mux.getAll() {
		closeErr := vs.close()
		if closeErr != nil {
			err = closeErr
		}
	}

	return err
}
