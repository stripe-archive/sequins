package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
)

var errNoVersions = errors.New("no versions available")

type db struct {
	sequins *sequins

	name        string
	mux         *versionMux
	refreshLock sync.Mutex
	status      dbStatus
}

type dbStatus struct {
	CurrentVersion versionStatus            `json:"current_version"`
	Versions       map[string]versionStatus `json:"versions"`
}

type versionStatus struct {
	Path    string `json:"path"`
	Created int64  `json:"created"`
	State   string `json:"state"`
}

func newDB(sequins *sequins, name string) *db {
	return &db{
		sequins: sequins,
		name:    name,
		mux:     newVersionMux(),
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
	vs, err := builder.build()
	if err != nil {
		return err
	}

	// Prepare the version, so that during the switching period we can respond
	// to requests for it.
	db.mux.prepare(vs)

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
			shouldWait := (db.sequins.peers != nil)
			db.mux.remove(currentVersion, shouldWait)
			log.Println("Removing and clearing version", currentVersion.name, "of", db.name)

			// TODO: this doesn't actually delete the data
			currentVersion.close()

			// Grab the lock again so we can re-cache the status JSON.
		}()
	}

	db.cacheStatus()
	return nil
}

func (db *db) cacheStatus() {

}

func (db *db) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	if key == "" {
		jsonBytes, err := json.Marshal(db.status)
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
