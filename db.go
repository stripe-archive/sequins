package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

var errNoVersions = errors.New("no versions available")

type db struct {
	sequins *sequins

	name        string
	mux         *versionMux
	refreshLock sync.Mutex
	buildLock   sync.Mutex
	upgradeLock sync.Mutex
	cleanupLock sync.Mutex
}

func newDB(sequins *sequins, name string) *db {
	db := &db{
		sequins: sequins,
		name:    name,
		mux:     newVersionMux(sequins.config.Test.VersionRemoveTimeout.Duration),
	}

	return db
}

func (db *db) listVersions(after string) ([]string, error) {
	versions, err := db.sequins.backend.ListVersions(db.name, after, db.sequins.config.RequireSuccessFile)
	if err != nil {
		return nil, err
	}
	return filterPaths(versions), nil
}

func (db *db) localVersions() ([]string, error) {
	files, err := ioutil.ReadDir(db.localPathDir())
	if err != nil {
		return nil, err
	}

	var versions []string
	for _, f := range files {
		versions = append(versions, f.Name())
	}
	sort.Strings(versions)
	return versions, nil
}

// backfillVersions is called at startup, and tries to grab any versions that
// are either downloaded locally or available entirely at peers. This allows a
// node to join a cluster with an existing version all set to go, and start up
// serving that version (but exclusively proxy it). It also allows it to start
// up with stale data, even if there's newer data available.
func (db *db) backfillVersions(initialLocal bool) error {
	db.refreshLock.Lock()
	defer db.refreshLock.Unlock()

	var versions []string
	var err error
	if initialLocal {
		log.Printf("Initial fetch of local versions only: db=%q", db.name)
		versions, err = db.localVersions()
	} else {
		versions, err = db.listVersions("")
	}
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
	// would happen if a bunch of nodes with stale data started up together). It's
	// important to do this now, synchronously before startup, so that the node
	// can come up with the version ready to go, and avoid an awkward period of
	// 404ing while we load versions asynchronously.
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]

		version, err := newVersion(db.sequins, db, db.localPath(v), v)
		if err != nil {
			log.Printf("Error initializing version %s of %s: %s", db.name, v, err)
			continue
		}

		if db.switchVersion(version) {
			break
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

	versions, err := db.listVersions(after)
	if err != nil {
		return err
	} else if len(versions) == 0 {
		if after == "" {
			return errNoVersions
		} else {
			return nil
		}
	}

	latest := versions[len(versions)-1]

	// Check if we already have this version in the pipeline.
	existingVersion := db.mux.getVersion(latest)
	db.mux.release(existingVersion)
	if existingVersion != nil {
		// If the build succeeded or is in progress, this is a noop. If it errored
		// before, this will retry.
		go existingVersion.build()
		return nil
	}

	vs, err := newVersion(db.sequins, db, db.localPath(latest), latest)
	if err != nil {
		return err
	}

	db.switchVersion(vs)
	return nil
}

// switchVersion goes through the upgrade process, making sure that we switch
// versions in step with our peers. It returns true if the version is ready,
// and false otherwise.
func (db *db) switchVersion(version *version) bool {
	// Prepare the version, so that during the switching period we can respond
	// to requests for it.
	db.mux.prepare(version)

	// Build any partitions we're missing in the background.
	go version.build()

	// Start advertising our partitions to peers.
	go version.partitions.Advertise()

	// If the version is ready now, we can switch to it synchronously. This is
	// important to do so that on startup, we fully initialize ready versions
	// before we start taking requests. For example, if our peers have a complete
	// set of partitions, then we want to start up being able to proxy to them.
	select {
	case <-version.ready:
		db.upgrade(version)
		return true
	default:
	}

	// Wait for a complete set of partitions to be available (in the non-
	// distributed case, this just means waiting for building to finish). All
	// peers should all see that everything is ready at roughly the same time. If
	// they switch before us, that's fine; the new version has been 'prepared' and
	// we can serve it to peers (but not clients). If they switch after us, that's
	// also fine, since we'll keep the old version around for a bit before
	// deleting it.
	go func() {
		<-version.ready
		db.upgrade(version)
	}()

	return false
}

// upgrade takes a new version and processes it, upgrading if necessary and then
// clearing old ones. If it gets a version that is older than the current one,
// it ignores it, ensuring that it always rolls forward.
func (db *db) upgrade(version *version) {
	db.upgradeLock.Lock()
	defer db.upgradeLock.Unlock()

	// This is just to make functional tests easier to write.
	delay := db.sequins.config.Test.UpgradeDelay.Duration
	if delay != 0 {
		time.Sleep(delay)
	}

	// Make sure we always roll forward.
	current := db.mux.getCurrent()
	db.mux.release(current)
	if current != nil && version.name < current.name {
		// The version is already out of date, so get rid of it.
		go db.removeVersion(version, false)
		return
	} else if version == current {
		return
	}

	log.Printf("Switching to version %s of %s!", version.name, db.name)
	db.mux.upgrade(version)
	version.setState(versionActive)

	if version.stats != nil {
		title := fmt.Sprintf("A new version is active in %s.", db.name)
		text := fmt.Sprintf("Version %s has been set to active in db %s.", version.name, db.name)
		event := statsd.NewEvent(title, text)
		event.Tags = []string{fmt.Sprintf("sequins_db:%s", db.name)}
		version.stats.Event(event)
	}

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

	old.setState(versionRemoving)

	// If we don't have any peers, we never need to wait until the versions
	// aren't being used.
	if db.sequins.peers == nil {
		shouldWait = false
	}

	// This will block until the version is no longer being used.
	if removed := db.mux.remove(old, shouldWait); removed != nil {
		removed.close()
		err := removed.delete()
		if err != nil {
			log.Printf("Error cleaning up version %s of %s: %s", removed.name, db.name, err)
		}
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
		db.mux.release(version)
		if version != nil {
			continue
		}

		log.Println("Clearing defunct version", v, "of", db.name)
		os.RemoveAll(db.localPath(v))
	}
}

// localPath returns the path where local data for the given version should be
// stored.
func (db *db) localPath(version string) string {
	return filepath.Join(db.localPathDir(), version)
}

func (db *db) localPathDir() string {
	return filepath.Join(db.sequins.config.LocalStore, "data", db.name)
}

func (db *db) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	if key == "" {
		db.serveStatus(w, r)
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
