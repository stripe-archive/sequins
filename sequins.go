package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tylerb/graceful"

	"github.com/stripe/sequins/backend"
)

type sequins struct {
	config  sequinsConfig
	http    *http.Server
	backend backend.Backend

	dbs     map[string]*db
	dbsLock sync.RWMutex

	peers       *peers
	zkWatcher   *zkWatcher
	proxyClient *http.Client

	refreshLock    sync.Mutex
	refreshWorkers chan bool
	refreshTicker  *time.Ticker
	sighups        chan os.Signal
}

type status struct {
	DBs map[string]dbStatus `json:"dbs"`
}

func newSequins(backend backend.Backend, config sequinsConfig) *sequins {
	return &sequins{
		config:      config,
		backend:     backend,
		refreshLock: sync.Mutex{},
	}
}

func (s *sequins) init() error {
	if s.config.ZK.Servers != nil {
		err := s.initCluster()
		if err != nil {
			return err
		}
	}

	// Create local directories, and load any cached versions we have.
	err := s.initLocalStore()
	if err != nil {
		return nil
	}

	// To limit the number of parallel loads, create a full buffered channel.
	// Workers grab one from the channel when they trigger a load, and put it
	// back when they're done.
	maxLoads := s.config.MaxParallelLoads
	if maxLoads != 0 {
		s.refreshWorkers = make(chan bool, maxLoads)
		for i := 0; i < maxLoads; i++ {
			s.refreshWorkers <- true
		}
	}

	// Trigger loads before we start up.
	s.refreshAll()

	// Automatically refresh, if configured to do so.
	refresh := s.config.RefreshPeriod.Duration
	if refresh != 0 {
		s.refreshTicker = time.NewTicker(refresh)
		go func() {
			log.Println("Automatically checking for new versions every", refresh.String())
			for range s.refreshTicker.C {
				s.refreshAll()
			}
		}()
	}

	// Refresh on SIGHUP.
	sighups := make(chan os.Signal)
	signal.Notify(sighups, syscall.SIGHUP)
	go func() {
		for range sighups {
			s.refreshAll()
		}
	}()

	s.sighups = sighups
	return nil
}

func (s *sequins) initCluster() error {
	prefix := path.Join("/", s.config.ZK.ClusterName)
	zkWatcher, err := connectZookeeper(s.config.ZK.Servers, prefix)
	if err != nil {
		return err
	}

	hostname := s.config.ZK.AdvertisedHostname
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return err
		}
	}

	_, port, err := net.SplitHostPort(s.config.Bind)
	if err != nil {
		return err
	}

	routableAddress := net.JoinHostPort(hostname, port)
	shardID := s.config.ZK.ShardID
	if shardID == "" {
		shardID = routableAddress
	}

	peers := watchPeers(zkWatcher, shardID, routableAddress)
	peers.waitToConverge(s.config.ZK.TimeToConverge.Duration)

	s.zkWatcher = zkWatcher
	s.peers = peers
	s.proxyClient = &http.Client{Timeout: s.config.ZK.ProxyTimeout.Duration}
	return nil
}

func (s *sequins) initLocalStore() error {
	// TODO: lock local filestore
	dataPath := filepath.Join(s.config.LocalStore, "data")
	os.MkdirAll(dataPath, 0755|os.ModeDir)

	// Attempt to load any versions we have cached locally, so that we can start
	// quickly even if there are newer versions to download.
	res, err := ioutil.ReadDir(dataPath)
	if err != nil {
		return err
	}

	localDirs := make(map[string]bool)
	for _, dir := range res {
		localDirs[dir.Name()] = true
	}

	dbs, err := s.backend.ListDBs()
	if err != nil {
		log.Println("Error listing DBs from %s: %s", s.backend.DisplayPath(""), err)
		return err
	}

	built := make(chan *db, len(dbs))
	for _, dbName := range dbs {
		if !localDirs[dbName] {
			built <- nil
			continue
		}

		go func(name string) {
			db := newDB(s, name)
			err := db.loadLatestLocalVersion()
			if err == nil {
				built <- db
			} else {
				built <- nil
			}
		}(dbName)
	}

	localDBs := make(map[string]*db)
	for i := 0; i < len(dbs); i++ {
		db := <-built
		if db != nil {
			localDBs[db.name] = db
		}
	}

	// TODO: clean up any local data we have that doesn't have a corresponding
	// remote DB or version

	s.dbsLock.Lock()
	s.dbs = localDBs
	s.dbsLock.Unlock()
	return nil
}

func (s *sequins) start() {
	defer s.shutdown()

	log.Println("Listening on", s.config.Bind)
	graceful.Run(s.config.Bind, time.Second, s)
}

func (s *sequins) shutdown() {
	log.Println("Shutting down...")
	signal.Stop(s.sighups)

	if s.refreshTicker != nil {
		s.refreshTicker.Stop()
	}

	// zk := s.zkWatcher
	// if zk != nil {
	// 	zk.close()
	// }

	// TODO: figure out how to cancel in-progress downloads
	// s.dbsLock.Lock()
	// defer s.dbsLock.Unlock()

	// for _, db := range s.dbs {
	// 	db.close()
	// }
}

func (s *sequins) refreshAll() {
	s.refreshLock.Lock()
	defer s.refreshLock.Unlock()

	dbs, err := s.backend.ListDBs()
	if err != nil {
		log.Println("Error listing DBs from %s: %s", s.backend.DisplayPath(""), err)
		return
	}

	// Add any new DBS, and trigger refreshes for existing ones. This
	// Lock pattern is a bit goofy, since the lock is on the request
	// path and we want to hold it for as short a time as possible.
	s.dbsLock.RLock()

	newDBs := make(map[string]*db)
	for _, name := range dbs {
		db := s.dbs[name]
		if db == nil {
			db = newDB(s, name)
		}

		go s.refresh(db)
		newDBs[name] = db
	}

	// Now, grab the full lock to switch the new map in.
	s.dbsLock.RUnlock()
	s.dbsLock.Lock()

	oldDBs := s.dbs
	s.dbs = newDBs

	// Finally, close any dbs that we're removing.
	s.dbsLock.Unlock()
	s.dbsLock.RLock()

	for name, db := range oldDBs {
		if s.dbs[name] == nil {
			db.close()
			db.delete()
		}
	}

	s.dbsLock.RUnlock()
}

func (s *sequins) refresh(db *db) {
	if s.refreshWorkers != nil {
		<-s.refreshWorkers
		defer func() {
			s.refreshWorkers <- true
		}()
	}

	err := db.refresh()
	if err != nil {
		log.Printf("Error refreshing %s: %s", db.name, err)
	}
}

func (s *sequins) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if r.URL.Path == "/" {
		s.serveStatus(w, r)
		return
	}

	var dbName, key string
	path := strings.TrimPrefix(r.URL.Path, "/")
	split := strings.Index(path, "/")
	if split == -1 {
		dbName = path
		key = ""
	} else {
		dbName = path[:split]
		key = path[split+1:]
	}

	if dbName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	s.dbsLock.RLock()
	db := s.dbs[dbName]
	s.dbsLock.RUnlock()

	// If this is a proxy request, we don't want to confuse "we don't have this
	// db" with "we don't have this key"; if this is a proxied request, then the
	// peer apparently does have the db, and thinks we do too (which should never
	// happen). So we use 501 Not Implemented to indicate the former. To users,
	// we present a uniform 404.
	if db == nil {
		if r.URL.Query().Get("proxy") != "" {
			w.WriteHeader(http.StatusNotImplemented)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}

		return
	}

	db.serveKey(w, r, key)
}

func (s *sequins) serveStatus(w http.ResponseWriter, r *http.Request) {
	s.dbsLock.RLock()

	status := status{DBs: make(map[string]dbStatus)}
	for name, db := range s.dbs {
		status.DBs[name] = db.status()
	}

	s.dbsLock.RUnlock()

	jsonBytes, err := json.Marshal(status)
	if err != nil {
		log.Println("Error serving status:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(jsonBytes)
}
