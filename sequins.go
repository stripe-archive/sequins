package main

import (
	"errors"
	"fmt"
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

	"github.com/DataDog/datadog-go/statsd"
	"github.com/juju/ratelimit"
	"github.com/nightlyone/lockfile"
	"github.com/stripe/goforit"
	"github.com/tylerb/graceful"

	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/sharding"
	"github.com/stripe/sequins/workqueue"
	"github.com/stripe/sequins/zk"
)

const defaultMaxLoads = 10

// While we wait to pick a shard ID, other nodes are prohibited from joining the cluster because
// we're holding a lock. This timeout can therefore be pretty short.
//
// Indeed, it needs to be shorter than the regular cluster convergence timeout, or
// the cluster will be seen to "converge" when it's really just waiting for someone to release
// the lock.
const shardIDConvergenceMax = 3 * time.Second

// Remote refresh of the underlying data store is toggle-able
// through a simple integration with goforit.  Before each refresh,
// Sequins will inspect a local JSON file (specified by
// GoforitFlagJsonPath in the goforitConfig) for the state
// of the flag sequins.prevent_download (or sequins.prevent_download.<CLUSTER NAME>
// if CLUSTER NAME is defined).  If the flag is not defined or is set to FALSE,
// remote refresh will proceed as expected. If it's set to TRUE, it won't.
const disableRemoteRefreshFlagPrefix = "sequins.prevent_download"

const goforitRefresh = goforit.DefaultInterval

var errDirLocked = errors.New("failed to acquire lock")

const (
	flapMax      = 5
	flapDuration = 20 * time.Minute
)

type sequins struct {
	config  sequinsConfig
	http    http.Handler
	backend backend.Backend

	dbs     map[string]*db
	dbsLock sync.RWMutex

	address   string
	peers     *sharding.Peers
	zkWatcher *zk.Watcher

	refreshLock   sync.Mutex
	workQueue     *workqueue.WorkQueue
	refreshTicker *time.Ticker
	sighups       chan os.Signal

	stats *statsd.Client

	storeLock lockfile.Lockfile

	downloadRateLimitBucket *ratelimit.Bucket

	goforit goforit.Backend
}

func newSequins(backend backend.Backend, config sequinsConfig) *sequins {
	return &sequins{
		config:      config,
		backend:     backend,
		refreshLock: sync.Mutex{},
	}
}

func (s *sequins) init() error {
	// Start Datadog client if configured
	if s.config.Datadog.Url != "" {
		statsdClient, err := statsd.New(s.config.Datadog.Url)
		if err != nil {
			log.Fatalf("Error connecting to statsd: %s", err)
		}
		statsdClient.Namespace = "sequins."
		statsdClient.Tags = append(statsdClient.Tags, fmt.Sprintf("sequins:%s", s.config.Sharding.ClusterName))
		s.stats = statsdClient
	}

	if s.config.Sharding.Enabled {
		err := s.initCluster()
		if err != nil {
			return err
		}
	}

	// Create local directories, and load any cached versions we have.
	err := s.initLocalStore()
	if err != nil {
		return fmt.Errorf("error initializing local store: %s", err)
	}

	// This lock limits load parallelism across all dbs.
	maxLoads := s.config.MaxParallelLoads
	if maxLoads == 0 {
		maxLoads = defaultMaxLoads
	}
	s.workQueue = workqueue.NewWorkQueue(maxLoads)

	// Create a token bucket if we need download bandwidth throttling
	maxDownloadBandwidth := int64(s.config.MaxDownloadBandwidthMBPerSecond * 1024 * 1024)
	if maxDownloadBandwidth > 0 {
		s.downloadRateLimitBucket = ratelimit.NewBucketWithRate(float64(maxDownloadBandwidth), maxDownloadBandwidth)
	}

	// Kick off feature flag cache refresh if GoforitFlagJsonPath configured
	if s.config.GoforitFlagJsonPath != "" {
		log.Printf("Enabling Goforit: GoforitFlagJsonPath=%q", s.config.GoforitFlagJsonPath)
		s.goforit = goforit.BackendFromJSONFile(s.config.GoforitFlagJsonPath)
		goforit.Init(goforitRefresh, s.goforit)
	}

	// Trigger loads before we start up.
	s.refreshAll(true)
	s.refreshLock.Lock()
	defer s.refreshLock.Unlock()

	// Automatically refresh, if configured to do so.
	refresh := s.config.RefreshPeriod.Duration
	if refresh != 0 {
		s.refreshTicker = time.NewTicker(refresh)
		go func() {
			log.Println("Automatically checking for new versions every", refresh.String())
			for range s.refreshTicker.C {
				s.refreshAll(false)
			}
		}()
	}

	// Refresh on SIGHUP.
	sighups := make(chan os.Signal)
	signal.Notify(sighups, syscall.SIGHUP)
	go func() {
		for range sighups {
			// Refresh any feature flags on HUP
			if s.goforit != nil {
				goforit.RefreshFlags(s.goforit)
			}
			s.refreshAll(false)
		}
	}()

	s.sighups = sighups

	// Setup the HTTP handler.
	s.http = trackQueries(s)

	return nil
}

func (s *sequins) ensureShardID(zkWatcher *zk.Watcher, routableIpAddress string) (*sharding.Peers, error) {
	lock := zkWatcher.CreateIDAssignmentLock()
	lock.Lock()
	defer lock.Unlock()

	convergenceTime := s.config.Sharding.TimeToConverge.Duration
	if convergenceTime > shardIDConvergenceMax {
		convergenceTime = shardIDConvergenceMax
	}

	shardID := s.config.Sharding.ShardID
	if shardID == "" {
		fp := filepath.Join(s.config.LocalStore, "self_assigned_id")
		_, err := os.Stat(fp)
		if err != nil {
			if os.IsNotExist(err) {
				log.Print("No self-assigned ID file found")

				peersNotJoined := sharding.WatchPeersNoJoin(zkWatcher)
				peersNotJoined.WaitToConverge(convergenceTime)

				shardID, err = peersNotJoined.SmallestAvailableShardID()
				if err != nil {
					return nil, fmt.Errorf("Error running SmallestAvailableShardID: %q", err)
				}
				log.Printf("Identified %q as smallest missing shardID", shardID)

				err := ioutil.WriteFile(fp, []byte(shardID), 0600)
				if err != nil {
					return nil, fmt.Errorf("Could not write shardID to file: %q", err)
				}
			} else {
				return nil, fmt.Errorf("Could not check %q exists: %q", fp, err)
			}
		} else {
			log.Print("Self-assigned ID file found. Attempting to read.")
			bytes, err := ioutil.ReadFile(fp)
			if err != nil {
				return nil, fmt.Errorf("Could not read %q: %q", fp, err)
			}
			shardID = string(bytes)
			log.Printf("Extracted shardID %q from file", shardID)
		}
	}
	log.Printf("Using %q as shardID", shardID)
	peers := sharding.WatchPeers(zkWatcher, shardID, routableIpAddress)
	return peers, nil
}

func (s *sequins) joinCluster(zkWatcher *zk.Watcher, routableIpAddress string) (*sharding.Peers, error) {
	// The shard-ID lock is released inside here--we don't need that lock to be held while we wait to converge.
	peers, err := s.ensureShardID(zkWatcher, routableIpAddress)
	if err != nil {
		return nil, err
	}
	peers.WaitToConverge(s.config.Sharding.TimeToConverge.Duration)
	return peers, nil
}

func (s *sequins) initCluster() error {
	// This config property is calculated if not set.
	if s.config.Sharding.ProxyStageTimeout.Duration == 0 {
		stageTimeout := s.config.Sharding.ProxyTimeout.Duration / time.Duration(s.config.Sharding.Replication)
		s.config.Sharding.ProxyStageTimeout = duration{stageTimeout}
	}

	prefix := path.Join("/", s.config.Sharding.ClusterName)
	zkWatcher, err := zk.Connect(s.config.ZK.Servers, prefix,
		s.config.ZK.ConnectTimeout.Duration, s.config.ZK.SessionTimeout.Duration)
	if err != nil {
		return err
	}

	flapNotify := zkWatcher.SetFlapThreshold(flapMax, flapDuration)
	go func() {
		<-flapNotify
		log.Fatal("Dying due to ZK flapping")
	}()

	hostname := s.config.Sharding.AdvertisedHostname
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

	ip := hostname
	ipAddresses, err := net.LookupHost(hostname)
	if err != nil {
		return err
	}
	if len(ipAddresses) == 1 {
		ip = ipAddresses[0]
	}

	routableIpAddress := net.JoinHostPort(ip, port)
	routableAddress := net.JoinHostPort(hostname, port)

	peers, err := s.joinCluster(zkWatcher, routableIpAddress)
	if err != nil {
		log.Fatalf("Error joining cluster: %q", err)
	}

	s.address = routableAddress
	s.zkWatcher = zkWatcher
	s.peers = peers
	return nil
}

func (s *sequins) initLocalStore() error {
	dataPath := filepath.Join(s.config.LocalStore, "data")
	err := os.MkdirAll(dataPath, 0755|os.ModeDir)
	if err != nil {
		return err
	}

	lock, _ := lockfile.New(filepath.Join(s.config.LocalStore, "sequins.lock"))
	s.storeLock = lock
	err = s.storeLock.TryLock()
	if err != nil {
		p, err := s.storeLock.GetOwner()
		if err == nil {
			log.Printf("The local store at %s is locked by process %d", s.config.LocalStore, p.Pid)
		}

		return errDirLocked
	}

	return nil
}

func (s *sequins) start() {
	defer s.shutdown()

	log.Println("Listening on", s.config.Bind)
	graceful.Run(s.config.Bind, time.Second, s.http)
}

func (s *sequins) shutdown() {
	log.Println("Shutting down...")
	signal.Stop(s.sighups)

	if s.refreshTicker != nil {
		s.refreshTicker.Stop()
	}

	zk := s.zkWatcher
	if zk != nil {
		zk.Close()
	}

	// TODO: figure out how to cancel in-progress downloads
	// s.dbsLock.Lock()
	// defer s.dbsLock.Unlock()

	// for _, db := range s.dbs {
	// 	db.close()
	// }

	s.storeLock.Unlock()
}

func (s *sequins) listDBs() ([]string, error) {
	dbs, err := s.backend.ListDBs()
	if err != nil {
		return nil, err
	}
	return filterPaths(dbs), nil
}

func (s *sequins) remoteRefresh() bool {
	if enabled, flag := s.checkFlag(disableRemoteRefreshFlagPrefix); enabled {
		log.Printf("Not allowing remote refresh: cluster=%q, flag=%q",
			s.config.Sharding.ClusterName, flag)
		if s.stats != nil {
			s.stats.Count(flag, 1, []string{}, 1.0)
		}
		return false
	} else {
		return true
	}
}

// Refresh all datasets.
//
// If initialStartup is set to TRUE and remoteRefresh() is disabled,
// initial loads will refrain from checking for new DBs/versions
func (s *sequins) refreshAll(initialStartup bool) {
	s.refreshLock.Lock()
	defer s.refreshLock.Unlock()

	var initialLocal bool
	if !s.remoteRefresh() {
		if !initialStartup {
			return
		}
		log.Printf("Kicking off initial local-only refresh")
		initialLocal = true
	}

	dbs, err := s.listDBs()
	if err != nil {
		log.Printf("Error listing DBs from %s: %s", s.backend.DisplayPath(""), err)
		return
	}

	// Add any new DBS, and trigger refreshes for existing ones. This
	// Lock pattern is a bit goofy, since the lock is on the request
	// path and we want to hold it for as short a time as possible.
	s.dbsLock.RLock()

	newDBs := make(map[string]*db)
	var backfills sync.WaitGroup
	for _, name := range dbs {
		db := s.dbs[name]
		if db == nil {
			db = newDB(s, name)

			backfills.Add(1)
			go func() {
				db.backfillVersions(initialLocal)
				backfills.Done()
			}()
		} else {
			go func() {
				err := db.refresh()
				if err != nil {
					log.Printf("Error refreshing %s: %s", db.name, err)
				}
			}()
		}

		newDBs[name] = db
	}

	backfills.Wait()

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
			log.Println("Removing and clearing database", name)
			db.close()
			db.delete()
		}
	}

	s.dbsLock.RUnlock()

	// Cleanup any zkNodes for deleted versions and dbs.
	if s.zkWatcher != nil {
		go s.zkWatcher.TriggerCleanup()
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

	if r.URL.Path == "/healthz" {
		s.serveHealth(w, r)
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

// Check if a feature flag is enabled. Adds the cluster to the flag prefix.
// Returns whether the flag is enabled, and if so the full flag name.
func (s *sequins) checkFlag(prefix string) (bool, string) {
	if s.goforit == nil {
		return false, ""
	}

	name := prefix
	cluster := s.config.Sharding.ClusterName
	if cluster != "" {
		name = name + "." + cluster
	}

	return goforit.Enabled(nil, name), name
}
