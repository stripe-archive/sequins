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

var errDirLocked = errors.New("failed to acquire lock")

const (
	flapMax      = 5
	flapDuration = 20 * time.Minute
)

type sequins struct {
	config     sequinsConfig
	http       http.Handler
	backend    backend.Backend
	httpClient *http.Client

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
}

func newSequins(backend backend.Backend, config sequinsConfig) *sequins {
	return &sequins{
		config:  config,
		backend: backend,
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        300,
				MaxIdleConnsPerHost: 3,
			},
		},
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

	// Trigger loads before we start up.
	s.refreshAll()
	s.refreshLock.Lock()
	defer s.refreshLock.Unlock()

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

	go zkWatcher.TriggerCleanup()

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

func (s *sequins) refreshAll() {
	s.refreshLock.Lock()
	defer s.refreshLock.Unlock()

	log.Println("Refreshing all DBs")
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
				db.backfillVersions()
				backfills.Done()
			}()
		} else {
			err := db.refresh()
			if err != nil {
				log.Printf("Error refreshing %s: %s", db.name, err)
			}
		}

		newDBs[name] = db
	}

	backfills.Wait()

	log.Println("Finishing freshing DBs")

	// Now, grab the full lock to switch the new map in.
	s.dbsLock.RUnlock()
	s.dbsLock.Lock()

	oldDBs := s.dbs
	s.dbs = newDBs

	// Finally, close any dbs that we're removing.
	s.dbsLock.Unlock()
	s.dbsLock.RLock()

	log.Println("Swapped DBs")

	for name, db := range oldDBs {
		if s.dbs[name] == nil {
			log.Printf("Removing and clearing database %s", name)
			db.close()
			db.delete()
		}
	}

	s.dbsLock.RUnlock()

	log.Println("Closed Old DBs")

	// Cleanup any zkNodes for deleted versions and dbs.
	if s.zkWatcher != nil {
		s.zkWatcher.TriggerCleanup()
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
