package main

import (
	"errors"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"

	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/log"
	"github.com/stripe/sequins/sharding"
)

const versionHeader = "X-Sequins-Version"

var (
	errNoAvailablePeers   = errors.New("no available peers")
	errProxiedIncorrectly = errors.New("this server doesn't have the requested partition")
)

// A version represents a single version of a particular sequins db: in
// other words, a collection of files. In the sharding-enabled case, it
// understands distribution of partitions and can route requests.
type version struct {
	sequins *sequins
	db      *db

	path          string
	name          string
	blockStore    *blocks.BlockStore
	partitions    *sharding.Partitions
	numPartitions int
	files         []string

	state     versionState
	created   time.Time
	available time.Time
	stateLock sync.RWMutex

	ready     chan bool
	cancel    chan bool
	built     bool
	buildLock sync.Mutex

	stats *statsd.Client
}

func newVersion(sequins *sequins, db *db, path, name string) (*version, error) {
	files, err := sequins.backend.ListFiles(db.name, name)
	if err != nil {
		return nil, err
	}

	vs := &version{
		sequins:       sequins,
		db:            db,
		path:          path,
		name:          name,
		files:         files,
		numPartitions: len(files),

		created: time.Now(),
		state:   versionBuilding,

		ready:  make(chan bool),
		cancel: make(chan bool),

		stats: sequins.stats,
	}

	minReplication := 1
	if sequins.config.Sharding.Enabled {
		minReplication = sequins.config.Sharding.MinReplication
	}

	vs.partitions = sharding.WatchPartitions(sequins.zkWatcher, sequins.peers,
		db.name, name, len(files), sequins.config.Sharding.Replication, minReplication)

	err = vs.initBlockStore(path)
	if err != nil {
		return nil, err
	}

	// If we're running in non-distributed mode, ready gets closed once the block
	// store is built.
	if vs.partitions != nil {
		select {
		case <-vs.partitions.Ready:
			close(vs.ready)
		default:
			go func() {
				select {
				case <-vs.cancel:
				case <-vs.partitions.Ready:
				}

				close(vs.ready)
			}()
		}
	}

	return vs, nil
}

func (vs *version) initBlockStore(path string) error {
	// Try loading anything we have locally. If it doesn't work out, that's ok.
	blockStore, manifest, err := blocks.NewFromManifest(path)
	if err != nil && err != blocks.ErrNoManifest {
		log.LogWithKVs(&log.KeyValue{
			"error_message": "manifest-load-error",
			"db_name":       vs.db.name,
			"db_version":    vs.name,
			"traceback":     err,
		})
	}

	if blockStore == nil {
		blockStore = blocks.New(vs.path, vs.numPartitions,
			vs.sequins.config.Storage.Compression, vs.sequins.config.Storage.BlockSize)
	} else {
		have := make(map[int]bool)
		for _, partition := range manifest.SelectedPartitions {
			have[partition] = true
		}

		vs.partitions.UpdateLocal(have)
	}

	vs.blockStore = blockStore
	return nil
}

func (vs *version) close() {
	close(vs.cancel)

	// This happens once the building goroutine gets the cancel and exits.
	go func() {
		vs.buildLock.Lock()
		defer vs.buildLock.Unlock()

		vs.partitions.Close()
		vs.blockStore.Close()
	}()
}

func (vs *version) delete() error {
	return vs.blockStore.Delete()
}
