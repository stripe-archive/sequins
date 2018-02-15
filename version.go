package main

import (
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"

	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sharding"
)

const versionHeader = "X-Sequins-Version"

var (
	errNoAvailablePeers   = errors.New("no available peers")
	errProxiedIncorrectly = errors.New("this server doesn't have the requested partition")
	errMixedFiles         = errors.New("Mixed Sparkey and Sequencefile directory")
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
	active    time.Time
	stateLock sync.RWMutex

	ready     chan bool
	cancel    chan bool
	built     bool
	buildLock sync.Mutex

	stats *statsd.Client

	closeOnce  sync.Once
	deleteOnce sync.Once
}

func newVersion(sequins *sequins, db *db, path, name string) (*version, error) {
	files, err := sequins.backend.ListFiles(db.name, name)
	if err != nil {
		return nil, err
	}
	files, numPartitions, err := filterVersionFiles(files)
	if err != nil {
		return nil, err
	}

	vs := &version{
		sequins:       sequins,
		db:            db,
		path:          path,
		name:          name,
		files:         files,
		numPartitions: numPartitions,

		created: time.Now(),
		state:   versionBuilding,

		ready:  make(chan bool),
		cancel: make(chan bool),

		stats: sequins.stats,
	}

	minReplication := 1
	maxReplication := 0
	if sequins.config.Sharding.Enabled {
		minReplication = sequins.config.Sharding.MinReplication
		maxReplication = sequins.config.Sharding.MaxReplication
	}

	vs.partitions = sharding.WatchPartitions(sequins.zkWatcher, sequins.peers,
		db.name, name, vs.numPartitions, sequins.config.Sharding.Replication, minReplication, maxReplication)

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
		log.Println("Error loading", vs.db.name, "version", vs.name, "from manifest:", err)
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

		// Assume that if we have a manifest, we have successfully fetched this version at some point.
		// We don't want to re-calculate partition assignment, but just use what the manifest told us.
		vs.built = true
	}

	vs.blockStore = blockStore
	return nil
}

func (vs *version) close() {
	vs.closeOnce.Do(vs.closeUnsafe)
}

func (vs *version) closeUnsafe() {
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
	vs.close()
	var err error
	vs.deleteOnce.Do(func() {
		err = vs.deleteUnsafe()
	})
	return err
}

func (vs *version) deleteUnsafe() error {
	return vs.blockStore.Delete()
}

var reSparkeyPart = regexp.MustCompile(`\d+`)

// Check if a file is a sparkey log file. If so, return (true, partition),
// where partition is the file's partition. See addSparkeyFile().
func isSparkeyFile(file string) (raw bool, partition int) {
	if !strings.HasSuffix(file, ".spl") {
		return false, 0
	}

	match := reSparkeyPart.FindString(file)
	if match == "" {
		return false, 0
	}
	part, err := strconv.Atoi(match)
	if err != nil {
		return false, 0
	}

	return true, part
}

// Identify auxiliary files, that should not be downloaded.
func isAuxiliaryFile(file string) bool {
	// Skip sparkey index files, we'll fetch them when we fetch the corresponding sparkey log file.
	return strings.HasSuffix(file, ".spi.sz")
}

// Filter files, yielding only non-auxiliary files. Also yield the number of partitions found.
func filterVersionFiles(files []string) (filtered []string, numPartitions int, err error) {
	filtered = []string{}
	partitions := map[int]bool{}
	sparkeyStatus := map[bool]bool{}

	for _, file := range files {
		if isAuxiliaryFile(file) {
			continue
		}
		filtered = append(filtered, file)

		isSparkey, partition := isSparkeyFile(file)
		sparkeyStatus[isSparkey] = true
		if len(sparkeyStatus) > 1 {
			return nil, 0, errMixedFiles
		}

		if isSparkey {
			partitions[partition] = true
		} else {
			partitions[len(partitions)] = true
		}
	}

	return filtered, len(partitions), nil
}
