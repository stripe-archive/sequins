package main

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/stripe/sequins/blocks"
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

	path                    string
	db                      string
	name                    string
	created                 time.Time
	blockStore              *blocks.BlockStore
	blockStoreLock          sync.RWMutex
	partitions              *partitions
	numPartitions           int
	selectedLocalPartitions map[int]bool
}

func newVersion(sequins *sequins, path, db, name string, numPartitions int) *version {
	vs := &version{
		sequins:       sequins,
		path:          path,
		db:            db,
		name:          name,
		created:       time.Now(),
		numPartitions: numPartitions,
	}

	var local map[int]bool
	if sequins.peers != nil {
		vs.partitions = watchPartitions(sequins.zkWatcher, sequins.peers,
			db, name, numPartitions, sequins.config.Sharding.Replication)

		local = vs.partitions.pickLocalPartitions()
		vs.selectedLocalPartitions = local
	}

	// Try loading anything we have locally. If it doesn't work out, that's ok.
	_, err := os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from manifest at", path)

		blockStore, err := blocks.NewFromManifest(path, local)
		if err != nil {
			log.Println("Error loading", vs.db, "version", vs.name, "from manifest:", err)
		}

		vs.blockStore = blockStore
		if vs.partitions != nil {
			vs.partitions.updateLocalPartitions(local)
		}
	}

	return vs
}

func (vs *version) getBlockStore() *blocks.BlockStore {
	vs.blockStoreLock.RLock()
	defer vs.blockStoreLock.RUnlock()

	return vs.blockStore
}

func (vs *version) ready() bool {

	if vs.numPartitions == 0 {
		return true
	} else if vs.sequins.peers == nil {
		return vs.getBlockStore() != nil
	}

	return vs.partitions.ready()
}

func (vs *version) advertiseAndWait() bool {
	if vs.sequins.peers == nil || vs.numPartitions == 0 {
		return true
	}

	return vs.partitions.advertiseAndWait()
}

// hasPartition returns true if we have the partition available locally.
func (vs *version) hasPartition(partition int) bool {
	return vs.getBlockStore() != nil && (vs.partitions == nil || vs.partitions.local[partition])
}

func (vs *version) close() {
	if vs.partitions != nil {
		vs.partitions.close()
	}

	bs := vs.getBlockStore()
	if bs != nil {
		bs.Close()
	}
}

func (vs *version) delete() error {
	bs := vs.getBlockStore()
	if bs != nil {
		return bs.Delete()
	}

	return nil
}
