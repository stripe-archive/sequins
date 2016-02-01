package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sequencefile"
)

// /sequins/
//   nodes/
//     node1
//     node2
//   partitions/
//     foo/
//       version1/
//         00000:node1
//         00001:node2

var ErrNoValidPartitions = errors.New("all partitions are fully replicated")

// TODO: need a way to clean up zookeeper cruft

// A dataset represents a single version of a particular sequins db: in
// other words, a collection of files. In the distributed case, it understands
// partitions and can route requests.
type dataset struct {
	name          string
	version       string
	numPartitions int
	blockStore    *blocks.BlockStore

	peers     *peers
	zkWatcher *zkWatcher

	localPartitions  map[int]bool
	remotePartitions map[int][]string
	partitionIds     map[int]string

	missingPartitions int
	partitionLock     sync.RWMutex
	partitionsReady   chan bool
}

func newDataset(version string, numPartitions int, peers *peers, zkWatcher *zkWatcher) *dataset {
	ds := &dataset{
		version:         version,
		numPartitions:   numPartitions,
		peers:           peers,
		zkWatcher:       zkWatcher,
		partitionsReady: make(chan bool),
	}

	// If we're running in standalone mode, these'll be nil.
	var localPartitions map[int]bool
	var remotePartitions map[int][]string
	var partitionIds map[int]string
	var missingPartitions int

	if peers != nil {
		// Select which partitions are local by iterating through them all, and
		// checking the hashring.
		localPartitions = make(map[int]bool)
		partitionIds = make(map[int]string)
		dispPartitions := make([]int, 0)
		replication := 2 // TODO

		for i := 0; i < numPartitions; i++ {
			partitionId := ds.partitionId(i)
			partitionIds[i] = partitionId

			replicas := peers.pick(partitionId, replication)
			for _, replica := range replicas {
				if replica == peerSelf {
					localPartitions[i] = true
					dispPartitions = append(dispPartitions, i)
				}
			}
		}

		log.Printf("Selected partitions for version %s: %v\n", version, dispPartitions)
		missingPartitions = (numPartitions - len(localPartitions))
	} else {
		close(ds.partitionsReady)
	}

	ds.localPartitions = localPartitions
	ds.remotePartitions = remotePartitions
	ds.partitionIds = partitionIds
	ds.missingPartitions = missingPartitions
	return ds
}

// sync syncs the remote partitions from zoolander, when they change. When it
// sees a full set of partitions
func (ds *dataset) sync(updates chan []string) {
	for {
		nodes := <-updates
		ds.updatePartitions(nodes)
	}
}

func (ds *dataset) updatePartitions(nodes []string) {
	ds.partitionLock.Lock()
	defer ds.partitionLock.Unlock()

	remotePartitions := make(map[int][]string)
	for _, node := range nodes {
		parts := strings.SplitN(node, "@", 2)
		p, _ := strconv.Atoi(parts[0])
		remotePartitions[p] = append(remotePartitions[p], parts[1])
	}

	// Check for each partition. If there's one available somewhere, then we're ready to
	// rumble.
	ds.remotePartitions = remotePartitions
	missing := 0
	for i := 0; i < ds.numPartitions; i++ {
		if _, ok := ds.localPartitions[i]; ok {
			continue
		}

		if _, ok := ds.remotePartitions[i]; ok {
			continue
		}

		missing += 1
	}

	ds.missingPartitions = missing
	if missing == 0 {
		select {
		case ds.partitionsReady <- true:
		default:
		}
	}
}

// build prepares the dataset, blocking until it is ready.
func (ds *dataset) build(be backend.Backend, storagePath string) error {
	if ds.localPartitions != nil && len(ds.localPartitions) == 0 {
		log.Println("All valid partitions for version", ds.version, "are already spoken for. Consider increasing the replication level.")
		return ErrNoValidPartitions
	}

	err := ds.buildLocalPartitions(be, storagePath)
	if err != nil {
		return err
	}

	if ds.peers != nil {
		partitionPath := path.Join("partitions", ds.version)
		err := ds.zkWatcher.createPath(partitionPath)
		if err != nil {
			return err
		}

		// Advertise all the partitions we have locally.
		// TODO: very short possible race, since aren't actually listening over HTTP.
		// We shouldn't advertise until we're listening on HTTP, and then we should
		// block proxied requests until we see all partitions ready (possibly just refuse outside connections?)
		// (in multiplexed world, we can just 404 if not a proxied request)
		ds.advertisePartitions()

		updates := ds.zkWatcher.watchChildren(partitionPath)
		go ds.sync(updates)

		// Wait for all remote partitions to be available.
		for {
			ds.partitionLock.RLock()
			ready := (ds.missingPartitions == 0)
			ds.partitionLock.RUnlock()
			if ready {
				break
			}

			log.Printf("Waiting for all partitions to be available (missing %d)", ds.missingPartitions)
			t := time.NewTimer(10 * time.Second)
			select {
			case <-t.C:
			case <-ds.partitionsReady:
			}
		}
	}

	return nil
}

// TODO: parallelize multiple files at once

func (ds *dataset) buildLocalPartitions(be backend.Backend, storagePath string) error {
	files, err := be.ListFiles(ds.version)
	if err != nil {
		return err
	}

	path := filepath.Join(storagePath, ds.version)
	_, err = os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from existing manifest at", path)
		blockStore, err := blocks.NewFromManifest(path, ds.localPartitions)
		if err == nil {
			ds.blockStore = blockStore
			return nil
		} else {
			log.Println("Error loading version", ds.version, "from manifest:", err)
		}
	}

	// TODO: make this logging less confusing
	log.Println("Loading version", ds.version, "from", be.DisplayPath(version), "into local directory", path)

	log.Println("Clearing local directory", path)
	os.RemoveAll(path)
	err = os.MkdirAll(path, 0755|os.ModeDir)
	if err != nil {
		return fmt.Errorf("Error creating local storage directory: %s", err)
	}

	blockStore := blocks.New(path, len(files), ds.localPartitions)
	for _, file := range files {
		err := ds.addFile(blockStore, be, file)
		if err != nil {
			return err
		}
	}

	blockStore.SaveManifest()
	ds.blockStore = blockStore
	return nil
}

// TODO: this probably belongs in blockstore
func (ds *dataset) addFile(bs *blocks.BlockStore, be backend.Backend, file string) error {
	log.Println("Reading records from", file, "at", be.DisplayPath(ds.version))

	stream, err := be.Open(ds.version, file)
	if err != nil {
		return fmt.Errorf("reading %s: %s", file, err)
	}

	sf := sequencefile.New(stream)
	err = sf.ReadHeader()
	if err != nil {
		return fmt.Errorf("reading header from %s: %s", file, err)
	}

	err = bs.AddFile(sf)
	if err != nil {
		return fmt.Errorf("reading %s: %s", file, err)
	}

	return nil
}

func (ds *dataset) advertisePartitions() {
	for i := 0; i < ds.numPartitions; i++ {
		node := fmt.Sprintf("%05d@%s", i, ds.peers.address)
		ds.zkWatcher.createEphemeral(path.Join("partitions", ds.version, node))
	}
}

// TODO: cleanup zk state. not everything is ephemeral, and we may be long-running w/ multiple versions

func (ds *dataset) close() error {
	return ds.blockStore.Close()
}

// partitionId returns a string id for the given partition, to be used for the
// consistent hashing ring. It's not really meant to be unique, but it should be
// different for different datasets with the same number of partitions, so that
// they don't shard identically.
func (ds *dataset) partitionId(partition int) string {
	return fmt.Sprintf("%s:%05d", ds.version, partition)
}
