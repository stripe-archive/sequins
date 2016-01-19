package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

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
	ready            chan bool
}

func newDataset(version string, numPartitions int, peers *peers, zkWatcher *zkWatcher) *dataset {
	ds := &dataset{
		version:       version,
		numPartitions: numPartitions,
		peers:         peers,
		zkWatcher:     zkWatcher,
		ready: make(chan bool),
	}

	// If we're running in standalone mode, these'll be nil.
	var localPartitions map[int]bool
	var remotePartitions map[int][]string
	var partitionIds map[int]string

	if peers != nil {
		// Select which partitions are local by iterating through them all, and
		// checking the hashring.
		localPartitions = make(map[int]bool)
		partitionIds = make(map[int]string)
		replication := 2 // TODO
		for i := 0; i < numPartitions; i++ {
			partitionId := ds.partitionId(i)
			partitionIds[i] = partitionId

			replicas := peers.pick(partitionId, replication)
			for _, replica := range replicas {
				if replica == peerSelf {
					localPartitions[i] = true
				}
			}
		}

		// Watch remote replicas
		updates := zkWatcher.watchChildren(path.Join("partitions", version))
		go ds.sync(updates)
	} else {
		close(ds.ready)
	}

	ds.localPartitions = localPartitions
	ds.remotePartitions = remotePartitions
	ds.partitionIds = partitionIds
	return ds
}

// sync syncs the remote partitions from zoolander, when they change. When it
// sees a full set of partitions, it closes ds.ready.
func (ds *dataset) sync(updates chan []string) {
	for {
		nodes := <-updates
		remotePartitions := make(map[int][]string)
		for _, node := range nodes {
			parts := strings.SplitN(node, "@", 2)
			p, _ := strconv.Atoi(parts[0])
			remotePartitions[p] = append(remotePartitions[p], parts[1])
		}

		// Check for each partition. If we have every one somewhere, we're ready to
		// rumble.
		ds.remotePartitions = remotePartitions
		complete := true
		for i := 0; i < ds.numPartitions; i++ {
			if _, ok := ds.localPartitions[i]; ok {
				continue
			}
			if _, ok := ds.remotePartitions[i]; ok {
				continue
			}
			complete = false
		}

		if complete {
			close(ds.ready)
		}
	}
}

// build prepares the dataset, blocking until it is ready.
func (ds *dataset) build(be backend.Backend, storagePath string) error {
	err := ds.buildLocalPartitions(be, storagePath)
	if err != nil {
		return err
	}

	// Wait for enough remote partitions
	// TODO: log better here
	<-ds.ready

	return nil
}

// TODO: parallelize

func (ds *dataset) buildLocalPartitions(be backend.Backend, storagePath string) error {
	files, err := be.ListFiles(ds.version)
	if err != nil {
		return err
	}

	path := filepath.Join(storagePath, version)
	_, err = os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from existing manifest at", path)
		blockStore, err := blocks.NewFromManifest(path, ds.localPartitions)
		if err == nil {
			ds.blockStore = blockStore
			return nil
		} else {
			log.Println("Error loading version", version, "from manifest:", err)
		}
	}

	// TODO: make this logging less confusing
	log.Println("Loading version", version, "from", be.DisplayPath(version), "into local directory", path)

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

// TODO: cleanup?

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
