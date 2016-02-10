package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sequencefile"
)

type versionBuilder struct {
	name          string
	numPartitions int

	peers     *peers
	zkWatcher *zkWatcher

	localPartitions  map[int]bool
	remotePartitions map[int][]string
}

func newVersion(name string, numPartitions int, peers *peers, zkWatcher *zkWatcher) *versionBuilder {
	vsb := &versionBuilder{
		name:          name,
		numPartitions: numPartitions,
		peers:         peers,
		zkWatcher:     zkWatcher,
	}

	// If we're running in standalone mode, these'll be nil.
	var localPartitions map[int]bool
	var remotePartitions map[int][]string

	if peers != nil {
		// Select which partitions are local by iterating through them all, and
		// checking the hashring.
		localPartitions = make(map[int]bool)
		dispPartitions := make([]int, 0)
		replication := 2 // TODO

		for i := 0; i < numPartitions; i++ {
			partitionId := vsb.partitionId(i)

			replicas := peers.pick(partitionId, replication)
			for _, replica := range replicas {
				if replica == peerSelf {
					localPartitions[i] = true
					dispPartitions = append(dispPartitions, i)
				}
			}
		}

		log.Printf("Selected partitions for version %s: %v\n", name, dispPartitions)
	}

	vsb.localPartitions = localPartitions
	vsb.remotePartitions = remotePartitions
	return vsb
}

// build prepares the version, blocking until all *local* partitions are ready,
// then returns it.
func (vsb *versionBuilder) build(be backend.Backend, storagePath string) (*version, error) {
	if vsb.localPartitions != nil && len(vsb.localPartitions) == 0 {
		log.Println("All valid partitions for version", vsb.name, "are already spoken for. Consider increasing the replication level.")
		return nil, errNoValidPartitions
	}

	blockStore, err := vsb.loadBlocks(be, storagePath)
	if err != nil {
		return nil, err
	}

	db := &version{
		name:             vsb.name,
		created:          time.Now(),
		numPartitions:    vsb.numPartitions,
		peers:            vsb.peers,
		zkWatcher:        vsb.zkWatcher,
		blockStore:       blockStore,
		localPartitions:  vsb.localPartitions,
		remotePartitions: vsb.remotePartitions,
	}

	return db, nil
}

// TODO: parallelize building

func (vsb *versionBuilder) loadBlocks(be backend.Backend, storagePath string) (*blocks.BlockStore, error) {
	files, err := be.ListFiles(vsb.name)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(storagePath, vsb.name)
	_, err = os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from existing manifest at", path)
		blockStore, err := blocks.NewFromManifest(path, vsb.localPartitions)
		if err == nil {
			return blockStore, nil
		} else {
			log.Println("Error loading version", vsb.name, "from manifest:", err)
		}
	}

	// TODO: make this logging less confusing
	log.Println("Loading version", vsb.name, "from", be.DisplayPath(vsb.name), "into local directory", path)

	log.Println("Clearing local directory", path)
	os.RemoveAll(path)
	err = os.MkdirAll(path, 0755|os.ModeDir)
	if err != nil {
		return nil, fmt.Errorf("Error creating local storage directory: %s", err)
	}

	blockStore := blocks.New(path, len(files), vsb.localPartitions)
	for _, file := range files {
		err := vsb.addFile(blockStore, be, file)
		if err != nil {
			return nil, err
		}
	}

	blockStore.SaveManifest()
	return blockStore, nil
}

// TODO: this probably belongs in blockstore
func (vsb *versionBuilder) addFile(bs *blocks.BlockStore, be backend.Backend, file string) error {
	log.Println("Reading records from", file, "at", be.DisplayPath(vsb.name))

	stream, err := be.Open(vsb.name, file)
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

// partitionId returns a string id for the given partition, to be used for the
// consistent hashing ring. It's not really meant to be unique, but it should be
// different for different versions with the same number of partitions, so that
// they don't shard identically.
func (vsb *versionBuilder) partitionId(partition int) string {
	return fmt.Sprintf("%s:%05d", vsb.name, partition)
}
