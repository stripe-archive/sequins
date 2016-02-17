package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sequencefile"
)

type versionBuilder struct {
	sequins       *sequins
	db            string
	name          string
	numPartitions int

	localPartitions  map[int]bool
	remotePartitions map[int][]string
}

func newVersion(sequins *sequins, db, name string, numPartitions int) *versionBuilder {
	vsb := &versionBuilder{
		sequins:       sequins,
		db:            db,
		name:          name,
		numPartitions: numPartitions,
	}

	// If we're running in standalone mode, these'll be nil.
	var localPartitions map[int]bool
	var remotePartitions map[int][]string

	if sequins.peers != nil {
		// Select which partitions are local by iterating through them all, and
		// checking the hashring.
		localPartitions = make(map[int]bool)
		dispPartitions := make([]int, 0)
		replication := vsb.sequins.config.ZK.Replication

		for i := 0; i < numPartitions; i++ {
			partitionId := vsb.partitionId(i)

			replicas := sequins.peers.pick(partitionId, replication)
			for _, replica := range replicas {
				if replica == peerSelf {
					localPartitions[i] = true
					dispPartitions = append(dispPartitions, i)
				}
			}
		}

		log.Printf("Selected partitions for %s version %s: %v\n", db, name, dispPartitions)
	}

	vsb.localPartitions = localPartitions
	vsb.remotePartitions = remotePartitions
	return vsb
}

// build prepares the version, blocking until all local partitions are ready,
// then returns it.
func (vsb *versionBuilder) build() (*version, error) {
	vs := &version{
		sequins:          vsb.sequins,
		db:               vsb.db,
		name:             vsb.name,
		created:          time.Now(),
		numPartitions:    vsb.numPartitions,
		localPartitions:  vsb.localPartitions,
		remotePartitions: vsb.remotePartitions,
	}

	if vsb.localPartitions != nil && len(vsb.localPartitions) == 0 {
		log.Println("All valid partitions for", vsb.db, "version", vsb.name,
			"are already spoken for. Consider increasing the replication level.")
		return vs, nil
	}

	path := filepath.Join(vsb.sequins.config.LocalStore, "data", vsb.db, vsb.name)
	blockStore, err := vsb.loadBlocks(path)
	if err != nil {
		return nil, err
	}

	vs.blockStore = blockStore
	return vs, nil
}

// TODO: parallelize building

func (vsb *versionBuilder) loadBlocks(path string) (*blocks.BlockStore, error) {
	files, err := vsb.sequins.backend.ListFiles(vsb.db, vsb.name)
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from existing manifest at", path)
		blockStore, err := blocks.NewFromManifest(path, vsb.localPartitions)
		if err == nil {
			return blockStore, nil
		} else {
			log.Println("Error loading", vsb.db, "version", vsb.name, "from manifest:", err)
		}
	}

	log.Println("Loading", vsb.db, "version", vsb.name, "from",
		vsb.sequins.backend.DisplayPath(vsb.name), "into local directory", path)

	log.Println("Clearing local directory", path)
	os.RemoveAll(path)
	err = os.MkdirAll(path, 0755|os.ModeDir)
	if err != nil {
		return nil, fmt.Errorf("Error creating local storage directory: %s", err)
	}

	blockStore := blocks.New(path, len(files), vsb.localPartitions)
	for _, file := range files {
		err := vsb.addFile(blockStore, file)
		if err != nil {
			return nil, err
		}
	}

	blockStore.SaveManifest()
	return blockStore, nil
}

func (vsb *versionBuilder) addFile(bs *blocks.BlockStore, file string) error {
	disp := vsb.sequins.backend.DisplayPath(vsb.db, vsb.name, file)
	log.Println("Reading records from", disp)

	stream, err := vsb.sequins.backend.Open(vsb.db, vsb.name, file)
	if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}

	sf := sequencefile.New(stream)
	err = sf.ReadHeader()
	if err != nil {
		return fmt.Errorf("reading header from %s: %s", disp, err)
	}

	err = bs.AddFile(sf)
	if err == blocks.ErrWrongPartition {
		log.Println("Skipping", disp, "because it contains no relevant partitions")
	} else if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
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
