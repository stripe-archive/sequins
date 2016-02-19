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
	created       time.Time
	numPartitions int
}

func newVersion(sequins *sequins, db, name string, numPartitions int) *versionBuilder {
	vsb := &versionBuilder{
		sequins:       sequins,
		db:            db,
		name:          name,
		created:       time.Now(),
		numPartitions: numPartitions,
	}

	return vsb
}

// build prepares the version, blocking until all local partitions are ready,
// then returns it.
func (vsb *versionBuilder) build() (*version, error) {
	vs := &version{
		sequins:       vsb.sequins,
		db:            vsb.db,
		name:          vsb.name,
		numPartitions: vsb.numPartitions,
		created:       vsb.created,
	}

	if vsb.numPartitions == 0 {
		log.Println("Version", vsb.name, "of", vsb.db, "has no data. Loading it anyway.")
		return vs, nil
	}

	var local map[int]bool
	if vsb.sequins.peers != nil {
		vs.partitions = watchPartitions(vsb.sequins.zkWatcher, vsb.sequins.peers,
			vsb.db, vsb.name, vsb.numPartitions, vsb.sequins.config.ZK.Replication)

		local = vs.partitions.local
		if len(local) == 0 {
			log.Println("All valid partitions for", vsb.db, "version", vsb.name,
				"are already spoken for. Consider increasing the replication level.")
			return vs, nil
		}

	}

	path := filepath.Join(vsb.sequins.config.LocalStore, "data", vsb.db, vsb.name)
	blockStore, err := vsb.loadBlocks(path, local)
	if err != nil {
		return nil, err
	}

	vs.blockStore = blockStore
	return vs, nil
}

// TODO: parallelize files

func (vsb *versionBuilder) loadBlocks(path string, localPartitions map[int]bool) (*blocks.BlockStore, error) {
	files, err := vsb.sequins.backend.ListFiles(vsb.db, vsb.name)
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from existing manifest at", path)
		blockStore, err := blocks.NewFromManifest(path, localPartitions)
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

	blockStore := blocks.New(path, len(files), localPartitions)
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
