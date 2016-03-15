package main

import (
	"fmt"
	"log"
	"os"

	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sequencefile"
)

// build prepares the version, blocking until all local partitions are ready,
// then returns it. If onlyFromManifest is true, it will only load data on local
// disk from a manifest, and fail otherwise.
func (vs *version) build(files []string) error {
	if vs.blockStore != nil {
		return nil
	}

	if len(files) == 0 {
		log.Println("Version", vs.name, "of", vs.db, "has no data. Loading it anyway.")
		return nil
	}

	var local map[int]bool
	if vs.sequins.peers != nil {
		local = vs.partitions.pickLocalPartitions()
		if len(local) == 0 {
			log.Println("All valid partitions for", vs.db, "version", vs.name,
				"are already spoken for. Consider increasing the replication level.")
			return nil
		}
	}

	log.Println("Loading", vs.db, "version", vs.name, "from",
		vs.sequins.backend.DisplayPath(vs.name), "into local directory", vs.path)

	blockStore, err := vs.createStore(files, local)
	if err != nil {
		return err
	}

	vs.blockStore = blockStore
	if vs.partitions != nil {
		vs.partitions.updateLocalPartitions(local)
	}

	return nil
}

// TODO: parallelize files

func (vs *version) createStore(files []string, partitions map[int]bool) (*blocks.BlockStore, error) {
	if _, err := os.Stat(vs.path); err == nil {
		log.Println("Clearing local directory", vs.path)
	}

	os.RemoveAll(vs.path)
	err := os.MkdirAll(vs.path, 0755|os.ModeDir)
	if err != nil {
		return nil, fmt.Errorf("error creating local storage directory: %s", err)
	}

	blockStore := blocks.New(vs.path, len(files), partitions)
	for _, file := range files {
		err := vs.addFile(blockStore, file)
		if err != nil {
			return nil, err
		}
	}

	blockStore.Save()
	return blockStore, nil
}

func (vs *version) addFile(bs *blocks.BlockStore, file string) error {
	disp := vs.sequins.backend.DisplayPath(vs.db, vs.name, file)
	log.Println("Reading records from", disp)

	stream, err := vs.sequins.backend.Open(vs.db, vs.name, file)
	if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}

	sf := sequencefile.New(stream)
	err = sf.ReadHeader()
	if err != nil {
		return fmt.Errorf("reading header from %s: %s", disp, err)
	}

	err = bs.AddFile(sf, vs.sequins.config.ThrottleLoads.Duration)
	if err == blocks.ErrWrongPartition {
		log.Println("Skipping", disp, "because it contains no relevant partitions")
	} else if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}

	return nil
}
