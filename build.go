package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sequencefile"
)

var errFilesChanged = errors.New("the list of remote files changed while building")

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
		vs.sequins.backend.DisplayPath(vs.db, vs.name), "into local directory", vs.path)

	blockStore, err := vs.createStore(files, local)
	if err != nil {
		return err
	}

	// Verify that the list of files stayed the same. If files do not match,
	// discard the new blockstore in order to prevent the manifest from updating.
	newFiles, err := vs.sequins.backend.ListFiles(vs.db, vs.name)
	if err != nil {
		return err
	} else {
		if vs.compareFileSets(files, newFiles) {
			blockStore.Close()
			blockStore.Delete()
			return errFilesChanged
		}
	}

	vs.blockStoreLock.Lock()
	defer vs.blockStoreLock.Unlock()
	vs.blockStore = blockStore
	if vs.partitions != nil {
		vs.partitions.updateLocalPartitions(local)
	}

	return nil
}

func (vs *version) compareFileSets(oldFiles, newFiles []string) bool {
	setOld := make(map[string]bool, len(oldFiles))
	setNew := make(map[string]bool, len(newFiles))
	different := false

	if len(oldFiles) != len(newFiles) {
		log.Printf("Number of files under %s changed (%d vs %d)",
			vs.sequins.backend.DisplayPath(vs.db, vs.name), len(oldFiles), len(newFiles))
		different = true
	}

	for _, f := range oldFiles {
		setOld[f] = true
	}

	for _, f := range newFiles {
		setNew[f] = true
		if !setOld[f] {
			log.Println("New file:", vs.sequins.backend.DisplayPath(vs.db, vs.name, f))
			different = true
		}
	}

	for _, f := range oldFiles {
		if !setNew[f] {
			log.Println("Missing file:", vs.sequins.backend.DisplayPath(vs.db, vs.name, f))
			different = true
		}
	}

	return different
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

	blockStore := blocks.New(vs.path, len(files), partitions,
		vs.sequins.config.Storage.Compression, vs.sequins.config.Storage.BlockSize)
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
