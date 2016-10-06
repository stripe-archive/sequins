package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/colinmarc/sequencefile"

	"github.com/stripe/sequins/blocks"
)

var (
	errFilesChanged   = errors.New("the list of remote files changed while building")
	errWrongPartition = errors.New("the file is cleanly partitioned, but doesn't contain a partition we want")
)

// build prepares the version, blocking until all local partitions are ready,
// then returns it. If onlyFromManifest is true, it will only load data on local
// disk from a manifest, and fail otherwise.
func (vs *version) build(files []string) error {
	if vs.blockStore != nil {
		return nil
	} else if vs.selectedLocalPartitions != nil && len(vs.selectedLocalPartitions) == 0 {
		return nil
	}

	if len(files) == 0 {
		log.Println("Version", vs.name, "of", vs.db, "has no data. Loading it anyway.")
		return nil
	} else if len(files) != vs.numPartitions {
		log.Printf("Number of files under %s changed (%d vs %d)",
			vs.sequins.backend.DisplayPath(vs.db, vs.name), len(files), vs.numPartitions)
		return errFilesChanged
	}

	log.Println("Loading", vs.db, "version", vs.name, "from",
		vs.sequins.backend.DisplayPath(vs.db, vs.name), "into local directory", vs.path)

	blockStore, err := vs.createStore(files)
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
		vs.partitions.updateLocalPartitions(vs.selectedLocalPartitions)
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

func (vs *version) createStore(files []string) (*blocks.BlockStore, error) {
	if _, err := os.Stat(vs.path); err == nil {
		log.Println("Clearing local directory", vs.path)
	}

	os.RemoveAll(vs.path)
	err := os.MkdirAll(vs.path, 0755|os.ModeDir)
	if err != nil {
		return nil, fmt.Errorf("error creating local storage directory: %s", err)
	}

	blockStore := blocks.New(vs.path, vs.numPartitions, vs.selectedLocalPartitions,
		vs.sequins.config.Storage.Compression, vs.sequins.config.Storage.BlockSize)
	for _, file := range files {
		err := vs.addFile(blockStore, file)
		if err != nil {
			blockStore.Close()
			blockStore.Delete()
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
	defer stream.Close()

	sf := sequencefile.NewReader(bufio.NewReader(stream))
	err = sf.ReadHeader()
	if err != nil {
		return fmt.Errorf("reading header from %s: %s", disp, err)
	}

	err = vs.addFileKeys(bs, sf)
	if err == errWrongPartition {
		log.Println("Skipping", disp, "because it contains no relevant partitions")
	} else if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}

	return nil
}

func (vs *version) addFileKeys(bs *blocks.BlockStore, reader *sequencefile.Reader) error {
	throttle := vs.sequins.config.ThrottleLoads.Duration
	canAssumePartition := true
	assumedPartition := -1
	assumedFor := 0

	for reader.Scan() {
		if throttle != 0 {
			time.Sleep(throttle)
		}

		key, value, err := unwrapKeyValue(reader)
		if err != nil {
			return err
		}

		partition, alternatePartition := blocks.KeyPartition(key, vs.numPartitions)

		// If we see the same partition (which is based on the hash) for the first
		// 5000 keys, it's safe to assume that this file only contains that
		// partition. This is often the case if the data has been shuffled by the
		// output key in a way that aligns with our own partitioning scheme.
		if canAssumePartition {
			if assumedPartition == -1 {
				assumedPartition = partition
			} else if partition != assumedPartition {
				if alternatePartition == assumedPartition {
					partition = alternatePartition
				} else {
					canAssumePartition = false
				}
			} else {
				assumedFor += 1
			}
		}

		// Once we see 5000 keys from the same partition, and it's a partition we
		// don't want, it's safe to assume the whole file is like that, and we can
		// skip the rest.
		if vs.selectedLocalPartitions != nil && !vs.selectedLocalPartitions[partition] {
			if canAssumePartition && assumedFor > 5000 {
				return errWrongPartition
			}

			continue
		}

		bs.Add(key, value)
	}

	if reader.Err() != nil {
		return reader.Err()
	}

	return nil
}

// unwrapKeyValue correctly prepares a key and value for storage, depending on
// how they are serialized in the original file; namely, BytesWritable and Text
// keys and values are unwrapped.
func unwrapKeyValue(reader *sequencefile.Reader) (key []byte, value []byte, err error) {
	// sequencefile.Text or sequencefile.BytesWritable can panic if the data is corrupted.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sequencefile: record deserialization failed: %s", r)
		}
	}()

	switch reader.Header.KeyClassName {
	case sequencefile.BytesWritableClassName:
		key = sequencefile.BytesWritable(reader.Key())
	case sequencefile.TextClassName:
		key = []byte(sequencefile.Text(reader.Key()))
	default:
		key = reader.Key()
	}

	switch reader.Header.ValueClassName {
	case sequencefile.BytesWritableClassName:
		value = sequencefile.BytesWritable(reader.Value())
	case sequencefile.TextClassName:
		value = []byte(sequencefile.Text(reader.Value()))
	default:
		value = reader.Value()
	}

	return
}
