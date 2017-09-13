package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/colinmarc/sequencefile"

	"github.com/stripe/sequins/blocks"
)

var (
	errWrongPartition = errors.New("the file is cleanly partitioned, but doesn't contain a partition we want")
	errCanceled       = errors.New("build canceled")
)

func (vs *version) build() {
	// Welcome to the sequins museum of lock acquisition. First, we grab the lock
	// for this version, and check that the previous holder didn't finish
	// building.
	vs.buildLock.Lock()
	defer vs.buildLock.Unlock()
	if vs.built {
		return
	}

	// Then the db-wide lock, and check that a newer version didn't obsolete us.
	vs.db.buildLock.Lock()
	defer vs.db.buildLock.Unlock()

	partitions := vs.partitions.NeededLocal()
	if len(partitions) == 0 {
		vs.built = true
		return
	}

	log.Println("Loading", len(partitions), "partitions of", vs.db.name, "version", vs.name,
		"from", vs.sequins.backend.DisplayPath(vs.db.name, vs.name))

	// We create the directory right before we load data into it, so we don't
	// leave empty directories laying around.
	err := os.MkdirAll(vs.path, 0755|os.ModeDir)
	if err != nil && !os.IsExist(err) {
		log.Printf("Error initializing version %s of %s: %s", vs.name, vs.db.name, err)
		vs.setState(versionError)
		return
	}

	err = vs.addFiles(partitions)
	if err != nil {
		if err != errCanceled {
			log.Printf("Error building version %s of %s: %s", vs.name, vs.db.name, err)
			vs.setState(versionError)
		}

		vs.blockStore.Revert()
		return
	}

	vs.partitions.UpdateLocal(partitions)
	vs.built = true
}

// addFiles adds the given files to the block store, selecting only the
// given partitions.
func (vs *version) addFiles(partitions map[int]bool) error {
	if len(vs.files) == 0 {
		log.Println("Version", vs.name, "of", vs.db.name, "has no data. Loading it anyway.")
		return nil
	}

	var tags []string
	var remaining int32
	if vs.stats != nil {
		tags = []string{fmt.Sprintf("sequins_db:%s", vs.db.name)}
		atomic.StoreInt32(&remaining, int32(len(vs.files)))
		vs.stats.Gauge("s3.queue_depth", float64(len(vs.files)), tags, 1)
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(vs.files))
	errs := make(chan error, len(vs.files))
	for _, file := range vs.files {
		// Ensure that `file` we reference below is the `file` we observed in this loop iteration.
		file := file
		f := func() {
			defer wg.Done()
			if vs.stats != nil {
				defer func() {
					vs.stats.Gauge("s3.queue_depth", float64(atomic.AddInt32(&remaining, -1)), tags, 1)
				}()
			}
			defer func() {
				if r := recover(); r != nil {
					errs <- fmt.Errorf("panic in addFiles[%q] task: %v", file, r)
				}
			}()

			select {
			case <-vs.cancel:
				return
			default:
			}

			err := vs.addFile(file, partitions)
			if err != nil {
				errs <- fmt.Errorf("addFiles[%q]: %v", file, err)
				return
			}
		}
		vs.sequins.workQueue.Schedule(f)
	}

	c := make(chan interface{}, 1)
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-vs.cancel:
		return errCanceled
	case err := <-errs:
		return err
	case <-c:
		return vs.blockStore.Save(vs.partitions.SelectedLocal())
	}
}

func (vs *version) addFile(file string, partitions map[int]bool) error {
	if vs.stats != nil {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			tags := []string{fmt.Sprintf("sequins_db:%s", vs.db.name)}
			vs.stats.Timing("s3.download_duration", duration, tags, 1)
		}()
	}

	disp := vs.sequins.backend.DisplayPath(vs.db.name, vs.name, file)
	log.Println("Reading records from", disp)

	stream, err := vs.sequins.backend.Open(vs.db.name, vs.name, file)
	if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}
	defer stream.Close()

	sf := sequencefile.NewReader(bufio.NewReader(stream))
	err = sf.ReadHeader()
	if err != nil {
		return fmt.Errorf("reading header from %s: %s", disp, err)
	}

	err = vs.addFileKeys(sf, partitions)
	if err == errWrongPartition {
		log.Println("Skipping", disp, "because it contains no relevant partitions")
	} else if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}

	// Intentionally hang, for tests.
	hang := &vs.sequins.config.Test.Hang
	if hang.Version == vs.name && hang.File == file {
		time.Sleep(time.Hour)
	}

	return nil
}

func (vs *version) addFileKeys(reader *sequencefile.Reader, partitions map[int]bool) error {
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

		if !partitions[partition] {
			// Once we see 5000 keys from the same partition, and it's a partition we
			// don't want, it's safe to assume the whole file is like that, and we can
			// skip the rest.
			if canAssumePartition && assumedFor > 5000 {
				return errWrongPartition
			}

			continue
		}

		err = vs.blockStore.Add(key, value)
		if err != nil {
			return err
		}
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
