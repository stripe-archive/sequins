package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/go-sparkey"
	"github.com/colinmarc/sequencefile"
	"github.com/golang/snappy"
	"github.com/juju/ratelimit"

	"github.com/stripe/sequins/blocks"
)

var (
	errWrongPartition = errors.New("the file is cleanly partitioned, but doesn't contain a partition we want")
	errCanceled       = errors.New("build canceled")
)

func compressionString(c sequencefile.Compression) string {
	switch c {
	case sequencefile.NoCompression:
		return "none"
	case sequencefile.RecordCompression:
		return "record"
	case sequencefile.BlockCompression:
		return "block"
	default:
		return strconv.Itoa(int(c))
	}
}

func compressionCodecString(c sequencefile.CompressionCodec) string {
	switch c {
	case sequencefile.GzipCompression:
		return "gzip"
	case sequencefile.SnappyCompression:
		return "snappy"
	default:
		return strconv.Itoa(int(c))
	}
}

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

// Create a ratelimited download stream
func (vs *version) rateLimitedDownloadStream(r io.Reader) io.Reader {
	if vs.sequins.downloadRateLimitBucket == nil {
		return r
	}
	return ratelimit.Reader(r, vs.sequins.downloadRateLimitBucket)
}

// Download a sparkey file into a local file
func (vs *version) sparkeyDownload(src, dst, fileType string, transform func(io.Reader) io.Reader) error {
	disp := vs.sequins.backend.DisplayPath(vs.db.name, vs.name, src)
	log.Printf("downloading sparkey %s file %s\n", fileType, disp)

	stream, err := vs.sequins.backend.Open(vs.db.name, vs.name, src)
	if err != nil {
		return fmt.Errorf("opening sparkey %s file for %s: %s", fileType, disp, err)
	}
	defer stream.Close()

	var trStream = vs.rateLimitedDownloadStream(stream)
	if transform != nil {
		trStream = transform(trStream)
	}

	err = WriteFileAligned(dst, trStream, vs.sequins.config.WriteBufferSize)
	if err != nil {
		return fmt.Errorf("copying sparkey %s file %s: %s", fileType, disp, err)
	}

	return nil
}

// Add a Sparkey file to this version.
func (vs *version) addSparkeyFile(file string, disp string, partition int) error {
	success := false

	tmp, err := ioutil.TempFile(vs.path, ".sparkey")
	if err != nil {
		return fmt.Errorf("creating temporary sparkey file for %s: %s", disp, err)
	}
	logPath := sparkey.LogFileName(tmp.Name())
	idxPath := sparkey.HashFileName(logPath)
	defer func() {
		os.Remove(tmp.Name())
		if !success {
			os.Remove(logPath)
			os.Remove(idxPath)
		}
	}()

	err = vs.sparkeyDownload(file, logPath, "log", nil)
	if err != nil {
		return err
	}

	idxSrc := strings.TrimSuffix(file, ".spl") + ".spi.sz"
	err = vs.sparkeyDownload(idxSrc, idxPath, "index", func(r io.Reader) io.Reader {
		return snappy.NewReader(r)
	})
	if err != nil {
		return err
	}

	return vs.blockStore.AddSparkeyBlock(logPath, partition)
}

// Add a sequencefile to this version.
func (vs *version) addSequenceFile(file string, disp string, partitions map[int]bool) error {
	log.Println("Reading records from", disp)

	stream, err := vs.sequins.backend.Open(vs.db.name, vs.name, file)
	if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}
	defer stream.Close()

	rateLimitedStream := vs.rateLimitedDownloadStream(stream)

	sf := sequencefile.NewReader(bufio.NewReader(rateLimitedStream))
	err = sf.ReadHeader()
	if err != nil {
		return fmt.Errorf("reading header from %s: %s", disp, err)
	}
	log.Printf(
		"sequencefile header: db=%q version=%q file=%q sequencefile_version=%d compression=%q compression_codec=%q compression_codec_class_name=%q key_class_name=%q value_class_name=%q metadata=%q",
		vs.db.name, vs.name, file, sf.Header.Version, compressionString(sf.Header.Compression), compressionCodecString(sf.Header.CompressionCodec),
		sf.Header.CompressionCodecClassName, sf.Header.KeyClassName, sf.Header.ValueClassName, fmt.Sprintf("%v", sf.Header.Metadata))

	err = vs.addFileKeys(sf, partitions)
	if err == errWrongPartition {
		log.Println("Skipping", disp, "because it contains no relevant partitions")
	} else if err != nil {
		return fmt.Errorf("reading %s: %s", disp, err)
	}

	return nil
}

func (vs *version) addFile(file string, partitions map[int]bool) error {
	disp := vs.sequins.backend.DisplayPath(vs.db.name, vs.name, file)

	raw, partition := isSparkeyFile(file)
	if raw && !partitions[partition] {
		// Since we already know the partition, we can skip this right away.
		log.Printf("Skipping sparkey file %s\n", disp)
		return nil
	}

	if vs.stats != nil {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			tags := []string{fmt.Sprintf("sequins_db:%s", vs.db.name)}
			vs.stats.Timing("s3.download_duration", duration, tags, 1)
		}()
	}

	var err error
	if raw {
		err = vs.addSparkeyFile(file, disp, partition)
	} else {
		err = vs.addSequenceFile(file, disp, partitions)
	}
	if err != nil {
		return err
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
