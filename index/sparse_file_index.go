package index

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/colinmarc/cdb"
	"github.com/stripe/sequins/sequencefile"
)

// This should be a multiple of the default SequenceFile sync interval, which is
// usually 2000 bytes.
const skipSize = 100000

var errNotSorted = errors.New("The sequencefile isn't sorted, so can't skip")

// A sparseFileIndex holds an in-memory subset of the keys in its file. On a
// get, instead of looking up the offset of the key directly, as you would with
// a total index, you binary search for the closest key lexicographically (that
// comes before the target key), and scan the file starting at that key until
// you either find your target key or pass it.
//
// This requires the file to be sorted, so sparseFileIndex will return
// errNotSorted if it tries to build an index over an unsorted file. In that
// case, the file has to be reindexed with a total index.
//
// In addition to keeping the set in memory, it serializes the subset of
// key -> offset pairs to a cdb file, so it doesn't have to rebuild on next
// load.
type sparseFileIndex struct {
	path      string
	indexPath string
	lastKey   []byte
	skip      bool
	table     []sparseIndexEntry

	sourceFile     *os.File
	bufferedReader *bufferedReadSeeker
	reader         *sequencefile.Reader
	readLock       sync.Mutex

	numFiles          int
	partitionDetector *partitionDetector
}

type sparseIndexEntry struct {
	key    []byte
	offset int64
}

func newSparseFileIndex(path string, numFiles int) *sparseFileIndex {
	dir, base := filepath.Split(path)

	return &sparseFileIndex{
		path:      path,
		indexPath: filepath.Join(dir, fmt.Sprintf(".index-sparse-%s.cdb", base)),
		table:     make([]sparseIndexEntry, 1024*1024),
		skip:      true,
		numFiles:  numFiles,
	}
}

// get works by binary-searching the in-memory index to find the closest key
// lexicographically, then scanning through the file until it finds the target
// key. If it finds a key that's lexicographically greater while scanning,
// then it knows the key isn't in the file.
func (sfi *sparseFileIndex) get(key []byte) ([]byte, error) {
	if !sfi.partitionDetector.test(key) {
		return nil, nil
	}

	sfi.readLock.Lock()
	defer sfi.readLock.Unlock()

	closest := sort.Search(len(sfi.table), func(i int) bool {
		return bytes.Compare(sfi.table[i].key, key) >= 0
	})

	// sort.Search actually returns the closest key *after* the target, so we need
	// to back up by one if we didn't nail it exactly.
	if closest == len(sfi.table) || bytes.Compare(sfi.table[closest].key, key) > 0 {
		closest -= 1
	}

	startOffset := sfi.table[closest].offset
	_, err := sfi.bufferedReader.Seek(startOffset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	for sfi.reader.Scan() {
		compare := bytes.Compare(sfi.reader.Key(), key)
		if compare == 0 {
			return sfi.reader.Value(), nil
		} else if compare > 0 {
			break
		}
	}

	return nil, sfi.reader.Err()
}

// load loads the existing subset of key -> offset pairs from the saved
// file. It does not re-check that the file is sorted.
func (sfi *sparseFileIndex) load(manifestEntry manifestEntry) error {
	sfi.partitionDetector = newPartitionDetectorFromManifest(sfi.numFiles, manifestEntry)

	err := sfi.open()
	if err != nil {
		return err
	}

	db, err := cdb.Open(sfi.indexPath)
	if err != nil {
		return err
	}

	iter := db.Iter()
	for iter.Next() {
		offset := deserializeIndexEntry(iter.Value())
		indexEntry := sparseIndexEntry{key: iter.Key(), offset: offset}
		sfi.table = append(sfi.table, indexEntry)
	}

	return iter.Err()
}

// build scans through the file and builds the sparse index, checking the whole
// time that the file is sorted. If it detects that the file is, in fact, not
// sorted, then it returns errNotSorted.
func (sfi *sparseFileIndex) build() error {
	sfi.partitionDetector = newPartitionDetector(sfi.numFiles)

	err := sfi.open()
	if err != nil {
		return err
	}

	// Store the subset of keys as we go along.
	cdbWriter, err := cdb.Create(sfi.indexPath)
	if err != nil {
		return err
	}

	// Reuse this byte slice.
	offsetBytes := make([]byte, 8)

	// Start jumping through the file, recording keys as we go.
	for {
		offset, err := sfi.bufferedReader.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		if !sfi.reader.ScanKey() {
			break
		}

		key := make([]byte, len(sfi.reader.Key()))
		copy(key, sfi.reader.Key())
		sfi.partitionDetector.update(key)

		// Keep track of the minimum and maximum keys, and check that the file is
		// sorted.

		include := true
		if sfi.lastKey == nil {
			sfi.lastKey = key
		} else {
			compare := bytes.Compare(key, sfi.lastKey)
			if compare < 0 {
				return errNotSorted
			} else if compare == 0 {
				// If it's the same as the last key, it's a duplicate and we can avoid
				// adding it to the index
				include = false
			} else {
				sfi.lastKey = key
			}
		}

		// Add key -> offset to the index, and store it on disk as well.
		if include {
			indexEntry := sparseIndexEntry{key: key, offset: offset}
			sfi.table = append(sfi.table, indexEntry)
			serializeIndexEntry(offsetBytes, offset)
			cdbWriter.Put(key, offsetBytes)
		}

		// Don't skip at all until we've read a clump of consecutive keys at the
		// beginning of the file, so that we can fail early if the data isn't
		// sorted, and so that we can make sure we find the most-minimum key.
		if sfi.skip && offset > skipSize {
			checkpoint := offset
			err = sfi.skipAndSync()

			if err == io.EOF {
				// If we reach the end of the file while skipping, jump back to the
				// last sync point and read from there sequentially. We want to make
				// sure we read the last record in order to get maxKey right, and
				// reading another consecutive range of keys will help make extra sure
				// that it's sorted.
				sfi.skip = false
				sfi.bufferedReader.Seek(checkpoint, os.SEEK_SET)

				// Skip the record we just read.
				sfi.reader.ScanKey()
			} else if err != nil {
				return err
			}
		}
	}

	err = cdbWriter.Close()
	if err != nil {
		return err
	}

	return sfi.reader.Err()
}

func (sfi *sparseFileIndex) open() error {
	f, err := os.Open(sfi.path)
	if err != nil {
		return err
	}

	// Since we know we might be scanning for a while before we find the
	// key, it's useful to use a buffered reader.
	sfi.sourceFile = f
	sfi.bufferedReader = newBufferedReadSeeker(sfi.sourceFile)
	sfi.reader = sequencefile.New(sfi.bufferedReader)
	return sfi.reader.ReadHeader()
}

func (sfi *sparseFileIndex) close() error {
	return sfi.sourceFile.Close()
}

func (sfi *sparseFileIndex) cleanup() error {
	return os.Remove(sfi.indexPath)
}

func (sfi *sparseFileIndex) manifestEntry() (manifestEntry, error) {
	m := manifestEntry{}

	stat, err := sfi.sourceFile.Stat()
	if err != nil {
		return m, err
	}

	m.Name = filepath.Base(sfi.sourceFile.Name())
	m.Size = stat.Size()
	m.IndexProperties = indexProperties{Sparse: true}

	sfi.partitionDetector.updateManifest(&m)
	return m, nil
}

func (sfi *sparseFileIndex) skipAndSync() error {
	_, err := sfi.bufferedReader.Seek(skipSize, os.SEEK_CUR)
	if err != nil {
		return err
	}

	return sfi.reader.Sync()
}
