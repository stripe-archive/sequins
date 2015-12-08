package index

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/colinmarc/cdb"
	"github.com/stripe/sequins/sequencefile"
)

// A totalFileIndex is a full index for a file, stored in a cdb database of
// key -> offset pairs alongside the data. It's a fallback for sparseFileIndex,
// and it doesn't require the data to be sorted.
type totalFileIndex struct {
	path      string
	indexPath string

	sourceFile     *os.File
	bufferedReader *bufferedReadSeeker
	reader         *sequencefile.Reader
	cdb            *cdb.CDB
	readLock       sync.Mutex

	numFiles          int
	partitionDetector *partitionDetector
}

func newTotalFileIndex(path string, numFiles int) *totalFileIndex {
	dir, base := filepath.Split(path)

	return &totalFileIndex{
		path:      path,
		indexPath: filepath.Join(dir, fmt.Sprintf(".index-%s.cdb", base)),
		numFiles:  numFiles,
	}
}

func (tfi *totalFileIndex) get(key []byte) ([]byte, error) {
	if !tfi.partitionDetector.test(key) {
		return nil, nil
	}

	tfi.readLock.Lock()
	defer tfi.readLock.Unlock()

	entry, err := tfi.cdb.Get(key)
	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	offset := deserializeIndexEntry(entry)
	_, err = tfi.bufferedReader.Seek(offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	ok := tfi.reader.Scan()
	if !ok {
		if tfi.reader.Err() != nil {
			return nil, tfi.reader.Err()
		} else {
			return nil, io.ErrUnexpectedEOF
		}
	}

	if bytes.Compare(tfi.reader.Key(), key) != 0 {
		return nil, errors.New("Unexpected key!")
	}

	return tfi.reader.Value(), nil
}

func (tfi *totalFileIndex) load(manifestEntry manifestEntry) error {
	tfi.partitionDetector = newPartitionDetectorFromManifest(tfi.numFiles, manifestEntry)

	err := tfi.open()
	if err != nil {
		return err
	}

	cdb, err := cdb.Open(tfi.indexPath)
	if err != nil {
		return err
	}

	tfi.cdb = cdb
	return nil
}

// build scans through the file and builds an index of key -> offset pairs
// in a cdb file alongside.w
func (tfi *totalFileIndex) build() error {
	tfi.partitionDetector = newPartitionDetector(tfi.numFiles)

	err := tfi.open()
	if err != nil {
		return err
	}

	cdbWriter, err := cdb.Create(tfi.indexPath)
	if err != nil {
		return err
	}

	entry := make([]byte, 8)
	for {
		offset, err := tfi.bufferedReader.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		if !tfi.reader.ScanKey() {
			break
		}

		// Add key -> offset to the index.
		key := make([]byte, len(tfi.reader.Key()))
		copy(key, tfi.reader.Key())
		tfi.partitionDetector.update(key)
		serializeIndexEntry(entry, offset)
		cdbWriter.Put(key, entry)
	}

	err = tfi.reader.Err()
	if err != nil {
		return err
	}

	db, err := cdbWriter.Freeze()
	tfi.cdb = db
	return err
}

func (tfi *totalFileIndex) open() error {
	f, err := os.Open(tfi.path)
	if err != nil {
		return err
	}

	tfi.sourceFile = f
	tfi.bufferedReader = newBufferedReadSeeker(tfi.sourceFile)
	tfi.reader = sequencefile.New(tfi.bufferedReader)
	return tfi.reader.ReadHeader()
}

func (tfi *totalFileIndex) close() error {
	return tfi.sourceFile.Close()
}

func (tfi *totalFileIndex) cleanup() error {
	return os.Remove(tfi.indexPath)
}

func (tfi *totalFileIndex) manifestEntry() (manifestEntry, error) {
	m := manifestEntry{}

	stat, err := tfi.sourceFile.Stat()
	if err != nil {
		return m, err
	}

	m.Name = filepath.Base(tfi.sourceFile.Name())
	m.Size = stat.Size()
	m.IndexProperties = indexProperties{Sparse: false}

	tfi.partitionDetector.updateManifest(&m)
	return m, nil
}
