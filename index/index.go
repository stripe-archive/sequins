package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/stripe/sequins/sequencefile"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("That key doesn't exist.")

type indexFile struct {
	file   *os.File
	reader *sequencefile.Reader
}

type Index struct {
	Path  string
	Ready bool

	files     []indexFile
	readLocks []sync.Mutex

	ldb   *leveldb.DB
	count int
}

// New creates a new Index instance.
func New(path string) *Index {
	index := Index{
		Path:  path,
		Ready: false,
	}

	return &index
}

// BuildIndex reads each of the files and adds a key -> (file, offset) pair
// to the master index for every key in every file.
func (index *Index) BuildIndex() error {
	err := index.buildFileList()
	if err != nil {
		return err
	}

	indexPath := filepath.Join(index.Path, ".index")
	err = os.RemoveAll(indexPath)
	if err != nil {
		return err
	}

	ldb, err := leveldb.OpenFile(indexPath, nil)
	if err != nil {
		return err
	}

	index.ldb = ldb

	for i, f := range index.files {
		path := f.file.Name()
		log.Printf("Indexing %s...\n", path)

		err = index.buildIndexOverFile(i, f)
		if err != nil {
			return err
		}

		log.Println("Finished indexing", path)
	}

	index.Ready = true
	return nil
}

func (index *Index) buildFileList() error {
	infos, err := ioutil.ReadDir(index.Path)
	if err != nil {
		return err
	}

	index.files = make([]indexFile, 0, len(infos))
	index.readLocks = make([]sync.Mutex, len(infos))

	for _, info := range infos {
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "_") {
			err := index.addFile(info.Name())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (index *Index) addFile(subPath string) error {
	path := filepath.Join(index.Path, subPath)

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	f := indexFile{
		file:   file,
		reader: sequencefile.New(file),
	}

	err = f.reader.ReadHeader()
	if err != nil {
		return err
	}

	index.files = append(index.files, f)
	return nil
}

func (index *Index) buildIndexOverFile(fileId int, f indexFile) error {
	entry := make([]byte, 12)

	for {
		offset, err := f.file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		if !f.reader.ScanKey() {
			break
		}

		serializeIndexEntry(entry, fileId, offset)
		index.ldb.Put(f.reader.Key(), entry, nil)
		index.count++
	}

	return f.reader.Err()
}

// Get returns the value for a given key.
func (index *Index) Get(key string) ([]byte, error) {
	if !index.Ready {
		return nil, errors.New("Index isn't finished being built yet.")
	}

	b, err := index.ldb.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	if len(b) != 12 {
		return nil, fmt.Errorf("Invalid value length: %d", len(b))
	}

	fileId, offset := deserializeIndexEntry(b)
	f := index.files[fileId]

	readLock := index.readLocks[fileId]
	readLock.Lock()
	defer readLock.Unlock()

	_, err = f.file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	ok := f.reader.Scan()
	if !ok {
		if f.reader.Err() != nil {
			return nil, f.reader.Err()
		} else {
			return nil, io.ErrUnexpectedEOF
		}
	}

	val := make([]byte, len(f.reader.Value()))
	copy(val, f.reader.Value())
	return val, err
}

// Count returns the total number of keys in the index.
func (index *Index) Count() (int, error) {
	if !index.Ready {
		return -1, errors.New("Index isn't finished being built yet.")
	}

	return index.count, nil
}

// Close closes the index, and any open files it has.
func (index *Index) Close() {
	index.ldb.Close()
	for _, f := range index.files {
		f.file.Close()
	}
}

func serializeIndexEntry(b []byte, fileId int, offset int64) {
	binary.BigEndian.PutUint32(b, uint32(fileId))
	binary.BigEndian.PutUint64(b[4:], uint64(offset))
}

func deserializeIndexEntry(b []byte) (int, int64) {
	fileId := binary.BigEndian.Uint32(b[:4])
	offset := binary.BigEndian.Uint64(b[4:])

	return int(fileId), int64(offset)
}
