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
	lock   sync.Mutex
	file   *os.File
	reader *sequencefile.Reader
}

type indexer struct {
	index     *Index
	fileIndex int
	path      string
	err       error
}

type Index struct {
	Path  string
	Ready bool

	files []indexFile

	ldb   *leveldb.DB
	count int

	readLocks []sync.Mutex
}

func New(path string) *Index {
	index := Index{
		Path:  path,
		Ready: false,
	}

	return &index
}

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

	indexers := make([]indexer, 0, len(index.files))
	wg := sync.WaitGroup{}
	wg.Add(len(index.files))

	for i := range index.files {
		path := index.files[i].file.Name()
		log.Printf("Indexing %s...\n", path)
		indexer := newIndexer(path, i, index)
		indexers = append(indexers, indexer)

		go func() {
			indexer.start()
			wg.Done()
		}()
	}

	wg.Wait()

	for _, indexer := range indexers {
		if indexer.err != nil {
			return indexer.err
		}
	}

	index.Ready = true
	return nil
}

func (index *Index) Get(key string) ([]byte, error) {
	if !index.Ready {
		return nil, errors.New("Index isn't finished being built yet.")
	}

	bytes, err := index.ldb.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	if len(bytes) != 12 {
		return nil, fmt.Errorf("Invalid value length: %d", len(bytes))
	}

	fileIndex, offset := deserializeIndexEntry(bytes)
	f := index.files[fileIndex]

	f.lock.Lock()
	defer f.lock.Unlock()

	_, err = f.file.Seek(offset, 0)
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

	return f.reader.Value(), nil
}

func (index *Index) Count() (int, error) {
	if !index.Ready {
		return -1, errors.New("Index isn't finished being built yet.")
	}

	return index.count, nil
}

func (index *Index) Close() {
	index.ldb.Close()
	for i := range index.files {
		index.files[i].file.Close()
	}
}

func (index *Index) buildFileList() error {
	infos, err := ioutil.ReadDir(index.Path)
	if err != nil {
		return err
	}

	index.files = make([]indexFile, 0, len(infos))

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

func newIndexer(path string, fileIndex int, index *Index) indexer {
	indexer := indexer{
		index:     index,
		fileIndex: fileIndex,
		path:      path,
	}

	return indexer
}

func (i *indexer) start() {
	file, err := os.Open(i.path)

	sf := sequencefile.New(file)
	err = sf.ReadHeader()
	if err != nil {
		i.err = err
		return
	}

	entry := make([]byte, 12)
	for {
		offset, err := file.Seek(0, 1)
		if err != nil {
			i.err = err
			return
		}

		if !sf.ScanKey() {
			break
		}

		serializeIndexEntry(entry, i.fileIndex, offset)
		i.index.ldb.Put(sf.Key(), entry, nil)
		i.index.count++
	}

	if sf.Err() != nil {
		i.err = sf.Err()
		return
	}

	log.Println("Finished indexing", i.path)
}

func serializeIndexEntry(b []byte, fileIndex int, offset int64) {
	binary.BigEndian.PutUint32(b, uint32(fileIndex))
	binary.BigEndian.PutUint64(b[4:], uint64(offset))
}

func deserializeIndexEntry(bytes []byte) (int, int64) {
	fileIndex := binary.BigEndian.Uint32(bytes[:4])
	offset := binary.BigEndian.Uint64(bytes[4:12])

	return int(fileIndex), int64(offset)
}
