package index

import (
	"bytes"
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

type indexer struct {
	index     *Index
	fileIndex int
	file      *sequencefile.SequenceFile
	error     error
}

type Index struct {
	Path  string
	Ready bool

	files []*sequencefile.SequenceFile
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

	for i, file := range index.files {
		log.Printf("Indexing %s...\n", file.Path)
		indexer := newIndexer(file, i, index)
		indexers = append(indexers, indexer)

		go func() {
			indexer.start()
			wg.Done()
		}()
	}

	wg.Wait()

	for _, indexer := range indexers {
		if indexer.error != nil {
			return indexer.error
		}
	}

	index.Ready = true
	return nil
}

func (index *Index) Get(key string) (*io.SectionReader, error) {
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
	index.readLocks[fileIndex].Lock()
	defer index.readLocks[fileIndex].Unlock()

	record, err := index.files[fileIndex].ReadRecordAtOffset(offset)
	return record.Value, err
}

func (index *Index) Count() (int, error) {
	if !index.Ready {
		return -1, errors.New("Index isn't finished being built yet.")
	}

	return index.count, nil
}

func (index *Index) Close() {
	index.ldb.Close()
}

func (index *Index) buildFileList() error {
	files, err := ioutil.ReadDir(index.Path)
	if err != nil {
		return err
	}

	index.files = make([]*sequencefile.SequenceFile, 0, len(files))
	index.readLocks = make([]sync.Mutex, len(files))

	for _, file := range files {
		if !file.IsDir() && !strings.HasPrefix(file.Name(), "_") {
			err := index.addFile(file.Name())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (index *Index) addFile(subPath string) error {
	path := filepath.Join(index.Path, subPath)

	file, err := sequencefile.New(path)
	if err != nil {
		return err
	}

	index.files = append(index.files, file)
	return nil
}

func newIndexer(file *sequencefile.SequenceFile, fileIndex int, index *Index) indexer {
	indexer := indexer{
		index:     index,
		fileIndex: fileIndex,
		file:      file,
	}

	return indexer
}

func (i *indexer) start() {
	var buf bytes.Buffer

	for {
		record, err := i.file.ReadNextRecord()
		if err == io.EOF {
			log.Println("Finished indexing", i.file.Path)
			return
		} else if err != nil {
			i.error = err
			return
		}

		entry := serializeIndexEntry(i.fileIndex, record.Offset)
		buf.Reset()
		io.Copy(&buf, record.Key)

		i.index.ldb.Put(buf.Bytes(), entry, nil)
		i.index.count++
	}
}

func serializeIndexEntry(fileIndex int, offset int64) []byte {
	bytes := make([]byte, 12)
	binary.BigEndian.PutUint32(bytes, uint32(fileIndex))
	binary.BigEndian.PutUint64(bytes[4:], uint64(offset))

	return bytes
}

func deserializeIndexEntry(bytes []byte) (int, int64) {
	fileIndex := binary.BigEndian.Uint32(bytes[:4])
	offset := binary.BigEndian.Uint64(bytes[4:12])

	return int(fileIndex), int64(offset)
}
