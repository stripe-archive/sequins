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
	Path    string
	Ready   bool
	Version string

	files     []indexFile
	readLocks []sync.Mutex

	ldb   *leveldb.DB
	count int

	refcount sync.WaitGroup
}

// New creates a new Index instance.
func New(path, version string) *Index {
	index := Index{
		Path:    path,
		Ready:   false,
		Version: version,
	}

	return &index
}

// Load reads each of the files and adds a key -> (file, offset) pair to the
// master index for every key in every file. If a manifest file and index
// already exist in the directory, it'll use that.
func (index *Index) Load() error {
	err := index.buildFileList()
	if err != nil {
		return err
	}

	// Try loading and checking the manifest first.
	manifestPath := filepath.Join(index.Path, ".manifest")
	manifest, err := readManifest(manifestPath)
	if err == nil {
		log.Println("Loading index from existing manifest at", manifestPath)
		err = index.loadIndexFromManifest(manifest)
		if err != nil {
			log.Println("Failed to load existing manifest with error:", err)
		} else {
			index.Ready = true
			return nil
		}
	}

	err = index.buildNewIndex()
	if err != nil {
		return err
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
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "_") && !strings.HasPrefix(info.Name(), ".") {
			err := index.addFile(info.Name())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (index *Index) loadIndexFromManifest(m manifest) error {
	for i, entry := range m.Files {
		indexFile := index.files[i]
		baseName := filepath.Base(indexFile.file.Name())
		if baseName != filepath.Base(entry.Name) {
			return fmt.Errorf("unmatched file: %s", entry.Name)
		}

		crc, err := fileCrc(indexFile.file.Name())
		if err != nil {
			return err
		}

		if crc != entry.CRC {
			return fmt.Errorf("local file %s has an invalid CRC, according to the manifest", baseName)
		}
	}

	indexPath := filepath.Join(index.Path, ".index")
	info, err := os.Stat(indexPath)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("missing or invalid ldb index at %s", indexPath)
	}

	ldb, err := leveldb.OpenFile(indexPath, nil)
	if err != nil {
		return err
	}

	index.ldb = ldb
	index.count = m.Count
	return nil
}

func (index *Index) buildNewIndex() error {
	indexPath := filepath.Join(index.Path, ".index")
	err := os.RemoveAll(indexPath)
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

	manifest, err := index.buildManifest()
	if err != nil {
		return fmt.Errorf("error building manifest: %s", err)
	}

	manifestPath := filepath.Join(index.Path, ".manifest")
	log.Println("Writing manifest file to", manifestPath)
	err = writeManifest(manifestPath, manifest)
	if err != nil {
		return fmt.Errorf("error writing manifest: %s", err)
	}

	return nil
}

func (index *Index) buildManifest() (manifest, error) {
	m := manifest{
		Files: make([]manifestEntry, len(index.files)),
		Count: index.count,
	}

	for i, f := range index.files {
		crc, err := fileCrc(f.file.Name())
		if err != nil {
			return m, err
		}

		m.Files[i] = manifestEntry{
			Name: filepath.Base(f.file.Name()),
			CRC:  crc,
		}
	}

	return m, nil
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

	index.readLocks[fileId].Lock()
	defer index.readLocks[fileId].Unlock()

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
