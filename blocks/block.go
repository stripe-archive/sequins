package blocks

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bsm/go-sparkey"
)

// A block represents a chunk of data, all of the keys of which match a
// particular partition number. The data is partitioned using the same method
// Hadoop uses by default for shuffling data:
//
//  key.hashCode % partitions
//
// So we can easily have blocks line up with files in a dataset.
type Block struct {
	ID        string
	Name      string
	Partition int
	Count     int

	minKey        []byte
	maxKey        []byte
	sparkeyReader *sparkey.HashReader
	iterPool      iterPool
	lock          sync.RWMutex
}

// Creates a new Block from Sparkey files. On success, takes ownership of the
// log file and index file.
func newBlockFromSparkey(storePath string, logPath string, partition int) (*Block, error) {
	success := false

	// Copy the files into place
	name, id := newBlockName(partition)
	dstLog := filepath.Join(storePath, name)
	dstIdx := sparkey.HashFileName(dstLog)
	defer func() {
		if !success {
			os.Remove(dstLog)
			os.Remove(dstIdx)
		}
	}()
	err := os.Rename(logPath, dstLog)
	if err != nil {
		return nil, fmt.Errorf("importing Sparkey log: %s", err)
	}
	err = os.Rename(sparkey.HashFileName(logPath), dstIdx)
	if err != nil {
		return nil, fmt.Errorf("importing Sparkey index: %s", err)
	}

	// Create a block struct
	b := &Block{
		ID:        id,
		Name:      name,
		Partition: partition,
	}
	err = b.open(storePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			b.Delete()
		}
	}()

	// Read minKey and count
	b.Count, err = b.readCount()
	if err != nil {
		return nil, err
	}
	if b.Count > 0 {
		b.minKey, err = b.readMinKey()
		if err != nil {
			return nil, err
		}
	}

	success = true
	return b, nil
}

func loadBlock(storePath string, manifest BlockManifest) (*Block, error) {
	b := &Block{
		ID:        manifest.ID,
		Name:      manifest.Name,
		Partition: manifest.Partition,
		Count:     manifest.Count,

		minKey: manifest.MinKey,
		maxKey: manifest.MaxKey,
	}

	err := b.open(storePath)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Block) open(storePath string) error {
	logPath := filepath.Join(storePath, b.Name)
	reader, err := sparkey.Open(logPath)
	if err != nil {
		return fmt.Errorf("opening block: %s", err)
	}

	b.sparkeyReader = reader
	b.iterPool = newIterPool(reader)
	return nil
}

func (b *Block) Get(key []byte) (*Record, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	if b.minKey != nil && bytes.Compare(key, b.minKey) < 0 {
		return nil, nil
	} else if b.maxKey != nil && bytes.Compare(key, b.maxKey) > 0 {
		return nil, nil
	}

	return b.get(key)
}

func (b *Block) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.sparkeyReader.Close()
}

// Delete this Block
func (b *Block) Delete() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.sparkeyReader.Close()
	err := os.Remove(b.sparkeyReader.Name())
	rerr := os.Remove(b.sparkeyReader.LogName())
	if rerr == nil {
		rerr = err
	}
	return rerr
}

func (b *Block) manifest() BlockManifest {
	return BlockManifest{
		ID:        b.ID,
		Name:      b.Name,
		Partition: b.Partition,
		Count:     b.Count,
		MinKey:    b.minKey,
		MaxKey:    b.maxKey,
	}
}

func (b *Block) readMinKey() ([]byte, error) {
	iter, err := b.sparkeyReader.Log().Iterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	err = iter.Next()
	if err != nil {
		return nil, err
	}
	if iter.State() != sparkey.ITERATOR_ACTIVE {
		return nil, err
	}
	return iter.Key()
}

const putsOffset = 16

func (b *Block) readCount() (int, error) {
	path := b.sparkeyReader.LogName()
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	_, err = file.Seek(putsOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	var buf [8]byte
	_, err = io.ReadFull(file, buf[:])
	if err != nil {
		return 0, err
	}

	count := binary.LittleEndian.Uint64(buf[:])
	return int(count), nil
}
