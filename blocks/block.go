package blocks

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"encoding/binary"

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

func loadBlockFromRaw(path string, id string, partition int) (b *Block, err error) {
	b = &Block{
		ID:        id,
		Name:      filepath.Base(path),
		Partition: partition,
	}

	b.sparkeyReader, err = sparkey.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening block: %s", err)
	}
	defer func() {
		if err != nil {
			b.sparkeyReader.Close()
		}
	}()

	// Read minKey and count
	b.Count, err = b.readCount()
	if err != nil {
		return
	}
	b.minKey, err = b.readMinKey()
	if err != nil {
		return
	}
	return
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

	reader, err := sparkey.Open(filepath.Join(storePath, b.Name))
	if err != nil {
		return nil, fmt.Errorf("opening block: %s", err)
	}

	b.sparkeyReader = reader
	b.iterPool = newIterPool(reader)
	return b, nil
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
		return []byte{}, err
	}
	defer iter.Close()
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
