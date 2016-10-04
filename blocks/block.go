package blocks

import (
	"bytes"
	"fmt"
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
	sync.RWMutex
}

func loadBlock(storePath string, manifest blockManifest) (*Block, error) {
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
	b.RLock()
	defer b.RUnlock()

	if b.minKey != nil && bytes.Compare(key, b.minKey) < 0 {
		return nil, nil
	} else if b.maxKey != nil && bytes.Compare(key, b.maxKey) > 0 {
		return nil, nil
	}

	return b.get(key)
}

func (b *Block) Close() {
	b.Lock()
	defer b.Unlock()

	b.sparkeyReader.Close()
}

func (b *Block) manifest() blockManifest {
	return blockManifest{
		ID:        b.ID,
		Name:      b.Name,
		Partition: b.Partition,
		Count:     b.Count,
		MinKey:    b.minKey,
		MaxKey:    b.maxKey,
	}
}
