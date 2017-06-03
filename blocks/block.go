package blocks

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
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
	Compression Compression

	minKey []byte
	maxKey []byte
	db     *bolt.DB
	lock   sync.RWMutex
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

	db, err := bolt.Open(filepath.Join(storePath, b.Name), 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("opening block: %s", err)
	}

	b.db = db
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
	b.db.Close()
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
