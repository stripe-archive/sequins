package blocks

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"

	"github.com/colinmarc/cdb"
	"github.com/spaolacci/murmur3"
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

	cdb    *cdb.CDB
	minKey []byte
	maxKey []byte

	readLock sync.Mutex
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

	f, err := os.Open(filepath.Join(storePath, b.Name))
	if err != nil {
		return nil, err
	}

	cdb, err := cdb.New(f, murmur3.New32())
	if err != nil {
		return nil, err
	}

	b.cdb = cdb
	return b, nil
}

func (b *Block) Get(key []byte) ([]byte, error) {
	b.readLock.Lock()
	defer b.readLock.Unlock()

	if bytes.Compare(key, b.minKey) < 0 {
		return nil, nil
	} else if bytes.Compare(key, b.maxKey) > 0 {
		return nil, nil
	}

	// TODO: bloom filter

	return b.cdb.Get(key)
}

func (b *Block) Close() error {
	return b.cdb.Close()
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

