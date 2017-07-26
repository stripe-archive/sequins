package blocks

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/pborman/uuid"
)

type blockWriter struct {
	minKey    []byte
	maxKey    []byte
	count     int
	partition int

	path string
	id   string
	db   *bolt.DB
}

func newBlock(storePath string, partition int, compression Compression, blockSize int) (*blockWriter, error) {
	id := uuid.New()
	name := fmt.Sprintf("block-%05d-%s.spl", partition, id)

	path := filepath.Join(storePath, name)
	log.Println("Initializing block at", path)

	// TODO: Compression
	/*
		c := sparkey.COMPRESSION_NONE
		if compression == SnappyCompression {
			c = sparkey.COMPRESSION_SNAPPY
		}
		options := &sparkey.Options{Compression: c, CompressionBlockSize: blockSize}
		sparkeyWriter, err := sparkey.CreateLogWriter(path, options)
	*/
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("initializing block %s: %s", path, err)
	}

	bw := &blockWriter{
		partition: partition,
		path:      path,
		id:        id,
		db:        db,
	}

	return bw, nil
}

func (bw *blockWriter) add(key, value []byte) error {
	// Update the count.
	bw.count++

	// Update the minimum and maximum keys seen.
	if bw.maxKey == nil || bytes.Compare(key, bw.maxKey) > 0 {
		bw.maxKey = make([]byte, len(key))
		copy(bw.maxKey, key)
	}

	if bw.minKey == nil || bytes.Compare(key, bw.minKey) < 0 {
		bw.minKey = make([]byte, len(key))
		copy(bw.minKey, key)
	}

	return bw.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(filepath.Base(bw.path)))
		if err != nil {
			return err
		}
		err = bucket.Put(key, value)
		return err

	})
}

func (bw *blockWriter) save() (*Block, error) {
	err := bw.db.Close()

	db, err := bolt.Open(bw.path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("opening block: %s", err)
	}

	b := &Block{
		ID:        bw.id,
		Name:      filepath.Base(bw.path),
		Partition: bw.partition,
		Count:     bw.count,

		minKey: bw.minKey,
		maxKey: bw.maxKey,
		db:     db,
	}

	return b, nil
}

func (bw *blockWriter) close() {
	bw.db.Close()
}

func (bw *blockWriter) delete() {
	os.Remove(bw.path)
}
