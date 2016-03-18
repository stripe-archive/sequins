package blocks

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/colinmarc/cdb"
	"github.com/pborman/uuid"
	"github.com/spaolacci/murmur3"
)

var errBlockFull = errors.New("The block is full")

type blockWriter struct {
	minKey    []byte
	maxKey    []byte
	count     int
	partition int

	path      string
	id        string
	cdbWriter *cdb.Writer
}

func newBlock(storePath string, partition int) (*blockWriter, error) {
	id := uuid.New()
	name := fmt.Sprintf("block-%05d-%s.cdb", partition, id)

	path := filepath.Join(storePath, name)
	log.Println("Initializing block at", path)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// The CDB has function is too similar to the default hadoop partitioning
	// scheme (which is also the one we use to sort into blocks), and this can
	// result in weirdly distributed CDB files. So we use murmur3 to be safe.
	cdbWriter, err := cdb.NewWriter(f, murmur3.New32())
	if err != nil {
		return nil, err
	}

	bw := &blockWriter{
		partition: partition,
		path:      path,
		id:        id,
		cdbWriter: cdbWriter,
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

	err := bw.cdbWriter.Put(key, value)
	if err == cdb.ErrTooMuchData {
		return errBlockFull
	}

	return err
}

func (bw *blockWriter) save() (*Block, error) {
	cdb, err := bw.cdbWriter.Freeze()
	if err != nil {
		return nil, err
	}

	b := &Block{
		ID:        bw.id,
		Name:      filepath.Base(bw.path),
		Partition: bw.partition,
		Count:     bw.count,

		cdb:    cdb,
		minKey: bw.minKey,
		maxKey: bw.maxKey,
	}

	return b, nil
}
