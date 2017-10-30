package blocks

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/bsm/go-sparkey"
	"github.com/pborman/uuid"
)

type blockWriter struct {
	minKey    []byte
	maxKey    []byte
	count     int
	partition int

	path          string
	id            string
	sparkeyWriter *sparkey.LogWriter

	addLock sync.Mutex
}

func newBlockName(partition int) (name, id string) {
	id = uuid.New()
	name = fmt.Sprintf("block-%05d-%s.spl", partition, id)
	return
}

func newBlock(storePath string, partition int, compression Compression, blockSize int) (*blockWriter, error) {
	name, id := newBlockName(partition)
	path := filepath.Join(storePath, name)
	log.Println("Initializing block at", path)

	c := sparkey.COMPRESSION_NONE
	if compression == SnappyCompression {
		c = sparkey.COMPRESSION_SNAPPY
	}
	options := &sparkey.Options{Compression: c, CompressionBlockSize: blockSize}
	sparkeyWriter, err := sparkey.CreateLogWriter(path, options)
	if err != nil {
		return nil, fmt.Errorf("initializing block %s: %s", path, err)
	}

	bw := &blockWriter{
		partition:     partition,
		path:          path,
		id:            id,
		sparkeyWriter: sparkeyWriter,
	}

	return bw, nil
}

func (bw *blockWriter) add(key, value []byte) error {
	bw.addLock.Lock()
	defer bw.addLock.Unlock()

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

	return bw.sparkeyWriter.Put(key, value)
}

func (bw *blockWriter) save() (*Block, error) {
	err := bw.sparkeyWriter.WriteHashFile(0)
	if err != nil {
		return nil, err
	}

	err = bw.sparkeyWriter.Close()
	if err != nil {
		return nil, err
	}

	reader, err := sparkey.Open(bw.path)
	if err != nil {
		return nil, fmt.Errorf("opening block: %s", err)
	}

	b := &Block{
		ID:        bw.id,
		Name:      filepath.Base(bw.path),
		Partition: bw.partition,
		Count:     bw.count,

		minKey:        bw.minKey,
		maxKey:        bw.maxKey,
		sparkeyReader: reader,
		iterPool:      newIterPool(reader),
	}

	return b, nil
}

func (bw *blockWriter) close() {
	bw.sparkeyWriter.Close()
}

func (bw *blockWriter) delete() {
	os.Remove(bw.path)
}
