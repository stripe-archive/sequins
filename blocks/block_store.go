package blocks

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/colinmarc/sequencefile"
)

var ErrNoManifest = errors.New("no manifest file found")
var ErrPartitionNotFound = errors.New("the block store doesn't have the correct partition for the key")
var ErrMissingPartitions = errors.New("existing block store missing partitions")
var ErrWrongPartition = errors.New("the file is cleanly partitioned, but doesn't contain a partition we want")

type Compression string

const SnappyCompression Compression = "snappy"
const NoCompression Compression = "none"

// A BlockStore stores ingested key/value data in discrete blocks, each stored
// as a separate CDB file. The blocks are arranged and sorted in a way that
// takes advantage of the way that the output of hadoop jobs are laid out.
type BlockStore struct {
	path               string
	compression        Compression
	blockSize          int
	numPartitions      int
	selectedPartitions map[int]bool
	count              int

	newBlocks map[int]*blockWriter
	Blocks    []*Block
	BlockMap  map[int][]*Block

	blockMapLock sync.RWMutex
}

func New(path string, numPartitions int, selectedPartitions map[int]bool, compression Compression, blockSize int) *BlockStore {
	return &BlockStore{
		path:               path,
		compression:        compression,
		blockSize:          blockSize,
		numPartitions:      numPartitions,
		selectedPartitions: selectedPartitions,

		newBlocks: make(map[int]*blockWriter),
		Blocks:    make([]*Block, 0),
		BlockMap:  make(map[int][]*Block),
	}
}

func NewFromManifest(path string, selectedPartitions map[int]bool) (*BlockStore, error) {
	manifestPath := filepath.Join(path, ".manifest")
	manifest, err := readManifest(manifestPath)
	if os.IsNotExist(err) {
		return nil, ErrNoManifest
	} else if err != nil {
		return nil, err
	}

	// TODO: don't throw away everything if we just need a few more partitions
	// TODO: GC blocks that aren't relevant
	if manifest.SelectedPartitions != nil {
		if selectedPartitions == nil {
			return nil, ErrMissingPartitions
		}

		// Validate that all the partitions we requested are there.
		manifestPartitions := make(map[int]bool)
		for _, partition := range manifest.SelectedPartitions {
			manifestPartitions[partition] = true
		}

		for partition := range selectedPartitions {
			if _, ok := manifestPartitions[partition]; !ok {
				return nil, ErrMissingPartitions
			}
		}
	}

	store := New(path, manifest.NumPartitions, selectedPartitions,
		manifest.Compression, manifest.BlockSize)
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()

	for _, blockManifest := range manifest.Blocks {
		block, err := loadBlock(path, blockManifest)
		if err != nil {
			return nil, err
		}

		partition := blockManifest.Partition
		store.Blocks = append(store.Blocks, block)
		store.BlockMap[partition] = append(store.BlockMap[partition], block)
		store.count += blockManifest.Count
	}

	return store, nil
}

// AddFile ingests the key/value pairs from the given sequencefile, writing
// them out to at least one block. If the data is not partitioned cleanly, it
// will sort it into blocks as it reads.
func (store *BlockStore) AddFile(reader *sequencefile.Reader, throttle time.Duration) error {
	canAssumePartition := true
	assumedPartition := -1
	assumedFor := 0

	for reader.Scan() {
		if throttle != 0 {
			time.Sleep(throttle)
		}

		key, value, err := store.unwrapKeyValue(reader)
		if err != nil {
			return err
		}

		partition, alternatePartition := KeyPartition(string(key), store.numPartitions)

		// If we see the same partition for the first 5000 keys, it's safe to assume
		// that this file only contains that partition. This is often the case if
		// the data has been shuffled by the output key in a way that aligns with
		// our own partitioning scheme.
		if canAssumePartition {
			if assumedPartition == -1 {
				assumedPartition = partition
			} else if partition != assumedPartition {
				if alternatePartition == assumedPartition {
					partition = alternatePartition
				} else {
					canAssumePartition = false
				}
			} else {
				assumedFor += 1
			}
		}

		// Once we see 5000 keys from the same partition, it's safe to assume
		// the whole file is like that, and we can skip the rest.
		if store.selectedPartitions != nil && !store.selectedPartitions[partition] {
			if canAssumePartition && assumedFor > 5000 {
				return ErrWrongPartition
			}
			continue
		}

		// Grab the open block for this partition.
		block, ok := store.newBlocks[partition]
		if !ok {
			block, err = newBlock(store.path, partition, store.compression, store.blockSize)
			if err != nil {
				return err
			}

			store.newBlocks[partition] = block
		}

		// Write the key/value pair. If the block is full, save it
		// and start a new one.
		err = block.add(key, value)
		if err != nil {
			return err
		}
	}

	if reader.Err() != nil {
		return reader.Err()
	}

	return nil
}

// unwrapKeyValue correctly prepares a key and value for storage, depending on
// how they are serialized in the original file; namely, BytesWritable and Text keys and
// values are unwrapped.
func (store *BlockStore) unwrapKeyValue(reader *sequencefile.Reader) (key []byte, value []byte, err error) {
	// sequencefile.Text or sequencefile.BytesWritable can panic if the data is corrupted.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sequencefile: record deserialization failed: %s", r)
		}
	}()

	switch reader.Header.KeyClassName {
	case sequencefile.BytesWritableClassName:
		key = sequencefile.BytesWritable(reader.Key())
	case sequencefile.TextClassName:
		key = []byte(sequencefile.Text(reader.Key()))
	default:
		key = reader.Key()
	}

	switch reader.Header.ValueClassName {
	case sequencefile.BytesWritableClassName:
		value = sequencefile.BytesWritable(reader.Value())
	case sequencefile.TextClassName:
		value = []byte(sequencefile.Text(reader.Value()))
	default:
		value = reader.Value()
	}

	return
}

func (store *BlockStore) Save() error {
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()

	// Flush any buffered blocks.
	for partition, block := range store.newBlocks {
		savedBlock, err := block.save()
		if err != nil {
			return err
		}

		store.Blocks = append(store.Blocks, savedBlock)
		store.BlockMap[partition] = append(store.BlockMap[partition], savedBlock)
		store.count += savedBlock.Count
	}

	store.newBlocks = make(map[int]*blockWriter)

	// Save the manifest.
	var partitions []int
	if store.selectedPartitions != nil {
		partitions = make([]int, 0, len(store.selectedPartitions))
		for partition := range store.selectedPartitions {
			partitions = append(partitions, partition)
		}
	}

	manifest := manifest{
		Version:            manifestVersion,
		Blocks:             make([]blockManifest, len(store.Blocks)),
		NumPartitions:      store.numPartitions,
		SelectedPartitions: partitions,
	}

	for i, block := range store.Blocks {
		blockManifest := block.manifest()
		manifest.Blocks[i] = blockManifest
	}

	return writeManifest(filepath.Join(store.path, ".manifest"), manifest)
}

// Get returns the value for a given key. It returns ErrPartitionNotFound if
// the partition requested is not available locally.
func (store *BlockStore) Get(key string) (*Record, error) {
	store.blockMapLock.RLock()
	defer store.blockMapLock.RUnlock()

	partition, alternatePartition := KeyPartition(key, store.numPartitions)
	if !store.hasPartition(partition) && !store.hasPartition(alternatePartition) {
		return nil, ErrPartitionNotFound
	}

	// There can be multiple blocks for each partition - in that case, we need to
	// check each one sequentially.
	for _, block := range store.BlockMap[partition] {
		res, err := block.Get([]byte(key))
		if err != nil {
			return nil, err
		} else if res != nil {
			return res, nil
		}
	}

	// See the comment for alternatePathologicalKeyPartition.
	if alternatePartition != partition {
		for _, block := range store.BlockMap[alternatePartition] {
			res, err := block.Get([]byte(key))
			if err != nil {
				return nil, err
			} else if res != nil {
				return res, nil
			}
		}
	}

	return nil, nil
}

func (store *BlockStore) hasPartition(partition int) bool {
	return store.selectedPartitions == nil || store.selectedPartitions[partition]
}

// Count returns the total number of records stored.
func (store *BlockStore) Count() int {
	store.blockMapLock.RLock()
	defer store.blockMapLock.RUnlock()

	return store.count
}

// Close closes the BlockStore, and any files it has open.
func (store *BlockStore) Close() {
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()

	for _, block := range store.Blocks {
		block.Close()
	}

	for _, newBlock := range store.newBlocks {
		newBlock.close()
	}
}

// Delete removes any local data the BlockStore has stored.
func (store *BlockStore) Delete() error {
	return os.RemoveAll(store.path)
}
