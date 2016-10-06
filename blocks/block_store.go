package blocks

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

var ErrNoManifest = errors.New("no manifest file found")
var ErrPartitionNotFound = errors.New("the block store doesn't have the correct partition for the key")
var ErrMissingPartitions = errors.New("existing block store missing partitions")

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
func (store *BlockStore) Add(key, value []byte) error {
	partition, _ := KeyPartition(key, store.numPartitions)

	block, ok := store.newBlocks[partition]
	var err error
	if !ok {
		block, err = newBlock(store.path, partition, store.compression, store.blockSize)
		if err != nil {
			return err
		}

		store.newBlocks[partition] = block
	}

	err = block.add(key, value)
	if err != nil {
		return err
	}

	return nil
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

	partition, alternatePartition := KeyPartition([]byte(key), store.numPartitions)
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
