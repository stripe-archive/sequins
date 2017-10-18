package blocks

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var ErrNoManifest = errors.New("no manifest file found")

type Compression string

const SnappyCompression Compression = "snappy"
const NoCompression Compression = "none"

// A BlockStore stores ingested key/value data in discrete blocks, each stored
// as a separate CDB file. The blocks are arranged and sorted in a way that
// takes advantage of the way that the output of hadoop jobs are laid out.
type BlockStore struct {
	path          string
	compression   Compression
	blockSize     int
	numPartitions int

	newBlocks        map[int]*blockWriter
	newSparkeyBlocks map[int][]*Block
	Blocks           []*Block
	BlockMap         map[int][]*Block

	blockMapLock sync.RWMutex
}

func New(path string, numPartitions int, compression Compression, blockSize int) *BlockStore {
	return &BlockStore{
		path:          path,
		compression:   compression,
		blockSize:     blockSize,
		numPartitions: numPartitions,

		newBlocks:        make(map[int]*blockWriter),
		newSparkeyBlocks: make(map[int][]*Block),

		Blocks:   make([]*Block, 0),
		BlockMap: make(map[int][]*Block),
	}
}

// NewFromManifest loads a block store from a directory with a manifest, and
// returns it, the parsed manifest, and any error encountered while loading.
func NewFromManifest(path string) (*BlockStore, Manifest, error) {
	manifestPath := filepath.Join(path, ".manifest")
	manifest, err := readManifest(manifestPath)
	if os.IsNotExist(err) {
		return nil, manifest, ErrNoManifest
	} else if err != nil {
		return nil, manifest, err
	}

	store := New(path, manifest.NumPartitions, manifest.Compression, manifest.BlockSize)
	for _, blockManifest := range manifest.Blocks {
		block, err := loadBlock(path, blockManifest)
		if err != nil {
			return nil, Manifest{}, err
		}

		partition := blockManifest.Partition
		store.Blocks = append(store.Blocks, block)
		store.BlockMap[partition] = append(store.BlockMap[partition], block)
	}

	return store, manifest, nil
}

// Get the block for new data
func (store *BlockStore) newBlock(partition int) (*blockWriter, error) {
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()

	block, found := store.newBlocks[partition]
	if found {
		return block, nil
	}

	block, err := newBlock(store.path, partition, store.compression, store.blockSize)
	if err != nil {
		return nil, err
	}
	store.newBlocks[partition] = block
	return block, nil
}

// Add adds a single key/value pair to the block store.
func (store *BlockStore) Add(key, value []byte) error {
	partition, _ := KeyPartition(key, store.numPartitions)
	block, err := store.newBlock(partition)
	if err != nil {
		return err
	}

	return block.add(key, value)
}

// Save saves flushes any newly created blocks, and writes a manifest file to
// the directory.
func (store *BlockStore) Save(selectedPartitions map[int]bool) error {
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
	}
	store.newBlocks = make(map[int]*blockWriter)

	for _, blocks := range store.newSparkeyBlocks {
		store.saveSparkeyBlocks(blocks)
	}
	store.newSparkeyBlocks = make(map[int][]*Block)

	// Save the manifest.
	var partitions []int
	partitions = make([]int, 0, len(selectedPartitions))
	for partition := range selectedPartitions {
		partitions = append(partitions, partition)
	}

	manifest := Manifest{
		Version:            manifestVersion,
		Blocks:             make([]BlockManifest, len(store.Blocks)),
		NumPartitions:      store.numPartitions,
		SelectedPartitions: partitions,
	}

	for i, block := range store.Blocks {
		blockManifest := block.manifest()
		manifest.Blocks[i] = blockManifest
	}

	return writeManifest(filepath.Join(store.path, ".manifest"), manifest)
}

// Revert removes any unflushed blocks, and resets the state to when the
// manifest was last saved. If the block store was never saved, then it reverts
// to being empty.
func (store *BlockStore) Revert() {
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()

	for _, block := range store.newBlocks {
		block.close()
		block.delete()
	}
	store.newBlocks = make(map[int]*blockWriter)

	for _, blocks := range store.newSparkeyBlocks {
		for _, block := range blocks {
			block.Delete()
		}
	}
	store.newSparkeyBlocks = make(map[int][]*Block)

	return
}

// Get returns the value for a given key.
func (store *BlockStore) Get(key string) (*Record, error) {
	store.blockMapLock.RLock()
	defer store.blockMapLock.RUnlock()

	partition, alternatePartition := KeyPartition([]byte(key), store.numPartitions)
	if store.BlockMap[partition] == nil && store.BlockMap[alternatePartition] == nil {
		return nil, nil
	}

	keyBytes := []byte(key)
	res, err := store.get(keyBytes, partition)
	if err != nil {
		return nil, err
	}

	// See the comment for alternatePathologicalKeyPartition.
	if res == nil && err == nil && alternatePartition != partition {
		res, err = store.get(keyBytes, alternatePartition)
	}

	return res, err
}

func (store *BlockStore) get(key []byte, partition int) (*Record, error) {

	// There can be multiple blocks for each partition - in that case, we need to
	// check each one sequentially.
	for _, block := range store.BlockMap[partition] {
		res, err := block.Get(key)
		if err != nil {
			return nil, err
		} else if res != nil {
			return res, nil
		}
	}

	return nil, nil
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
	for _, blocks := range store.newSparkeyBlocks {
		for _, block := range blocks {
			block.Close()
		}
	}
}

// Delete removes any local data the BlockStore has stored.
func (store *BlockStore) Delete() error {
	return os.RemoveAll(store.path)
}

type blocks []*Block

func (bs blocks) Len() int {
	return len(bs)
}
func (bs blocks) Swap(i, j int) {
	bs[i], bs[j] = bs[j], bs[i]
}
func (bs blocks) Less(i, j int) bool {
	return bytes.Compare(bs[i].minKey, bs[j].minKey) < 0
}

func (store *BlockStore) saveSparkeyBlocks(bs blocks) {
	// The blockMapLock is already held.
	sort.Sort(bs)
	for i, block := range bs {
		if i > 0 {
			bs[i-1].maxKey = block.minKey
		}
		partition := block.Partition
		store.Blocks = append(store.Blocks, block)
		store.BlockMap[partition] = append(store.BlockMap[partition], block)
	}
}

// Adds an externally-created Sparkey log file and index file to this store.
// The files will be moved into place, and will no longer exist in the original location.
func (store *BlockStore) AddSparkeyBlock(logPath string, partition int) error {
	block, err := newBlockFromSparkey(store.path, logPath, partition)
	if err != nil {
		return err
	}

	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()
	store.newSparkeyBlocks[block.Partition] = append(store.newSparkeyBlocks[block.Partition], block)
	return nil
}
