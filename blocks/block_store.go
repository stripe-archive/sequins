package blocks

import (
	"errors"
	"math"
	"path/filepath"
	"sync"
	"os"

	"github.com/stripe/sequins/sequencefile"
)

var ErrNoManifest = errors.New("No manifest could be found")
var ErrNotFound = errors.New("That key doesn't exist.")

// A BlockStore stores ingested key/value data in discrete blocks, each stored
// as a separate CDB file. The blocks are arranged and sorted in a way that
// takes advantage of the way that the output of hadoop jobs are laid out.
type BlockStore struct {
	path               string
	numPartitions      int
	selectedPartitions map[int]bool

	Blocks       []*Block
	BlockMap     map[int][]*Block
	blockMapLock sync.RWMutex
}

func New(path string, numPartitions int, selectedPartitions map[int]bool) *BlockStore {
	return &BlockStore{
		path:               path,
		numPartitions:      numPartitions,
		selectedPartitions: selectedPartitions,

		Blocks:   make([]*Block, 0),
		BlockMap: make(map[int][]*Block),
	}
}

func NewFromManifest(path string) (*BlockStore, error) {
	manifestPath := filepath.Join(path, ".manifest")
	manifest, err := readManifest(manifestPath)
	if os.IsNotExist(err) {
		return nil, ErrNoManifest
	} else if err != nil {
		return nil, err
	}

	store := New(path, manifest.NumPartitions, nil)
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
	}

	return store, nil
}

// TODO: add files in parallel. Safe async add/remove partition operations

// AddFile ingests the key/value pairs in the given sequencefile, writing it
// out to at least one block. If the data is not partitioned cleanly, it will
// sort it into blocks as it reads.
func (store *BlockStore) AddFile(reader *sequencefile.Reader) error {
	newBlocks := make(map[int]*blockWriter)
	savedBlocks := make(map[int][]*Block)

	for reader.Scan() {
		// TODO: we need to consider pathological cases so we don't end up
		// with a block with ~one key
		partition := partition(reader.Key(), store.numPartitions)
		if store.selectedPartitions != nil && store.selectedPartitions[partition] {

		}

		block, ok := newBlocks[partition]
		var err error
		if !ok {
			block, err = newBlock(store.path, partition)
			if err != nil {
				return err
			}

			newBlocks[partition] = block
		}

		err = block.add(reader.Key(), reader.Value())
		if err == errBlockFull {
			savedBlock, err := block.save()
			if err != nil {
				return err
			}

			savedBlocks[partition] = append(savedBlocks[partition], savedBlock)

			// Create a new block, and write to that.
			block, err = newBlock(store.path, partition)
			if err != nil {
				return err
			}

			newBlocks[partition] = block
			err = block.add(reader.Key(), reader.Value())
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}

	if reader.Err() != nil {
		return reader.Err()
	}

	// Flush all the buffering blocks.
	for partition, block := range newBlocks {
		savedBlock, err := block.save()
		if err != nil {
			return err
		}

		savedBlocks[partition] = append(savedBlocks[partition], savedBlock)
	}

  // Update the block map with the new blocks.
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()

	for partition, blocks := range savedBlocks {
		for _, block := range blocks {
			store.Blocks = append(store.Blocks, block)
			store.BlockMap[partition] = append(store.BlockMap[partition], block)
		}
	}

	return nil
}

func (store *BlockStore) SaveManifest() error {
	store.blockMapLock.RLock()
	defer store.blockMapLock.RUnlock()

	manifest := manifest{
		Version: manifestVersion,
		Blocks:  make([]blockManifest, len(store.Blocks)),
		NumPartitions: store.numPartitions,
	}

	for i, block := range store.Blocks {
		blockManifest := block.manifest()
		manifest.Blocks[i] = blockManifest
	}

	return writeManifest(filepath.Join(store.path, ".manifest"), manifest)
}

// Get returns the value for a given key.
func (store *BlockStore) Get(key string) ([]byte, error) {
	keyBytes := []byte(key)
	store.blockMapLock.RLock()
	defer store.blockMapLock.RUnlock()

	// There can be multiple blocks for each partition - in that case, we need to
	// check each one sequentially.
	partition := partition(keyBytes, store.numPartitions)
	for _, block := range store.BlockMap[partition] {
		res, err := block.Get(keyBytes)
		if err != nil {
			return nil, err
		} else if res != nil {
			return res, nil
		}
	}

	return nil, ErrNotFound
}

// Close closes the BlockStore, and any files it has open.
func (store *BlockStore) Close() {
	for _, block := range store.Blocks {
		block.Close()
	}
}

// hashCode implements java.lang.String#hashCode.
func hashCode(b []byte) int32 {
	var v int32
	for _, b := range b {
		v = (v * 31) + int32(b)
	}

	return v
}

func partition(key []byte, totalPartitions int) int {
	return int(hashCode(key)&math.MaxInt32) % totalPartitions
}

// isPathologicalHashCode filters out keys where the partition returned by the
// java.lang.String#hashCode would be consistent with other keys, but the
// partition returned by cascading.tuple.Tuple would not. I'm going to explain
// what that means, but first, consider how much you value your sanity.
//
// Still here? Alright.
//
// While "key".hashCode % numPartitions is the default partitioning for
// vanilla hadoop jobs, many people (including yours truly) use scalding,
// which in turn uses cascading. Cascading uses almost exactly the same
// method to partition (pre 2.7, anyway), but with a very slight change.
// The key is wrapped in a cascading.tuple.Tuple, and the hashCode of that
// Tuple is used instead of the hashCode of the key itself. In other words,
// instead of:
//
//     (key.hashCode & Integer.MAX_VALUE) % numPartitions
//
// It's:
//
//     (new Tuple(key).hashCode & Integer.MAX_VALUE) % numPartitions
//
// The Tuple#hashCode implementation combines the hashCodes of the
// constituent parts using the same algorithm as the regular string
// hashCode in a loop, ie:
//
//     (31 * h) + item.hashCode
//
// For single-value tuples, this should be identical to just the hashCode of
// the first part, right? Wrong. It's almost identical. It's exactly 31
// higher, because h starts at 1 instead of 0.
//
// For the vast majority of values, using the string hashCode intsead of the
// Tuple hashCode works fine; the partition numbers are different, but
// consistently so. But this breaks down when the hashCode of the string is
// within 31 of the max int32 value, or within 31 of 0, because we wrap before
// doing the mod. One inconsistent partition number could cause us to assume a
// whole file isn't actually partitioned, or, worse, ignore a key that's
// actually in the dataset but that we skipped over while building a sparse
// index.
//
// Luckily, we can test for these pathological values, and just pretend those
// keys don't exist. We have to also then give them a pass when testing against
// our partitioning scheme.
func isPathologicalHashCode(hc int32) bool {
	return (hc > (math.MaxInt32-32) || (hc > -32 && hc < 0))
}
