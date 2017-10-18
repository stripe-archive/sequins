package blocks

import (
	"bytes"
	"os"
	"sort"
)

type rawBlocks []*Block

func (obs rawBlocks) Len() int {
	return len(obs)
}
func (obs rawBlocks) Swap(i, j int) {
	obs[i], obs[j] = obs[j], obs[i]
}
func (obs rawBlocks) Less(i, j int) bool {
	return bytes.Compare(obs[i].minKey, obs[j].minKey) < 0
}

func (store *BlockStore) saveRawBlocks(blocks rawBlocks) {
	// We assume the blockMapLock is already held.
	sort.Sort(blocks)
	for i, block := range blocks {
		if i > 0 {
			blocks[i-1].maxKey = block.minKey
		}
		partition := block.Partition
		store.Blocks = append(store.Blocks, block)
		store.BlockMap[partition] = append(store.BlockMap[partition], block)
	}
}

func (store *BlockStore) addRawBlock(block *Block) {
	store.blockMapLock.Lock()
	defer store.blockMapLock.Unlock()
	store.newRawBlocks[block.Partition] = append(store.newRawBlocks[block.Partition], block)
}

type RawBlockAdder struct {
	store     *BlockStore
	partition int
	id        string
	finished  bool

	Log  *os.File
	Hash *os.File
}

func (store *BlockStore) AddRawBlock(partition int) (adder *RawBlockAdder, err error) {
	adder = &RawBlockAdder{store: store, partition: partition}
	defer func() {
		if err != nil {
			adder.Close()
		}
	}()

	path, id := newBlockPath(store.path, partition)
	adder.id = id

	adder.Log, err = os.Create(path)
	if err != nil {
		return
	}
	adder.Hash, err = os.Create(path)
	if err != nil {
		return
	}
	return
}

func (adder *RawBlockAdder) Close() error {
	rerr := adder.Log.Close()
	err := adder.Hash.Close()
	if rerr == nil && err != nil {
		rerr = err
	}
	if !adder.finished {
		err = os.Remove(adder.Log.Name())
		if rerr == nil && err != nil {
			rerr = err
		}
		err = os.Remove(adder.Hash.Name())
		if rerr == nil && err != nil {
			rerr = err
		}
	}
	return err
}

func (adder *RawBlockAdder) Finish() error {
	err := adder.Log.Close()
	if err != nil {
		return err
	}
	err = adder.Hash.Close()
	if err != nil {
		return err
	}

	block, err := loadBlockFromRaw(adder.Log.Name(), adder.id, adder.partition)
	if err != nil {
		return err
	}
	adder.store.addRawBlock(block)
	adder.finished = true
	return nil
}
