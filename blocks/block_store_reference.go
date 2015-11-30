package blocks

import (
	"sync"
)

// BlockStoreReference can be used to safely manage references to BlockStore
// objects without needing to exclude requests to switch between them.
//
// It works by counting held references to a given BlockStore. When switching
// (usually when we upgrade versions), the "current" BlockStore is immediately
// and atomically replaced, but the old one is kept around until the reference
// count goes to zero.
type BlockStoreReference struct {
	version string
	rwMutex    sync.RWMutex
	blockStore *BlockStore
	refcounts  map[*BlockStore]*sync.WaitGroup
}

// Get atomically acquires a reference to an index, and increments the refcount
// the index will not be closed until the reference is released.
func (bsr *BlockStoreReference) Get() (*BlockStore, string) {
	bsr.rwMutex.RLock()
	defer bsr.rwMutex.RUnlock()

	blockStore := bsr.blockStore
	if blockStore != nil {
		bsr.refcounts[blockStore].Add(1)
	}

	return blockStore, bsr.version
}

// Release decrements the refcount for an index
// Sequins may close the index after this point, if no other requests
// hold a reference to it.
func (bsr *BlockStoreReference) Release(blockStore *BlockStore) {
	if blockStore != nil {
		bsr.refcounts[blockStore].Done()
	}
}

// Replace replaces the current BlockStore with a new one, waits for the old one
// to no longer be in use, then returns. At that point, no more references are
// held for the BlockStore, and it should be safe to close.
func (bsr *BlockStoreReference) Replace(new *BlockStore, version string) (old *BlockStore) {
	if (bsr.refcounts == nil) {
		bsr.refcounts = make(map[*BlockStore]*sync.WaitGroup)
	}


	bsr.rwMutex.Lock()

	old = bsr.blockStore
	bsr.blockStore = new
	bsr.refcounts[new] = &sync.WaitGroup{}
	bsr.version = version

	bsr.rwMutex.Unlock()

	if old != nil {
		bsr.refcounts[old].Wait()
	}

	return old
}
