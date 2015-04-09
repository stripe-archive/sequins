package index

// helpers to atomically switch from using one index to another

import (
	"sync"
)

// IndexReference can be used to safely acquire, release, and upgrade indices
type IndexReference struct {
	rwMutex sync.RWMutex
	index   *Index
}

// Get atomically acquires a reference to an index, and increments the refcount
// the index will not be closed until the reference is released.
func (im *IndexReference) Get() *Index {
	im.rwMutex.RLock()
	defer im.rwMutex.RUnlock()
	index := im.index
	if index != nil {
		index.refcount.Add(1)
	}

	return index
}

// Release decrements the refcount for an index
// Sequins may close the index after this point, if no other requests
// hold a reference to it.
func (im *IndexReference) Release(index *Index) {
	if index != nil {
		index.refcount.Done()
	}
}

// UnsafeGet returns an index reference without doing any kind of locking or refcounts.
// This is only safe to use when you only care about the index object in memory,
// not the underlying index on disk that it represents. For example, you can
// safely use this reference to find the version of the index.
func (im *IndexReference) UnsafeGet() *Index {
	return im.index
}

// Replace replaces the current index with a new one, waits for the old one to no longer
// be in use, then returns.
// At that point, no more requests are using the index, and it should be safe to close / destroy
func (im *IndexReference) Replace(newIndex *Index) (oldIndex *Index) {
	im.rwMutex.Lock()
	oldIndex = im.index
	im.index = newIndex
	im.rwMutex.Unlock()

	if oldIndex != nil {
		oldIndex.refcount.Wait()
	}

	return oldIndex
}
