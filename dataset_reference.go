package main

import (
	"sync"
)

// datasetReference can be used to safely manage references to a dataset
// objects without needing to exclude requests to switch between them.
//
// It works by counting held references to a given dataset. When switching
// (usually when we upgrade versions), the "current" dataset is immediately
// and atomically replaced, but the old one is kept around until the reference
// count goes to zero.
type datasetReference struct {
	lock      sync.RWMutex
	ds        *dataset
	refcounts map[*dataset]*sync.WaitGroup
}

// Get atomically acquires a reference to an index, and increments the refcount
// the index will not be closed until the reference is released.
func (bsr *datasetReference) get() *dataset {
	bsr.lock.RLock()
	defer bsr.lock.RUnlock()

	ds := bsr.ds
	if ds != nil {
		bsr.refcounts[ds].Add(1)
	}

	return ds
}

// Release decrements the refcount for an index
// Sequins may close the index after this point, if no other requests
// hold a reference to it.
func (bsr *datasetReference) release(ds *dataset) {
	if ds != nil {
		bsr.refcounts[ds].Done()
	}
}

// Replace replaces the current dataset with a new one, waits for the old one
// to no longer be in use, then returns. At that point, no more references are
// held for the dataset, and it should be safe to close.
func (bsr *datasetReference) replace(new *dataset) *dataset {
	if bsr.refcounts == nil {
		bsr.refcounts = make(map[*dataset]*sync.WaitGroup)
	}

	bsr.lock.Lock()

	old := bsr.ds
	bsr.ds = new
	bsr.refcounts[new] = &sync.WaitGroup{}

	bsr.lock.Unlock()

	if old != nil {
		bsr.refcounts[old].Wait()
	}

	return old
}
