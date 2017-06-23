package rpc

import (
	"container/heap"
	"bytes"
)

// An Item is something we manage in a Key queue.
type Item struct {
	Value *Record // The Value of the item; arbitrary.
	Key   []byte  // The Key of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return bytes.Compare(pq[i].Key, pq[j].Key) < 0
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the Key and Value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value *Record, key []byte) {
	item.Value = value
	item.Key = key
	heap.Fix(pq, item.index)
}
