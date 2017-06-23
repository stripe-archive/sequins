package rpc

import "testing"
import (
	"container/heap"
	"fmt"
)

func TestPriorityQueue(t *testing.T) {
	items := []*Record{
		&Record{
			Key:   []byte("C"),
			Value: []byte("C IS THE KEY"),
		},
		&Record{
			Key:   []byte("A"),
			Value: []byte("A IS THE KEY"),
		},
		&Record{
			Key:   []byte("Z"),
			Value: []byte("Z IS THE KEY"),
		},
		&Record{
			Key:   []byte("B"),
			Value: []byte("B IS THE KEY"),
		},
	}
	pq := make(PriorityQueue, len(items))
	for i, itemRecord := range items {
		item := &Item{
			Key:   itemRecord.Key,
			Value: itemRecord,
			index: i,
		}
		pq[i] = item
	}
	heap.Init(&pq)
	heap.Push(&pq, &Item{
		Key: []byte("S"),
		Value: &Record{
			Key: []byte("S"),
			Value: []byte("S IS THE KEY"),
		},
	})
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("%.2d:%s ", item.Key, item.Value)
	}
}
