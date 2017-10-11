package workqueue

import (
	"container/heap"
	"log"
	"sync"
)

type task struct {
	work     func()
	priority int64
}

type WorkQueue struct {
	queue priorityQueue
	cond  *sync.Cond
}

type priorityQueue []task

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	// heap implements a min-heap, but we want a max heap.
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(task))
}

func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	i := (*pq)[n-1]
	*pq = (*pq)[0 : n-1]
	return i
}

func NewWorkQueue(num int) *WorkQueue {
	w := &WorkQueue{
		queue: make(priorityQueue, 0),
		cond:  sync.NewCond(&sync.Mutex{}),
	}
	for i := 0; i < num; i++ {
		go w.work()
	}
	return w
}

func (w *WorkQueue) Length() int {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	return len(w.queue)
}

// Schedule work with a given priority; larger integer values denote higher priorities.
func (w *WorkQueue) Schedule(work func(), priority int64) {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	heap.Push(&w.queue, task{work, priority})
	w.cond.Signal()
}

func (w *WorkQueue) work() {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	for {
		if len(w.queue) > 0 {
			w.doWork()
		} else {
			w.cond.Wait()
		}
	}
}

// Must be called with `w.cond.L` locked.
func (w *WorkQueue) doWork() {
	task := heap.Pop(&w.queue).(task)
	w.cond.L.Unlock()
	defer w.cond.L.Lock()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered from a panic in a work task: %v", r)
		}
	}()
	task.work()
}
