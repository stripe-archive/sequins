package workqueue

import (
	"log"
	"sync"
)

type task struct {
	work func()
}

type WorkQueue struct {
	queue []*task
	cond  *sync.Cond
}

func NewWorkQueue(num int) *WorkQueue {
	w := &WorkQueue{
		queue: make([]*task, 0),
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

func (w *WorkQueue) Schedule(work func()) {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	w.queue = append(w.queue, &task{work})
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
	task := w.queue[0]
	w.queue = w.queue[1:]
	w.cond.L.Unlock()
	defer w.cond.L.Lock()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered from a panic in a work task: %v", r)
		}
	}()
	task.work()
}
