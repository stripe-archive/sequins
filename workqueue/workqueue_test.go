package workqueue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkQueue(t *testing.T) {
	w := NewWorkQueue(8)
	c := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(8)
	for i := 0; i < 20; i++ {
		w.Schedule(func() {
			wg.Done()
			<-c
			wg.Done()
		})
	}

	// Wait for first 8 workers to run (and block on `c`).
	wg.Wait()
	assert.Equal(t, 12, w.Length(), "should have 12 tasks blocked")

	wg.Add(32)
	close(c)
	wg.Wait()
	assert.Equal(t, 0, w.Length(), "all tasks should have run")
}

func TestWorkQueueRecover(t *testing.T) {
	w := NewWorkQueue(8)
	wg := &sync.WaitGroup{}
	wg.Add(8)
	for i := 0; i < 8; i++ {
		w.Schedule(func() {
			panic("womp")
		})
	}
	for i := 0; i < 8; i++ {
		wg.Done()
	}

	// If the recover logic was broken, we'd expect all of the work queue's goroutines to be dead,
	// so our second set of work items wouldn't run and `wg.Wait()` would time out.
	wg.Wait()
}
