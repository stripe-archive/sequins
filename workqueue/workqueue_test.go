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
		}, 0)
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
		}, 0)
	}
	for i := 0; i < 8; i++ {
		wg.Done()
	}

	// If the recover logic was broken, we'd expect all of the work queue's goroutines to be dead,
	// so our second set of work items wouldn't run and `wg.Wait()` would time out.
	wg.Wait()
}

func TestWorkQueuePriority(t *testing.T) {
	w := NewWorkQueue(1)
	wg := &sync.WaitGroup{}
	w.Schedule(func() {
		wg.Done()
	}, 0)

	// The queue is blocked, so anything additional we add should be prioritized.
	priorities := []int64{17, 23, 2, -5, 3, 15, -11}
	var out []int64
	for _, p := range priorities {
		p := p
		w.Schedule(func() {
			out = append(out, p)
			wg.Done()
		}, p)
	}

	// Now let everything run.
	wg.Add(1 + len(priorities))
	wg.Wait()

	assert.Equal(t, []int64{23, 17, 15, 3, 2, -5, -11}, out)
}
