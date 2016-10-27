package multilock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultilock(t *testing.T) {
	t.Parallel()

	m := New(5)
	wg := sync.WaitGroup{}
	var v int32

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			l := m.Lock()
			defer close(l)

			// Check that only 5 goroutines hold the lock.
			total := atomic.AddInt32(&v, 1)
			if total > 5 {
				t.Errorf("%d concurrent writers!", total)
			}

			// Ensure that goroutines pile up.
			time.Sleep(1 * time.Millisecond)

			total = atomic.AddInt32(&v, -1)
			if total < 0 {
				t.Error("more than 5 concurrent writers!")
			}
		}(i)
	}

	wg.Wait()
}
