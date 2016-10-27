// package multilock provides a locking mechanism that can be locked by N
// cilents simultaneously.
package multilock

type Multilock struct {
	workers chan bool
}

func New(n int) *Multilock {
	m := &Multilock{
		workers: make(chan bool, n),
	}

	for i := 0; i < n; i++ {
		m.workers <- true
	}

	return m
}

// Lock blocks until the lock is acquired, and returns a channel which must be
// closed to release the lock. It ensures that only N clients hold the lock
// overall, but makes no garauntee that locks are granted in the order they are
// requested.
func (m *Multilock) Lock() chan bool {
	<-m.workers

	l := make(chan bool)
	go func() {
		<-l
		m.workers <- true
	}()

	return l
}
