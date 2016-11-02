// package multilock provides a locking mechanism that can be locked by N
// cilents simultaneously.
package multilock

type Multilock struct {
	c chan bool
}

func New(n int) *Multilock {
	m := &Multilock{
		c: make(chan bool, n),
	}

	for i := 0; i < n; i++ {
		m.c <- true
	}

	return m
}

// Lock blocks until the lock is acquired. It ensures that only N clients hold
// the lock overall, but makes no guarantee that locks are granted in the order
// they are requested.
func (m *Multilock) Lock() {
	<-m.c
}

// Unlock unlocks the lock, freeing up a slot for another client.
func (m *Multilock) Unlock() {
	m.c <- true
}
