package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

const size = 10 * 1000 * 1000
const delta = 0.15

type mockReader struct {
	remain uint64
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	size := m.remain
	if size == 0 {
		return 0, io.EOF
	}
	if size > uint64(len(p)) {
		size = uint64(len(p))
	}
	m.remain -= size

	return int(size), nil
}

func testRead(t *testing.T, limiter *rate.Limiter) float64 {
	in := &mockReader{10 * size}
	out := ioutil.Discard

	start := time.Now()
	err := copyStreamWithRateLimiter("test", limiter, out, in)
	assert.NoError(t, err)
	return time.Now().Sub(start).Seconds()
}

func TestNoRateLimit(t *testing.T) {
	t.Parallel()

	dur := testRead(t, nil)
	assert.InDelta(t, 0, dur, delta)
}

func TestRateLimit(t *testing.T) {
	t.Parallel()

	r := 2
	lim := downloadThrottle(r * size)
	dur := testRead(t, lim)
	assert.InDelta(t, 1.0 / float32(r), dur, delta)
}

func TestConcurrentRateLimit(t *testing.T) {
	t.Parallel()

	r := 10
	cnt := 5

	lim := downloadThrottle(r * size)
	var wg sync.WaitGroup
	wg.Add(cnt)
	start := time.Now()
	for i := 0; i < cnt; i++ {
		go func() {
			testRead(t, lim)
			wg.Done()
		}()
	}

	wg.Wait()
	dur := time.Now().Sub(start).Seconds()
	fmt.Printf("Passed seconds: %f", dur)
	assert.InDelta(t, 1.0 / float32(r) * float32(cnt), dur, delta)
}
