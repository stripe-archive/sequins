package main

import (
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/juju/ratelimit"
	"github.com/stretchr/testify/assert"
)

// Test juju/ratelimit in a way similar to how we use it in our code base.

// The juju/ratelimit API we use here doesn't allow us to control the token bucket precisely.
// We cannot set the initial token bucket size and neither can we control how frequently
// it refills the token bucket. So we have to assume: 1) it sets the initial token bucket
// size to its capacity; 2) it refills the token bucket frequently enough.
const size = 1 * 1024 * 1024 // 1 MB
const rate = 4 * 1024 * 1024 // 4 MB/s
const delta = 0.15

// Set this to a small number but not too small.
// If we set this too big, the initial token bucket size may affect our measurement
// of the copy time. If we set it too small, we have to assume the token bucket is
// refilled very frequently.
const bucketCapacity = size / 128

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

func testRead(t *testing.T, fileSize uint64, rateLimitBucket *ratelimit.Bucket) float64 {
	in := &mockReader{fileSize}
	out := ioutil.Discard

	start := time.Now()

	var rateLimitedIn io.Reader
	if rateLimitBucket == nil {
		rateLimitedIn = in
	} else {
		rateLimitedIn = ratelimit.Reader(in, rateLimitBucket)
	}
	_, err := io.Copy(out, rateLimitedIn)
	assert.NoError(t, err)
	return time.Now().Sub(start).Seconds()
}

func TestNoRateLimit(t *testing.T) {
	t.Parallel()

	dur := testRead(t, size, nil)
	assert.InDelta(t, 0, dur, delta)
}

func TestRateLimit(t *testing.T) {
	t.Parallel()

	rateLimitBucket := ratelimit.NewBucketWithRate(float64(rate), bucketCapacity)
	dur := testRead(t, size, rateLimitBucket)
	assert.InDelta(t, float64(size)/rate, dur, delta)
}

func TestConcurrentRateLimit(t *testing.T) {
	t.Parallel()
	// goroutine count
	cnt := 8
	rateLimitBucket := ratelimit.NewBucketWithRate(float64(rate), bucketCapacity)
	var wg sync.WaitGroup
	wg.Add(cnt)
	start := time.Now()
	for i := 0; i < cnt; i++ {
		go func() {
			testRead(t, uint64(size/cnt), rateLimitBucket)
			wg.Done()
		}()
	}
	wg.Wait()
	dur := time.Now().Sub(start).Seconds()
	assert.InDelta(t, float64(size)/rate, dur, delta)
}
