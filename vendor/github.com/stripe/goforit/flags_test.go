package goforit

import (
	"bytes"
	"context"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
)

// arbitrary but fixed for reproducible testing
const seed = 5194304667978865136

const ε = .02

type mockStatsd struct {
	lock            sync.RWMutex
	histogramValues map[string][]float64
}

func (m *mockStatsd) Gauge(string, float64, []string, float64) error {
	return nil
}

func (m *mockStatsd) Count(string, int64, []string, float64) error {
	return nil
}

func (m *mockStatsd) SimpleServiceCheck(string, statsd.ServiceCheckStatus) error {
	return nil
}

func (m *mockStatsd) Histogram(name string, value float64, tags []string, rate float64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.histogramValues == nil {
		m.histogramValues = make(map[string][]float64)
	}
	m.histogramValues[name] = append(m.histogramValues[name], value)
	return nil
}

func (m *mockStatsd) getHistogramValues(name string) []float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	s := make([]float64, len(m.histogramValues[name]))
	copy(s, m.histogramValues[name])
	return s
}

// Build a goforit for testing
// Also return the log output
func testGoforit(interval time.Duration, backend Backend) (*goforit, *bytes.Buffer) {
	g := newWithoutInit()
	g.rnd = rand.New(rand.NewSource(seed))
	var buf bytes.Buffer
	g.logger = log.New(&buf, "", 9)
	g.stats = &mockStatsd{}

	if backend != nil {
		g.init(interval, backend)
	}

	return g, &buf
}

func TestGlobal(t *testing.T) {
	// Not parallel, testing global behavior
	backend := BackendFromFile(filepath.Join("fixtures", "flags_example.csv"))
	globalGoforit.stats = &mockStatsd{} // prevent logging real metrics

	Init(DefaultInterval, backend)
	defer Close()

	assert.False(t, Enabled(nil, "go.sun.money"))
	assert.True(t, Enabled(nil, "go.moon.mercury"))
}

func TestEnabled(t *testing.T) {
	t.Parallel()

	const iterations = 100000

	backend := BackendFromFile(filepath.Join("fixtures", "flags_example.csv"))
	g, _ := testGoforit(DefaultInterval, backend)
	defer g.Close()

	assert.False(t, g.Enabled(context.Background(), "go.sun.money"))
	assert.True(t, g.Enabled(context.Background(), "go.moon.mercury"))

	// nil is equivalent to empty context
	assert.False(t, g.Enabled(nil, "go.sun.money"))
	assert.True(t, g.Enabled(nil, "go.moon.mercury"))

	count := 0
	for i := 0; i < iterations; i++ {
		if g.Enabled(context.Background(), "go.stars.money") {
			count++
		}
	}
	actualRate := float64(count) / float64(iterations)

	assert.InEpsilon(t, 0.5, actualRate, ε)
}

// dummyBackend lets us test the RefreshFlags
// by returning the flags only the second time the Refresh
// method is called
type dummyBackend struct {
	// tally how many times Refresh() has been called
	refreshedCount int
}

func (b *dummyBackend) Refresh() (map[string]Flag, time.Time, error) {
	defer func() {
		b.refreshedCount++
	}()

	if b.refreshedCount == 0 {
		return map[string]Flag{}, time.Time{}, nil
	}

	f, err := os.Open(filepath.Join("fixtures", "flags_example.csv"))
	if err != nil {
		return nil, time.Time{}, err
	}
	defer f.Close()
	return parseFlagsCSV(f)
}

func TestRefresh(t *testing.T) {
	t.Parallel()

	backend := &dummyBackend{}
	g, _ := testGoforit(10*time.Millisecond, backend)

	assert.False(t, g.Enabled(context.Background(), "go.sun.money"))
	assert.False(t, g.Enabled(context.Background(), "go.moon.mercury"))

	defer g.Close()

	// ensure refresh runs twice to avoid race conditions
	// in which the Refresh method returns but the assertions get called
	// before the flags are actually updated
	for backend.refreshedCount < 2 {
		<-time.After(10 * time.Millisecond)
	}

	assert.False(t, g.Enabled(context.Background(), "go.sun.money"))
	assert.True(t, g.Enabled(context.Background(), "go.moon.mercury"))
}

// BenchmarkEnabled runs a benchmark for a feature flag
// that is enabled for 50% of operations.
func BenchmarkEnabled(b *testing.B) {
	backend := BackendFromFile(filepath.Join("fixtures", "flags_example.csv"))
	g, _ := testGoforit(10*time.Millisecond, backend)
	defer g.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Enabled(context.Background(), "go.stars.money")
	}
}

// assertFlagsEqual is a helper function for asserting
// that two maps of flags are equal
func assertFlagsEqual(t *testing.T, expected, actual map[string]Flag) {
	assert.Equal(t, len(expected), len(actual))

	for k, v := range expected {
		assert.Equal(t, v, actual[k])
	}
}

func TestOverride(t *testing.T) {
	t.Parallel()

	backend := BackendFromFile(filepath.Join("fixtures", "flags_example.csv"))
	g, _ := testGoforit(10*time.Millisecond, backend)
	defer g.Close()
	g.RefreshFlags(backend)

	// Empty context gets values from backend.
	assert.False(t, g.Enabled(context.Background(), "go.sun.money"))
	assert.True(t, g.Enabled(context.Background(), "go.moon.mercury"))
	assert.False(t, g.Enabled(context.Background(), "go.extra"))

	// Nil is equivalent to empty context.
	assert.False(t, g.Enabled(nil, "go.sun.money"))
	assert.True(t, g.Enabled(nil, "go.moon.mercury"))
	assert.False(t, g.Enabled(nil, "go.extra"))

	// Can override to true in context.
	ctx := context.Background()
	ctx = Override(ctx, "go.sun.money", true)
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.True(t, g.Enabled(ctx, "go.moon.mercury"))
	assert.False(t, g.Enabled(ctx, "go.extra"))

	// Can override to false.
	ctx = Override(ctx, "go.moon.mercury", false)
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.False(t, g.Enabled(ctx, "go.moon.mercury"))
	assert.False(t, g.Enabled(ctx, "go.extra"))

	// Can override brand new flag.
	ctx = Override(ctx, "go.extra", true)
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.False(t, g.Enabled(ctx, "go.moon.mercury"))
	assert.True(t, g.Enabled(ctx, "go.extra"))

	// Can override an override.
	ctx = Override(ctx, "go.extra", false)
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.False(t, g.Enabled(ctx, "go.moon.mercury"))
	assert.False(t, g.Enabled(ctx, "go.extra"))

	// Separate contexts don't interfere with each other.
	// This allows parallel tests that use feature flags.
	ctx2 := Override(context.Background(), "go.extra", true)
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.False(t, g.Enabled(ctx, "go.moon.mercury"))
	assert.False(t, g.Enabled(ctx, "go.extra"))
	assert.False(t, g.Enabled(ctx2, "go.sun.money"))
	assert.True(t, g.Enabled(ctx2, "go.moon.mercury"))
	assert.True(t, g.Enabled(ctx2, "go.extra"))

	// Overrides apply to child contexts.
	child := context.WithValue(ctx, "foo", "bar")
	assert.True(t, g.Enabled(child, "go.sun.money"))
	assert.False(t, g.Enabled(child, "go.moon.mercury"))
	assert.False(t, g.Enabled(child, "go.extra"))

	// Changes to child contexts don't affect parents.
	child = Override(child, "go.moon.mercury", true)
	assert.True(t, g.Enabled(child, "go.sun.money"))
	assert.True(t, g.Enabled(child, "go.moon.mercury"))
	assert.False(t, g.Enabled(child, "go.extra"))
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.False(t, g.Enabled(ctx, "go.moon.mercury"))
	assert.False(t, g.Enabled(ctx, "go.extra"))
}

func TestOverrideWithoutInit(t *testing.T) {
	t.Parallel()

	g, _ := testGoforit(0, nil)

	// Everything is false by default.
	assert.False(t, g.Enabled(context.Background(), "go.sun.money"))
	assert.False(t, g.Enabled(context.Background(), "go.moon.mercury"))

	// Can override.
	ctx := Override(context.Background(), "go.sun.money", true)
	assert.True(t, g.Enabled(ctx, "go.sun.money"))
	assert.False(t, g.Enabled(ctx, "go.moon.mercury"))
}

type dummyAgeBackend struct {
	t   time.Time
	mtx sync.RWMutex
}

func (b *dummyAgeBackend) Refresh() (map[string]Flag, time.Time, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	return map[string]Flag{}, b.t, nil
}

// Test to see proper monitoring of age of the flags dump
func TestCacheFileMetric(t *testing.T) {
	t.Parallel()

	backend := &dummyAgeBackend{t: time.Now().Add(-10 * time.Minute)}
	g, _ := testGoforit(10*time.Millisecond, backend)
	defer g.Close()

	time.Sleep(50 * time.Millisecond)
	func() {
		backend.mtx.Lock()
		defer backend.mtx.Unlock()
		backend.t = time.Now()
	}()
	time.Sleep(50 * time.Millisecond)

	// We expect something like: [600, 600.01, ..., 0.0, 0.01, ...]
	last := math.Inf(-1)
	old := 0
	recent := 0
	for _, v := range g.stats.(*mockStatsd).getHistogramValues("goforit.flags.cache_file_age_s") {
		if v > 300 {
			// Should be older than last time
			assert.True(t, v > last)
			// Should be about 10 minutes
			assert.InDelta(t, 600, v, 3)
			old++
			assert.Zero(t, recent, "Should never go from new -> old")
		} else {
			// Should be older (unless we just wrote the file)
			if recent > 0 {
				assert.True(t, v > last)
			}
			// Should be about zero
			assert.InDelta(t, 0, v, 3)
			recent++
		}
		last = v
	}
	assert.True(t, old > 2)
	assert.True(t, recent > 2)
}

// Test to see proper monitoring of refreshing the flags dump file from disc
func TestRefreshCycleMetric(t *testing.T) {
	t.Parallel()

	backend := &dummyAgeBackend{t: time.Now().Add(-10 * time.Minute)}
	g, _ := testGoforit(10*time.Millisecond, backend)
	defer g.Close()

	for i := 0; i < 10; i++ {
		g.Enabled(nil, "go.sun.money")
		time.Sleep(3 * time.Millisecond)
	}

	// want to stop ticker to simulate Refresh() hanging
	g.Close()

	for i := 0; i < 10; i++ {
		g.Enabled(nil, "go.sun.money")
		time.Sleep(3 * time.Millisecond)
	}

	values := g.stats.(*mockStatsd).getHistogramValues("goforit.flags.last_refresh_s")
	// We expect something like: [0, 0.01, 0, 0.01, ..., 0, 0.01, 0.02, 0.03]
	for i := 0; i < 10; i++ {
		v := values[i]
		// Should be ~< 10ms
		assert.InDelta(t, 0.005, v, 0.010)
	}

	last := math.Inf(-1)
	large := 0
	for i := 10; i < 20; i++ {
		v := values[i]
		assert.True(t, v > last)
		last = v
		if v > 0.012 {
			large++
		}
	}
	assert.True(t, large > 2)
}

func TestStaleFile(t *testing.T) {
	t.Parallel()

	backend := &dummyAgeBackend{t: time.Now().Add(-1000 * time.Hour)}
	g, buf := testGoforit(10*time.Millisecond, backend)
	defer g.Close()
	g.SetStalenessThreshold(10*time.Minute + 42*time.Second)

	time.Sleep(50 * time.Millisecond)

	// Should see staleness warnings for backend
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	assert.True(t, len(lines) > 2)
	for _, line := range lines {
		assert.Contains(t, line, "10m42")
		assert.Contains(t, line, "Backend")
	}
}

func TestNoStaleFile(t *testing.T) {
	t.Parallel()

	backend := &dummyAgeBackend{t: time.Now().Add(-1000 * time.Hour)}
	g, buf := testGoforit(10*time.Millisecond, backend)
	defer g.Close()

	time.Sleep(50 * time.Millisecond)

	// Never set staleness, so no warnings
	assert.Zero(t, buf.String())
}

func TestStaleRefresh(t *testing.T) {
	t.Parallel()

	backend := &dummyBackend{}
	g, buf := testGoforit(10*time.Millisecond, backend)
	g.SetStalenessThreshold(50 * time.Millisecond)

	// Simulate stopping refresh
	g.Close()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		g.Enabled(nil, "go.sun.money")
	}

	// Should see just one staleness warning
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	assert.Equal(t, 1, len(lines))
	assert.Contains(t, lines[0], "Refresh")
	assert.Contains(t, lines[0], "50ms")
}
