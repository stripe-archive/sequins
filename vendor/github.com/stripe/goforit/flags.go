package goforit

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

const statsdAddress = "127.0.0.1:8200"

const lastAssertInterval = 5 * time.Minute

// An interface reflecting the parts of statsd that we need, so we can mock it
type statsdClient interface {
	Histogram(string, float64, []string, float64) error
	Gauge(string, float64, []string, float64) error
	Count(string, int64, []string, float64) error
	SimpleServiceCheck(string, statsd.ServiceCheckStatus) error
}

type goforit struct {
	ticker *time.Ticker

	stalenessMtx       sync.RWMutex
	stalenessThreshold time.Duration

	flagsMtx            sync.RWMutex
	flags               map[string]Flag
	lastFlagRefreshTime time.Time

	stats statsdClient

	// Last time we alerted that flags may be out of date
	lastAssertMtx sync.Mutex
	lastAssert    time.Time

	// rand is not concurrency safe, in general
	rndMtx sync.Mutex
	rnd    *rand.Rand

	logger *log.Logger
}

const DefaultInterval = 30 * time.Second

type Flag struct {
	Name string
	Rate float64
}

func newWithoutInit() *goforit {
	stats, _ := statsd.New(statsdAddress)
	return &goforit{
		stats:  stats,
		flags:  map[string]Flag{},
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
		logger: log.New(os.Stderr, "[goforit] ", log.LstdFlags),
	}
}

// New creates a new goforit
func New(interval time.Duration, backend Backend) *goforit {
	g := newWithoutInit()
	g.init(interval, backend)
	return g
}

func (g *goforit) rand() float64 {
	g.rndMtx.Lock()
	defer g.rndMtx.Unlock()
	return g.rnd.Float64()
}

func (g *goforit) getStalenessThreshold() time.Duration {
	g.stalenessMtx.RLock()
	defer g.stalenessMtx.RUnlock()
	return g.stalenessThreshold
}

// Check if a time is stale.
func (g *goforit) staleCheck(t time.Time, metric string, metricRate float64, msg string, checkLastAssert bool) {
	if t.IsZero() {
		// Not really useful to treat this as a real time
		return
	}

	// Report the staleness
	staleness := time.Since(t)
	g.stats.Histogram(metric, staleness.Seconds(), nil, metricRate)

	// Log if we're old
	thresh := g.getStalenessThreshold()
	if thresh == 0 {
		return
	}
	if staleness <= thresh {
		return
	}

	if checkLastAssert {
		// Don't log too often!
		g.lastAssertMtx.Lock()
		defer g.lastAssertMtx.Unlock()
		if time.Since(g.lastAssert) < lastAssertInterval {
			return
		}
		g.lastAssert = time.Now()
	}
	g.logger.Printf(msg, staleness, thresh)
}

// Enabled returns a boolean indicating
// whether or not the flag should be considered
// enabled. It returns false if no flag with the specified
// name is found
func (g *goforit) Enabled(ctx context.Context, name string) (enabled bool) {
	var lastRefreshTime time.Time
	defer func() {
		var gauge float64
		if enabled {
			gauge = 1
		}
		g.stats.Gauge("goforit.flags.enabled", gauge, []string{fmt.Sprintf("flag:%s", name)}, .1)
		g.staleCheck(lastRefreshTime, "goforit.flags.last_refresh_s", .01,
			"Refresh cycle has not run in %s, past our threshold (%s)", true)
	}()

	// Check for an override.
	if ctx != nil {
		if ov, ok := ctx.Value(overrideContextKey).(overrides); ok {
			if enabled, ok = ov[name]; ok {
				return
			}
		}
	}

	g.flagsMtx.RLock()
	defer g.flagsMtx.RUnlock()
	if g.flags == nil {
		enabled = false
		return
	}
	lastRefreshTime = g.lastFlagRefreshTime
	flag := g.flags[name]

	// equality should be strict
	// because Float64() can return 0
	if f := g.rand(); f < flag.Rate {
		enabled = true
		return
	}
	enabled = false
	return
}

// RefreshFlags will use the provided thunk function to
// fetch all feature flags and update the internal cache.
// The thunk provided can use a variety of mechanisms for
// querying the flag values, such as a local file or
// Consul key/value storage.
func (g *goforit) RefreshFlags(backend Backend) {
	// Ask the backend for the flags
	var checkStatus statsd.ServiceCheckStatus
	defer func() {
		g.stats.SimpleServiceCheck("goforit.refreshFlags.present", checkStatus)
	}()
	refreshedFlags, updated, err := backend.Refresh()
	if err != nil {
		checkStatus = statsd.Warn
		g.stats.Count("goforit.refreshFlags.errors", 1, nil, 1)
		g.logger.Printf("Error refreshing flags: %s", err)
		return
	}

	fmap := map[string]Flag{}
	for _, flag := range refreshedFlags {
		fmap[flag.Name] = flag
	}
	g.staleCheck(updated, "goforit.flags.cache_file_age_s", 0.1,
		"Backend is stale (%s) past our threshold (%s)", false)

	// update the package-level flags
	// which are protected by the mutex
	g.flagsMtx.Lock()
	g.flags = fmap
	g.lastFlagRefreshTime = time.Now()
	g.flagsMtx.Unlock()

	return
}

func (g *goforit) SetStalenessThreshold(threshold time.Duration) {
	g.stalenessMtx.Lock()
	defer g.stalenessMtx.Unlock()
	g.stalenessThreshold = threshold
}

// init initializes the flag backend, using the provided refresh function
// to update the internal cache of flags periodically, at the specified interval.
func (g *goforit) init(interval time.Duration, backend Backend) {
	g.RefreshFlags(backend)
	if interval != 0 {
		ticker := time.NewTicker(interval)
		g.ticker = ticker

		go func() {
			for _ = range ticker.C {
				g.RefreshFlags(backend)
			}
		}()
	}
}

// A unique context key for overrides
type overrideContextKeyType struct{}

var overrideContextKey = overrideContextKeyType{}

type overrides map[string]bool

// Override allows overriding the value of a goforit flag within a context.
// This is mainly useful for tests.
func Override(ctx context.Context, name string, value bool) context.Context {
	ov := overrides{}
	if old, ok := ctx.Value(overrideContextKey).(overrides); ok {
		for k, v := range old {
			ov[k] = v
		}
	}
	ov[name] = value
	return context.WithValue(ctx, overrideContextKey, ov)
}

// Close releases resources held
// It's still safe to call Enabled()
func (g *goforit) Close() error {
	if g.ticker != nil {
		g.ticker.Stop()
		g.ticker = nil
	}
	return nil
}
