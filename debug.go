package main

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/codahale/hdrhistogram"
)

const requestLogEnableFlagPrefix = "sequins.request_log"

var expStats *sequinsStats

type sequinsStats struct {
	Qps struct {
		Total    int64
		ByStatus map[string]int64

		total     int64
		status200 int64
		status400 int64
		status404 int64
		status499 int64
		status500 int64
		status501 int64
		status502 int64
		status504 int64
	}
	Latency struct {
		Max   float64
		Mean  float64
		P9999 float64
		P99   float64
		P95   float64
		P90   float64
		P75   float64
		P50   float64
		P25   float64
	}

	latencyHist *hdrhistogram.Histogram
	queries     chan *queryStats

	DiskUsed int64
	lock     sync.RWMutex
}

type queryStats struct {
	duration time.Duration
	status   int
	path     string
}

func startDebugServer(config sequinsConfig) {
	mux := http.NewServeMux()

	s := &http.Server{
		Addr:    config.Debug.Bind,
		Handler: mux,
	}

	if config.Debug.Expvars {
		mux.HandleFunc("/debug/vars", expvarHandler)
		expStats = newStats(config.LocalStore)
		expvar.Publish("sequins", expStats)
	}

	if config.Debug.Pprof {
		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	}

	go s.ListenAndServe()
}

// expvarHandler is copied from the stdlib.
func expvarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

func newStats(localStorePath string) *sequinsStats {
	s := &sequinsStats{
		latencyHist: hdrhistogram.New(0, int64(10*time.Second/time.Microsecond), 5),
		queries:     make(chan *queryStats, 1024),
	}

	go s.updateRequestStats()
	go s.updateDiskStats(localStorePath)
	return s
}

func (s *sequinsStats) updateRequestStats() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			s.snapshotRequestStats()

			s.latencyHist.Reset()

			s.Qps.total = 0
			s.Qps.status200 = 0
			s.Qps.status400 = 0
			s.Qps.status404 = 0
			s.Qps.status499 = 0
			s.Qps.status500 = 0
			s.Qps.status501 = 0
			s.Qps.status502 = 0
			s.Qps.status504 = 0
		case q := <-s.queries:
			s.latencyHist.RecordValue(int64(q.duration / time.Microsecond))

			s.Qps.total++
			switch q.status {
			case 0, 200:
				s.Qps.status200++
			case 400:
				s.Qps.status400++
			case 404:
				s.Qps.status404++
			case 499:
				s.Qps.status499++
			case 500:
				s.Qps.status500++
			case 501:
				s.Qps.status501++
			case 502:
				s.Qps.status502++
			case 504:
				s.Qps.status504++
			default:
				log.Println("Untrackable http status:", q.status)
			}
		}
	}
}

func (s *sequinsStats) snapshotRequestStats() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Qps.Total = s.Qps.total
	s.Qps.ByStatus = make(map[string]int64)
	s.Qps.ByStatus["200"] = s.Qps.status200
	s.Qps.ByStatus["400"] = s.Qps.status400
	s.Qps.ByStatus["404"] = s.Qps.status404
	s.Qps.ByStatus["499"] = s.Qps.status499
	s.Qps.ByStatus["500"] = s.Qps.status500
	s.Qps.ByStatus["501"] = s.Qps.status501
	s.Qps.ByStatus["502"] = s.Qps.status502
	s.Qps.ByStatus["504"] = s.Qps.status504

	ms := float64(1000)
	s.Latency.Max = float64(s.latencyHist.Max()) / ms
	s.Latency.Mean = s.latencyHist.Mean() / ms
	s.Latency.P9999 = float64(s.latencyHist.ValueAtQuantile(99.99)) / ms
	s.Latency.P99 = float64(s.latencyHist.ValueAtQuantile(99.0)) / ms
	s.Latency.P95 = float64(s.latencyHist.ValueAtQuantile(95.0)) / ms
	s.Latency.P90 = float64(s.latencyHist.ValueAtQuantile(90.0)) / ms
	s.Latency.P75 = float64(s.latencyHist.ValueAtQuantile(75.0)) / ms
	s.Latency.P50 = float64(s.latencyHist.ValueAtQuantile(50.0)) / ms
	s.Latency.P25 = float64(s.latencyHist.ValueAtQuantile(25.0)) / ms
}

func (s *sequinsStats) updateDiskStats(path string) {
	s.calculateDiskUsage(path)

	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		s.calculateDiskUsage(path)
	}
}

func (s *sequinsStats) calculateDiskUsage(path string) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return err
	})

	if err == nil {
		s.lock.Lock()
		defer s.lock.Unlock()

		s.DiskUsed = size
	}
}

func (s *sequinsStats) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	return string(b)
}

// Tracks queries to each db
type dbTracker struct {
	stats  *statsd.Client
	counts map[string]uint
	paths  chan string
}

func newDBTracker(stats *statsd.Client) *dbTracker {
	d := &dbTracker{stats, map[string]uint{}, make(chan string, 1024)}
	go d.trackDBs()
	return d
}

func (t *dbTracker) sendStats(counts map[string]uint) {
	for db, count := range counts {
		if count != 0 {
			t.stats.Count("db.requests", int64(count), []string{"db:" + db}, 1)
		}
	}
}

func (t *dbTracker) trackDBs() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			go t.sendStats(t.counts)
			t.counts = map[string]uint{}
		case path := <-t.paths:
			parts := strings.SplitN(path, "/", 2)
			if len(parts) == 2 && parts[0] != "" {
				db := parts[0]
				t.counts[db]++
			}
		}
	}
}

func (t *dbTracker) trackDB(path string) {
	select {
	case t.paths <- path:
	default:
	}
}

// trackingHandler is an http.Handler that tracks request times.
type trackingHandler struct {
	*sequins
	trackStats bool
	requestLog chan *queryStats
	dbTracker  *dbTracker
}

func trackQueries(s *sequins) http.Handler {
	debug := s.config.Debug
	trackStats := debug.Bind != "" && debug.Expvars

	var requestLog chan *queryStats
	if debug.RequestLogFile != "" {
		requestLog = make(chan *queryStats, 1024)
		go logRequests(s, requestLog)
	}

	if trackStats || requestLog != nil {
		return &trackingHandler{s, trackStats, requestLog, newDBTracker(s.stats)}
	} else {
		return s
	}
}

func (t *trackingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Don't track queries to the status pages, and don't track proxied
	// queries.
	path := strings.TrimPrefix(r.URL.Path, "/")
	if strings.Index(path, "/") > 0 && r.URL.Query().Get("proxy") == "" {
		w = trackQuery(t, w)
		defer w.(*queryTracker).done(path)
	}

	t.sequins.ServeHTTP(w, r)
}

type queryTracker struct {
	http.ResponseWriter
	handler *trackingHandler
	start   time.Time
	status  int
}

func trackQuery(t *trackingHandler, w http.ResponseWriter) *queryTracker {
	return &queryTracker{
		ResponseWriter: w,
		handler:        t,
		start:          time.Now(),
		status:         200,
	}
}

func (t *queryTracker) WriteHeader(status int) {
	t.status = status
	t.ResponseWriter.WriteHeader(status)
}

func (t *queryTracker) done(path string) {
	q := &queryStats{
		duration: time.Now().Sub(t.start),
		status:   t.status,
		path:     path,
	}

	if expStats != nil {
		select {
		case expStats.queries <- q:
		default:
		}
	}

	select {
	case t.handler.requestLog <- q:
	default:
	}

	t.handler.dbTracker.trackDB(path)
}

func logRequests(s *sequins, stats chan *queryStats) {
	var logger *log.Logger
	path := s.config.Debug.RequestLogFile
	if path != "stdout" {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Can't create request log: %s\n", err)
			return
		}
		defer file.Close()
		logger = log.New(file, "", log.LstdFlags|log.LUTC)
	} else {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	}

	flag := "sequins.request_log"
	if s.config.Sharding.ClusterName != "" {
		flag = flag + "." + s.config.Sharding.ClusterName
	}

	for q := range stats {
		flagEnabled, _ := s.checkFlag(requestLogEnableFlagPrefix)
		if s.config.Debug.RequestLogEnable || flagEnabled {
			logger.Printf("CANONICAL-SEQUINS-REQUEST-LINE %q %d\n", q.path, q.status)
		}
	}
}
