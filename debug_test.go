package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stripe/sequins/backend"
)

// Record what we expect to see in the request log.
type requestLog struct {
	path   string
	status int
	time   time.Time
}

type requestLogs []requestLog

func (l *requestLogs) request(t *testing.T, addr string, path string, expectedStatus int, shouldBeInLog bool) {
	start := time.Now()
	url := fmt.Sprintf("http://%s/%s", addr, path)
	resp, err := http.Get(url)
	require.NoError(t, err)
	assert.Equal(t, expectedStatus, resp.StatusCode)
	if shouldBeInLog {
		*l = append(*l, requestLog{path, resp.StatusCode, start})
	}
}

func (l requestLogs) testEquality(t *testing.T, actual requestLogs) {
	for i, e := range l {
		require.True(t, len(actual) > i, "missing actual request log entry")
		a := actual[i]

		assert.Equal(t, e.path, a.path)
		assert.Equal(t, e.status, a.status)
		assert.WithinDuration(t, e.time, a.time, 3*time.Second)
	}
	assert.Equal(t, len(l), len(actual))
}

func readRequestLog(logPath string) (requestLogs, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	logs := requestLogs{}
	for {
		var datePart, timePart string
		var log requestLog
		_, err := fmt.Fscanf(f, "%s %s CANONICAL-SEQUINS-REQUEST-LINE %q %d\n", &datePart, &timePart, &log.path, &log.status)
		if err == io.EOF {
			return logs, nil
		}
		if err != nil {
			return nil, err
		}

		logTime := datePart + " " + timePart
		log.time, err = time.Parse("2006/01/02 15:04:05", logTime)
		if err != nil {
			return nil, err
		}

		logs = append(logs, log)
	}
}

func TestRequestLog(t *testing.T) {
	// Create source data.
	sourceDir, err := ioutil.TempDir("", "sequins-source-")
	require.NoError(t, err)
	defer os.RemoveAll(sourceDir)
	versionDir := filepath.Join(sourceDir, "baby-names", "1")
	require.NoError(t, directoryCopy(t, versionDir, "test_databases/healthy/baby-names/1"))
	localBackend := backend.NewLocalBackend(sourceDir)

	// Create local store.
	localStore, err := ioutil.TempDir("", "sequins-store-")
	require.NoError(t, err)
	defer os.RemoveAll(sourceDir)

	// Create a place for request logs.
	requestLog, err := ioutil.TempFile("", "sequins-requests-")
	require.NoError(t, err)
	requestLog.Close()
	defer os.Remove(requestLog.Name())

	// Start the server.
	config := defaultConfig()
	config.Bind = "localhost:9599"
	config.LocalStore = localStore
	config.MaxParallelLoads = 1
	config.Debug.RequestLogEnable = true
	config.Debug.RequestLogFile = requestLog.Name()
	s := newSequins(localBackend, config)
	require.NoError(t, s.init())
	waitForDBs(t, s)

	// httptest does weird things with weird paths, so we want a real server for this.
	ln, err := net.Listen("tcp4", "localhost:")
	require.NoError(t, err)
	defer ln.Close()
	addr := ln.Addr().String()
	srv := http.Server{
		Addr:    addr,
		Handler: s.http,
	}
	go srv.Serve(ln)
	defer srv.Shutdown(context.Background())

	// Make some requests.
	expected := requestLogs{}
	expected.request(t, addr, "", 200, false)
	expected.request(t, addr, "healthz", 200, false)
	expected.request(t, addr, "fake/fake", 404, true)
	expected.request(t, addr, "baby-names/fake", 404, true)
	expected.request(t, addr, "baby-names/1881/boy", 200, true)
	expected.request(t, addr, "baby-names/1992/girl", 200, true)
	expected.request(t, addr, "test/contains space", 404, true)
	expected.request(t, addr, "test/contains\nnewline", 404, true)
	expected.request(t, addr, "binary/\x01\x02\x03\x04", 404, true)

	// Hack: Force the request log to be closed.
	h, ok := s.http.(*trackingHandler)
	require.True(t, ok)
	close(h.requestLog)
	time.Sleep(100 * time.Millisecond)

	// Read the actual request log.
	actual, err := readRequestLog(requestLog.Name())
	assert.NoError(t, err)

	// Compare them!
	expected.testEquality(t, actual)
}
