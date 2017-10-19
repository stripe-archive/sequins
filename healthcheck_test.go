package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Spin up a sequins instance and ensure that the healthcheck returns StatusOK
// (200) as expected.
func TestGoodHealthCheck(t *testing.T) {
	config := defaultConfig()
	dir, err := ioutil.TempDir("", "sequins_test")
	if err != nil {
		t.Fatalf("can't create temp dir. %q", err)
	}
	config.LocalStore = dir
	config.Source = "test_databases/healthy"

	s := localSetup("test_databases/healthy", config)

	err = s.init()
	require.NoError(t, err, "sequins should initialize without problems")

	// Because the version status is set asynchronously, we need to wait a
	// few seconds so that it will report its true status as opposed to
	// the default (AVAILABLE).
	time.Sleep(time.Duration(1) * time.Second)

	r := httptest.NewRequest("GET", "http://localhost/healthz", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// Spin up a sequins instance with a healthy database and an unhealthy database
// and test that the healthcheck returns (200) as expected.
func TestPartialHealthCheck(t *testing.T) {
	config := defaultConfig()
	dir, err := ioutil.TempDir("", "sequins_test")
	if err != nil {
		t.Fatalf("can't create temp dir. %q", err)
	}
	config.LocalStore = dir
	config.Source = "test_databases/partially_healthy"

	s := localSetup("test_databases/partially_healthy", config)

	err = s.init()
	require.NoError(t, err, "sequins should initialize without problems")

	// Because the version status is set asynchronously, we need to wait a
	// few seconds so that it will report its true status as opposed to
	// the default (AVAILABLE).
	time.Sleep(time.Duration(1) * time.Second)

	r := httptest.NewRequest("GET", "http://localhost/healthz", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// Spin up a sequins instance with a bad database and test that the healthcheck
// returns StatusNotFound (404) as expected.
func TestBadHealthCheck(t *testing.T) {
	config := defaultConfig()
	dir, err := ioutil.TempDir("", "sequins_test_bad")
	if err != nil {
		t.Fatalf("can't create temp dir. %q", err)
	}
	config.LocalStore = dir
	config.Source = "test_databases/unhealthy"

	s := localSetup("test_databases/unhealthy", config)

	err = s.init()
	require.NoError(t, err, "sequins should initialize without problems")

	// Because the version status is set asynchronously, we need to wait a
	// few seconds so that it will report its true status as opposed to
	// the default (AVAILABLE).
	time.Sleep(time.Duration(1) * time.Second)

	r := httptest.NewRequest("GET", "http://localhost/healthz", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
