package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/backend"
)

// TODO: we can run these tests in travis using gohdfs' minicluster script

func setupHdfs(t *testing.T) *backend.HdfsBackend {
	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Skip("Skipping hdfs tests because HADOOP_NAMENODE isn't set")
	}

	client, err := hdfs.New(nn)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: we can create the test data now
	if _, err = client.Stat("/_test_sequins"); os.IsNotExist(err) {
		t.Skip("Skipping hdfs tests because there's no test data in /_test_sequins")
	}

	return backend.NewHdfsBackend(client, nn, "/_test_sequins")
}

func getHdfsSequins(t *testing.T) *sequins {
	backend := setupHdfs(t)
	s := getSequins(t, backend, "")

	require.NoError(t, s.init())
	return s
}

func TestHdfsBackend(t *testing.T) {
	h := setupHdfs(t)

	versions, err := h.ListVersions("names", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"0", "1"}, versions)

	versions, err = h.ListVersions("names", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"0"}, versions)
}

func TestHdfsSequins(t *testing.T) {
	ts := getHdfsSequins(t)

	req, _ := http.NewRequest("GET", "/Alice", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "Practice", w.Body.String())

	req, _ = http.NewRequest("GET", "/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
	assert.Equal(t, "", w.Body.String())

	req, _ = http.NewRequest("GET", "/", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	status := &status{}
	err := json.Unmarshal(w.Body.Bytes(), status)
	require.NoError(t, err)
	assert.Equal(t, 200, w.Code)
	// TODO
}
