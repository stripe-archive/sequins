package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
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

	client.Mkdir("/_test_sequins", 0777|os.ModeDir)
	putHDFS(client, "test/names/0/part-00000")
	putHDFS(client, "test/names/0/part-00001")
	putHDFS(client, "test/names/0/_SUCCESS")

	putHDFS(client, "test/names/1/part-00000")
	putHDFS(client, "test/names/1/part-00001")

	return backend.NewHdfsBackend(client, nn, "/_test_sequins/test")
}

func tearDownHdfs(t *testing.T) {
	nn := os.Getenv("HADOOP_NAMENODE")

	_, err := hdfs.New(nn)
	if err != nil {
		t.Fatal(err)
	}

	// client.Remove("/_test_sequins")
}

func putHDFS(client *hdfs.Client, src string) {
	dest := path.Join("/_test_sequins", src)
	dir, _ := path.Split(dest)

	client.MkdirAll(dir, 0777|os.ModeDir)
	client.CopyToRemote(src, dest)
}

func getHdfsSequins(t *testing.T) *sequins {
	backend := setupHdfs(t)
	s := getSequins(t, backend, "")

	require.NoError(t, s.init())
	return s
}

func TestHdfsBackend(t *testing.T) {
	h := setupHdfs(t)

	dbs, err := h.ListDBs()
	require.NoError(t, err, "it should be able to list dbs")
	assert.Equal(t, []string{"names"}, dbs, "the list of dbs should be correct")

	versions, err := h.ListVersions("names", false)
	require.NoError(t, err, "it should be able to list versions")
	assert.Equal(t, []string{"0", "1"}, versions, "it should be able to list versions")

	versions, err = h.ListVersions("names", true)
	require.NoError(t, err, "it should be able to list versions with a _SUCCESS file")
	assert.Equal(t, []string{"0"}, versions, "the list of versions with a _SUCCESS file should be correct")

	files, err := h.ListFiles("names", "0")
	require.NoError(t, err, "it should be able to list files")
	assert.Equal(t, []string{"part-00000", "part-00001"}, files, "the list of files should be correct")

	tearDownHdfs(t)
}

func TestHdfsSequins(t *testing.T) {
	ts := getHdfsSequins(t)

	req, _ := http.NewRequest("GET", "/names/Alice", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code, "fetching an existing key should 200")
	assert.Equal(t, "Practice", w.Body.String(), "fetching an existing key should return the value")
	assert.Equal(t, "1", w.HeaderMap.Get("X-Sequins-Version"), "when fetching an existing key, the X-Sequins-Version header should be set")

	req, _ = http.NewRequest("GET", "/names/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code, "fetching a nonexistent key should 404")
	assert.Equal(t, "", w.Body.String(), "fetching a nonexistent key should return no body")
	assert.Equal(t, "", w.HeaderMap.Get("X-Sequins-Version"), "when fetchin a nonexistent key, the X-Sequins-Version header shouldn't be set")

	req, _ = http.NewRequest("GET", "/names/", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	err := json.Unmarshal(w.Body.Bytes(), &dbStatus{})
	require.NoError(t, err, "fetching status should work and be valid")
	assert.Equal(t, 200, w.Code, "fetching status should work and be valid")
	// TODO

	tearDownHdfs(t)
}
