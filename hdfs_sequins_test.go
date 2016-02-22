package main

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/backend"
)

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

	client, err := hdfs.New(nn)
	if err != nil {
		t.Fatal(err)
	}

	client.Remove("/_test_sequins")
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
	testBasicSequins(t, ts, fmt.Sprintf("hdfs://%s/_test_sequins/test/names/1", os.Getenv("HADOOP_NAMENODE")))
	tearDownHdfs(t)
}
