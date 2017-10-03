package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
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

	infos, _ := ioutil.ReadDir("test_databases/healthy/baby-names/1")
	sourceDest := path.Join("/", "_test_sequins", "baby-names")
	for _, info := range infos {
		putHDFS(client, path.Join(sourceDest, "0", info.Name()), filepath.Join("test_databases/healthy/baby-names/1", info.Name()))
		putHDFS(client, path.Join(sourceDest, "1", info.Name()), filepath.Join("test_databases/healthy/baby-names/1", info.Name()))
	}

	client.CreateEmptyFile(path.Join(sourceDest, "0", "_SUCCESS"))
	return backend.NewHdfsBackend(client, nn, "/_test_sequins")
}

func tearDownHdfs(t *testing.T) {
	nn := os.Getenv("HADOOP_NAMENODE")

	client, err := hdfs.New(nn)
	if err != nil {
		t.Fatal(err)
	}

	client.Remove("/_test_sequins")
}

func putHDFS(client *hdfs.Client, dest, src string) {
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
	assert.Equal(t, []string{"baby-names"}, dbs, "the list of dbs should be correct")

	versions, err := h.ListVersions("baby-names", "", false)
	require.NoError(t, err, "it should be able to list versions")
	assert.Equal(t, []string{"0", "1"}, versions, "it should be able to list versions")

	versions, err = h.ListVersions("baby-names", "", true)
	require.NoError(t, err, "it should be able to list versions with a _SUCCESS file")
	assert.Equal(t, []string{"0"}, versions, "the list of versions with a _SUCCESS file should be correct")

	files, err := h.ListFiles("baby-names", "0")
	require.NoError(t, err, "it should be able to list files")
	assert.Equal(t, 5, len(files), "the list of files should be correct")

	tearDownHdfs(t)
}

func TestHdfsSequins(t *testing.T) {
	ts := getHdfsSequins(t)
	testBasicSequins(t, ts, fmt.Sprintf("hdfs://%s/_test_sequins/baby-names/1", os.Getenv("HADOOP_NAMENODE")))
	tearDownHdfs(t)
}
