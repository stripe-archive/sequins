package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colinmarc/sequencefile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stripe/sequins/backend"
)

type tuple struct {
	key   string
	value string
}

var babyNames []tuple

func init() {
	infos, _ := ioutil.ReadDir("test/baby-names/1")
	for _, info := range infos {
		f, _ := os.Open(filepath.Join("test/baby-names/1", info.Name()))
		defer f.Close()

		r := sequencefile.NewReader(f)
		r.ReadHeader()

		for r.Scan() {
			babyNames = append(babyNames, tuple{
				string(sequencefile.BytesWritable(r.Key())),
				string(sequencefile.BytesWritable(r.Value())),
			})
		}
	}
}

func getSequins(t *testing.T, backend backend.Backend, localStore string) *sequins {
	if localStore == "" {
		tmpDir, err := ioutil.TempDir("", "sequins-")
		require.NoError(t, err)

		localStore = tmpDir
	}

	config := defaultConfig()
	config.Bind = "localhost:9599"
	config.LocalStore = localStore
	config.MaxParallelLoads = 1

	s := newSequins(backend, config)
	require.NoError(t, s.init())

	// This is a hack to wait until all DBs are ready.
	dbs, err := backend.ListDBs()
	require.NoError(t, err)
	for _, dbName := range dbs {
		for {
			s.dbsLock.RLock()
			db := s.dbs[dbName]
			s.dbsLock.RUnlock()

			if db != nil {
				current := db.mux.getCurrent()
				db.mux.release(current)
				if current != nil {
					break
				}
			}

			time.Sleep(time.Millisecond)
		}
	}

	return s
}

func testBasicSequins(t *testing.T, ts *sequins, expectedDBPath string) {
	for i := 0; i < 20; i++ {
		tuple := babyNames[rand.Intn(len(babyNames))]

		req, _ := http.NewRequest("GET", fmt.Sprintf("/baby-names/%s", tuple.key), nil)
		w := httptest.NewRecorder()
		ts.ServeHTTP(w, req)

		assert.Equal(t, 200, w.Code, "fetching an existing key (%s) should 200", tuple.key)
		assert.Equal(t, tuple.value, w.Body.String(), "fetching an existing key (%s) should return the value", tuple.key)
		assert.Equal(t, "1", w.HeaderMap.Get(versionHeader), "when fetching an existing key, the sequins version header should be set")
	}

	req, _ := http.NewRequest("GET", "/baby-names/foo", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code, "fetching a nonexistent key should 404")
	assert.Equal(t, "", w.Body.String(), "fetching a nonexistent key should return no body")
	assert.Equal(t, "1", w.HeaderMap.Get(versionHeader), "when fetchin a nonexistent key, the sequins version header should still be set")

	req, _ = http.NewRequest("GET", "/otherdb/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code, "fetching from a nonexistent db should 404")
	assert.Equal(t, "", w.Body.String(), "fetching from a nonexistent db should return no body")
	assert.Equal(t, "", w.HeaderMap.Get(versionHeader), "when fetching from a nonexistent db, the sequins version header shouldn't be set")

	testBasicStatus(t, ts, expectedDBPath)
}

func testBasicStatus(t *testing.T, ts *sequins, expectedDBPath string) {
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	status := status{}
	err := json.Unmarshal(w.Body.Bytes(), &status)
	require.NoError(t, err, "fetching global status should work and be valid")
	assert.Equal(t, 200, w.Code, "fetching global status should work and be valid")

	require.Equal(t, 1, len(status.DBs), "there should be a single db")
	validateStatus(t, status.DBs["baby-names"], expectedDBPath)

	req, _ = http.NewRequest("GET", "/baby-names/", nil)
	req.Header.Set("Accept", "application/json")
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	dbStatus := dbStatus{}
	err = json.Unmarshal(w.Body.Bytes(), &dbStatus)
	require.NoError(t, err, "fetching db status should work and be valid")
	assert.Equal(t, 200, w.Code, "fetching db status should work and be valid")
	validateStatus(t, dbStatus, expectedDBPath)
}

func validateStatus(t *testing.T, status dbStatus, expectedPath string) {
	require.Equal(t, 1, len(status.Versions), "dbStatus should have one version registered")

	versionStatus := status.Versions["1"]
	assert.Equal(t, expectedPath, versionStatus.Path, "versionStatus path should be correct")
	for _, nodeStatus := range versionStatus.Nodes {
		assert.True(t, nodeStatus.Current, "dbStatus should be registered as the current one")
	}
}

func TestSequins(t *testing.T) {
	scratch, err := ioutil.TempDir("", "sequins-")
	require.NoError(t, err, "setup")

	dst := filepath.Join(scratch, "baby-names", "1")
	require.NoError(t, directoryCopy(t, dst, "test/baby-names/1"), "setup: copy data")

	backend := backend.NewLocalBackend(scratch)
	ts := getSequins(t, backend, "")
	testBasicSequins(t, ts, filepath.Join(scratch, "baby-names/1"))
}

func TestEmptySequins(t *testing.T) {
	scratch, err := ioutil.TempDir("", "sequins-")
	require.NoError(t, err, "setup")

	dst := filepath.Join(scratch, "baby-names", "1")
	require.NoError(t, os.MkdirAll(dst, os.ModeDir|0777), "setup: mkdir")

	backend := backend.NewLocalBackend(scratch)
	ts := getSequins(t, backend, "")
	testBasicStatus(t, ts, filepath.Join(scratch, "baby-names/1"))

	req, _ := http.NewRequest("GET", "/baby-names/foo", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code, "fetching a nonexistent key should 404")
	assert.Equal(t, "", w.Body.String(), "fetching a nonexistent key should return no body")
	assert.Equal(t, "1", w.HeaderMap.Get(versionHeader), "when fetchin a nonexistent key, the sequins version header should still be set")
}

func TestVeryEmptySequins(t *testing.T) {
	scratch, err := ioutil.TempDir("", "sequins-")
	require.NoError(t, err, "setup")

	backend := backend.NewLocalBackend(scratch)
	ts := getSequins(t, backend, "")

	req, _ := http.NewRequest("GET", "/baby-names/foo", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code, "fetching a nonexistent key should 404")
	assert.Equal(t, "", w.Body.String(), "fetching a nonexistent key should return no body")
}

// TestSequinsThreadsafe makes sure that reads that occur during an update DTRT
func TestSequinsThreadsafe(t *testing.T) {
	scratch, err := ioutil.TempDir("", "sequins-")
	require.NoError(t, err)
	createTestIndex(t, scratch, 0)

	config := defaultConfig()
	config.Source = scratch
	config.LocalStore = filepath.Join(scratch, "blocks")
	config.RequireSuccessFile = true
	ts := newSequins(backend.NewLocalBackend(filepath.Join(scratch, "data")), config)
	require.NoError(t, ts.init())

	var wg sync.WaitGroup
	threads := 50
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				req, err := http.NewRequest("GET", "/names/Alice", nil)
				assert.NoError(t, err)

				w := httptest.NewRecorder()
				ts.ServeHTTP(w, req)

				if w.Code != 200 && w.Code != 404 {
					// we might get either response, depending on which version of the db we see
					// we just want to make sure that we don't 500 (or anything else)
					t.Errorf("Got error response when making a request (status=%d)", w.Code)
				}
			}
			wg.Done()
		}()
	}

	for i := 1; i < 100; i++ {
		createTestIndex(t, scratch, i)
		ts.refreshAll()
	}
	wg.Wait()
	os.RemoveAll(scratch)
}

func directoryCopy(t *testing.T, dest, src string) error {
	t.Logf("Copying %s -> %s\n", src, dest)
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		// Swallow errors, since sometimes other jobs will clean up the .manifest partway through
		// the directory copy, which causes errors
		if err != nil {
			return nil
		} else if strings.HasPrefix(filepath.Base(path), ".") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		} else if info.IsDir() {
			return nil
		}

		reldir, err := filepath.Rel(src, filepath.Dir(path))
		require.NoError(t, err)
		if err != nil {
			return err
		}

		err = os.MkdirAll(filepath.Join(dest, reldir), 0755)
		require.NoError(t, err)
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		require.NoError(t, err)
		if err != nil {
			return err
		}

		destfilename := filepath.Join(dest, rel)
		srcfile, err := os.Open(path)
		require.NoError(t, err)
		if err != nil {
			return err
		}
		defer srcfile.Close()

		destfile, err := os.Create(destfilename)
		require.NoError(t, err)
		if err != nil {
			return err
		}
		defer destfile.Close()

		_, err = io.Copy(destfile, srcfile)
		require.NoError(t, err)
		return err
	})
}

func createTestIndex(t *testing.T, scratch string, i int) {
	t.Logf("Creating test version %d\n", i)
	path := fmt.Sprintf("%s/data/baby-names/%d", scratch, i)

	require.NoError(t, directoryCopy(t, path, "test/baby-names/1"))
	_, err := os.Create(filepath.Join(path, "_SUCCESS"))
	require.NoError(t, err)
}
