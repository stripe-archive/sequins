package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/backend"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func getSequins(t *testing.T, opts sequinsOptions) *sequins {
	os.RemoveAll("test_data/0/.manifest")
	os.RemoveAll("test_data/1/.manifest")
	backend := backend.NewLocalBackend("test_data")
	s := newSequins(backend, opts)

	require.NoError(t, s.init())
	return s
}

func TestSequins(t *testing.T) {
	ts := getSequins(t, sequinsOptions{"test_data", false})

	req, _ := http.NewRequest("GET", "/Alice", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "Practice", w.Body.String(), "Practice")
	assert.Equal(t, "1", w.HeaderMap.Get("X-Sequins-Version"))

	req, _ = http.NewRequest("GET", "/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
	assert.Equal(t, "", w.Body.String())
	assert.Equal(t, "", w.HeaderMap.Get("X-Sequins-Version"))

	req, _ = http.NewRequest("GET", "/", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	now := time.Now().Unix() - 1
	status := &status{}
	err := json.Unmarshal(w.Body.Bytes(), status)
	require.NoError(t, err)
	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "test_data/1", status.Path)
	assert.True(t, status.Started >= now)
	assert.Equal(t, 3, status.Count)
	assert.Equal(t, "1", status.Version)
}

func TestSequinsNoValidDirectories(t *testing.T) {
	backend := backend.NewLocalBackend("test_data/0")
	s := newSequins(backend, sequinsOptions{"test_data/0", false})
	err := s.init()
	assert.Error(t, err)
}

// TestSequinsThreadsafe makes sure that reads that occur during an update DTRT
func TestSequinsThreadsafe(t *testing.T) {
	scratch, err := ioutil.TempDir("", "sequins-")
	require.NoError(t, err)
	createTestIndex(t, scratch, 0)
	ts := newSequins(backend.NewLocalBackend(scratch), sequinsOptions{scratch, false})
	require.NoError(t, ts.init())

	var wg sync.WaitGroup
	threads := 50
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				req, err := http.NewRequest("GET", "/Alice", nil)
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
		ts.reloadLatest()
	}
	wg.Wait()
	require.NoError(t, os.RemoveAll(scratch))
}

func directoryCopy(t *testing.T, dest, src string) error {
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
		t.Logf("Copying %s -> %s\n", path, destfilename)

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
	path := fmt.Sprintf("%s/%d", scratch, i)
	src := fmt.Sprintf("test_data/%d/", i%2)

	require.NoError(t, directoryCopy(t, path, src))
}
