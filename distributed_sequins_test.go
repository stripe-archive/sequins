package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stripe/sequins/backend"
)

func getDistributedSequins(t *testing.T, backend backend.Backend, zkServers []string, index int) *sequins {
	tmpDir, err := ioutil.TempDir("", "sequins-")
	require.NoError(t, err)

	port := 9590 + index
	config := defaultConfig()
	config.Bind = fmt.Sprintf("localhost:%d", port)
	config.LocalStore = tmpDir
	config.MaxParallelLoads = 1
	config.ZK.Servers = zkServers
	config.ZK.TimeToConverge = duration{10 * time.Millisecond}
	config.ZK.ProxyTimeout = duration{10 * time.Millisecond}
	config.ZK.AdvertisedHostname = "localhost"

	return newSequins(backend, config)
}

func TestDistributedSequins(t *testing.T) {
	scratch, err := ioutil.TempDir("", "sequins-")
	expectedPath := filepath.Join(scratch, "names/1")
	backend := backend.NewLocalBackend(scratch)
	require.NoError(t, err, "setup")

	dst := filepath.Join(scratch, "names", "1")
	require.NoError(t, directoryCopy(t, dst, "test/names/1"), "setup")

	servers, _ := createTestZkCluster(t)
	sequinses := make([]*sequins, 7)
	var wg sync.WaitGroup
	for i := 0; i < 7; i++ {
		s := getDistributedSequins(t, backend, servers, i)
		sequinses[i] = s

		wg.Add(1)
		go func() {
			s.init()
			go s.start()
			wg.Done()
		}()
	}

	wg.Wait()

	// This is all a hack to wait until all DBs are ready.
	require.NoError(t, err)
	for _, s := range sequinses {
		for {
			s.dbsLock.RLock()
			db := s.dbs["names"]
			s.dbsLock.RUnlock()

			if db != nil && db.mux.getCurrent() != nil {
				break
			}

			time.Sleep(time.Millisecond)
		}
	}

	for _, s := range sequinses {
		testBasicSequins(t, s, expectedPath)
	}
}
