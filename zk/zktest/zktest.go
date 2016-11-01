// package zktest provides utilities for testing against a live zookeeper.
package zktest

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	zk "launchpad.net/gozk/zookeeper"
)

// TestCluster is a wrapper around a temporary zookeeper.
type TestCluster struct {
	*testing.T
	Addr string

	home string
	dir  string
	port int
	zk   *zk.Server
}

// New returns a new TestCluster rooted at a temporary dir.
func New(t *testing.T) *TestCluster {
	zkHome := os.Getenv("ZOOKEEPER_HOME")
	if zkHome == "" {
		t.Skip("Skipping zk tests because ZOOKEEPER_HOME isn't set")
	}

	dir, err := ioutil.TempDir("", "sequins-zk")
	require.NoError(t, err, "zk setup")

	port := RandomPort()
	zk, err := zk.CreateServer(port, dir, zkHome)
	require.NoError(t, err, "zk setup")

	tzk := TestCluster{
		T:    t,
		home: zkHome,
		dir:  dir,
		port: port,
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
		zk:   zk,
	}

	tzk.Start()
	return &tzk
}

// Start starts the zookeeper cluster.
func (tzk *TestCluster) Start() {
	err := tzk.zk.Start()
	require.NoError(tzk.T, err, "zk start")
	time.Sleep(time.Second)
}

// Close shuts down and cleans up the zookeeper cluster.
func (tzk *TestCluster) Close() {
	log, err := ioutil.TempFile("", "sequins-test-zookeeper-")
	require.NoError(tzk.T, err, "setup: copying log")
	log.Close()

	err = os.Rename(filepath.Join(tzk.dir, "log.txt"), log.Name())
	require.NoError(tzk.T, err, "setup: copying log")

	tzk.T.Logf("Zookeeper output at %s", log.Name())
	tzk.zk.Destroy()
}

// Restart stops the cluster, then brings it back up again after a second.
func (tzk *TestCluster) Restart() {
	tzk.zk.Stop()
	time.Sleep(time.Second)
	tzk.Start()
}

// RandomPort returns a port in the range 16000-22000.
func RandomPort() int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Int31n(6000) + 16000)
}
