package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	zk "launchpad.net/gozk/zookeeper"
)

func randomPort() int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Int31n(6000) + 16000)
}

func createTestZk(t *testing.T) (string, *zk.Server) {
	zkHome := os.Getenv("ZOOKEEPER_HOME")
	if zkHome == "" {
		t.Skip("Skipping zk tests because ZOOKEEPER_HOME isn't set")
	}

	dir, err := ioutil.TempDir("", "sequins-zk")
	require.NoError(t, err, "zk setup")

	port := randomPort()
	zk, err := zk.CreateServer(port, dir, zkHome)
	require.NoError(t, err, "zk setup")

	err = zk.Start()
	require.NoError(t, err, "zk setup")

	// Give it time to start up.
	time.Sleep(5 * time.Second)

	return fmt.Sprintf("127.0.0.1:%d", port), zk
}

func connectZookeeperTest(t *testing.T) (*zkWatcher, *zk.Server) {
	addr, zk := createTestZk(t)
	defer zk.Destroy()

	zkWatcher, err := connectZookeeper([]string{addr}, "/test-sequins")
	require.NoError(t, err, "zkWatcher should connect")

	return zkWatcher, zk
}

func TestZKWatcher(t *testing.T) {
	// TODO this test needs to be fleshed out
	connectZookeeperTest(t)
}
