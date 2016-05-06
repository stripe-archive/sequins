package main

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

func randomPort() int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Int31n(6000) + 16000)
}

type testZK struct {
	*testing.T
	dir  string
	addr string
	zk   *zk.Server
}

func (zk testZK) printLogs() {
	log, _ := ioutil.ReadFile(filepath.Join(zk.dir, "log.txt"))
	zk.T.Logf("===== ZOOKEEPER LOGS:\n%s", log)
}

func (zk testZK) close() {
	zk.printLogs()
	zk.zk.Destroy()
}

func createTestZk(t *testing.T) testZK {
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

	return testZK{
		T:    t,
		dir:  dir,
		addr: fmt.Sprintf("127.0.0.1:%d", port),
		zk:   zk,
	}
}

func connectZookeeperTest(t *testing.T) (*zkWatcher, testZK) {
	zk := createTestZk(t)
	defer zk.close()

	zkWatcher, err := connectZookeeper([]string{zk.addr}, "/test-sequins", 5*time.Second, 10*time.Second)
	require.NoError(t, err, "zkWatcher should connect")

	return zkWatcher, zk
}

func TestZKWatcher(t *testing.T) {
	// TODO this test needs to be fleshed out
	connectZookeeperTest(t)
}
