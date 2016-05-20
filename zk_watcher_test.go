package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	zk "launchpad.net/gozk/zookeeper"
)

func randomPort() int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Int31n(6000) + 16000)
}

type testZK struct {
	*testing.T
	home string
	dir  string
	port int
	addr string
	zk   *zk.Server
}

func (tzk testZK) printLogs() {
	log, _ := ioutil.ReadFile(filepath.Join(tzk.dir, "log.txt"))
	tzk.T.Logf("===== ZOOKEEPER LOGS:\n%s", log)
}

func (tzk *testZK) start() {
	err := tzk.zk.Start()
	require.NoError(tzk.T, err, "zk start")
	time.Sleep(time.Second)
}

func (tzk *testZK) close() {
	tzk.printLogs()
	tzk.zk.Destroy()
}

func (tzk *testZK) restart() {
	tzk.zk.Stop()
	time.Sleep(time.Second)
	tzk.start()
}

func createTestZk(t *testing.T) *testZK {
	zkHome := os.Getenv("ZOOKEEPER_HOME")
	if zkHome == "" {
		t.Skip("Skipping zk tests because ZOOKEEPER_HOME isn't set")
	}

	dir, err := ioutil.TempDir("", "sequins-zk")
	require.NoError(t, err, "zk setup")

	port := randomPort()
	zk, err := zk.CreateServer(port, dir, zkHome)
	require.NoError(t, err, "zk setup")

	tzk := testZK{
		T:    t,
		home: zkHome,
		dir:  dir,
		port: port,
		addr: fmt.Sprintf("127.0.0.1:%d", port),
		zk:   zk,
	}

	tzk.start()
	return &tzk
}

func connectZookeeperTest(t *testing.T) (*zkWatcher, *testZK) {
	tzk := createTestZk(t)

	zkWatcher, err := connectZookeeper([]string{tzk.addr}, "/sequins-test", 5*time.Second, 5*time.Second)
	require.NoError(t, err, "zkWatcher should connect")

	return zkWatcher, tzk
}

func expectWatchUpdate(t *testing.T, expected []string, updates chan []string, msg string) {
	sort.Strings(expected)
	timer := time.NewTimer(20 * time.Second)
	select {
	case update := <-updates:
		sort.Strings(update)
		assert.Equal(t, expected, update, msg)
	case <-timer.C:
		require.FailNow(t, "timed out waiting for update")
	}
}

func TestZKWatcher(t *testing.T) {
	w, tzk := connectZookeeperTest(t)
	defer w.close()
	defer tzk.close()

	err := w.createPath("/foo")
	require.NoError(t, err, "createPath should work")

	updates, _ := w.watchChildren("/foo")
	go func() {
		w.createEphemeral("/foo/bar")
		time.Sleep(100 * time.Millisecond)
		w.removeEphemeral("/foo/bar")
	}()

	expectWatchUpdate(t, nil, updates, "the list of children should be updated to be empty first")
	expectWatchUpdate(t, []string{"bar"}, updates, "the list of children should be updated with the new node")
	expectWatchUpdate(t, nil, updates, "the list of children should be updated to be empty again")
}

func TestZKWatcherReconnect(t *testing.T) {
	w, tzk := connectZookeeperTest(t)
	defer w.close()
	defer tzk.close()

	err := w.createPath("/foo")
	require.NoError(t, err, "createPath should work")

	updates, _ := w.watchChildren("/foo")
	go func() {
		w.createEphemeral("/foo/bar")
		time.Sleep(100 * time.Millisecond)
		tzk.restart()
		w.createEphemeral("/foo/baz")
	}()

	expectWatchUpdate(t, nil, updates, "the list of children should be updated to be empty first")
	expectWatchUpdate(t, []string{"bar"}, updates, "the list of children should be updated with the new node")
	expectWatchUpdate(t, []string{"bar", "baz"}, updates, "the list of children should be updated with the second new node")
}

func TestZKWatchesCanceled(t *testing.T) {
	w, tzk := connectZookeeperTest(t)
	defer w.close()
	defer tzk.close()

	err := w.createPath("/foo")
	require.NoError(t, err, "createPath should work")

	w.watchChildren("/foo")

	for i := 0; i < 3; i++ {
		tzk.restart()
	}

	assert.Equal(t, 1, zk.CountPendingWatches(), "there should only be a single watch open")
}
