package zk

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/zk/zktest"
)

func connectTest(t *testing.T) (*Watcher, *zk.TestCluster) {
	tzk := zktest.New(t)

	zkWatcher, err := Connect([]string{fmt.Sprintf("localhost:%d", tzk.Servers[0].Port)}, "/sequins-test", 5*time.Second, 5*time.Second)
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
	w, tzk := connectTest(t)
	defer w.Close()
	defer tzk.Stop()

	updates, _ := w.WatchChildren("/foo")
	go func() {
		w.CreateEphemeral("/foo/bar")
		time.Sleep(100 * time.Millisecond)
		w.RemoveEphemeral("/foo/bar")
	}()

	expectWatchUpdate(t, []string{}, updates, "the list of children should be updated to be empty first")
	expectWatchUpdate(t, []string{"bar"}, updates, "the list of children should be updated with the new node")
	expectWatchUpdate(t, []string{}, updates, "the list of children should be updated to be empty again")
}

func TestZKWatcherReconnect(t *testing.T) {
	w, tzk := connectTest(t)
	defer w.Close()
	defer tzk.Stop()

	updates, _ := w.WatchChildren("/foo")
	go func() {
		w.CreateEphemeral("/foo/bar")
		time.Sleep(100 * time.Millisecond)
		tzk.StopAllServers()
		tzk.StartAllServers()
		w.CreateEphemeral("/foo/baz")
	}()

	expectWatchUpdate(t, []string{}, updates, "the list of children should be updated to be empty first")
	expectWatchUpdate(t, []string{"bar"}, updates, "the list of children should be updated with the new node")
	expectWatchUpdate(t, []string{"bar", "baz"}, updates, "the list of children should be updated with the second new node")
}

/*
func TestZKWatchesCanceled(t *testing.T) {
	w, tzk := connectTest(t)
	defer w.Close()
	defer tzk.Close()

	w.WatchChildren("/foo")

	for i := 0; i < 3; i++ {
		tzk.Restart()
	}

	assert.Equal(t, 1, zk.CountPendingWatches(), "there should only be a single watch open")
}
*/
func TestZKRemoveWatch(t *testing.T) {
	w, tzk := connectTest(t)
	defer w.Close()
	defer tzk.Stop()

	updates, disconnected := w.WatchChildren("/foo")

	w.CreateEphemeral("/foo/bar")
	expectWatchUpdate(t, []string{}, updates, "the list of children should be updated to be empty first")
	expectWatchUpdate(t, []string{"bar"}, updates, "the list of children should be updated with the new node")

	w.RemoveWatch("/foo")

	// This is a sketchy way to make sure the updates channel gets closed.
	closed := make(chan bool)
	go func() {
		for range updates {
		}
		closed <- true
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-closed:
	case <-timer.C:
		assert.Fail(t, "the updates channel should be closed")
	}

	// And again for disconnected. This can't be a method, since updates and
	// disconnected don't have the same type.
	go func() {
		for range disconnected {
		}
		closed <- true
	}()

	timer.Reset(100 * time.Millisecond)
	select {
	case <-closed:
	case <-timer.C:
		assert.Fail(t, "the disconnected channel should be closed")
	}
}
