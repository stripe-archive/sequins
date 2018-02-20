package zk

import (
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/zk/zktest"
)

const defaultReconnect = 5 * time.Second

func connectTest(t *testing.T, reconnect time.Duration) (*Watcher, *zk.TestCluster) {
	tzk := zktest.New(t)

	zkWatcher, err := Connect([]string{fmt.Sprintf("localhost:%d", tzk.Servers[0].Port)}, "/sequins-test", 5*time.Second, reconnect)
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
	w, tzk := connectTest(t, defaultReconnect)
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
	w, tzk := connectTest(t, defaultReconnect)
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
	w, tzk := connectTest(t, defaultReconnect)
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

func simulateError(w *Watcher) {
	sendErr("testing errors", w.errs, errors.New("test error"), "test path", "test server")
	// Wait for reconnect
	time.Sleep(25 * time.Millisecond)
}

func TestZKFlapping(t *testing.T) {
	w, tzk := connectTest(t, 10*time.Millisecond)
	defer tzk.Stop()
	defer w.Close()

	flapNotify := w.SetFlapThreshold(5, time.Second)

	for i := 0; i < 4; i++ {
		simulateError(w)
	}
	select {
	case <-flapNotify:
		assert.Fail(t, "Small number of flaps should not trigger notification")
	case <-time.After(100 * time.Millisecond):
	}

	time.Sleep(time.Second)
	for i := 0; i < 4; i++ {
		simulateError(w)
	}
	select {
	case <-flapNotify:
		assert.Fail(t, "Flaps should expire")
	case <-time.After(100 * time.Millisecond):
	}

	for i := 0; i < 5; i++ {
		simulateError(w)
	}
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Flaps in a short period should notify")
	case <-flapNotify:
	}
}
