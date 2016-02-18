package main

import (
	"errors"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var defaultACL = zk.WorldACL(zk.PermAll)

// nullLogger is a noop logger for the zk client.
type nullLogger struct{}

func (n nullLogger) Printf(string, ...interface{}) {}

func setNullLogger(c *zk.Conn) { c.SetLogger(nullLogger{}) }

// A zkWatcher manages a single connection to zookeeper, watching for changes
// to directories and managing ephemeral nodes. It lazily connects and
// reconnects to zookeeper, and tries its best to be resilient to failures, but
// defaults to silently not providing updates.
type zkWatcher struct {
	zkServers    []string
	prefix       string
	conn         *zk.Conn
	errs         chan error
	shutdown     chan bool
	shutdownOnce sync.Once

	hooksLock      sync.Mutex
	ephemeralNodes []string
	watchedNodes   map[string]chan []string
}

func connectZookeeper(zkServers []string, prefix string) (*zkWatcher, error) {
	w := &zkWatcher{
		zkServers:      zkServers,
		prefix:         prefix,
		errs:           make(chan error),
		shutdown:       make(chan bool),
		ephemeralNodes: make([]string, 0),
		watchedNodes:   make(map[string]chan []string),
	}

	err := w.reconnect()
	if err != nil {
		return nil, err
	}

	go w.run()
	return w, nil
}

func (w *zkWatcher) reconnect() error {
	log.Println("Connecting to zookeeper at", strings.Join(w.zkServers, ","))
	conn, events, err := zk.Connect(w.zkServers, 100*time.Millisecond, setNullLogger)
	if err != nil {
		return err
	}

	w.conn = conn

	// TODO: recreate permanent paths? What if zookeeper dies and loses data?
	// TODO: clear data on setup? or just hope that it's uniquely namespaced enough
	err = w.createPath("")
	if err != nil {
		return err
	}

	log.Println("Successfully connected to zookeeper!")

	go func() {
		for {
			ev := <-events
			if ev.Err != nil {
				sendErr(w.errs, ev.Err)
				return
			} else if ev.State == zk.StateDisconnected {
				sendErr(w.errs, errors.New("zk disconnected"))
				return
			}
		}
	}()

	return nil
}

// sync runs the main loop. On any errors, it resets the connection.
func (w *zkWatcher) run() {
	for {
		// Every time we connect, reset watches and recreate ephemeral nodes.
		w.hooksLock.Lock()
		for _, node := range w.ephemeralNodes {
			w.hookCreateEphemeral(node)
		}

		for node, updates := range w.watchedNodes {
			go w.hookWatchChildren(node, updates)
		}

		w.hooksLock.Unlock()

		select {
		case <-w.shutdown:
			break
		case <-w.errs:
		}

		time.Sleep(time.Second)
		w.reconnect()
	}
}

func (w *zkWatcher) createEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	w.ephemeralNodes = append(w.ephemeralNodes, node)
	w.hookCreateEphemeral(node)
}

func (w *zkWatcher) hookCreateEphemeral(node string) {
	_, err := w.conn.Create(node, nil, zk.FlagEphemeral, defaultACL)
	if err != nil && err != zk.ErrNodeExists {
		sendErr(w.errs, err)
	}
}

func (w *zkWatcher) watchChildren(node string) chan []string {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	updates := make(chan []string)

	w.watchedNodes[node] = updates
	go w.hookWatchChildren(node, updates)
	return updates
}

// TODO: I think in the case we restart but this didn't error, this will cause
// us to double-send updates
func (w *zkWatcher) hookWatchChildren(node string, updates chan []string) {
	for {
		children, _, events, err := w.conn.ChildrenW(node)
		if err != nil {
			sendErr(w.errs, err)
			return
		}

		updates <- children
		ev := <-events
		if ev.Err != nil {
			sendErr(w.errs, ev.Err)
			return
		}
	}
}

// createPath creates a node and all its parents permanently.
func (w *zkWatcher) createPath(node string) error {
	node = path.Join(w.prefix, node)
	err := w.createAll(node)
	if err != nil {
		return fmt.Errorf("create %s: %s", node, err)
	} else {
		return err
	}
}

func (w *zkWatcher) createAll(fullNode string) error {
	base, _ := path.Split(path.Clean(fullNode))
	if base != "" && base != "/" {
		err := w.createAll(base)
		if err != nil {
			return err
		}
	}

	_, err := w.conn.Create(path.Clean(fullNode), nil, 0, defaultACL)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	return nil
}

func (w *zkWatcher) close() {
	w.shutdownOnce.Do(func() {
		w.shutdown <- true
		w.conn.Close()
	})
}

// sendErr sends the error over the channel, or discards it if the error is full.
func sendErr(errs chan error, err error) {
	log.Println("Zookeeper error:", err)

	select {
	case errs <- err:
	default:
	}
}
