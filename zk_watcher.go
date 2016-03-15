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

// TODO testable

const coordinationVersion = "v1"

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
	zkServers []string
	prefix    string
	conn      *zk.Conn
	errs      chan error
	shutdown  chan bool

	hooksLock      sync.Mutex
	ephemeralNodes map[string]bool
	watchedNodes   map[string]watchedNode
}

type watchedNode struct {
	updates      chan []string
	disconnected chan bool
	cancel       chan bool
}

func connectZookeeper(zkServers []string, prefix string) (*zkWatcher, error) {
	w := &zkWatcher{
		zkServers:      zkServers,
		prefix:         path.Join(prefix, coordinationVersion),
		errs:           make(chan error, 1),
		shutdown:       make(chan bool),
		ephemeralNodes: make(map[string]bool),
		watchedNodes:   make(map[string]watchedNode),
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

	if w.conn != nil {
		w.conn.Close()
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
		for node := range w.ephemeralNodes {
			log.Println("creating ephemeral node", node)
			w.hookCreateEphemeral(node)
		}

		for node, wn := range w.watchedNodes {
			go w.hookWatchChildren(node, wn)
		}

		w.hooksLock.Unlock()

		select {
		case <-w.shutdown:
			break
		case <-w.errs:
		}

		w.hooksLock.Lock()
		for _, wn := range w.watchedNodes {
			select {
			case wn.disconnected <- true:
			default:
			}

			wn.cancel <- true
		}
		w.hooksLock.Unlock()

		time.Sleep(time.Second)
		w.reconnect()
	}
}

func (w *zkWatcher) createEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	w.ephemeralNodes[node] = true
	w.hookCreateEphemeral(node)
}

func (w *zkWatcher) removeEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	w.conn.Delete(node, -1)
	delete(w.ephemeralNodes, node)
}

func (w *zkWatcher) hookCreateEphemeral(node string) {
	_, err := w.conn.Create(node, nil, zk.FlagEphemeral, defaultACL)
	if err != nil && err != zk.ErrNodeExists {
		sendErr(w.errs, err)
	}
}

func (w *zkWatcher) watchChildren(node string) (chan []string, chan bool) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	updates := make(chan []string)
	disconnected := make(chan bool)
	cancel := make(chan bool)

	wn := watchedNode{updates: updates, disconnected: disconnected, cancel: cancel}
	w.watchedNodes[node] = wn
	go w.hookWatchChildren(node, wn)
	return updates, disconnected
}

func (w *zkWatcher) removeWatch(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	if wn, ok := w.watchedNodes[node]; ok {
		delete(w.watchedNodes, node)

		wn.cancel <- true
		close(wn.updates)
		close(wn.disconnected)
	}
}

func (w *zkWatcher) hookWatchChildren(node string, wn watchedNode) {
	for {
		children, _, events, err := w.conn.ChildrenW(node)
		if err != nil {
			sendErr(w.errs, err)
			return
		}

		wn.updates <- children

		select {
		case ev := <-events:
			if ev.Err != nil {
				sendErr(w.errs, ev.Err)
				<-wn.cancel
				return
			}
		case <-wn.cancel:
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
	w.shutdown <- true
	w.conn.Close()
}

// sendErr sends the error over the channel, or discards it if the error is full.
func sendErr(errs chan error, err error) {
	if err != zk.ErrClosing {
		log.Println("Zookeeper error:", err)
	}

	select {
	case errs <- err:
	default:
	}
}
