package main

import (
	"errors"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	zk "launchpad.net/gozk/zookeeper"
)

const (
	coordinationVersion = "v1"
	zkReconnectPeriod   = 1 * time.Second
	defaultZKPort       = 2181
)

var defaultZkACL = zk.WorldACL(zk.PERM_ALL)

// A zkWatcher manages a single connection to zookeeper, watching for changes
// to directories and managing ephemeral nodes. It lazily connects and
// reconnects to zookeeper, and tries its best to be resilient to failures, but
// defaults to silently not providing updates.
type zkWatcher struct {
	sync.RWMutex
	zkServers      []string
	connectTimeout time.Duration
	sessionTimeout time.Duration
	prefix         string
	conn           *zk.Conn
	errs           chan error
	shutdown       chan bool

	hooksLock      sync.Mutex
	ephemeralNodes map[string]bool
	watchedNodes   map[string]watchedNode
}

type watchedNode struct {
	updates      chan []string
	disconnected chan bool
	cancel       chan bool
}

func connectZookeeper(zkServers []string, prefix string, connectTimeout, sessionTimeout time.Duration) (*zkWatcher, error) {
	w := &zkWatcher{
		zkServers:      zkServers,
		connectTimeout: connectTimeout,
		sessionTimeout: sessionTimeout,
		prefix:         path.Join(prefix, coordinationVersion),
		errs:           make(chan error, 1),
		shutdown:       make(chan bool),
		ephemeralNodes: make(map[string]bool),
		watchedNodes:   make(map[string]watchedNode),
	}

	err := w.reconnect()
	if err != nil {
		return nil, fmt.Errorf("Zookeeper error: %s", err)
	}

	go w.run()
	return w, nil
}

func (w *zkWatcher) reconnect() error {
	var conn *zk.Conn
	var events <-chan zk.Event
	var err error

	for i, s := range w.zkServers {
		if strings.Index(s, ":") < 0 {
			w.zkServers[i] = fmt.Sprintf("%s:%d", s, defaultZKPort)
		}
	}

	w.Lock()

	servers := strings.Join(w.zkServers, ",")
	log.Println("Connecting to zookeeper at", servers)
	conn, events, err = zk.Dial(servers, w.sessionTimeout)
	if err != nil {
		return err
	}

	if w.conn != nil {
		w.conn.Close()
	}
	w.conn = conn

	connectTimeout := time.NewTimer(w.connectTimeout)
	select {
	case <-connectTimeout.C:
		return errors.New("connection timeout")
	case event := <-events:
		if event.State != zk.STATE_CONNECTED {
			return fmt.Errorf("connection error: %s", event)
		}
	}

	w.Unlock()

	// TODO: recreate permanent paths? What if zookeeper dies and loses data?
	// TODO: clear data on setup? or just hope that it's uniquely namespaced enough
	err = w.createPath("")
	if err != nil {
		return err
	}

	go func() {
		for ev := range events {
			if !ev.Ok() {
				sendErr(w.errs, errors.New(ev.String()))
				return
			}
		}
	}()

	return nil
}

func (w *zkWatcher) runHooks() error {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	for node := range w.ephemeralNodes {
		err := w.hookCreateEphemeral(node)
		if err != nil {
			return err
		}
	}

	for node, wn := range w.watchedNodes {
		err := w.hookWatchChildren(node, wn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *zkWatcher) notifyDisconnected() {
	for _, wn := range w.watchedNodes {
		select {
		case wn.disconnected <- true:
		default:
		}
	}
}

func (w *zkWatcher) cancelWatches() {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	w.notifyDisconnected()

	for _, wn := range w.watchedNodes {
		wn.cancel <- true
	}
}

// sync runs the main loop. On any errors, it resets the connection.
func (w *zkWatcher) run() {
	first := true

Reconnect:
	for {
		if !first {
			// Wait before trying to reconnect again.
			wait := time.NewTimer(zkReconnectPeriod)
			select {
			case <-w.shutdown:
				break Reconnect
			case <-wait.C:
			}

			err := w.reconnect()
			if err != nil {
				log.Println("Error reconnecting to zookeeper:", err)
				continue Reconnect
			}
		} else {
			first = false
		}

		// Every time we connect, reset watches and recreate ephemeral nodes.
		err := w.runHooks()
		if err != nil {
			log.Println("Error running zookeeper hooks:", err)
			continue Reconnect
		}

		select {
		case <-w.shutdown:
			break Reconnect
		case err := <-w.errs:
			log.Println("Disconnecting from zookeeper because of error:", err)
			w.cancelWatches()
			continue Reconnect
		}
	}

	w.cancelWatches()
}

func (w *zkWatcher) createEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	w.ephemeralNodes[node] = true
	err := w.hookCreateEphemeral(node)
	if err != nil {
		sendErr(w.errs, err)
	}
}

func (w *zkWatcher) removeEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	w.RLock()
	defer w.RUnlock()

	node = path.Join(w.prefix, node)
	w.conn.Delete(node, -1)
	delete(w.ephemeralNodes, node)
}

func (w *zkWatcher) hookCreateEphemeral(node string) error {
	w.RLock()
	defer w.RUnlock()

	_, err := w.conn.Create(node, "", zk.EPHEMERAL, defaultZkACL)
	if err != nil {
		return err
	}

	return nil
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
	err := w.hookWatchChildren(node, wn)
	if err != nil {
		sendErr(w.errs, err)
	}

	return updates, disconnected
}

func (w *zkWatcher) removeWatch(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	if wn, ok := w.watchedNodes[node]; ok {
		delete(w.watchedNodes, node)

		close(wn.cancel)
		close(wn.updates)
		close(wn.disconnected)
	}
}

func (w *zkWatcher) hookWatchChildren(node string, wn watchedNode) error {
	w.RLock()
	defer w.RUnlock()

	children, _, events, err := w.conn.ChildrenW(node)
	if err != nil {
		return err
	}

	go func() {
		for {
			wn.updates <- children

			select {
			case <-wn.cancel:
				return
			case ev := <-events:
				if !ev.Ok() {
					sendErr(w.errs, errors.New(ev.String()))
					<-wn.cancel
					return
				}
			}

			w.RLock()
			children, _, events, err = w.conn.ChildrenW(node)
			w.RUnlock()

			if err != nil {
				sendErr(w.errs, err)
				<-wn.cancel
				return
			}
		}
	}()

	return nil
}

// createPath creates a node and all its parents permanently.
func (w *zkWatcher) createPath(node string) error {
	w.RLock()
	defer w.RUnlock()

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

	_, err := w.conn.Create(path.Clean(fullNode), "", 0, defaultZkACL)
	if err != nil && !isNodeExists(err) {
		return err
	}

	return nil
}

func (w *zkWatcher) close() {
	w.Lock()
	defer w.Unlock()

	w.shutdown <- true
	w.conn.Close()
}

// sendErr sends the error over the channel, or discards it if the error is full.
func sendErr(errs chan error, err error) {
	log.Println("Zookeeper error:", err)

	select {
	case errs <- err:
	default:
	}
}

func isNodeExists(err error) bool {
	if zkErr, ok := err.(*zk.Error); ok && zkErr.Code == zk.ZNODEEXISTS {
		return true
	}

	return false
}
