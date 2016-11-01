package zk

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
	maxCreateRetries    = 5
)

var defaultZkACL = zk.WorldACL(zk.PERM_ALL)

// A Watcher manages a single connection to zookeeper, watching for changes to
// directories and managing ephemeral nodes. It lazily connects and reconnects
// to zookeeper, and tries its best to be resilient to failures, but defaults to
// silently not providing updates.
type Watcher struct {
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

// Connect connects to the zookeeper cluster specified by zkServers, and returns
// a Watcher. All future operations on the Watcher are rooted at the given
// prefix.
func Connect(zkServers []string, prefix string, connectTimeout, sessionTimeout time.Duration) (*Watcher, error) {
	w := &Watcher{
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

func (w *Watcher) reconnect() error {
	var conn *zk.Conn
	var events <-chan zk.Event
	var err error

	for i, s := range w.zkServers {
		if strings.Index(s, ":") < 0 {
			w.zkServers[i] = fmt.Sprintf("%s:%d", s, defaultZKPort)
		}
	}

	w.Lock()
	defer w.Unlock()

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

	// TODO: recreate permanent paths? What if zookeeper dies and loses data?
	// TODO: clear data on setup? or just hope that it's uniquely namespaced enough
	err = w.createAll(w.prefix)
	if err != nil {
		return fmt.Errorf("creating base path: %s", err)
	}

	go func() {
		for ev := range events {
			if ev.State != zk.STATE_CONNECTED && ev.State != zk.STATE_CONNECTING {
				sendErr(w.errs, errors.New(ev.String()))
				return
			}
		}
	}()

	return nil
}

func (w *Watcher) runHooks() error {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	for node := range w.ephemeralNodes {
		err := w.createEphemeral(node)
		if err != nil {
			return err
		}
	}

	for node, wn := range w.watchedNodes {
		err := w.watchChildren(node, wn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Watcher) notifyDisconnected() {
	for _, wn := range w.watchedNodes {
		select {
		case wn.disconnected <- true:
		default:
		}
	}
}

func (w *Watcher) cancelWatches() {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	w.notifyDisconnected()

	for _, wn := range w.watchedNodes {
		wn.cancel <- true
	}
}

// run runs the main loop. On any errors, it resets the connection.
func (w *Watcher) run() {
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

			// Every time we connect, reset watches and recreate ephemeral nodes.
			err = w.runHooks()
			if err != nil {
				log.Println("Error running zookeeper hooks:", err)
				continue Reconnect
			}
		} else {
			first = false
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

// CreateEphemeral creates an ephemeral node on the zookeeper cluster. Any
// needed parent nodes are also created, as permanent nodes. The ephemeral is
// then recreated any time the Watcher reconnects.
func (w *Watcher) CreateEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	w.ephemeralNodes[node] = true
	err := w.createEphemeral(node)
	if err != nil {
		sendErr(w.errs, err)
	}
}

func (w *Watcher) createEphemeral(node string) error {
	w.RLock()
	defer w.RUnlock()

	// Retry a few times, in case the node is removed in between the two following
	// steps.
	for i := 0; i < maxCreateRetries; i++ {
		_, err := w.conn.Create(node, "", zk.EPHEMERAL, defaultZkACL)
		if err == nil {
			break
		} else if err != nil && !isNoNode(err) {
			return err
		}

		// Create the parent nodes.
		parent, _ := path.Split(node)
		err = w.createAll(parent)
		if err != nil {
			return fmt.Errorf("create %s: %s", node, err)
		}
	}

	return nil
}

// RemoveEphemeral removes the given ephemeral node from the zookeeper cluster.
// The node is then no longer recreated when the Watcher reconnects.
func (w *Watcher) RemoveEphemeral(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	w.RLock()
	defer w.RUnlock()

	node = path.Join(w.prefix, node)
	w.conn.Delete(node, -1)
	delete(w.ephemeralNodes, node)
}

// WatchChildren sets up a watch on the given node, and returns two channels.
// The first is updated whenever the list of children for the node changes; the
// second, whenever the cluster is disconnected. The watch persists through
// reconnects. Before the watch is set, WatchChildren creates the node and any
// parents.
func (w *Watcher) WatchChildren(node string) (chan []string, chan bool) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	updates := make(chan []string)
	disconnected := make(chan bool)
	cancel := make(chan bool)

	wn := watchedNode{updates: updates, disconnected: disconnected, cancel: cancel}
	w.watchedNodes[node] = wn
	err := w.watchChildren(node, wn)
	if err != nil {
		sendErr(w.errs, err)
		go func() {
			<-cancel
		}()
	}

	return updates, disconnected
}

func (w *Watcher) watchChildren(node string, wn watchedNode) error {
	w.RLock()
	defer w.RUnlock()

	children, _, events, err := w.childrenW(node)
	if err != nil {
		return err
	}

	go func() {
		// Normally, a watchChildren loop closes just so it can be reestablished
		// once we're reconnected to zookeeper. In that case wn.cancel just gets an
		// update, rather than being closed. If wn.cancel is closed, then
		// reconnecting gets set to false below, and we also close wn.updates and
		// wn.disconnected on our way out.
		reconnecting := true
		defer func() {
			if !reconnecting {
				close(wn.updates)
				close(wn.disconnected)
			}
		}()

		for {
			select {
			case reconnecting = <-wn.cancel:
				return
			case wn.updates <- children:
			}

			select {
			case reconnecting = <-wn.cancel:
				return
			case ev := <-events:
				if !ev.Ok() {
					sendErr(w.errs, errors.New(ev.String()))
					<-wn.cancel
					return
				}
			}

			w.RLock()
			children, _, events, err = w.childrenW(node)
			w.RUnlock()

			if err != nil {
				sendErr(w.errs, err)
				reconnecting = <-wn.cancel
				return
			}
		}
	}()

	return nil
}

func (w *Watcher) childrenW(node string) (children []string, stat *zk.Stat, events <-chan zk.Event, err error) {
	// Retry a few times, in case the node is removed in between the two following
	// steps.
	for i := 0; i < maxCreateRetries; i++ {
		children, stat, events, err = w.conn.ChildrenW(node)
		if !isNoNode(err) {
			return
		}

		// Create the node so we can watch it.
		err = w.createAll(node)
		if err != nil {
			err = fmt.Errorf("create %s: %s", node, err)
			return
		}
	}

	return
}

// RemoveWatch removes a watch previously set by WatchChildren.
func (w *Watcher) RemoveWatch(node string) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	if wn, ok := w.watchedNodes[node]; ok {
		delete(w.watchedNodes, node)
		close(wn.cancel)
	}
}

func (w *Watcher) createAll(node string) error {
	base, _ := path.Split(path.Clean(node))
	if base != "" && base != "/" {
		err := w.createAll(base)
		if err != nil {
			return err
		}
	}

	_, err := w.conn.Create(path.Clean(node), "", 0, defaultZkACL)
	if err != nil && !isNodeExists(err) {
		return err
	}

	return nil
}

// TriggerCleanup walks the prefix and deletes any non-ephemeral, empty
// nodes under it. It ignores any errors encountered.
func (w *Watcher) TriggerCleanup() {
	w.RLock()
	defer w.RUnlock()

	w.cleanupTree(w.prefix)
}

func (w *Watcher) cleanupTree(node string) {
	children, stat, err := w.conn.Children(node)
	if err != nil {
		return
	} else if stat.EphemeralOwner() != 0 {
		return
	}

	for _, child := range children {
		w.cleanupTree(path.Join(node, child))
	}

	w.conn.Delete(node, -1)
}

func (w *Watcher) Close() error {
	w.Lock()
	defer w.Unlock()

	w.shutdown <- true
	return w.conn.Close()
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

func isNoNode(err error) bool {
	if zkErr, ok := err.(*zk.Error); ok && zkErr.Code == zk.ZNONODE {
		return true
	}

	return false
}
