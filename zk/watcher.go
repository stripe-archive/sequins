package zk

import (
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	coordinationVersion = "v1"
	defaultZKPort       = 2181
	maxCreateRetries    = 5
)

var defaultZkACL = zk.WorldACL(zk.PermAll)

// A Watcher manages a single connection to zookeeper, watching for changes to
// directories and managing ephemeral nodes. It lazily connects and reconnects
// to zookeeper, and tries its best to be resilient to failures, but defaults to
// silently not providing updates.
type Watcher struct {
	lock           sync.RWMutex
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

	// How much flapping is allowed before we just die?
	flapLock     sync.Mutex
	flapMax      uint
	flapDuration time.Duration
	flaps        []time.Time
	flapNotify   chan bool
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
	var err error

	for i, s := range w.zkServers {
		if strings.Index(s, ":") < 0 {
			w.zkServers[i] = fmt.Sprintf("%s:%d", s, defaultZKPort)
		}
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	servers := strings.Join(w.zkServers, ",")
	log.Println("sequins ZK connecting to zookeeper at ", servers)
	conn, _, err = zk.Connect(w.zkServers, w.sessionTimeout)
	if err != nil {
		return err
	}

	if w.conn != nil {
		w.conn.Close()
	}
	w.conn = conn

	err = w.createAll(w.prefix)
	if err != nil {
		return fmt.Errorf("creating base path: %s", err)
	}

	// Every time we connect, reset watches and recreate ephemeral nodes.
	err = w.runHooks()
	if err != nil {
		return fmt.Errorf("zookeeper hooks error: %v", err)
	}

	return nil
}

// Must be called with `w.lock` locked for write.
func (w *Watcher) runHooks() error {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	log.Println("sequins ZK recreating ephemeral nodes")
	for node := range w.ephemeralNodes {
		err := w.createEphemeral(node)
		if err != nil {
			return err
		}
	}

	log.Println("sequins ZK re-adding watches")
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

func (w *Watcher) SetFlapThreshold(max uint, duration time.Duration) chan bool {
	w.flapLock.Lock()
	defer w.flapLock.Unlock()

	w.flapMax = max
	w.flapDuration = duration
	w.flapNotify = make(chan bool)
	return w.flapNotify
}

// Update that we flapped, and check if we exceeded our threshold.
func (w *Watcher) isFlapping() bool {
	w.flapLock.Lock()
	defer w.flapLock.Unlock()

	if w.flapNotify == nil {
		return false
	}

	now := time.Now()

	var newFlaps []time.Time
	for _, flap := range w.flaps {
		if now.Sub(flap) < w.flapDuration {
			newFlaps = append(newFlaps, flap)
		}
	}
	newFlaps = append(newFlaps, now)
	w.flaps = newFlaps
	flapCount := uint(len(newFlaps))

	log.Printf("sequins ZK flap check: flaps=%d flapMax=%d flapDuration=%v\n", flapCount, w.flapMax, w.flapDuration)
	return flapCount > w.flapMax
}

// run runs the main loop. On any errors, it resets the connection.
func (w *Watcher) run() {
	first := true

Reconnect:
	for {
		if !first {
			if w.isFlapping() {
				w.lock.Lock()
				defer w.lock.Unlock()
				log.Println("sequins ZK flapping, shutting down watcher")
				w.flapNotify <- true
				w.shutdown = nil
				return
			}

			// Wait before trying to reconnect again.
			// Let's wait longer than the ZK client.
			wait := time.NewTimer(w.sessionTimeout * 2)
			select {
			case <-w.shutdown:
				break Reconnect
			case <-wait.C:
			}

			err := w.reconnect()
			if err != nil && w.conn.State() == zk.StateDisconnected {
				log.Println("sequins ZK error reconnecting to zookeeper:", err)
				continue Reconnect
			}
		} else {
			first = false
		}

		select {
		case <-w.shutdown:
			break Reconnect
		case err := <-w.errs:
			log.Println("sequins ZK disconnecting from zookeeper because of error:", err)
			w.cancelWatches()
			continue Reconnect
		}
	}

	w.cancelWatches()
}

func (w *Watcher) CreateIDAssignmentLock() *zk.Lock {
	node := path.Join(w.prefix, "id_assignment_lock")

	w.lock.RLock()
	defer w.lock.RUnlock()

	return zk.NewLock(w.conn, node, zk.WorldACL(zk.PermAll))
}

// CreateEphemeral creates an ephemeral node on the zookeeper cluster. Any
// needed parent nodes are also created, as permanent nodes. The ephemeral is
// then recreated any time the Watcher reconnects.
func (w *Watcher) CreateEphemeral(node string) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	w.ephemeralNodes[node] = true
	err := w.createEphemeral(node)
	// It's fine if we're currently reconnecting to zookeeper, since `runHooks`
	// will create the node for us.
	if err != nil && err != zk.ErrClosing {
		sendErr("creating new ephmeral node", w.errs, err, node, "")
	}
}

// Must be called with `w.lock` locked (either for read or write).
func (w *Watcher) createEphemeral(node string) error {
	// Retry a few times, in case the node is removed in between the two following
	// steps.
	for i := 0; i < maxCreateRetries; i++ {
		_, err := w.conn.Create(node, []byte{}, zk.FlagEphemeral, defaultZkACL)
		if err == nil {
			return nil
		} else if err != zk.ErrNoNode && err != zk.ErrNodeExists {
			return err
		}

		// Create the parent nodes.
		parent, _ := path.Split(node)
		err = w.createAll(parent)
		if err != nil {
			return fmt.Errorf("create %s: %s", node, err)
		}
	}

	return fmt.Errorf("failed to create ephemeral znode %s after %d retries", node, maxCreateRetries)
}

// RemoveEphemeral removes the given ephemeral node from the zookeeper cluster.
// The node is then no longer recreated when the Watcher reconnects.
func (w *Watcher) RemoveEphemeral(node string) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	if err := w.conn.Delete(node, -1); err != nil {
		log.Printf("sequins ZK failed to remove ephemeral znode %s", node)
	}
	delete(w.ephemeralNodes, node)
}

// WatchChildren sets up a watch on the given node, and returns two channels.
// The first is updated whenever the list of children for the node changes; the
// second, whenever the cluster is disconnected. The watch persists through
// reconnects. Before the watch is set, WatchChildren creates the node and any
// parents.
func (w *Watcher) WatchChildren(node string) (chan []string, chan bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	node = path.Join(w.prefix, node)
	updates := make(chan []string)
	disconnected := make(chan bool)
	cancel := make(chan bool)

	wn := watchedNode{updates: updates, disconnected: disconnected, cancel: cancel}
	w.watchedNodes[node] = wn
	err := w.watchChildren(node, wn)
	// It's fine if we're currently reconnecting to zookeeper, since `runHooks`
	// will set up the watch for us.
	if err != nil && err != zk.ErrClosing {
		sendErr("adding watch", w.errs, err, node, "")
		go func() {
			<-cancel
		}()
	}

	return updates, disconnected
}

// Must be called with `w.lock` locked (either for read or write).
func (w *Watcher) watchChildren(node string, wn watchedNode) error {
	children, _, events, err := w.childrenW(node)
	if err != nil {
		return err
	}

	go func() {
		// Use current timestamp in nanoseconds as the goroutine id for logging purpose
		id := time.Now().Nanosecond()
		sessionID := w.conn.SessionID()
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
				if ev.Err != nil {
					where := fmt.Sprintf("checking for watch results in goroutine %d with zk session %d",
						id, sessionID)
					sendErr(where, w.errs, ev.Err, ev.Path, ev.Server)
					<-wn.cancel
					return
				}
			}

			w.lock.RLock()
			children, _, events, err = w.childrenW(node)
			w.lock.RUnlock()

			if err != nil {
				where := fmt.Sprintf("re-adding watch in goroutine %d with zk session %d",
					id, w.conn.SessionID())
				sendErr(where, w.errs, err, node, "")
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
		if err != zk.ErrNoNode {
			return
		}

		// Create the node so we can watch it.
		err = w.createAll(node)
		if err != nil {
			err = fmt.Errorf("create %s: %s", node, err)
			return
		}
	}

	err = fmt.Errorf("failed to watch children on %s after %d retries", node, maxCreateRetries)
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
		log.Printf("sequins ZK removing watch on znode %s", node)
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

	_, err := w.conn.Create(path.Clean(node), []byte{}, 0, defaultZkACL)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	return nil
}

// TriggerCleanup walks the prefix and deletes any non-ephemeral, empty
// nodes under it. It ignores any errors encountered.
func (w *Watcher) TriggerCleanup() {
	w.lock.RLock()
	defer w.lock.RUnlock()

	log.Printf("sequins ZK cleaning up znodes in %s", w.prefix)
	w.cleanupTree(w.prefix)
	log.Printf("sequins ZK finished cleaning up znodes in %s", w.prefix)
}

func (w *Watcher) cleanupTree(node string) {
	children, stat, err := w.conn.Children(node)
	if err != nil {
		return
	} else if stat.EphemeralOwner != 0 {
		return
	} else if stat.Ctime > (time.Now().Unix() - 3600) * 1000 {
		// Skip the znodes created within last hour to avoid the race with creating
		// the znode path for new db versions.
		return
	}

	for _, child := range children {
		w.cleanupTree(path.Join(node, child))
	}

	// Do not even attempt to delete if this znode has children
	if len(children) != 0 {
		return
	}

	if err = w.conn.Delete(node, -1); err == nil {
		log.Printf("sequins ZK deleted znode %s", node)
	}
}

func (w *Watcher) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.shutdown != nil {
		w.shutdown <- true
	}
	w.conn.Close()
}

// sendErr sends the error over the channel, or discards it if the error is full.
func sendErr(where string, errs chan error, err error, path string, server string) {
	log.Printf("sequins ZK Zookeeper error while %s: err=%q, path=%q, server=%q", where, err, path, server)

	select {
	case errs <- err:
	default:
	}
}
