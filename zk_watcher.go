package main

import (
	"fmt"
	"log"
	"path"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var defaultACL = zk.WorldACL(zk.PermAll)

// nullLogger is a noop logger for the zk client.
type nullLogger struct{}

func (n nullLogger) Printf(string, ...interface{}) {}

type zkHook func(conn *zk.Conn, errs chan error)

// A zkWatcher manages a single connection to zookeeper, watching for changes
// to directories or keys. It lazily connects and reconnects to zookeeper, and
// tries its best to be resilient to failures, but will default to not providing
// updates silently.
type zkWatcher struct {
	zkServers []string
	prefix    string
	conn      *zk.Conn
	hooks     []zkHook
	hooksLock sync.Mutex
	errs      chan error
	shutdown  chan bool
}

func connectZookeeper(zkServers []string, prefix string) (*zkWatcher, error) {
	w := &zkWatcher{
		zkServers: zkServers,
		prefix:    prefix,
		hooks:     make([]zkHook, 0),
		errs:      make(chan error),
		shutdown:  make(chan bool),
	}

	err := w.reconnect()
	if err != nil {
		return nil, err
	}

	err = w.setup()
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (w *zkWatcher) reconnect() error {
	conn, _, err := zk.Connect(w.zkServers, 100*time.Millisecond)
	if err != nil {
		return err
	}

	// Don't log anything ever.
	conn.SetLogger(nullLogger{})
	w.conn = conn
	return nil
}

// TODO: these maybe belong somewhere else
func (w *zkWatcher) setup() error {
	err := createZKPath(w.conn, path.Join(w.prefix, "nodes"))
	if err != nil {
		return err
	}

	err = createZKPath(w.conn, path.Join(w.prefix, "partitions"))
	if err != nil {
		return err
	}

	return nil
}

// sync runs the main loop. On any errors, it resets the connection.
func (w *zkWatcher) run() {
	for {
		log.Println("Connecting to zookeeper")
		w.reconnect()

		// Every time we connect, reset watches and recreate ephemeral nodes.
		for _, hook := range w.hooks {
			go hook(w.conn, w.errs)
		}

		select {
		case <-w.shutdown:
			break
		case <-w.errs:
		}

		time.Sleep(time.Second)
	}

	conn := w.conn
	if conn != nil {
		conn.Close()
	}
}

func (w *zkWatcher) addHook(hook zkHook) {
	w.hooksLock.Lock()
	defer w.hooksLock.Unlock()

	w.hooks = append(w.hooks, hook)
}

func (w *zkWatcher) watchChildren(node string) chan []string {
	updates := make(chan []string)

	w.addHook(func(conn *zk.Conn, errs chan error) {
		for {
			children, _, events, err := conn.ChildrenW(node)
			if err != nil {
				sendErr(errs, err)
				return
			}

			updates <- children
			ev := <-events
			if ev.Err != nil {
				sendErr(errs, ev.Err)
				return
			}
		}
	})

	return updates
}

func (w *zkWatcher) createEphemeral(node string) {
	node = path.Join(w.prefix, node)

	w.addHook(func(conn *zk.Conn, errs chan error) {
		_, err := w.conn.Create(node, nil, zk.FlagEphemeral, defaultACL)
		if err != nil && err != zk.ErrNodeExists {
			sendErr(errs, err)
		}
	})
}

// createZKPath creates a node and all its parents permanently.
func createZKPath(conn *zk.Conn, node string) error {
	base, _ := path.Split(path.Clean(node))
	if base != "" && base != "/" {
		err := createZKPath(conn, base)
		if err != nil {
			return err
		}
	}

	_, err := conn.Create(path.Clean(node), nil, 0, defaultACL)
	if err != nil && err != zk.ErrNodeExists {
		return fmt.Errorf("create %s: %s", node, err)
	}

	return nil
}

// sendErr sends the error over the channel, or discards it if the error is full.
func sendErr(errs chan error, err error) {
	log.Println("Zookeeper error:", err)

	select {
	case errs <- err:
	default:
	}
}
