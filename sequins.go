package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/stripe/sequins/backend"
)

type sequins struct {
	config  sequinsConfig
	http    *http.Server
	backend backend.Backend
	mux     *versionMux

	peers     *peers
	zkWatcher *zkWatcher

	started    time.Time
	updated    time.Time
	reloadLock sync.Mutex
}

type status struct {
	Path    string `json:"path"`
	Started int64  `json:"started"`
	Updated int64  `json:"updated"`
	Version string `json:"version"`
}

func newSequins(backend backend.Backend, config sequinsConfig) *sequins {
	return &sequins{
		config:     config,
		backend:    backend,
		reloadLock: sync.Mutex{},
		mux:        newVersionMux(),
	}
}

func (s *sequins) init() error {
	err := s.refresh()
	if err != nil {
		return err
	}

	now := time.Now()
	s.started = now
	s.updated = now

	return nil
}

func (s *sequins) initDistributed() error {
	zkWatcher, err := connectZookeeper(s.config.ZK.Servers, s.config.ZK.Prefix)
	if err != nil {
		return err
	}

	hostname := s.config.ZK.AdvertisedHostname
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return err
		}
	}

	_, port, err := net.SplitHostPort(s.config.Bind)
	if err != nil {
		return err
	}

	routableAddress := net.JoinHostPort(hostname, port)
	peers := watchPeers(zkWatcher, routableAddress)
	peers.waitToConverge(s.config.ZK.TimeToConverge.Duration)

	s.zkWatcher = zkWatcher
	s.peers = peers
	return nil
}

func (s *sequins) start() error {
	// TODO: We need to gracefully shutdown. Most of the time, we just
	// go down hard. We can use https://github.com/tylerb/graceful for
	// this.
	defer s.shutdown()

	log.Println("Listening on", s.config.Bind)
	return http.ListenAndServe(s.config.Bind, s)
}

func (s *sequins) shutdown() {
	// Swallow errors here.
	s.mux.prepare(nil)
	s.mux.upgrade().close()
	zk := s.zkWatcher
	if zk != nil {
		zk.close()
	}
}

func (s *sequins) reloadLatest() error {
	err := s.refresh()
	if err != nil {
		return err
	}

	// TODO: need to synchronize this memory write?
	s.updated = time.Now()
	return nil
}

func (s *sequins) refresh() error {
	s.reloadLock.Lock()
	defer s.reloadLock.Unlock()

	lasestVersion, err := s.backend.LatestVersion(s.config.RequireSuccessFile)
	if err != nil {
		return err
	}

	currentVersion := s.mux.getCurrent()
	s.mux.release(currentVersion)
	if currentVersion != nil && lasestVersion == currentVersion.name {
		// TODO: we log this a bunch uneccessarily.
		log.Printf("%s is already the newest version, so not reloading.", lasestVersion)
		return nil
	}

	files, err := s.backend.ListFiles(lasestVersion)
	if err != nil {
		return err
	}

	builder := newVersion(lasestVersion, len(files), s.peers, s.zkWatcher)
	vs, err := builder.build(s.backend, s.config.LocalStore)
	if err != nil {
		return err
	}

	// Prepare the version, so that during the switching period we can respond
	// to requests for it. TODO: this flow doesn't work for startup (but will
	// when we multiplex)
	s.mux.prepare(vs)

	// Then, wait for all our peers to be ready. All peers should all see that
	// everything is ready at roughly the same time. If they switch before us,
	// that's fine; we can serve the new version we've 'prepared' when peers
	// ask for it.
	vs.waitReady()

	// Now we can start serving requests for the new version to clients, as well.
	log.Printf("Switching to version %s!", vs.name)
	old := s.mux.upgrade()
	if old != nil {
		old.close()
	}

	return nil
}

func (s *sequins) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		currentVersion := s.mux.getCurrent()
		s.mux.release(currentVersion)

		status := status{
			Path:    s.backend.DisplayPath(currentVersion.name),
			Version: currentVersion.name,
			Started: s.started.Unix(),
			Updated: s.updated.Unix(), // TODO: this may be different from Last-Modified
		}

		jsonBytes, err := json.Marshal(status)
		if err != nil {
			log.Println("Error serving status:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header()["Content-Type"] = []string{"application/json"}
		w.Write(jsonBytes)
		return
	}

	s.mux.ServeHTTP(w, r)
}
