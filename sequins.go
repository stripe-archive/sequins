package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/blocks"
)

type sequinsOptions struct {
	address             string
	localPath           string
	checkForSuccessFile bool

	zkPrefix  string
	zkServers []string
	hostname  string
}

type sequins struct {
	options sequinsOptions
	http    *http.Server
	backend backend.Backend

	peers     *peers
	zkWatcher *zkWatcher

	dataset    datasetReference
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

func newSequins(backend backend.Backend, options sequinsOptions) *sequins {
	return &sequins{
		options:    options,
		backend:    backend,
		reloadLock: sync.Mutex{},
		dataset:    datasetReference{},
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
	// zkWatcher := connectZookeeper(s.options.zkServers, s.options.zkPrefix) TODO
	zkWatcher, err := connectZookeeper([]string{"localhost:2181"}, "/sequins-test")
	if err != nil {
		return err
	}

	hostname := s.options.hostname
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return err
		}
	}

	_, port, err := net.SplitHostPort(s.options.address)
	if err != nil {
		return err
	}

	routableAddress := net.JoinHostPort(hostname, port)
	peers := watchPeers(zkWatcher, routableAddress)
	peers.waitToConverge(10 * time.Second) // TODO configurable

	s.zkWatcher = zkWatcher
	s.peers = peers
	return nil
}

func (s *sequins) start() error {
	// TODO: we may need a more graceful way of shutting down, since this will
	// cause requests that start processing after this runs to 500
	// However, this may not be a problem, since you have to shift traffic to
	// another instance before shutting down anyway, otherwise you'd have downtime
	defer s.shutdown()

	log.Println("Listening on", s.options.address)
	return http.ListenAndServe(s.options.address, s)
}

func (s *sequins) shutdown() {
	// Swallow errors here.
	s.dataset.replace(nil).close()
	zk := s.zkWatcher
	if zk != nil{
		zk.close()
	}
}

func (s *sequins) reloadLatest() error {
	err := s.refresh()
	if err != nil {
		return err
	}

	s.updated = time.Now()
	return nil
}

func (s *sequins) refresh() error {
	s.reloadLock.Lock()
	defer s.reloadLock.Unlock()

	version, err := s.backend.LatestVersion(s.options.checkForSuccessFile)
	if err != nil {
		return err
	}

	ds := s.dataset.get()
	s.dataset.release(ds)
	if ds != nil && version == ds.version {
		log.Printf("%s is already the newest version, so not reloading.", version)
		return nil
	}

	files, err := s.backend.ListFiles(version)
	if err != nil {
		return err
	}

	ds = newDataset(version, len(files), s.peers, s.zkWatcher)
	err = ds.build(s.backend, s.options.localPath)
	if err != nil {
		return err
	}

	log.Printf("Switching to version %s!", ds.version)
	old := s.dataset.replace(ds)
	if old != nil {
		old.close()
	}

	return nil
}

func (s *sequins) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		ds := s.dataset.get()
		version := ds.version
		s.dataset.release(ds)

		status := status{
			Path:    s.backend.DisplayPath(version),
			Version: version,
			Started: s.started.Unix(),
			Updated: s.updated.Unix(),
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

	key := strings.TrimPrefix(r.URL.Path, "/")
	ds := s.dataset.get()
	res, err := ds.blockStore.Get(key)
	s.dataset.release(ds)

	if err == blocks.ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		log.Printf("Error fetching value for %s: %s\n", key, err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		// Explicitly unset Content-Type, so ServeContent doesn't try to do any
		// sniffing.
		w.Header()["Content-Type"] = nil
		w.Header().Add("X-Sequins-Version", ds.version)
		http.ServeContent(w, r, key, s.updated, bytes.NewReader(res))
	}
}
