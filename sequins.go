package main

import (
	"encoding/json"
	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/index"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type sequinsOptions struct {
	LocalPath           string
	CheckForSuccessFile bool
}

type sequins struct {
	options        sequinsOptions
	backend        backend.Backend
	index          *index.Index
	http           *http.Server
	currentVersion string
	started        time.Time
	updated        time.Time
	reloadLock     sync.Mutex
}

type status struct {
	Path    string `json:"path"`
	Started int64  `json:"started"`
	Updated int64  `json:"updated"`
	Count   int    `json:"count"`
}

func newSequins(backend backend.Backend, options sequinsOptions) *sequins {
	return &sequins{
		options:    options,
		backend:    backend,
		reloadLock: sync.Mutex{},
	}
}

func (s *sequins) start(address string) error {
	err := s.refresh()
	if err != nil {
		return err
	}

	now := time.Now()
	s.started = now
	s.updated = now

	defer s.index.Close()

	log.Printf("Listening on %s", address)
	return http.ListenAndServe(address, s)
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

	version, err := s.backend.LatestVersion(s.options.CheckForSuccessFile)
	if err != nil {
		return err
	}

	if version != s.currentVersion {
		path := filepath.Join(s.options.LocalPath, version)

		err := os.Mkdir(path, 0700|os.ModeDir)
		if err != nil && !os.IsExist(err) {
			return err
		}

		if os.IsExist(err) {
			log.Printf("Version %s is already downloaded.", version)
		} else {
			log.Printf("Downloading version %s from %s.", version, s.backend.DisplayPath(version))
			err = s.backend.Download(version, path)
			if err != nil {
				return err
			}
		}

		log.Printf("Building index over version %s at %s.", version, path)
		index := index.New(path)
		err = index.BuildIndex()
		if err != nil {
			return err
		}

		log.Println("Switching to new directory!")
		s.currentVersion = version
		s.index = index
	} else {
		log.Printf("%s is already the newest version, so not reloading.", version)
	}

	return nil
}

func (s *sequins) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		count, err := s.index.Count()
		if err != nil {
			log.Fatal(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		status := status{
			Path:    s.backend.DisplayPath(s.currentVersion),
			Started: s.started.Unix(),
			Updated: s.updated.Unix(),
			Count:   count,
		}

		jsonBytes, err := json.Marshal(status)
		if err != nil {
			log.Fatal(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write(jsonBytes)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/")
	res, err := s.index.Get(key)
	if err == index.ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		http.ServeContent(w, r, key, s.updated, res)
	}
}
