package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/blocks"
	"github.com/stripe/sequins/sequencefile"
)

type sequinsOptions struct {
	localPath           string
	checkForSuccessFile bool
}

type sequins struct {
	options    sequinsOptions
	backend    backend.Backend
	blockStore blocks.BlockStoreReference
	http       *http.Server

	started        time.Time
	updated        time.Time
	reloadLock     sync.Mutex
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

func (s *sequins) start(address string) error {
	// TODO: we may need a more graceful way of shutting down, since this will
	// cause requests that start processing after this runs to 500
	// However, this may not be a problem, since you have to shift traffic to
	// another instance before shutting down anyway, otherwise you'd have downtime
	defer s.blockStore.Replace(nil, "").Close()

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

	version, err := s.backend.LatestVersion(s.options.checkForSuccessFile)
	if err != nil {
		return err
	}

	currentBlockStore, currentVersion := s.blockStore.Get()
	s.blockStore.Release(currentBlockStore)
	if version == currentVersion {
		log.Printf("%s is already the newest version, so not reloading.", version)
		return nil
	}

	files, err := s.backend.ListFiles(version)
	if err != nil {
		return err
	}

	path := filepath.Join(s.options.localPath, version)

	_, err = os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from existing manifest at", path)
		blockStore, err := blocks.NewFromManifest(path)
		if err == nil {
			s.updateBlockStore(blockStore, version)
			return nil
		} else {
			log.Println("Error loading version", version, "from manifest:", err)
		}
	}

	log.Println("Loading version", version, "from", s.backend.DisplayPath(version), "into local directory", path)

	log.Println("Clearing local directory", path)
	os.RemoveAll(path)
	err = os.MkdirAll(path, 0755 | os.ModeDir)
	if err != nil {
		return fmt.Errorf("Error creating local storage directory: %s", err)
	}

	blockStore := blocks.New(path, len(files), nil)
	for _, file := range files {
		log.Println("Reading records from", file, "at", s.backend.DisplayPath(version))

		stream, err := s.backend.Open(version, file)
		if err != nil {
			return fmt.Errorf("Error reading file %s: %s", file, err)
		}

		sf := sequencefile.New(stream)
		err = sf.ReadHeader()
		if err != nil {
			return fmt.Errorf("Error reading header from file %s: %s", file, err)
		}

		err = blockStore.AddFile(sf)
		if err != nil {
			return fmt.Errorf("Error loading version %s: %s", version, err)
		}
	}

	blockStore.SaveManifest()
	s.updateBlockStore(blockStore, version)
	return nil
}

func (s *sequins) updateBlockStore(blockStore *blocks.BlockStore, version string) {
	log.Printf("Switching to version %s!", version)
	oldBlockStore := s.blockStore.Replace(blockStore, version)
	if oldBlockStore != nil {
		oldBlockStore.Close()
	}
}

func (s *sequins) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		currentBlockStore, currentVersion := s.blockStore.Get()
		s.blockStore.Release(currentBlockStore)

		status := status{
			Path:    s.backend.DisplayPath(currentVersion),
			Version: currentVersion,
			Started: s.started.Unix(),
			Updated: s.updated.Unix(),
		}

		jsonBytes, err := json.Marshal(status)
		if err != nil {
			log.Println("Error serving status:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write(jsonBytes)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/")

	currentBlockStore, currentVersion := s.blockStore.Get()
	res, err := currentBlockStore.Get(key)
	s.blockStore.Release(currentBlockStore)

	if err == blocks.ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		log.Printf("Error fetching value for %s: %s\n", key, err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		// Explicitly unset Content-Type, so ServeContent doesn't try to do any
		// sniffing.
		w.Header()["Content-Type"] = nil

		w.Header().Add("X-Sequins-Version", currentVersion)

		http.ServeContent(w, r, key, s.updated, bytes.NewReader(res))
	}
}
