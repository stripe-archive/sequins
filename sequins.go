package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"github.com/NYTimes/gziphandler"

	"github.com/stripe/sequins/backend"
	"github.com/stripe/sequins/index"
)

type sequinsOptions struct {
	LocalPath           string
	CheckForSuccessFile bool
}

type sequins struct {
	options        sequinsOptions
	backend        backend.Backend
	indexReference index.IndexReference
	http           *http.Server
	started        time.Time
	updated        time.Time
	reloadLock     sync.Mutex
}

type status struct {
	Path    string `json:"path"`
	Started int64  `json:"started"`
	Updated int64  `json:"updated"`
	Count   int    `json:"count"`
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

	defer func() {
		s.indexReference.Replace(nil).Close()
	}()

	log.Printf("Listening on %s", address)
	return http.ListenAndServe(address, s.handler())
}

func (s *sequins) handler() http.Handler {
	return gziphandler.GzipHandler(s)
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

	// We can use unsafe ref, since closing the index would not affect the version string
	var currentVersion string
	currentIndex := s.indexReference.UnsafeGet()
	if currentIndex != nil {
		currentVersion = currentIndex.Version
	}

	if version != currentVersion {
		path := filepath.Join(s.options.LocalPath, version)

		err := os.Mkdir(path, 0700|os.ModeDir)
		if err != nil && !os.IsExist(err) {
			return err
		}

		if os.IsExist(err) {
			log.Printf("Version %s is already downloaded", version)
		} else {
			log.Printf("Downloading version %s from %s", version, s.backend.DisplayPath(version))
			err = s.download(version, path)
			if err != nil {
				return err
			}
		}

		log.Printf("Preparing version %s at %s", version, path)
		index := index.New(path, version)
		err = index.Load()
		if err != nil {
			return fmt.Errorf("Error while indexing: %s", err)
		}

		log.Printf("Switching to version %s!", version)

		oldIndex := s.indexReference.Replace(index)
		if oldIndex != nil {
			oldIndex.Close()
		}
	} else {
		log.Printf("%s is already the newest version, so not reloading.", version)
	}

	return nil
}

func (s *sequins) download(version, destPath string) (rterr error) {
	// To avoid loading an incomplete download (#12), download into a temp dir
	// then rename the temp dir to destPath only if all downloads succeed.
	baseDir := path.Dir(destPath)
	workDir, err := ioutil.TempDir(baseDir, fmt.Sprintf("version-%v", version))
	if err != nil {
		return err
	}
	defer func() {
		// Clean up the temp download dir in the event of a download error
		if err := os.RemoveAll(workDir); err != nil && !os.IsNotExist(err) {
			rterr = err
		}
	}()

	if err := s.backend.Download(version, workDir); err != nil {
		return err
	}

	if err := os.Rename(workDir, destPath); err != nil {
		return err
	}
	return nil
}

func (s *sequins) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		index := s.indexReference.Get()
		count, err := index.Count()
		currentVersion := index.Version
		s.indexReference.Release(index)

		if err != nil {
			log.Fatal(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		status := status{
			Path:    s.backend.DisplayPath(currentVersion),
			Version: currentVersion,
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

	currentIndex := s.indexReference.Get()
	res, err := currentIndex.Get(key)
	currentVersion := currentIndex.Version
	s.indexReference.Release(currentIndex)

	if err == index.ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		log.Fatal(fmt.Errorf("Error fetching value for %s: %s", key, err))
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		// Explicitly unset Content-Type, so ServeContent doesn't try to do any
		// sniffing.
		w.Header()["Content-Type"] = nil

		w.Header().Add("X-Sequins-Version", currentVersion)

		http.ServeContent(w, r, key, s.updated, bytes.NewReader(res))
	}
}
