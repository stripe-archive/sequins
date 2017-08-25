package backend

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Backend interface {
	// Lists all valid DBs (the first set of directories under the source root).
	ListDBs() ([]string, error)

	// ListVersions returns a sorted list of valid versions for the given db. If
	// after is possed, only versions greater than it will be returned. If
	// checkForSuccess is passed, only versions for which there is a _SUCCESS file
	// present will be returned.
	ListVersions(db, after string, checkForSuccessFile bool) ([]string, error)

	// ListFiles returns a sorted list of all valid-looking data files for a db
	// and version. It excludes files that begin with '_' or '.'.
	ListFiles(db, version string) ([]string, error)

	// Open returns an io.ReadCloser for a given file from a specific version
	// of a db.
	Open(db, version, file string) (io.ReadCloser, error)

	// DisplayPath returns a human-readable path indicating where the backend
	// is rooted.
	DisplayPath(parts ...string) string
}

// A basic backend for the local filesystem
type LocalBackend struct {
	path string
}

func NewLocalBackend(path string) *LocalBackend {
	return &LocalBackend{path}
}

func (lb *LocalBackend) ListDBs() ([]string, error) {
	files, err := ioutil.ReadDir(lb.path)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		res = append(res, f.Name())
	}

	return res, nil
}

func (lb *LocalBackend) ListVersions(db, after string, checkForSuccess bool) ([]string, error) {
	files, err := ioutil.ReadDir(filepath.Join(lb.path, db))
	if err != nil {
		return nil, err
	}

	var res []string
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		name := f.Name()
		fullPath := filepath.Join(lb.path, db, name)
		if name > after && (!checkForSuccess || lb.checkForSuccessFile(fullPath)) {
			res = append(res, name)
		}
	}

	return res, nil
}

func (lb *LocalBackend) ListFiles(db, version string) ([]string, error) {
	infos, err := ioutil.ReadDir(filepath.Join(lb.path, db, version))
	if err != nil {
		return nil, err
	}

	datasetSize := int64(0)
	numFiles := int64(len(infos))

	var res []string
	for _, info := range infos {
		name := info.Name()
		if !info.IsDir() && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
			res = append(res, filepath.Base(info.Name()))
		}
	}

	log.Printf("call_site=files.ListFiles sequins_db=%q sequins_db_version=%q dataset_size=%d file_count=%d", db, version, datasetSize, numFiles)

	return res, nil
}

func (lb *LocalBackend) Open(db, version, file string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(lb.path, db, version, file))
}

func (lb *LocalBackend) DisplayPath(parts ...string) string {
	allParts := append([]string{lb.path}, parts...)
	return filepath.Join(allParts...)
}

func (lb *LocalBackend) checkForSuccessFile(path string) bool {
	if _, err := os.Stat(filepath.Join(path, "_SUCCESS")); err == nil {
		return true
	} else {
		return false
	}
}
