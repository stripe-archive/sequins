package backend

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type Backend interface {
	// Returns the latest valid version for the given dataset. If checkForSuccess
	// is passed, it will only return versions for which there is a _SUCCESS file
	// present.
	LatestVersion(checkForSuccessFile bool) (string, error)

	// ListFiles lists all valid-looking data files for a dataset version. It
	// excludes files that begin with '_' or '.'.
	ListFiles(version string) ([]string, error)

	// Open returns an io.ReadCloser for a given file from a specific version
	// of a dataset.
	Open(version string, file string) (io.ReadCloser, error)

	// DisplayPath returns a human-readable path for a specific version of a
	// dataset.
	DisplayPath(version string) string
}

// A basic backend for the local filesystem
type LocalBackend struct {
	path string
}

func NewLocalBackend(path string) *LocalBackend {
	return &LocalBackend{path}
}

func (lb *LocalBackend) LatestVersion(checkForSuccess bool) (string, error) {
	files, err := ioutil.ReadDir(lb.path)
	if err != nil {
		return "", err
	}

	// ReadDir returns sorted paths. pick the last directory
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].IsDir() {
			name := files[i].Name()
			fullPath := filepath.Join(lb.path, name)

			// Skip empty directories.
			children, err := ioutil.ReadDir(fullPath)
			if err != nil {
				return "", err
			} else if len(children) == 0 {
				continue
			}

			if !checkForSuccess || lb.checkForSuccessFile(fullPath) {
				return name, nil
			}
		}
	}

	return "", fmt.Errorf("No valid subdirectories in %s!", lb.path)
}

func (lb *LocalBackend) ListFiles(version string) ([]string, error) {
	infos, err := ioutil.ReadDir(filepath.Join(lb.path, version))
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(infos))
	for _, info := range infos {
		name := info.Name()
		if !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
			names = append(names, filepath.Base(info.Name()))
		}
	}

	return names, nil
}

func (lb *LocalBackend) Open(version, file string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(lb.path, version, file))
}

func (lb *LocalBackend) DisplayPath(version string) string {
	return filepath.Join(lb.path, version)
}

func (lb *LocalBackend) checkForSuccessFile(path string) bool {
	if _, err := os.Stat(filepath.Join(path, "_SUCCESS")); err == nil {
		return true
	} else {
		return false
	}
}
