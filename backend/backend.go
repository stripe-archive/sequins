package backend

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Backend interface {
	LatestVersion(checkForSuccessFile bool) (string, error)
	Download(version string, destPath string) error
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
			if !checkForSuccess || lb.checkForSuccessFile(fullPath) {
				return name, nil
			}
		}
	}

	return "", fmt.Errorf("No valid subdirectories in %s!", lb.path)
}

func (lb *LocalBackend) Download(version string, destPath string) error {
	return nil // noop
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
