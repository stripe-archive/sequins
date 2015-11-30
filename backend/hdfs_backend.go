package backend

import (
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/colinmarc/hdfs"
)

type HdfsBackend struct {
	client   *hdfs.Client
	namenode string
	path     string
}

func NewHdfsBackend(client *hdfs.Client, namenode string, hdfsPath string) *HdfsBackend {
	return &HdfsBackend{
		client:   client,
		namenode: namenode,
		path:     path.Clean(hdfsPath),
	}
}

func (h *HdfsBackend) LatestVersion(checkForSuccess bool) (string, error) {
	files, err := h.client.ReadDir(h.path)
	if err != nil {
		return "", err
	}

	for i := len(files) - 1; i >= 0; i-- {
		if files[i].IsDir() {
			name := files[i].Name()
			fullPath := path.Join(h.path, name)
			if !checkForSuccess || h.checkForSuccessFile(fullPath) {
				return name, nil
			}
		}
	}

	return "", fmt.Errorf("No valid versions at %s", h.displayURL(h.path))
}

func (h *HdfsBackend) ListFiles(version string) ([]string, error) {
	files, err := h.client.ReadDir(path.Join(h.path, version))
	if err != nil {
		return nil, err
	}

	res := make([]string, 0, len(files))
	for i, info := range files {
		if !info.IsDir() {
			name := info.Name()
			if !strings.HasPrefix(name, "_") || !strings.HasPrefix(name, ".") {
				res[i] = info.Name()
			}
		}
	}

	return res, nil
}

func (h *HdfsBackend) Open(version, file string) (io.ReadCloser, error) {
	src := path.Join(h.path, version, file)

	f, err := h.client.Open(src)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (h *HdfsBackend) DisplayPath(version string) string {
	return h.displayURL(h.path, version)
}

func (h *HdfsBackend) displayURL(pathElements ...string) string {
	p := strings.TrimPrefix(path.Join(pathElements...), "/")
	return fmt.Sprintf("hdfs://%s/%s", h.namenode, p)
}

func (h *HdfsBackend) checkForSuccessFile(versionPath string) bool {
	successPath := path.Join(versionPath, "_SUCCESS")
	if _, err := h.client.Stat(successPath); err == nil {
		return true
	} else {
		return false
	}
}
