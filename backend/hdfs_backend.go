package backend

import (
	"fmt"
	"log"
	"path"
	"path/filepath"
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

func (h *HdfsBackend) Download(version string, destPath string) error {
	versionPath := path.Join(h.path, version)
	files, err := h.client.ReadDir(versionPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		src := path.Join(versionPath, file.Name())
		dest := filepath.Join(destPath, file.Name())

		log.Printf("Downloading %s to %s", h.displayURL(src), dest)
		err = h.client.CopyToLocal(src, dest)
		if err != nil {
			return err
		}
	}

	return nil
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
