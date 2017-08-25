package backend

import (
	"fmt"
	"io"
	"log"
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

func (h *HdfsBackend) ListDBs() ([]string, error) {
	files, err := h.client.ReadDir(h.path)
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

func (h *HdfsBackend) ListVersions(db, after string, checkForSuccess bool) ([]string, error) {
	files, err := h.client.ReadDir(path.Join(h.path, db))
	if err != nil {
		return nil, err
	}

	var res []string
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		name := f.Name()
		fullPath := path.Join(h.path, db, name)
		if name > after && (!checkForSuccess || h.checkForSuccessFile(fullPath)) {
			res = append(res, name)
		}
	}

	return res, nil
}

func (h *HdfsBackend) ListFiles(db, version string) ([]string, error) {
	infos, err := h.client.ReadDir(path.Join(h.path, db, version))
	if err != nil {
		return nil, err
	}

	datasetSize := int64(0)
	numFiles := int64(len(infos))

	var res []string
	for _, info := range infos {
		name := info.Name()
		if !info.IsDir() && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
			res = append(res, path.Base(info.Name()))
		}
		datasetSize += info.Size()
	}

	log.Printf("call_site=hdfs.ListFiles sequins_db=%q sequins_db_version=%q dataset_size=%d file_count=%d", db, version, datasetSize, numFiles)

	return res, nil
}

func (h *HdfsBackend) Open(db, version, file string) (io.ReadCloser, error) {
	src := path.Join(h.path, db, version, file)

	f, err := h.client.Open(src)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (h *HdfsBackend) DisplayPath(parts ...string) string {
	allParts := append([]string{h.path}, parts...)
	return h.displayURL(allParts...)
}

func (h *HdfsBackend) displayURL(parts ...string) string {
	p := strings.TrimPrefix(path.Join(parts...), "/")
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
