package index

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("That key doesn't exist.")

// An index is a wrapper for all the per-file indexes, providing an entry point
// to indexing datasets and fetching values from them.
type Index struct {
	Path    string
	Ready   bool
	Version string

	paths    []string
	files    []fileIndex
	refcount sync.WaitGroup
}

// New creates a new Index instance.
func New(path, version string) *Index {
	index := Index{
		Path:    path,
		Ready:   false,
		Version: version,
	}

	return &index
}

// Load reads each of the files and indexes them. If a manifest file already
// exists in the directory, it'll use that and load pre-built indexes instead.
func (index *Index) Load() error {
	paths, err := index.buildFileList()
	if err != nil {
		return err
	}

	index.paths = paths

	// Try loading and checking the manifest first.
	manifestPath := filepath.Join(index.Path, ".manifest")
	manifest, err := readManifest(manifestPath)
	if err == nil {
		log.Println("Loading index from existing manifest at", manifestPath)
		err = index.loadIndexFromManifest(manifest)
		if err != nil {
			log.Println("Failed to load existing manifest with error:", err)
		} else {
			index.Ready = true
			return nil
		}
	}

	// Either the manifest didn't exist or we failed to load from it, so build
	// a new index over the data.
	err = index.buildNewIndex()
	if err != nil {
		return err
	}

	index.Ready = true
	return nil
}

func (index *Index) buildFileList() ([]string, error) {
	infos, err := ioutil.ReadDir(index.Path)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(infos))
	for _, info := range infos {
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "_") && !strings.HasPrefix(info.Name(), ".") {
			paths = append(paths, filepath.Join(index.Path, info.Name()))
		}
	}

	return paths, nil
}

func (index *Index) loadIndexFromManifest(m manifest) error {
	index.files = make([]fileIndex, len(m.Files))

	for i, entry := range m.Files {
		if filepath.Base(index.paths[i]) != filepath.Base(entry.Name) {
			return fmt.Errorf("unmatched file: %s", entry.Name)
		}

		var fi fileIndex
		if entry.IndexProperties.Sparse {
			fi = newSparseFileIndex(index.paths[i], len(index.paths))
		} else {
			fi = newTotalFileIndex(index.paths[i], len(index.paths))
		}

		err := fi.load(entry)
		if err != nil {
			return err
		}

		index.files[i] = fi
	}

	return nil
}

func (index *Index) buildNewIndex() error {
	index.files = make([]fileIndex, len(index.paths))

	for i, path := range index.paths {
		log.Printf("Indexing %s...\n", path)

		fi, err := index.buildFileIndex(path)
		if err != nil {
			return err
		}

		index.files[i] = fi
	}

	manifest, err := index.buildManifest()
	if err != nil {
		return fmt.Errorf("error building manifest: %s", err)
	}

	manifestPath := filepath.Join(index.Path, ".manifest")
	log.Println("Writing manifest file to", manifestPath)
	err = writeManifest(manifestPath, manifest)
	if err != nil {
		return fmt.Errorf("error writing manifest: %s", err)
	}

	return nil
}

func (index *Index) buildFileIndex(path string) (fileIndex, error) {
	var fi fileIndex

	// First try building a sparse index, but fall back if the file is unsorted
	// TODO: if any of the files are unsorted, assume they all are
	fi = newSparseFileIndex(path, len(index.paths))
	err := fi.build()
	if err == errNotSorted {
		log.Println("Constructing a total index for", path, "as it isn't sorted")
		fi = newTotalFileIndex(path, len(index.paths))
		err = fi.build()
	}

	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (index *Index) buildManifest() (manifest, error) {
	m := manifest{
		Version: manifestVersion,
		Files:   make([]manifestEntry, len(index.files)),
	}

	for i, fi := range index.files {
		entry, err := fi.manifestEntry()
		if err != nil {
			return m, err
		}

		m.Files[i] = entry
	}

	return m, nil
}

// Get returns the value for a given key.
func (index *Index) Get(key string) ([]byte, error) {
	if !index.Ready {
		return nil, errors.New("Index isn't finished being built yet.")
	}

	keyBytes := []byte(key)

	// Each file has its own index (enabling useful local optimizations)
	// so try each one in sequence.
	for _, fi := range index.files {
		res, err := fi.get(keyBytes)
		if err != nil {
			return nil, err
		} else if res != nil {
			return res, nil
		}
	}

	return nil, ErrNotFound
}

// Close closes the index, and any open files it has.
func (index *Index) Close() {
	for _, f := range index.files {
		f.close()
	}
}
