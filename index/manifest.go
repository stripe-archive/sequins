package index

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

const manifestVersion = 1

var ErrWrongVersion = errors.New("Wrong manifest version")

type manifest struct {
	Version int             `json:"version"`
	Files   []manifestEntry `json:"files"`
}

type manifestEntry struct {
	Name            string          `json:"name"`
	Size            int64           `json:"size"`
	IndexProperties indexProperties `json:"index_properties"`
}

type indexProperties struct {
	Sparse          bool          `json:"sparse"`
	PartitionType   partitionType `json:"partition_type"`
	PartitionNumber int           `json:"partition_number"`
	MinKey          []byte        `json:"min_key"`
	MaxKey          []byte        `json:"max_key"`
}

func readManifest(path string) (manifest, error) {
	m := manifest{}

	reader, err := os.Open(path)
	if err != nil {
		return m, err
	}

	defer reader.Close()
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return m, err
	}

	err = json.Unmarshal(bytes, &m)
	if err != nil {
		return m, err
	}

	if m.Version != manifestVersion {
		return m, ErrWrongVersion
	}

	return m, nil
}

func writeManifest(path string, m manifest) error {
	bytes, err := json.Marshal(m)
	if err != nil {
		return err
	}

	writer, err := os.Create(path)
	if err != nil {
		return err
	}

	defer writer.Close()
	_, err = writer.Write(bytes)
	return err
}
