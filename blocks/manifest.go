package blocks

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

const manifestVersion = 2

var ErrWrongVersion = errors.New("Wrong manifest version")

type manifest struct {
	Version            int             `json:"version"`
	Blocks             []blockManifest `json:"blocks"`
	NumPartitions      int             `json:"num_partitions"`
	SelectedPartitions []int           `json:"selected_partitions,omitempty"`
}

type blockManifest struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Partition int    `json:"partition"`
	Count     int    `json:"count"`
	MinKey    []byte `json:"min_key"`
	MaxKey    []byte `json:"max_key"`
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
