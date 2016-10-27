package blocks

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

const manifestVersion = 3

var ErrWrongVersion = errors.New("wrong manifest version")

type Manifest struct {
	Version            int             `json:"version"`
	Blocks             []BlockManifest `json:"blocks"`
	NumPartitions      int             `json:"num_partitions"`
	SelectedPartitions []int           `json:"selected_partitions"`
	Compression        Compression     `json:"compression"`
	BlockSize          int             `json:"block_size"`
}

type BlockManifest struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Partition int    `json:"partition"`
	Count     int    `json:"count"`
	MinKey    []byte `json:"min_key"`
	MaxKey    []byte `json:"max_key"`
}

func readManifest(path string) (Manifest, error) {
	m := Manifest{}

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

	// TODO: remove
	if m.Compression == "" {
		m.Compression = SnappyCompression
	}

	// TODO: this too
	if m.SelectedPartitions == nil {
		m.SelectedPartitions = make([]int, m.NumPartitions)
		for i := range m.SelectedPartitions {
			m.SelectedPartitions[i] = i
		}
	}

	return m, nil
}

func writeManifest(path string, m Manifest) error {
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
