package index

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type manifest struct {
	Files []manifestEntry `json:"files"`
	Count int             `json:"count"`
}

type manifestEntry struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
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
	return m, err
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
