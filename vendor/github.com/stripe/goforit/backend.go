package goforit

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"time"
)

type Backend interface {
	// Refresh returns a new set of flags.
	// It also returns the age of these flags, or an empty time if no age is known.
	Refresh() (map[string]Flag, time.Time, error)
}

type csvFileBackend struct {
	filename string
}

type jsonFileBackend struct {
	filename string
}

type JSONFormat struct {
	Flags       []Flag  `json:"flags"`
	UpdatedTime float64 `json:"updated"`
}

func readFile(file string, backend string, parse func(io.Reader) (map[string]Flag, time.Time, error)) (map[string]Flag, time.Time, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer f.Close()
	return parse(f)
}

func (b jsonFileBackend) Refresh() (map[string]Flag, time.Time, error) {
	return readFile(b.filename, "json", parseFlagsJSON)
}

func (b csvFileBackend) Refresh() (map[string]Flag, time.Time, error) {
	return readFile(b.filename, "csv", parseFlagsCSV)
}

func flagsToMap(flags []Flag) map[string]Flag {
	flagsMap := map[string]Flag{}
	for _, flag := range flags {
		flagsMap[flag.Name] = Flag{Name: flag.Name, Rate: flag.Rate}
	}
	return flagsMap
}

func parseFlagsCSV(r io.Reader) (map[string]Flag, time.Time, error) {
	// every row is guaranteed to have 2 fields
	const FieldsPerRecord = 2

	cr := csv.NewReader(r)
	cr.FieldsPerRecord = FieldsPerRecord
	cr.TrimLeadingSpace = true

	rows, err := cr.ReadAll()
	if err != nil {
		return nil, time.Time{}, err
	}

	flags := map[string]Flag{}
	for _, row := range rows {
		name := row[0]

		rate, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			// TODO also track somehow
			rate = 0
		}

		flags[name] = Flag{Name: name, Rate: rate}
	}
	return flags, time.Time{}, nil
}

func parseFlagsJSON(r io.Reader) (map[string]Flag, time.Time, error) {
	dec := json.NewDecoder(r)
	var v JSONFormat
	err := dec.Decode(&v)
	if err != nil {
		return nil, time.Time{}, err
	}
	return flagsToMap(v.Flags), time.Unix(int64(v.UpdatedTime), 0), nil
}

// BackendFromFile is a helper function that creates a valid
// FlagBackend from a CSV file containing the feature flag values.
// If the same flag is defined multiple times in the same file,
// the last result will be used.
func BackendFromFile(filename string) Backend {
	return csvFileBackend{filename}
}

// BackendFromJSONFile creates a backend powered by JSON file
// instead of CSV
func BackendFromJSONFile(filename string) Backend {
	return jsonFileBackend{filename}
}
