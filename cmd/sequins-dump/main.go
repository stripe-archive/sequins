package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/stripe/sequins/sequencefile"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string

	keys   = kingpin.Flag("keys", "Display keys.").Short('k').Bool()
	values = kingpin.Flag("values", "Display values.").Short('v').Bool()

	path = kingpin.Arg("PATH", "Path to dump").Required().String()
)

func main() {
	kingpin.Version("sequins-dump version " + version)
	kingpin.Parse()

	// By default, display keys and values.
	if !*keys && !*values {
		*keys = true
		*values = true
	}

	stat, err := os.Stat(*path)
	if err != nil {
		fatal(err)
	}

	if stat.IsDir() {
		infos, err := ioutil.ReadDir(*path)
		if err != nil {
			fatal(err)
		}

		for _, info := range infos {
			name := info.Name()
			if name[0] != '.' && name[0] != '_' {
				dump(filepath.Join(*path, name))
			}
		}
	} else {
		dump(*path)
	}
}

func dump(path string) {
	f, err := os.Open(path)
	if err != nil {
		fatal(err)
	}

	reader := sequencefile.New(f)
	err = reader.ReadHeader()
	if err != nil {
		fatal(err)
	}

	for reader.Scan() {
		row := make([]string, 0)

		if *keys {
			row = append(row, string(reader.Key()))
		}

		if *values {
			row = append(row, string(reader.Value()))
		}

		fmt.Println(strings.Join(row, "\t"))
	}

	if err = reader.Err(); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
