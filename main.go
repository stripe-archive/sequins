package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"path/filepath"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3" // TODO: use aws-sdk-go
	"github.com/stripe/sequins/backend"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	sequinsVersion string

	bind       = kingpin.Flag("bind", "Address to bind to. Overrides the config option of the same name.").Short('b').PlaceHolder("ADDRESS").String()
	root       = kingpin.Flag("root", "Where the sequencefiles are. Overrides the config option of the same name.").Short('r').PlaceHolder("URI").String()
	localStore = kingpin.Flag("local-store", "Where to store local data. Overrides the config option of the same name.").Short('l').PlaceHolder("PATH").String()
	configPath = kingpin.Flag("config", "The config file to use. By default, either sequins.conf in the local directory or /etc/sequins.conf will be used.").PlaceHolder("PATH").String()
	pprof      = kingpin.Flag("pprof", "Address to bind to for pprof, which provides profiling information over HTTP.").PlaceHolder("ADDRESS").String()
)

func main() {
	kingpin.Version("sequins version " + sequinsVersion)
	kingpin.Parse()

	if *pprof != "" {
		go func() {
			log.Println("Starting pprof server at", *pprof)
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	config, err := loadConfig(*configPath)
	if err == errNoConfig {
		// If --root was specified, we can just use that and the default config.
		if *root != "" {
			config = defaultConfig()
		} else {
			log.Fatal("No config file found! Please see the README for instructions.")
		}
	} else if err != nil {
		log.Fatal("Error loading config:", err)
	}

	if *root != "" {
		config.Root = *root
	}

	if *bind != "" {
		config.Bind = *bind
	}

	if *localStore != "" {
		config.LocalStore = *localStore
	}

	parsed, err := url.Parse(config.Root)
	if err != nil {
		log.Fatal(err)
	}

	var s *sequins
	switch parsed.Scheme {
	case "", "file":
		// TODO: don't allow distributed setup with local paths
		s = localSetup(config.Root, config)
	case "s3":
		s = s3Setup(parsed.Host, parsed.Path, config)
	case "hdfs":
		s = hdfsSetup(parsed.Host, parsed.Path, config)
	}

	// Do a basic test that the backend is valid.
	dbs, err := s.backend.ListDBs()
	if err != nil {
		log.Fatalf("Error listing DBs from %s: %s", s.backend.DisplayPath(""), err)
	} else if len(dbs) == 0 {
		log.Fatal("No data found at ", s.backend.DisplayPath(""))
	}

	err = s.init()
	if err != nil {
		log.Fatal(err)
	}

	s.start()
}

func localSetup(localPath string, config sequinsConfig) *sequins {
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		log.Fatal(err)
	}

	backend := backend.NewLocalBackend(absPath)
	return newSequins(backend, config)
}

func s3Setup(bucketName string, path string, config sequinsConfig) *sequins {
	auth, err := aws.GetAuth(config.S3.AccessKeyId, config.S3.SecretAccessKey, "", time.Time{})
	if err != nil {
		log.Fatal(err)
	}

	regionName := config.S3.Region
	if regionName == "" {
		regionName = aws.InstanceRegion()
		if regionName == "" {
			log.Fatal("Unspecified S3 region, and no instance region found.")
		}
	}

	region, exists := aws.Regions[regionName]
	if !exists {
		log.Fatalf("Invalid AWS region: %s", regionName)
	}

	bucket := s3.New(auth, region).Bucket(bucketName)
	backend := backend.NewS3Backend(bucket, path)
	return newSequins(backend, config)
}

func hdfsSetup(namenode string, path string, config sequinsConfig) *sequins {
	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatal(fmt.Errorf("Error connecting to HDFS: %s", err))
	}

	backend := backend.NewHdfsBackend(client, namenode, path)
	return newSequins(backend, config)
}
