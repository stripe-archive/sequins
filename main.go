package main

import (
	"fmt"
	"log"
	"net/url"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/colinmarc/hdfs"
	"github.com/stripe/sequins/backend"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	sequinsVersion string

	bind       = kingpin.Flag("bind", "Address to bind to. Overrides the config option of the same name.").Short('b').PlaceHolder("ADDRESS").String()
	source     = kingpin.Flag("source", "Where the sequencefiles are. Overrides the config option of the same name.").Short('r').PlaceHolder("URI").String()
	localStore = kingpin.Flag("local-store", "Where to store local data. Overrides the config option of the same name.").Short('l').PlaceHolder("PATH").String()
	configPath = kingpin.Flag("config", "The config file to use. By default, either sequins.conf in the local directory or /etc/sequins.conf will be used.").PlaceHolder("PATH").String()
	debugBind  = kingpin.Flag("debug-bind", "Address to bind to for pprof and expvars. Overrides the config option of the same name.").PlaceHolder("ADDRESS").String()
)

func main() {
	kingpin.Version("sequins version " + sequinsVersion)
	kingpin.Parse()

	config, err := loadConfig(*configPath)
	if err == errNoConfig {
		// If --source was specified, we can just use that and the default config.
		if *source != "" {
			config = defaultConfig()
		} else {
			log.Fatal("No config file found! Please see the README for instructions.")
		}
	} else if err != nil {
		log.Fatal("Error loading config: ", err)
	}

	if *source != "" {
		config.Source = *source
	}

	if config.Source == "" {
		log.Fatal("The source root must be defined, either in the config file or with --source. Please see the README for instructions.")
	}

	if *bind != "" {
		config.Bind = *bind
	}

	if *localStore != "" {
		config.LocalStore = *localStore
	}

	if *debugBind != "" {
		config.Debug.Bind = *debugBind
	}

	parsed, err := url.Parse(config.Source)
	if err != nil {
		log.Fatal(err)
	}

	var s *sequins
	switch parsed.Scheme {
	case "", "file":
		if config.Sharding.Enabled && !config.Test.AllowLocalCluster {
			log.Fatal("You can't run sequins with sharding enabled on local paths.")
		}
		s = localSetup(config.Source, config)
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

	if config.Debug.Bind != "" {
		startDebugServer(config)
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
	metadata := ec2metadata.New(session.New())
	regionName := config.S3.Region
	if regionName == "" {
		var err error
		regionName, err = metadata.Region()
		if regionName == "" || err != nil {
			log.Fatal("Unspecified S3 region, and no instance region found.")
		}
	}

	creds := credentials.NewChainCredentials([]credentials.Provider{
		&ec2rolecreds.EC2RoleProvider{Client: metadata},
		&credentials.EnvProvider{},
		&credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     config.S3.AccessKeyId,
			SecretAccessKey: config.S3.SecretAccessKey,
		}},
	})

	sess := session.New(&aws.Config{
		Region:      aws.String(regionName),
		Credentials: creds,
	})

	backend := backend.NewS3Backend(bucketName, path, s3.New(sess))
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
