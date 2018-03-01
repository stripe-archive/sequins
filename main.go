package main

import (
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"time"

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
			log.Fatal("No config file found! Please see the \"Getting Started\" guide for instructions: http://sequins.io/manual.")
		}
	} else if err != nil {
		log.Fatal("Error loading config: ", err)
	}

	if *source != "" {
		parsed, err := url.Parse(*source)
		if err != nil {
			log.Fatal(err)
		}

		switch parsed.Scheme {
		case "":
			absPath, err := filepath.Abs(parsed.Path)
			if err != nil {
				log.Fatal(err)
			}

			config.Source = absPath
		default:
			config.Source = *source
		}
	}

	if config.Source == "" {
		log.Fatal("The source root must be defined, either in the config file or with --source. Please see the README for instructions.")
	}

	if *bind != "" {
		config.Bind = *bind
	}

	if *localStore != "" {
		absPath, err := filepath.Abs(*localStore)
		if err != nil {
			log.Fatal(err)
		}

		config.LocalStore = absPath
	}

	if *debugBind != "" {
		config.Debug.Bind = *debugBind
	}

	config, err = validateConfig(config)
	if err != nil {
		log.Fatalf("Configuration error: %s\n", err)
	}

	parsed, err := url.Parse(config.Source)
	if err != nil {
		log.Fatal(err)
	}

	var s *sequins
	switch parsed.Scheme {
	case "", "file":
		s = localSetup(parsed.Path, config)
	case "s3":
		s = s3Setup(parsed.Host, parsed.Path, config)
	case "hdfs":
		s = hdfsSetup(parsed.Host, parsed.Path, config)
	default:
		log.Fatalf("Unrecognized scheme for path: %s://\n", parsed.Scheme)
	}

	// Do a basic test that the backend is valid.
	_, err = s.backend.ListDBs()
	if err != nil {
		log.Fatalf("Error listing DBs from %s: %s", s.backend.DisplayPath(""), err)
	}

	err = s.init()
	if err != nil {
		log.Fatal(err)
	}

	if config.Debug.Bind != "" {
		startDebugServer(config)
	}

	// This goroutine is supposed to wake up every 20 milliseconds. If it
	// finds itself slept for an unusually long time, it writes a log entry
	// to record the delay. The delay could be due to GC pause, scheduler
	// issue or others. This may help us investigate the 504 issue that we
	// have seen recently.
	go func() {
		lastWakeUpTime := time.Now()
		for {
			time.Sleep(20 * time.Millisecond)
			now := time.Now()
			delay := now.Sub(lastWakeUpTime) - 20*time.Millisecond
			lastWakeUpTime = now
			if delay > 20*time.Millisecond {
				log.Printf("Detected %d milliseconds unusual delay", delay.Nanoseconds()/1000000)
			}
		}
	}()

	s.start()
}

func localSetup(localPath string, config sequinsConfig) *sequins {
	backend := backend.NewLocalBackend(localPath)
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

	backend := backend.NewS3Backend(bucketName, path, config.S3.MaxRetries, s3.New(sess))
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
