package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/stripe/sequins/backend"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	uriDesc = `
URL or directory where the sequence files are. This can be a local path, or an
S3 url in the form of s3://<bucket>/<path>, or an HDFS url in the form of
hfds://<namenode>:<port>/<path>. In the case of an S3 or HDFS url, the files
will be downloaded to the local directory specified by --local-path, or a
temporary directory by default, before being indexed and served.
`

	localPathDesc = `
The local path to download files to, if an S3 or HDFS url is specified.
`

	s3AccessKeyDesc = `
The access key to use for S3. If unspecified, the env variable AWS_ACCESS_KEY_ID
will be used, or IAM instance role credentials if they are available.
`

	s3SecretKeyDesc = `
The secret key to use for S3. Like --s3-access-key, this will default to
AWS_SECRET_ACCESS_KEY or IAM credentials.
`

	s3RegionDesc = `
The region to use for S3, eg 'us-west-2'. If unspecified, and sequins is running
on ec2, this will default to the instance region.
`

	checkForSuccessFileDesc = `
If this flag is set, sequins will check for a _SUCCESS file in the remote or
local directory before updating to it. _SUCCESS files are created by hadoop
when a job finishes, so this is useful to make sure that partial results aren't
indexed.
`

	refreshPeriodDesc = `
If this flag is passed with a value like '30s' or '10m', sequins will
automatically check for new versions and update to them. Best used with
--check-for-success.
`
)

var (
	sequinsVersion string

	address             = kingpin.Flag("bind", "Address to bind to.").Short('b').Default("localhost:9599").PlaceHolder("ADDRESS").String()
	localPath           = kingpin.Flag("local-path", localPathDesc).Short('l').String()
	checkForSuccessFile = kingpin.Flag("check-for-success", checkForSuccessFileDesc).Short('c').Bool()
	refreshPeriod       = kingpin.Flag("refresh-period", refreshPeriodDesc).Short('r').Duration()

	s3AccessKey = kingpin.Flag("s3-access-key", s3AccessKeyDesc).String()
	s3SecretKey = kingpin.Flag("s3-secret-key", s3SecretKeyDesc).String()
	s3Region    = kingpin.Flag("s3-region", s3RegionDesc).String()

	uri = kingpin.Arg("URI", uriDesc).Required().String()
)

func main() {
	kingpin.Version("sequins version " + sequinsVersion)
	kingpin.Parse()

	opts := sequinsOptions{
		address:             *address,
		localPath:           *localPath,
		checkForSuccessFile: *checkForSuccessFile,
		zkPrefix:            "/sequins/test/123",        // TODO
		zkServers:           []string{"localhost:2181"}, // TODO
	}

	parsed, err := url.Parse(*uri)
	if err != nil {
		log.Fatal(err)
	}

	var s *sequins
	switch parsed.Scheme {
	case "", "file":
		s = localSetup(*uri, opts)
	case "s3":
		s = s3Setup(parsed.Host, parsed.Path, opts)
	case "hdfs":
		s = hdfsSetup(parsed.Host, parsed.Path, opts)
	}

	// TODO only do this if zk config passed
	// TODO no distributed if local path
	err = s.initDistributed()
	if err != nil {
		log.Fatal(err)
	}

	err = s.init()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		log.Fatal(s.start())
	}()

	refresh := *refreshPeriod
	if refresh != 0 {
		ticker := time.NewTicker(refresh)
		go func() {
			log.Println("Automatically checking for new versions every", refresh.String())
			for range ticker.C {
				err = s.reloadLatest()
				if err != nil {
					log.Println(fmt.Errorf("Error reloading: %s", err))
				}
			}
		}()
	}

	sighups := make(chan os.Signal)
	signal.Notify(sighups, syscall.SIGHUP)

	for range sighups {
		err = s.reloadLatest()
		if err != nil {
			log.Println(fmt.Errorf("Error reloading: %s", err))
		}
	}
}

func localSetup(localPath string, opts sequinsOptions) *sequins {
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		log.Fatal(err)
	}

	backend := backend.NewLocalBackend(absPath)
	if opts.localPath == "" {
		tmpDir, err := ioutil.TempDir("", "sequins-")
		if err != nil {
			log.Fatal(err)
		}

		opts.localPath = tmpDir
	}

	return newSequins(backend, opts)
}

func s3Setup(bucketName string, path string, opts sequinsOptions) *sequins {
	auth, err := aws.GetAuth(*s3AccessKey, *s3SecretKey, "", time.Time{})
	if err != nil {
		log.Fatal(err)
	}

	regionName := *s3Region
	if regionName == "" {
		regionName = aws.InstanceRegion()
		if regionName == "" {
			log.Fatal("Unspecified --s3-region, and no instance region found.")
		}
	}

	region, exists := aws.Regions[regionName]
	if !exists {
		log.Fatalf("Invalid AWS region: %s", regionName)
	}

	bucket := s3.New(auth, region).Bucket(bucketName)
	backend := backend.NewS3Backend(bucket, path)
	if opts.localPath == "" {
		tmpDir, err := ioutil.TempDir("", "sequins-")
		if err != nil {
			log.Fatal(err)
		}

		opts.localPath = tmpDir
	}

	return newSequins(backend, opts)
}

func hdfsSetup(namenode string, path string, opts sequinsOptions) *sequins {
	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatal(fmt.Errorf("Error connecting to HDFS: %s", err))
	}

	backend := backend.NewHdfsBackend(client, namenode, path)
	if opts.localPath == "" {
		tmpDir, err := ioutil.TempDir("", "sequins-")
		if err != nil {
			log.Fatal(err)
		}

		opts.localPath = tmpDir
	}

	return newSequins(backend, opts)
}
