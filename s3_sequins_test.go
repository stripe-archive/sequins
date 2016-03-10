package main

import (
	"io/ioutil"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/crowdmob/goamz/s3/s3test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/backend"
)

func setupS3() *backend.S3Backend {
	fakeS3, _ := s3test.NewServer(&s3test.Config{})

	// cargo-culted from s3test
	fakeRegion := aws.Region{
		Name:                 "faux-region-1",
		S3Endpoint:           fakeS3.URL(),
		S3LocationConstraint: true,
	}

	auth, _ := aws.GetAuth("foo", "bar", "", time.Time{})
	bucket := s3.New(auth, fakeRegion).Bucket("sequinstest")
	bucket.PutBucket("")

	infos, _ := ioutil.ReadDir("test/baby-names/1")
	rootDest := path.Join("test", "baby-names")
	for _, info := range infos {
		putS3(bucket, path.Join(rootDest, "0", info.Name()), filepath.Join("test/baby-names/1", info.Name()))
		putS3(bucket, path.Join(rootDest, "1", info.Name()), filepath.Join("test/baby-names/1", info.Name()))
	}

	bucket.Put("test/baby-names/0/_SUCCESS", nil, "", "", s3.Options{})
	bucket.Put("test/baby-names/foo", []byte("rando file"), "", "", s3.Options{})
	return backend.NewS3Backend(bucket, "test")
}

func putS3(bucket *s3.Bucket, dst, src string) {
	bytes, _ := ioutil.ReadFile(src)
	bucket.Put(dst, bytes, "", "", s3.Options{})
}

func getS3Sequins(t *testing.T) *sequins {
	backend := setupS3()
	s := getSequins(t, backend, "")

	return s
}

func TestS3Backend(t *testing.T) {
	s := setupS3()

	dbs, err := s.ListDBs()
	require.NoError(t, err, "it should be able to list dbs")
	assert.Equal(t, []string{"baby-names"}, dbs, "the list of dbs should be correct")

	versions, err := s.ListVersions("baby-names", false)
	require.NoError(t, err, "it should be able to list versions")
	assert.Equal(t, []string{"0", "1"}, versions, "it should be able to list versions")

	versions, err = s.ListVersions("baby-names", true)
	require.NoError(t, err, "it should be able to list versions with a _SUCCESS file")
	assert.Equal(t, []string{"0"}, versions, "the list of versions with a _SUCCESS file should be correct")

	files, err := s.ListFiles("baby-names", "0")
	require.NoError(t, err, "it should be able to list files")
	assert.Equal(t, 20, len(files), "the list of files should be correct")
}

func TestS3Sequins(t *testing.T) {
	ts := getS3Sequins(t)
	testBasicSequins(t, ts, "s3://sequinstest/test/baby-names/1")
}
