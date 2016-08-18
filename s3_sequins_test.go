package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/backend"
)

func setupS3(t *testing.T) *backend.S3Backend {

	bucket := os.Getenv("SEQUINS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping s3 tests because SEQUINS_TEST_BUCKET isn't set")
	}

	sess := session.New(nil)
	svc := s3.New(sess)
	testBackend := backend.NewS3Backend(bucket, "test", svc)

	infos, _ := ioutil.ReadDir("test/baby-names/1")
	sourceDest := path.Join("test", "baby-names")
	for _, info := range infos {
		err := putS3(svc, bucket, path.Join(sourceDest, "0", info.Name()), filepath.Join("test/baby-names/1", info.Name()))
		require.NoError(t, err, "setup: putting %s", path.Join(sourceDest, "0", info.Name()))
		err = putS3(svc, bucket, path.Join(sourceDest, "1", info.Name()), filepath.Join("test/baby-names/1", info.Name()))
		require.NoError(t, err, "setup: putting %s", path.Join(sourceDest, "1", info.Name()))
	}

	putS3Blob(svc, bucket, "test/baby-names/0/_SUCCESS", nil)
	putS3Blob(svc, bucket, "test/baby-names/foo", bytes.NewReader([]byte("rando file")))

	return testBackend
}

func getS3Sequins(t *testing.T) *sequins {
	backend := setupS3(t)
	s := getSequins(t, backend, "")

	return s
}

func TestS3Backend(t *testing.T) {
	s := setupS3(t)

	dbs, err := s.ListDBs()
	require.NoError(t, err, "it should be able to list dbs")
	assert.Equal(t, []string{"baby-names"}, dbs, "the list of dbs should be correct")

	versions, err := s.ListVersions("baby-names", "", false)
	require.NoError(t, err, "it should be able to list versions")
	assert.Equal(t, []string{"0", "1"}, versions, "it should be able to list versions")

	versions, err = s.ListVersions("baby-names", "", true)
	require.NoError(t, err, "it should be able to list versions with a _SUCCESS file")
	assert.Equal(t, []string{"0"}, versions, "the list of versions with a _SUCCESS file should be correct")

	files, err := s.ListFiles("baby-names", "0")
	require.NoError(t, err, "it should be able to list files")
	assert.Equal(t, 20, len(files), "the list of files should be correct")
}

func putS3(svc *s3.S3, bucket, dst, src string) error {
	data, err := os.Open(src)
	if err != nil {
		return err
	}
	defer data.Close()

	return putS3Blob(svc, bucket, dst, data)
}

func putS3Blob(svc *s3.S3, bucket, dst string, data io.ReadSeeker) error {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dst),
		Body:   data,
	}

	_, err := svc.PutObject(params)
	if err != nil {
		return err
	}

	return nil
}

func TestS3Sequins(t *testing.T) {
	ts := getS3Sequins(t)
	bucket := os.Getenv("SEQUINS_TEST_BUCKET")
	testBasicSequins(t, ts, fmt.Sprintf("s3://%s/test/baby-names/1", bucket))
}
