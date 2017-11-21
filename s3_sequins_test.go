package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/backend"
)

type S3ClientMock struct {
	s3iface.S3API
	counter            int
	returnGenericError bool
}

func (c *S3ClientMock) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if c.counter < 3 {
		c.counter++
		if c.returnGenericError {
			return nil, errors.New("this is a mocked generic error")
		}
		return nil, awserr.New(s3.ErrCodeNoSuchKey, "this is a mocked NoSuchKey error", nil)
	}

	return &s3.GetObjectOutput{Body: ioutil.NopCloser(bytes.NewReader([]byte("")))}, nil
}

func setupS3(t *testing.T) *backend.S3Backend {
	bucket := os.Getenv("SEQUINS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping s3 tests because SEQUINS_TEST_BUCKET isn't set")
	}

	sess := session.New(nil)
	svc := s3.New(sess)
	testBackend := backend.NewS3Backend(bucket, "test", 3, svc)

	// Remove old files from previous tests
	err := delS3Prefix(svc, bucket, "test/")
	require.NoError(t, err, "should be able to delete old test files on s3")

	infos, _ := ioutil.ReadDir("test_databases/healthy/baby-names/1")
	s3Dest := "test/baby-names"

	for _, info := range infos {
		err = putS3(svc, bucket, path.Join(s3Dest, "0", info.Name()), path.Join("test_databases/healthy/baby-names/1", info.Name()))
		require.NoError(t, err, "setup: putting %s", path.Join(s3Dest, "0", info.Name()))
		err = putS3(svc, bucket, path.Join(s3Dest, "1", info.Name()), path.Join("test_databases/healthy/baby-names/1", info.Name()))
		require.NoError(t, err, "setup: putting %s", path.Join(s3Dest, "1", info.Name()))
	}

	err = putS3Blob(svc, bucket, path.Join(s3Dest, "/0/_SUCCESS"), nil)
	require.NoError(t, err, "setup: putting _SUCCESS file")
	err = putS3Blob(svc, bucket, path.Join(s3Dest, "foo"), nil)
	require.NoError(t, err, "setup: putting random file")

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
	assert.Equal(t, 5, len(files), "the list of files should be correct")
}

func TestS3Retries(t *testing.T) {
	retryBackend := backend.NewS3Backend("", "test", 3, &S3ClientMock{})
	_, err := retryBackend.Open("", "", "")
	require.NoError(t, err, "backend should retry and succeed the second time")

	retryBackend = backend.NewS3Backend("", "test", 3, &S3ClientMock{returnGenericError: true})
	_, err = retryBackend.Open("", "", "")
	require.Error(t, err, "backend should not retry for errors other than s3.ErrCodeNoSuchKey")

	noRetryBackend := backend.NewS3Backend("", "test", 0, &S3ClientMock{})
	_, err = noRetryBackend.Open("", "", "")
	require.Error(t, err, "backend should return an error on the first attempt and not retry")
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

func delS3Prefix(svc *s3.S3, bucket, prefix string) error {
	listParams := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	result, err := svc.ListObjects(listParams)
	if err != nil || len(result.Contents) == 0 {
		return err
	}

	objects := make([]*s3.ObjectIdentifier, len(result.Contents))
	for i, object := range result.Contents {
		objects[i] = &s3.ObjectIdentifier{
			Key: aws.String(*object.Key),
		}
	}

	deleteParams := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	}

	_, err = svc.DeleteObjects(deleteParams)
	return err
}

func TestS3Sequins(t *testing.T) {
	ts := getS3Sequins(t)
	bucket := os.Getenv("SEQUINS_TEST_BUCKET")
	testBasicSequins(t, ts, fmt.Sprintf("s3://%s/test/baby-names/1", bucket))
}
