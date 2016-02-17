package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
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

	putFile(bucket, "test/names/0/part-00000")
	putFile(bucket, "test/names/0/part-00001")
	putFile(bucket, "test/names/0/_SUCCESS")

	putFile(bucket, "test/names/1/part-00000")
	putFile(bucket, "test/names/1/part-00001")

	bucket.Put("test/names/foo", []byte("nothing"), "", "", s3.Options{})

	return backend.NewS3Backend(bucket, "test")
}

func getS3Sequins(t *testing.T) *sequins {
	backend := setupS3()
	s := getSequins(t, backend, "")

	time.Sleep(100 * time.Millisecond)
	return s
}

func putFile(bucket *s3.Bucket, src string) {
	bytes, _ := ioutil.ReadFile(src)
	bucket.Put(src, bytes, "", "", s3.Options{})
}

func TestS3Backend(t *testing.T) {
	s := setupS3()

	versions, err := s.ListVersions("names", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"0", "1"}, versions)

	versions, err = s.ListVersions("names", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"0"}, versions)
}

func TestS3Sequins(t *testing.T) {
	ts := getS3Sequins(t)

	req, _ := http.NewRequest("GET", "/names/Alice", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "Practice", w.Body.String())

	req, _ = http.NewRequest("GET", "/names/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
	assert.Equal(t, "", w.Body.String())

	req, _ = http.NewRequest("GET", "/names/", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	status := &status{}
	err := json.Unmarshal(w.Body.Bytes(), status)
	require.NoError(t, err)
	assert.Equal(t, 200, w.Code)
	// TODO (also hdfs)
	// assert.Equal(t, "s3://sequinstest/test/names/1", status.Path)
	// now := time.Now().Unix() - 1
	// assert.True(t, status.Started >= now)
}
