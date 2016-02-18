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

	putS3(bucket, "test/names/0/part-00000")
	putS3(bucket, "test/names/0/part-00001")
	putS3(bucket, "test/names/0/_SUCCESS")

	putS3(bucket, "test/names/1/part-00000")
	putS3(bucket, "test/names/1/part-00001")

	bucket.Put("test/names/foo", []byte("nothing"), "", "", s3.Options{})

	return backend.NewS3Backend(bucket, "test")
}

func putS3(bucket *s3.Bucket, src string) {
	bytes, _ := ioutil.ReadFile(src)
	bucket.Put(src, bytes, "", "", s3.Options{})
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
	assert.Equal(t, []string{"names"}, dbs, "the list of dbs should be correct")

	versions, err := s.ListVersions("names", false)
	require.NoError(t, err, "it should be able to list versions")
	assert.Equal(t, []string{"0", "1"}, versions, "it should be able to list versions")

	versions, err = s.ListVersions("names", true)
	require.NoError(t, err, "it should be able to list versions with a _SUCCESS file")
	assert.Equal(t, []string{"0"}, versions, "the list of versions with a _SUCCESS file should be correct")

	files, err := s.ListFiles("names", "0")
	require.NoError(t, err, "it should be able to list files")
	assert.Equal(t, []string{"part-00000", "part-00001"}, files, "the list of files should be correct")
}

func TestS3Sequins(t *testing.T) {
	ts := getS3Sequins(t)

	req, _ := http.NewRequest("GET", "/names/Alice", nil)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code, "fetching an existing key should 200")
	assert.Equal(t, "Practice", w.Body.String(), "fetching an existing key should return the value")
	assert.Equal(t, "1", w.HeaderMap.Get("X-Sequins-Version"), "when fetching an existing key, the X-Sequins-Version header should be set")

	req, _ = http.NewRequest("GET", "/names/foo", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code, "fetching a nonexistent key should 404")
	assert.Equal(t, "", w.Body.String(), "fetching a nonexistent key should return no body")
	assert.Equal(t, "", w.HeaderMap.Get("X-Sequins-Version"), "when fetchin a nonexistent key, the X-Sequins-Version header shouldn't be set")

	req, _ = http.NewRequest("GET", "/names/", nil)
	w = httptest.NewRecorder()
	ts.ServeHTTP(w, req)

	err := json.Unmarshal(w.Body.Bytes(), &dbStatus{})
	require.NoError(t, err, "fetching status should work and be valid")
	assert.Equal(t, 200, w.Code, "fetching status should work and be valid")
	// TODO (also hdfs)
	// assert.Equal(t, "s3://sequinstest/test/names/1", status.Path)
	// now := time.Now().Unix() - 1
	// assert.True(t, status.Started >= now)
}
