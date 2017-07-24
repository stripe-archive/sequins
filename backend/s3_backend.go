package backend

import (
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Backend struct {
	bucket     string
	path       string
	maxRetries int
	svc        s3iface.S3API
}

func NewS3Backend(bucket string, s3path string, maxRetries int, svc s3iface.S3API) *S3Backend {
	return &S3Backend{
		bucket:     bucket,
		path:       strings.TrimPrefix(path.Clean(s3path), "/"),
		maxRetries: maxRetries,
		svc:        svc,
	}
}

func (s *S3Backend) ListDBs() ([]string, error) {
	return s.listDirs(s.path, "")
}

func (s *S3Backend) ListVersions(db, after string, checkForSuccess bool) ([]string, error) {
	versions, err := s.listDirs(path.Join(s.path, db), after)
	if err != nil {
		return nil, err
	}

	if checkForSuccess {
		var filtered []string
		for _, version := range versions {
			successFile := path.Join(s.path, db, version, "_SUCCESS")
			exists := s.exists(successFile)

			if exists {
				filtered = append(filtered, version)
			}
		}

		versions = filtered
	}

	return versions, nil
}

func (s *S3Backend) listDirs(dir, after string) ([]string, error) {
	// This code assumes you're using S3 like a filesystem, with directories
	// separated by /'s. It also ignores the trailing slash on a prefix (for the
	// purposes of sorting lexicographically), to be consistent with other
	// backends.
	var res []string

	for {
		params := &s3.ListObjectsInput{
			Bucket:    aws.String(s.bucket),
			Delimiter: aws.String("/"),
			Marker:    aws.String(after),
			MaxKeys:   aws.Int64(1000),
			Prefix:    aws.String(dir + "/"),
		}
		resp, err := s.svc.ListObjects(params)

		if err != nil {
			return nil, s.s3error(err)
		} else if resp.CommonPrefixes == nil {
			break
		}

		for _, p := range resp.CommonPrefixes {
			prefix := strings.TrimSuffix(*p.Prefix, "/")

			// List the prefix, to make sure it's a "directory"
			isDir := false
			params := &s3.ListObjectsInput{
				Bucket:    aws.String(s.bucket),
				Delimiter: aws.String(""),
				Marker:    aws.String(after),
				MaxKeys:   aws.Int64(3),
				Prefix:    aws.String(prefix),
			}
			resp, err := s.svc.ListObjects(params)
			if err != nil {
				return nil, err
			}

			for _, key := range resp.Contents {
				if strings.TrimSpace(path.Base(*key.Key)) != "" {
					isDir = true
					break
				}
			}

			if isDir {
				res = append(res, path.Base(prefix))
			}
		}

		if !*resp.IsTruncated || len(resp.CommonPrefixes) == 0 {
			break
		} else {
			after = resp.CommonPrefixes[len(resp.CommonPrefixes)-1].String()
		}
	}

	sort.Strings(res)
	return res, nil
}

func (s *S3Backend) ListFiles(db, version string) ([]string, error) {
	versionPrefix := path.Join(s.path, db, version)

	// We use a set here because S3 sometimes returns duplicate keys.
	res := make(map[string]bool)

	params := &s3.ListObjectsInput{
		Bucket:    aws.String(s.bucket),
		Delimiter: aws.String(""),
		MaxKeys:   aws.Int64(1000),
		Prefix:    aws.String(versionPrefix),
	}

	err := s.svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, isLastPage bool) bool {
		for _, key := range page.Contents {
			name := path.Base(*key.Key)
			// S3 sometimes has keys that are the same as the "directory"
			if strings.TrimSpace(name) != "" && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
				res[name] = true
			}
		}

		return true
	})

	if err != nil {
		return nil, s.s3error(err)
	}

	sorted := make([]string, 0, len(res))
	for name := range res {
		sorted = append(sorted, name)
	}

	sort.Strings(sorted)
	return sorted, nil
}

func (s *S3Backend) Open(db, version, file string) (io.ReadCloser, error) {
	src := path.Join(s.path, db, version, file)
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(src),
	}
	resp, err := s.svc.GetObject(params)

	// If the download failed, retry maxRetries number of times with an
	// exponential backoff.
	backoff := time.Duration(1)
	for i := 0; i < s.maxRetries && err != nil; i++ {
		time.Sleep(backoff * time.Second)
		resp, err = s.svc.GetObject(params)
		backoff *= 2
	}

	if err != nil {
		return nil, fmt.Errorf("error opening S3 path %s: %s", s.path, err)
	}

	return resp.Body, nil
}

func (s *S3Backend) DisplayPath(parts ...string) string {
	allParts := append([]string{s.path}, parts...)
	return s.displayURL(allParts...)
}

func (s *S3Backend) displayURL(parts ...string) string {
	key := strings.TrimPrefix(path.Join(parts...), "/")
	return fmt.Sprintf("s3://%s/%s", s.bucket, key)
}

func (s *S3Backend) exists(key string) bool {
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	_, err := s.svc.GetObject(params)

	if err != nil {
		return false
	}

	return true
}

func (s *S3Backend) s3error(err error) error {
	return fmt.Errorf("unexpected S3 error on bucket %s: %s", s.bucket, err)
}
