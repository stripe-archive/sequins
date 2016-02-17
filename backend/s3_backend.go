package backend

import (
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/crowdmob/goamz/s3"
)

type S3Backend struct {
	bucket *s3.Bucket
	path   string
}

func NewS3Backend(bucket *s3.Bucket, s3path string) *S3Backend {
	return &S3Backend{
		bucket: bucket,
		path:   strings.TrimPrefix(path.Clean(s3path), "/"),
	}
}

func (s *S3Backend) ListDBs() ([]string, error) {
	return s.listDirs(s.path)
}

func (s *S3Backend) ListVersions(db string, checkForSuccess bool) ([]string, error) {
	versions, err := s.listDirs(path.Join(s.path, db))
	if err != nil {
		return nil, err
	}

	if checkForSuccess {
		var filtered []string
		for _, version := range versions {
			successFile := path.Join(s.path, db, version, "_SUCCESS")
			exists, err := s.bucket.Exists(successFile)
			if err != nil {
				return nil, s.s3error(err)
			}

			if exists {
				filtered = append(filtered, version)
			}
		}

		versions = filtered
	}

	return versions, nil
}

func (s *S3Backend) listDirs(dir string) ([]string, error) {
	// This code assumes you're using S3 like a filesystem, with directories
	// separated by /'s. It also ignores the trailing slash on a prefix (for the
	// purposes of sorting lexicographically), to be consistent with other
	// backends.
	var res []string
	var marker string

	for {
		resp, err := s.bucket.List(dir+"/", "/", marker, 1000)

		if err != nil {
			return nil, s.s3error(err)
		} else if resp.CommonPrefixes == nil {
			break
		}

		for _, p := range resp.CommonPrefixes {
			prefix := strings.TrimSuffix(p, "/")

			// List the prefix, to make sure it's a "directory"
			isDir := false
			resp, err := s.bucket.List(prefix, "", marker, 3)
			if err != nil {
				return nil, err
			}

			for _, key := range resp.Contents {
				if strings.TrimSpace(path.Base(key.Key)) != "" {
					isDir = true
					break
				}
			}

			if isDir {
				res = append(res, path.Base(prefix))
			}
		}

		if !resp.IsTruncated {
			break
		} else {
			marker = resp.CommonPrefixes[len(resp.CommonPrefixes)-1]
		}
	}

	sort.Strings(res)
	return res, nil
}

func (s *S3Backend) ListFiles(db, version string) ([]string, error) {
	versionPrefix := path.Join(s.path, db, version)
	marker := ""
	res := make([]string, 0)

	for {
		resp, err := s.bucket.List(versionPrefix, "", marker, 1000)
		if err != nil {
			return nil, s.s3error(err)
		} else if resp.Contents == nil || len(resp.Contents) == 0 {
			break
		}

		for _, key := range resp.Contents {
			name := path.Base(key.Key)
			// S3 sometimes has keys that are the same as the "directory"
			if strings.TrimSpace(name) != "" && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
				res = append(res, name)
			}
		}

		if resp.IsTruncated {
			marker = resp.CommonPrefixes[len(resp.CommonPrefixes)-1]
		} else {
			break
		}
	}

	sort.Strings(res)
	return res, nil
}

func (s *S3Backend) Open(db, version, file string) (io.ReadCloser, error) {
	src := path.Join(s.path, db, version, file)
	reader, err := s.bucket.GetReader(src)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (s *S3Backend) DisplayPath(parts ...string) string {
	allParts := []string{s.path}
	allParts = append(allParts, parts...)
	return s.displayURL(allParts...)
}

func (s *S3Backend) displayURL(parts ...string) string {
	key := strings.TrimPrefix(path.Join(parts...), "/")
	return fmt.Sprintf("s3://%s/%s", s.bucket.Name, key)
}

// goamz error messages are always unqualified
func (s *S3Backend) s3error(err error) error {
	return fmt.Errorf("unexpected S3 error on bucket %s: %s", s.bucket.Name, err)
}
