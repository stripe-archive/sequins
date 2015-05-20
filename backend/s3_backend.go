package backend

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
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

func (s *S3Backend) LatestVersion(checkForSuccess bool) (string, error) {
	// This code assumes you're using S3 like a filesystem, with directories
	// separated by /'s. It also ignores the trailing slash on a prefix (for the
	// purposes of sorting lexicographically), to be consistent with other
	// backends.
	var version, marker string

	for {
		resp, err := s.bucket.List(s.path+"/", "/", marker, 1000)

		if err != nil {
			return "", s.s3error(err)
		} else if resp.CommonPrefixes == nil {
			break
		}

		var prefix string
		// Search backwards for a valid version
		for i := len(resp.CommonPrefixes) - 1; i >= 0; i-- {
			prefix = strings.TrimSuffix(resp.CommonPrefixes[i], "/")
			if path.Base(prefix) <= version {
				continue
			}

			valid := false
			if checkForSuccess {
				// If the 'directory' has a key _SUCCESS inside it, that implies that
				// there might be other keys with the same prefix
				successFile := path.Join(prefix, "_SUCCESS")
				exists, err := s.bucket.Exists(successFile)
				if err != nil {
					return "", s.s3error(err)
				}

				valid = exists
			} else {
				// Otherwise, we just check the prefix itself. If it doesn't exist, then
				// it's a prefix for some other key, and we can call it a directory
				exists, err := s.bucket.Exists(prefix)
				if err != nil {
					return "", s.s3error(err)
				}

				valid = !exists
			}

			if valid {
				version = path.Base(prefix)
			}
		}

		if !resp.IsTruncated {
			break
		} else {
			marker = resp.CommonPrefixes[len(resp.CommonPrefixes)-1]
		}
	}

	if version != "" {
		return version, nil
	} else {
		return "", fmt.Errorf("No valid versions at %s", s.displayURL(s.path))

	}
}

func (s *S3Backend) Download(version string, destPath string) error {
	versionPrefix := path.Join(s.path, version)

	// we'll assume large-ish files, and only download one at a time
	marker := ""
	for {
		resp, err := s.bucket.List(versionPrefix, "", marker, 1000)
		if err != nil {
			return s.s3error(err)
		} else if resp.Contents == nil || len(resp.Contents) == 0 {
			break
		}

		for _, key := range resp.Contents {
			name := strings.TrimPrefix(key.Key, versionPrefix)

			// S3 sometimes has keys that are the same as the "directory"
			if strings.TrimSpace(name) == "" {
				continue
			}

			err = s.downloadFile(key.Key, filepath.Join(destPath, name))
			if err != nil {
				return err
			}
		}

		if resp.IsTruncated {
			marker = resp.CommonPrefixes[len(resp.CommonPrefixes)-1]
		} else {
			break
		}
	}

	return nil
}

func (s *S3Backend) DisplayPath(version string) string {
	return s.displayURL(s.path, version)
}

func (s *S3Backend) downloadFile(src string, dest string) error {
	log.Printf("Downloading %s to %s", s.displayURL(src), dest)

	reader, err := s.bucket.GetReader(src)
	if err != nil {
		return fmt.Errorf("Error downloading S3 path %s: %s", s.path, err)
	}

	defer reader.Close()

	localFile, err := os.Create(dest)
	if err != nil {
		// If the local file already exists, check
		return err
	}

	defer localFile.Close()

	_, err = io.Copy(localFile, reader)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Backend) displayURL(pathElements ...string) string {
	key := strings.TrimPrefix(path.Join(pathElements...), "/")
	return fmt.Sprintf("s3://%s/%s", s.bucket.Name, key)
}

// goamz error messages are always unqualified
func (s *S3Backend) s3error(err error) error {
	return fmt.Errorf("Unexpected S3 error on bucket %s: %s", s.bucket.Name, err)
}
