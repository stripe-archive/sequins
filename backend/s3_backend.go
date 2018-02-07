package backend

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
			symlinkFile := path.Join(s.path, db, version, "_SYMLINK")

			if s.exists(successFile) {
				filtered = append(filtered, version)
			} else if s.exists(symlinkFile) {
				// For symlinked versions, we check that the
				// target version has a success file.
				targetVersion, err := s.getTargetVersion(db, version)
				if err != nil {
					continue
				}

				targetSuccessFile := path.Join(s.path, db, targetVersion, "_SUCCESS")
				if s.exists(targetSuccessFile) {
					filtered = append(filtered, version)
				}
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
	// If this directory is a symlink (denoted with the _SYMLINK file), we
	// should return the contents of the directory it is linking to.
	targetVersion, err := s.getTargetVersion(db, version)
	if err != nil {
		return nil, err
	}
	versionPrefix := path.Join(s.path, db, targetVersion)

	// We use a set here because S3 sometimes returns duplicate keys.
	res := make(map[string]bool)

	params := &s3.ListObjectsInput{
		Bucket:    aws.String(s.bucket),
		Delimiter: aws.String(""),
		MaxKeys:   aws.Int64(1000),
		Prefix:    aws.String(versionPrefix),
	}

	datasetSize := int64(0)
	numFiles := int64(0)

	err = s.svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, isLastPage bool) bool {
		for _, key := range page.Contents {
			name := path.Base(*key.Key)
			// S3 sometimes has keys that are the same as the "directory"
			if strings.TrimSpace(name) != "" && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
				datasetSize += *key.Size
				numFiles++
				res[name] = true
			}
		}
		return true
	})

	if err != nil {
		return nil, s.s3error(err)
	}

	if version != targetVersion {
		log.Printf("call_site=s3.ListFiles sequins_db=%q sequins_db_version=%q symlinked_version=%q dataset_size=%d file_count=%d", db, version, targetVersion, datasetSize, numFiles)
	} else {
		log.Printf("call_site=s3.ListFiles sequins_db=%q sequins_db_version=%q symlinked_version=\"nil\" dataset_size=%d file_count=%d", db, version, datasetSize, numFiles)
	}

	sorted := make([]string, 0, len(res))
	for name := range res {
		sorted = append(sorted, name)
	}

	sort.Strings(sorted)
	return sorted, nil
}

func (s *S3Backend) open(db, version, file string) (io.ReadCloser, error) {
	src := path.Join(s.path, db, version, file)
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(src),
	}
	resp, err := s.svc.GetObject(params)

	// If the download failed, due to the key not being found, retry
	// maxRetries number of times with an exponential backoff as it may
	// have been due to latency.
	backoff := time.Duration(1)
	for i := 0; i < s.maxRetries && err != nil; i++ {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			time.Sleep(backoff * time.Second)
			resp, err = s.svc.GetObject(params)
			backoff *= 2
		} else {
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("error opening S3 path %s: %s", s.path, err)
	}

	return resp.Body, nil
}

func (s *S3Backend) Open(db, version, file string) (io.ReadCloser, error) {
	// If this is a symlinked version, we should open the file at the
	// target version directory.
	targetVersion, err := s.getTargetVersion(db, version)
	if err != nil {
		return nil, err
	}

	return s.open(db, targetVersion, file)
}

// getTargetVersion returns the target version of a symlinked version if the
// _SYMLINK file exists or the same version otherwise. Whitespace is trimmed
// when reading the file in case it was manually generated.
func (s *S3Backend) getTargetVersion(db, version string) (string, error) {
	src := path.Join(s.path, db, version, "_SYMLINK")
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(src),
	}

	resp, err := s.svc.GetObject(params)
	if err != nil {
		return version, nil
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(b)), nil
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

	resp, err := s.svc.GetObject(params)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	return true
}

func (s *S3Backend) s3error(err error) error {
	return fmt.Errorf("unexpected S3 error on bucket %s: %s", s.bucket, err)
}
