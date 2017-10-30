package blocks

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	"os"
	"path/filepath"

	"github.com/bsm/go-sparkey"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randBytes(min, max int) []byte {
	n := rand.Intn(max) + 1
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return b
}

func readAll(t *testing.T, r io.Reader) string {
	b, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}

func TestBlock(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "sequins-test-")
	require.NoError(t, err, "creating a test tmpdir")

	bw, err := newBlock(tmpDir, 1, "snappy", 8192)
	require.NoError(t, err, "initializing a block")

	err = bw.add([]byte("foo"), []byte("bar"))
	require.NoError(t, err, "writing a key")

	err = bw.add([]byte("baz"), []byte("qux"))
	require.NoError(t, err, "writing a key")

	block, err := bw.save()
	require.NoError(t, err, "saving the block")

	assert.Equal(t, 1, block.Partition, "the partition should be carried through")
	assert.Equal(t, "foo", string(block.maxKey), "the maxKey should be correct")
	assert.Equal(t, "baz", string(block.minKey), "the minKey should be correct")

	record, err := block.get([]byte("foo"))
	require.NoError(t, err, "fetching reader for 'foo'")
	assert.NotNil(t, record, "the record should exist")

	buf := new(bytes.Buffer)
	_, err = record.WriteTo(buf)
	assert.NoError(t, err, "WriteTo should work, too")
	assert.Equal(t, "bar", readAll(t, buf), "fetching value for 'foo'")

	record, err = block.get([]byte("nonexistent"))
	require.NoError(t, err, "fetching reader for 'nonexistent'")
	assert.Nil(t, record, "the record should not exist")

	res, err := block.Get([]byte("foo"))
	require.NoError(t, err, "fetching value for 'foo'")
	assert.Equal(t, "bar", readAll(t, res), "fetching value for 'foo'")

	res, err = block.Get([]byte("baz"))
	require.NoError(t, err, "fetching value for 'baz'")
	assert.Equal(t, "qux", readAll(t, res), "fetching value for 'baz'")

	// Close the block and load it from the manifest.
	manifest := block.manifest()
	require.NotNil(t, manifest, "manifest shouldn't be nil")

	block.Close()

	block, err = loadBlock(tmpDir, manifest)
	require.NoError(t, err, "loading the block from a manifest")

	assert.Equal(t, 1, block.Partition, "the partition should be loaded")
	assert.Equal(t, "foo", string(block.maxKey), "the maxKey should be loaded")
	assert.Equal(t, "baz", string(block.minKey), "the minKey should be loaded")

	res, err = block.Get([]byte("foo"))
	require.NoError(t, err, "fetching value for 'foo'")
	assert.Equal(t, "bar", readAll(t, res), "fetching value for 'foo'")

	res, err = block.Get([]byte("baz"))
	require.NoError(t, err, "fetching value for 'baz'")
	assert.Equal(t, "qux", readAll(t, res), "fetching value for 'baz'")
}

func TestBlockParallelReads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping block reads test in short mode.")
	}

	tmpDir, err := ioutil.TempDir("", "sequins-test-")
	require.NoError(t, err, "creating a test tmpdir")

	bw, err := newBlock(tmpDir, 1, "snappy", 8192)
	require.NoError(t, err, "initializing a block")

	expected := make([][][]byte, 0, 100)
	for i := 0; i < cap(expected); i++ {
		key := randBytes(1, 32)
		value := randBytes(0, 1024*1024)
		err := bw.add(key, value)
		require.NoError(t, err)

		expected = append(expected, [][]byte{key, value})
	}

	block, err := bw.save()
	require.NoError(t, err, "saving the block")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			shuffled := make([][][]byte, len(expected)*5)
			for i, v := range rand.Perm(len(expected) * 5) {
				shuffled[v] = expected[i%len(expected)]
			}

			for _, record := range shuffled {
				val, err := block.Get(record[0])
				require.NoError(t, err)
				assert.Equal(t, string(record[1]), readAll(t, val))

				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}

			wg.Done()
		}()
	}

	wg.Wait()
}

func copySparkey(t *testing.T, name, dir string) string {
	cur, err := os.Getwd()
	require.NoError(t, err)
	testDir := filepath.Join(filepath.Dir(cur), "test_databases", "sparkey")

	srcLog, err := os.Open(sparkey.LogFileName(filepath.Join(testDir, name)))
	require.NoError(t, err)
	defer srcLog.Close()
	dstLogPath := sparkey.LogFileName(filepath.Join(dir, name))
	dstLog, err := os.Create(dstLogPath)
	require.NoError(t, err)
	defer dstLog.Close()
	_, err = io.Copy(dstLog, srcLog)
	require.NoError(t, err)

	srcIdx, err := os.Open(sparkey.HashFileName(filepath.Join(testDir, name)) + ".sz")
	require.NoError(t, err)
	defer srcIdx.Close()
	uncIdx := snappy.NewReader(srcIdx)
	dstIdx, err := os.Create(sparkey.HashFileName(filepath.Join(dir, name)))
	require.NoError(t, err)
	defer dstIdx.Close()
	_, err = io.Copy(dstIdx, uncIdx)
	require.NoError(t, err)

	return dstLogPath
}

func TestBlockSparkey(t *testing.T) {
	tmpSparkey, err := ioutil.TempDir("", "sequins-sparkey-")
	require.NoError(t, err)
	defer os.RemoveAll(tmpSparkey)
	tmpStorePath, err := ioutil.TempDir("", "sequins-store-")
	require.NoError(t, err)
	defer os.RemoveAll(tmpStorePath)

	// Empty file
	empty := copySparkey(t, "part1", tmpSparkey)
	block, err := newBlockFromSparkey(tmpStorePath, empty, 11)
	require.NoError(t, err)
	assert.Equal(t, 11, block.Partition)
	assert.Equal(t, 0, block.Count)
	assert.Nil(t, block.minKey)
	assert.Nil(t, block.maxKey)
	rec, err := block.Get([]byte("bad"))
	assert.NoError(t, err)
	assert.Nil(t, rec)
	_, err = os.Stat(empty)
	// newBlockFromSparkey should have removed the original.
	assert.True(t, os.IsNotExist(err))

	// Try Delete()
	_, err = os.Stat(filepath.Join(tmpStorePath, block.Name))
	assert.False(t, os.IsNotExist(err))
	err = block.Delete()
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(tmpStorePath, block.Name))
	assert.True(t, os.IsNotExist(err))

	// Non-empty file
	nonEmpty0 := copySparkey(t, "part0-file0", tmpSparkey)
	block, err = newBlockFromSparkey(tmpStorePath, nonEmpty0, 12)
	require.NoError(t, err)
	assert.Equal(t, 12, block.Partition)
	assert.Equal(t, 2, block.Count)
	assert.Equal(t, "Alice", string(block.minKey))
	assert.Nil(t, block.maxKey)
	rec, err = block.Get([]byte("bad"))
	assert.NoError(t, err)
	assert.Nil(t, rec)
	rec, err = block.Get([]byte("Betty"))
	assert.NoError(t, err)
	assert.NotNil(t, rec)
	buf, err := ioutil.ReadAll(rec)
	assert.NoError(t, err)
	rec.Close()
	assert.Equal(t, "White", string(buf))
	_, err = os.Stat(empty)
	assert.True(t, os.IsNotExist(err))
}
