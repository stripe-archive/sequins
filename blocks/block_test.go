package blocks

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	res, err := block.Get([]byte("foo"))
	require.NoError(t, err, "fetching value for 'foo'")
	assert.Equal(t, "bar", string(res), "fetching value for 'foo'")

	res, err = block.Get([]byte("baz"))
	require.NoError(t, err, "fetching value for 'baz'")
	assert.Equal(t, "qux", string(res), "fetching value for 'baz'")

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
	assert.Equal(t, "bar", string(res), "fetching value for 'foo'")

	res, err = block.Get([]byte("baz"))
	require.NoError(t, err, "fetching value for 'baz'")
	assert.Equal(t, "qux", string(res), "fetching value for 'baz'")
}
