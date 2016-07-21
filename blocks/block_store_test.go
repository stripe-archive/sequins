package blocks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stripe/sequins/sequencefile"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBlockStoreCompression(t *testing.T, compression Compression) {
	tmpDir, err := ioutil.TempDir("", "sequins-test-")
	require.NoError(t, err, "creating a test tmpdir")

	bs := New(tmpDir, 2, nil, compression, 8192)

	f, err := os.Open("../testdata/uncompressed.sequencefile")
	require.NoError(t, err, "opening a test file")

	sf := sequencefile.New(f)
	err = sf.ReadHeader()
	require.NoError(t, err, "reading the test file")

	err = bs.AddFile(sf, 0)
	require.NoError(t, err, "adding the file to the block store")

	err = bs.Save()
	require.NoError(t, err, "saving the manifest")
	assert.Equal(t, 2, len(bs.Blocks), "should have the correct number of blocks")

	res, err := bs.Get("Alice")
	require.NoError(t, err, "fetching value for 'Alice'")
	assert.Equal(t, "Practice", string(res), "fetching value for 'Alice'")

	res, err = bs.Get("Bob")
	require.NoError(t, err, "fetching value for 'Bob'")
	assert.Equal(t, "Hope", string(res), "fetching value for 'Bob'")

	// Close the index, then load it from the manifest.
	bs.Close()

	bs, err = NewFromManifest(tmpDir, nil)
	require.NoError(t, err, "loading from manifest")

	assert.Equal(t, 2, len(bs.Blocks), "should have the correct number of blocks")

	res, err = bs.Get("Alice")
	require.NoError(t, err, "fetching value for 'Alice'")
	assert.Equal(t, "Practice", string(res), "fetching value for 'Alice'")

	res, err = bs.Get("Bob")
	require.NoError(t, err, "fetching value for 'Bob'")
	assert.Equal(t, "Hope", string(res), "fetching value for 'Bob'")
}

func TestBlockStoreSnappy(t *testing.T) {
	testBlockStoreCompression(t, SnappyCompression)
}

func TestBlockStoreNoCompression(t *testing.T) {
	testBlockStoreCompression(t, NoCompression)
}
