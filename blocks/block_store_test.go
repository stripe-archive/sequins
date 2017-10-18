package blocks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBlockStoreCompression(t *testing.T, compression Compression) {
	tmpDir, err := ioutil.TempDir("", "sequins-test-")
	require.NoError(t, err, "creating a test tmpdir")

	bs := New(tmpDir, 2, compression, 8192)

	err = bs.Add([]byte("Alice"), []byte("Practice"))
	require.NoError(t, err, "adding keys to the block store")

	err = bs.Add([]byte("Bob"), []byte("Hope"))
	require.NoError(t, err, "adding keys to the block store")

	err = bs.Save(nil)
	require.NoError(t, err, "saving the manifest")
	assert.Equal(t, 2, len(bs.Blocks), "should have the correct number of blocks")

	res, err := bs.Get("Alice")
	require.NoError(t, err, "fetching value for 'Alice'")
	assert.Equal(t, "Practice", readAll(t, res), "fetching value for 'Alice'")

	res, err = bs.Get("Bob")
	require.NoError(t, err, "fetching value for 'Bob'")
	assert.Equal(t, "Hope", readAll(t, res), "fetching value for 'Bob'")

	// Close the index, then load it from the manifest.
	bs.Close()

	bs, _, err = NewFromManifest(tmpDir)
	require.NoError(t, err, "loading from manifest")

	assert.Equal(t, 2, len(bs.Blocks), "should have the correct number of blocks")

	res, err = bs.Get("Alice")
	require.NoError(t, err, "fetching value for 'Alice'")
	assert.Equal(t, "Practice", readAll(t, res), "fetching value for 'Alice'")

	res, err = bs.Get("Bob")
	require.NoError(t, err, "fetching value for 'Bob'")
	assert.Equal(t, "Hope", readAll(t, res), "fetching value for 'Bob'")
}

func TestBlockStoreSnappy(t *testing.T) {
	testBlockStoreCompression(t, SnappyCompression)
}

func TestBlockStoreNoCompression(t *testing.T) {
	testBlockStoreCompression(t, NoCompression)
}

func TestBlockStoreSparkey(t *testing.T) {
	tmpSparkey, err := ioutil.TempDir("", "sequins-sparkey-")
	require.NoError(t, err)
	defer os.RemoveAll(tmpSparkey)
	tmpStorePath, err := ioutil.TempDir("", "sequins-store-")
	require.NoError(t, err)
	defer os.RemoveAll(tmpStorePath)

	// Add some things
	empty := copySparkey(t, "part1", tmpSparkey)
	nonEmpty0 := copySparkey(t, "part0-file0", tmpSparkey)
	nonEmpty1 := copySparkey(t, "part0-file1", tmpSparkey)
	store := New(tmpStorePath, 2, SnappyCompression, 4096)
	err = store.AddSparkeyBlock(empty, 1)
	require.NoError(t, err)
	// Add some things out of order!
	err = store.AddSparkeyBlock(nonEmpty1, 0)
	require.NoError(t, err)
	err = store.AddSparkeyBlock(nonEmpty0, 0)
	require.NoError(t, err)
	err = store.Save(map[int]bool{0: true, 1: true})
	require.NoError(t, err)

	// Internal properties
	assert.Equal(t, 2, len(store.BlockMap[0]))
	assert.Equal(t, "Alice", string(store.BlockMap[0][0].minKey))
	assert.Equal(t, "Charlie", string(store.BlockMap[0][0].maxKey))
	assert.Equal(t, "Charlie", string(store.BlockMap[0][1].minKey))
	assert.Nil(t, store.BlockMap[0][1].maxKey)
	assert.Equal(t, 1, len(store.BlockMap[1]))
	assert.Nil(t, store.BlockMap[1][0].minKey)
	assert.Nil(t, store.BlockMap[1][0].maxKey)

	// External behaviour
	// No record, partition 0
	rec, err := store.Get("Alan")
	assert.NoError(t, err)
	assert.Nil(t, rec)
	// No record, partition 1
	rec, err = store.Get("Bob")
	assert.NoError(t, err)
	assert.Nil(t, rec)
	// Things we really have
	rec, err = store.Get("Alice")
	assert.NoError(t, err)
	assert.NotNil(t, rec)
	buf, err := ioutil.ReadAll(rec)
	assert.Equal(t, "Practice", string(buf))
	rec, err = store.Get("Betty")
	assert.NoError(t, err)
	assert.NotNil(t, rec)
	buf, err = ioutil.ReadAll(rec)
	assert.Equal(t, "White", string(buf))
	rec, err = store.Get("Charlie")
	assert.NoError(t, err)
	assert.NotNil(t, rec)
	buf, err = ioutil.ReadAll(rec)
	assert.Equal(t, "Chaplin", string(buf))
	rec, err = store.Get("Dylan")
	assert.NoError(t, err)
	assert.NotNil(t, rec)
	buf, err = ioutil.ReadAll(rec)
	assert.Equal(t, "Thomas", string(buf))

	store.Close()
	store.Delete()

	// Test Revert
	revertStorePath, err := ioutil.TempDir("", "sequins-store-")
	require.NoError(t, err)
	defer os.RemoveAll(revertStorePath)
	store = New(revertStorePath, 2, SnappyCompression, 4096)
	revertBlock := copySparkey(t, "part0-file0", tmpSparkey)
	err = store.AddSparkeyBlock(revertBlock, 0)
	require.NoError(t, err)
	store.Revert()
	err = store.Save(map[int]bool{0: true, 1: true})
	require.NoError(t, err)
	rec, err = store.Get("Alice")
	assert.NoError(t, err)
	assert.Nil(t, rec)
}
