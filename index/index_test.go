package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	os.Remove("../test_data/0/.manifest")
	index := New("../test_data/0", "0")
	err := index.Load()
	require.NoError(t, err)

	assert.Equal(t, index.Path, "../test_data/0")
	assert.Equal(t, len(index.files), 2)
	assert.Equal(t, index.files[0].file.Name(), "../test_data/0/part-00000")
	assert.Equal(t, index.files[1].file.Name(), "../test_data/0/part-00001")

	val, err := index.Get("Alice")
	require.NoError(t, err)
	assert.Equal(t, string(val), "Practice")

	val, err = index.Get("foo")
	assert.Equal(t, ErrNotFound, err)

	count, err := index.Count()
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestIndexManifest(t *testing.T) {
	os.Remove("../test_data/0/.manifest")
	index := New("../test_data/0", "0")
	err := index.Load()
	require.NoError(t, err)

	count, err := index.Count()
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	index.Close()
	index = New("../test_data/0", "0")
	err = index.Load()
	require.NoError(t, err)

	newCount, err := index.Count()
	require.NoError(t, err)
	assert.Equal(t, count, newCount)
}
