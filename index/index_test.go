package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	os.Remove("../test/names/0/.manifest")
	index := New("../test/names/0", "0")
	err := index.Load()
	require.NoError(t, err)

	assert.Equal(t, index.Path, "../test/names/0")
	assert.Equal(t, len(index.files), 2)

	val, err := index.Get("Alice")
	require.NoError(t, err)
	assert.Equal(t, string(val), "Practice")

	val, err = index.Get("foo")
	assert.Equal(t, ErrNotFound, err)
}

func TestIndexManifest(t *testing.T) {
	os.Remove("../test/names/0/.manifest")
	index := New("../test/names/0", "0")
	err := index.Load()
	require.NoError(t, err)

	index.Close()
	index = New("../test/names/0", "0")
	err = index.Load()
	require.NoError(t, err)
}
