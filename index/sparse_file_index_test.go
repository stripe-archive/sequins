package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparseIndexUnsorted(t *testing.T) {
	index := newSparseFileIndex("../test/flights/0/part-00000", 1)
	err := index.build()
	assert.Equal(t, errNotSorted, err)
}

func TestSparseIndexSorted(t *testing.T) {

	index := newSparseFileIndex("../test/flights-sorted/0/part-00000", 1)
	err := index.build()
	require.Nil(t, err)
	testRandomKeys(index, "../test/flights-sorted/0/part-00000", t)

	// Close the index, load it fresh from the manifest
	index.close()
	newIndex := newSparseFileIndex("../test/flights-sorted/0/part-00000", 1)
	manifestEntry := manifestEntry{
		Name: "part-00000",
		Size: 0,
		IndexProperties: indexProperties{
			MinKey: index.minKey,
			MaxKey: index.maxKey,
		},
	}

	err = newIndex.load(manifestEntry)
	require.NoError(t, err)
	testRandomKeys(newIndex, "../test/flights-sorted/0/part-00000", t)
}
