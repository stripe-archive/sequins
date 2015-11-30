package index

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTotalIndex(t *testing.T) {
	index := newTotalFileIndex("../test/flights/0/part-00000", 1)
	err := index.build()
	require.NoError(t, err)
	testRandomKeys(index, "../test/flights/0/part-00000", t)

	// Close the index, load it fresh from the manifest
	index.close()
	newIndex := newTotalFileIndex("../test/flights/0/part-00000", 1)
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
	testRandomKeys(newIndex, "../test/flights/0/part-00000", t)
}
