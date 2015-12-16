package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTotalIndex(t *testing.T) {
	index := newTotalFileIndex("../test/flights/0/part-00000", 1)
	err := index.build()
	require.NoError(t, err)
	assert.Equal(t, true, index.partitionDetector.usingHashPartition, "it should detect the file as partitioned")

	testRandomKeys(index, "../test/flights/0/part-00000", t)

	// Close the index, load it fresh from the manifest
	index.close()
	newIndex := newTotalFileIndex("../test/flights/0/part-00000", 1)
	manifestEntry := manifestEntry{
		Name:            "part-00000",
		Size:            0,
		IndexProperties: indexProperties{},
	}
	index.partitionDetector.updateManifest(&manifestEntry)

	err = newIndex.load(manifestEntry)
	require.NoError(t, err)
	assert.Equal(t, true, newIndex.partitionDetector.usingHashPartition, "it should detect the file as partitioned")

	testRandomKeys(newIndex, "../test/flights/0/part-00000", t)
}
