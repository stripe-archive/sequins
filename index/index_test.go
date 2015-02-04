package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDB(t *testing.T) {
	index := New("../test_data/0")
	err := index.BuildIndex()
	require.Nil(t, err)
	if err != nil {
		t.FailNow()
	}

	assert.Equal(t, index.Path, "../test_data/0")
	assert.Equal(t, len(index.files), 2)
	assert.Equal(t, index.files[0].file.Name(), "../test_data/0/part-00000")
	assert.Equal(t, index.files[1].file.Name(), "../test_data/0/part-00001")

	val, err := index.Get("Alice")
	require.Nil(t, err)
	assert.Equal(t, string(val), "Practice")

	val, err = index.Get("foo")
	assert.Equal(t, ErrNotFound, err)

	count, err := index.Count()
	require.Nil(t, err)
	assert.Equal(t, 3, count)
}
