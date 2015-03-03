package sequencefile

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestReadFile(t *testing.T) {
	file, err := os.Open("../test_data/0/part-00000")
	require.NoError(t, err)

	r := New(file)
	r.ReadHeader()
	assert.Equal(t, r.Header.Version, 6)
	assert.Equal(t, r.Header.Compression, NotCompressed)
	assert.Equal(t, r.Header.CompressionCodecClassName, "")
	assert.Equal(t, r.Header.KeyClassName, "org.apache.hadoop.io.BytesWritable")
	assert.Equal(t, r.Header.ValueClassName, "org.apache.hadoop.io.BytesWritable")
	assert.Equal(t, r.Header.Metadata, map[string]string{})

	offset1, _ := file.Seek(0, os.SEEK_CUR)
	ok := r.ScanKey()
	require.True(t, ok)
	require.Nil(t, r.Err())

	assert.Equal(t, "Alice", string(r.Key()))
	assert.Equal(t, []byte(nil), r.Value())

	offset2, _ := file.Seek(0, os.SEEK_CUR)
	ok = r.Scan()
	require.True(t, ok)
	require.Nil(t, r.Err())

	assert.Equal(t, "Bob", string(r.Key()))
	assert.Equal(t, "Hope", string(r.Value()))

	// EOF
	ok = r.Scan()
	require.False(t, ok)
	require.Nil(t, r.Err())

	file.Seek(offset1, os.SEEK_SET)
	ok = r.Scan()
	require.True(t, ok)
	require.Nil(t, r.Err())

	assert.Equal(t, "Alice", string(r.Key()))
	assert.Equal(t, "Practice", string(r.Value()))

	file.Seek(offset2, os.SEEK_SET)
	ok = r.ScanKey()
	require.True(t, ok)
	require.Nil(t, r.Err())

	assert.Equal(t, "Bob", string(r.Key()))
	assert.Equal(t, []byte(nil), r.Value())
}
