package sequencefile

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadFile(t *testing.T) {
	file, err := os.Open("../test/names/0/part-00000")
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

func TestSync(t *testing.T) {
	file, err := os.Open("../test-huge/20150826/part-00000")

	// We need a big file for this one.
	if os.IsNotExist(err) {
		t.Skip()
	} else if err != nil {
		require.NoError(t, err)
	}

	r := New(file)
	r.ReadHeader()

	rand.Seed(time.Now().Unix())
	for i := 0; i < 1000; i++ {
		offset := int64(rand.Intn(1000000000)) + 1000
		file.Seek(offset, os.SEEK_SET)
		err = r.Sync()
		require.NoError(t, err)

		ok := r.Scan()
		require.True(t, ok)
		require.Nil(t, r.Err())
	}
}
