package sequencefile

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testReadFile(t *testing.T, path string, compression Compression) {
	file, err := os.Open(path)
	require.NoError(t, err)

	r := New(file)
	r.ReadHeader()
	assert.Equal(t, 6, r.Header.Version, "The version should be set")
	assert.Equal(t, compression, r.Header.Compression, "The compression should be set")

	if compression == NotCompressed {
		assert.Equal(t, "", r.Header.CompressionCodecClassName, "The compression codec should be empty")
	} else {
		assert.Equal(t, "org.apache.hadoop.io.compress.SnappyCodec", r.Header.CompressionCodecClassName, "The compression codec should be set")
	}

	assert.Equal(t, "org.apache.hadoop.io.BytesWritable", r.Header.KeyClassName, "The key class name should be set")
	assert.Equal(t, "org.apache.hadoop.io.BytesWritable", r.Header.ValueClassName, "The value class name should be set")
	assert.Equal(t, map[string]string{}, r.Header.Metadata, "The metadata should be set")

	offset1, _ := file.Seek(0, os.SEEK_CUR)
	ok := r.ScanKey()
	require.Nil(t, r.Err(), "ScanKey should succeed")
	require.True(t, ok, "ScanKey should succeed")

	assert.Equal(t, "Alice", string(r.Key()), "The key should be correct")
	assert.Equal(t, []byte(nil), r.Value(), "The value should be correct")

	offset2, _ := file.Seek(0, os.SEEK_CUR)
	ok = r.Scan()
	require.Nil(t, r.Err(), "Scan should succeed")
	require.True(t, ok, "Scan should succeed")

	assert.Equal(t, "Bob", string(r.Key()), "The key should be correct")
	assert.Equal(t, "Hope", string(r.Value()), "The value should be correct")

	// EOF
	ok = r.Scan()
	require.Nil(t, r.Err(), "Scan at the end of the file should fail without an error")
	require.False(t, ok, "Scan at the end of the file should fail without an error")

	file.Seek(offset1, os.SEEK_SET)
	ok = r.Scan()
	require.Nil(t, r.Err(), "Scan should succeed")
	require.True(t, ok, "Scan should succeed")

	assert.Equal(t, "Alice", string(r.Key()), "The key should be correct")
	assert.Equal(t, "Practice", string(r.Value()), "The value should be correct")

	file.Seek(offset2, os.SEEK_SET)
	ok = r.ScanKey()
	require.True(t, ok)
	require.Nil(t, r.Err())

	assert.Equal(t, "Bob", string(r.Key()))
	assert.Equal(t, []byte(nil), r.Value())
}

func TestReadUncompressed(t *testing.T) {
	testReadFile(t, "../testdata/uncompressed.sequencefile", NotCompressed)
}

func TestReadBlockCompressed(t *testing.T) {
	testReadFile(t, "../testdata/block_compressed.sequencefile", BlockCompressed)
}

func TestReadRecordCompressed(t *testing.T) {
	testReadFile(t, "../testdata/record_compressed.sequencefile", RecordCompressed)
}
