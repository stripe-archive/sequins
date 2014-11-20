package sequencefile

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"testing"
)

func readKeyValue(t *testing.T, r *Record) (string, string) {
	keyBytes, err := ioutil.ReadAll(r.Key)
	require.Nil(t, err)

	valueBytes, err := ioutil.ReadAll(r.Value)
	require.Nil(t, err)

	return string(keyBytes), string(valueBytes)
}

func TestReadFile(t *testing.T) {
	f, err := New("../test_data/0/part-00000")
	require.Nil(t, err)

	assert.Equal(t, f.Path, "../test_data/0/part-00000")
	assert.Equal(t, f.Header.Version, 6)
	assert.Equal(t, f.Header.Compression, NotCompressed)
	assert.Equal(t, f.Header.CompressionCodecClassName, "")
	assert.Equal(t, f.Header.KeyClassName, "org.apache.hadoop.io.BytesWritable")
	assert.Equal(t, f.Header.ValueClassName, "org.apache.hadoop.io.BytesWritable")
	assert.Equal(t, f.Header.Metadata, map[string]string{})

	var r *Record
	r, err = f.ReadNextRecord()
	require.Nil(t, err)

	key, value := readKeyValue(t, r)
	assert.Equal(t, "Alice", key)
	assert.Equal(t, "Practice", value)
	assert.Equal(t, 29, r.TotalLength)
	offset1 := r.Offset

	r, err = f.ReadNextRecord()
	require.Nil(t, err)

	key, value = readKeyValue(t, r)
	assert.Equal(t, "Bob", key)
	assert.Equal(t, "Hope", value)
	offset2 := r.Offset

	r, err = f.ReadNextRecord()
	assert.Equal(t, io.EOF, err)

	r, err = f.ReadRecordAtOffset(offset1)
	require.Nil(t, err)

	key, value = readKeyValue(t, r)
	assert.Equal(t, "Alice", key)
	assert.Equal(t, "Practice", value)
	assert.Equal(t, 29, r.TotalLength)

	r, err = f.ReadRecordAtOffset(offset2)
	require.Nil(t, err)

	key, value = readKeyValue(t, r)
	assert.Equal(t, "Bob", key)
	assert.Equal(t, "Hope", value)
}
