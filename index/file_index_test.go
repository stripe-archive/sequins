package index

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/sequins/sequencefile"
)

// testRandomKeys picks 100 random keys out of the file and makes sure that the
// index can return them.
func testRandomKeys(fi fileIndex, path string, t *testing.T) {
	f, err := os.Open(path)
	require.NoError(t, err)

	reader := sequencefile.New(f)
	err = reader.ReadHeader()
	require.NoError(t, err)

	// Test the first key
	reader.Scan()
	v, err := fi.get(reader.Key())

	msg := fmt.Sprintf("When fetching %s", string(reader.Key()))
	require.NoError(t, err, msg)
	assert.Equal(t, string(reader.Value()), string(v), msg)

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	checkpoint, err := f.Seek(0, os.SEEK_CUR)
	require.NoError(t, err)

	// Skip through randomly, picking some keys and checking them.
	for {
		_, err := f.Seek(int64(random.Intn(10000)), os.SEEK_CUR)
		require.NoError(t, err)

		err = reader.Sync()
		if err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}

		offset, err := f.Seek(0, os.SEEK_CUR)
		require.NoError(t, err)

		if !reader.Scan() {
			require.NoError(t, reader.Err())
			break
		}

		v, err := fi.get(reader.Key())
		require.NoError(t, err)

		msg = fmt.Sprintf("When fetching %s", string(reader.Key()))
		assert.Equal(t, string(reader.Value()), string(v), msg)

		checkpoint = offset
	}

	// Read the tail end of the file.
	_, err = f.Seek(checkpoint, os.SEEK_SET)
	require.NoError(t, err)

	for reader.Scan() {
		v, err = fi.get(reader.Key())
		require.NoError(t, err)

		msg = fmt.Sprintf("When fetching %s", string(reader.Key()))
		assert.Equal(t, string(reader.Value()), string(v), msg)
	}
}
