package index

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const eecummings = `
love is more thicker than forget
more thinner than recall
more seldom than a wave is wet
more frequent than to fail

it is most mad and moonly
and less it shall unbe
than all the sea which only
is deeper than the sea

love is less always than to win
less never than alive
less bigger than the least begin
less littler than forgive

it is most sane and sunly
and more it cannot die
than all the sky which only
is higher than the sky
`

func TestBufferedReadSeeker(t *testing.T) {
	poemBytes := []byte(eecummings)

	brs := newBufferedReadSeekerSize(bytes.NewReader(poemBytes), 16)
	b := make([]byte, 10)

	msg := "A basic read should succeed"

	n, err := io.ReadFull(brs, b)
	require.Nil(t, err, msg)
	assert.Equal(t, 10, n, msg)
	assert.Equal(t, string(poemBytes[:10]), string(b), msg)

	off, err := brs.Seek(0, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 10, off, msg)

	msg = "A second read beyond the buffered amount should succeed"

	n, err = io.ReadFull(brs, b)
	require.Nil(t, err, msg)
	assert.Equal(t, 10, n, msg)
	assert.Equal(t, string(poemBytes[10:20]), string(b), msg)

	off, err = brs.Seek(0, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 20, off, msg)

	msg = "A seek beyond the buffered point plus a read should succeed"

	off, err = brs.Seek(200, os.SEEK_SET)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 200, off, msg)

	n, err = io.ReadFull(brs, b)
	require.Nil(t, err, msg)
	assert.Equal(t, 10, n, msg)
	assert.Equal(t, string(poemBytes[200:210]), string(b), msg)

	off, err = brs.Seek(0, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 210, off, msg)

	msg = "A seek within the buffered data should succeed"

	off, err = brs.Seek(1, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 211, off, msg)

	n, err = io.ReadFull(brs, b[:3])
	require.Nil(t, err, msg)
	assert.Equal(t, 3, n, msg)
	assert.Equal(t, string(poemBytes[211:214]), string(b[:3]), msg)

	off, err = brs.Seek(0, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 214, off, msg)

	msg = "A forward seek beyond the buffered point should succeed"

	off, err = brs.Seek(6, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 220, off, msg)

	n, err = io.ReadFull(brs, b)
	require.Nil(t, err, msg)
	assert.Equal(t, 10, n, msg)
	assert.Equal(t, string(poemBytes[220:230]), string(b), msg)

	off, err = brs.Seek(0, os.SEEK_CUR)
	require.Nil(t, err, msg)
	assert.EqualValues(t, 230, off, msg)
}
