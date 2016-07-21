package sequencefile

//#include <stdlib.h>
//#include <snappy-c.h>
import "C"
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"
)

// snappyFrameReader is a decompressor that implements the hadoop framing format
// for snappy. The format consists of a number of blocks. Each block has:
//  - A big-endian uint32 with the uncompressed size of the data
//  - A big-endian uint32 with the compressed size of the data
//  - The actual snappy stream
type snappyFrameReader struct {
	r            io.Reader
	buf          bytes.Buffer
	uncompressed bytes.Buffer
}

func newSnappyFrameReader(r io.Reader) *snappyFrameReader {
	return &snappyFrameReader{r: r}
}

func (s *snappyFrameReader) Read(b []byte) (int, error) {
	// If anything is left over from a previous partial read, return that.
	if s.uncompressed.Len() > 0 {
		return s.uncompressed.Read(b)
	}

	sizes := make([]byte, 8)
	_, err := io.ReadFull(s.r, sizes)
	if err != nil {
		return 0, err
	}

	uncompressedLength := int(binary.BigEndian.Uint32(sizes[:4]))
	compressedLength := int(binary.BigEndian.Uint32(sizes[4:]))

	// If the amount asked for is greater than the uncompressed size, we can read
	// off the uncompressed data directloy. Otherwise, we have to spill into a
	// buffer.
	if len(b) >= uncompressedLength {
		return s.readBlock(b, compressedLength)
	} else {
		s.uncompressed.Reset()
		s.uncompressed.Grow(uncompressedLength)
		_, err := s.readBlock(s.uncompressed.Bytes()[:uncompressedLength], compressedLength)
		if err != nil {
			return 0, err
		}

		return s.uncompressed.Read(b)
	}
}

func (s *snappyFrameReader) readBlock(b []byte, compressedLength int) (int, error) {
	s.buf.Reset()
	s.buf.Grow(compressedLength)
	compressed := s.buf.Bytes()[:compressedLength]
	_, err := io.ReadFull(s.r, compressed)
	if err != nil {
		return 0, err
	}

	outLength := C.size_t(len(b))
	st := C.snappy_uncompress(
		(*C.char)(unsafe.Pointer(&compressed[0])), C.size_t(compressedLength),
		(*C.char)(unsafe.Pointer(&b[0])), &outLength)

	n := int(outLength)
	switch st {
	case C.SNAPPY_OK:
		return n, nil
	case C.SNAPPY_BUFFER_TOO_SMALL:
		// This should never happen, since we control the input, and intentionally
		// always decompress a whole block.
		panic("snappy buffer too small")
	case C.SNAPPY_INVALID_INPUT:
		return n, errors.New("snappy: invalid input")
	default:
		return n, fmt.Errorf("snappy: unknown status: %d", st)
	}
}

// Reset prepares the snappyFrameReader to read a new block. It never returns
// an error.
func (s *snappyFrameReader) Reset(r io.Reader) error {
	s.r = r
	s.buf.Reset()
	return nil
}

// Close is a noop; it only exists to satisfy the decompressor interface.
func (s *snappyFrameReader) Close() error {
	return nil
}
