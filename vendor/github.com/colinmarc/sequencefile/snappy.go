package sequencefile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/golang/snappy"
)

// snappyFrameReader is a decompressor that implements the hadoop framing format
// for snappy. The format consists of:
//  - A big-endian uint32 with the total uncompressed size of the data
//  - A number of blocks, each of which has:
//    - A big-endian uint32 with the compressed size of the data
//    - An actual snappy chunk
type snappyFrameReader struct {
	r         io.Reader
	remaining int

	compressed   bytes.Buffer
	uncompressed bytes.Buffer
	currentBlock *bytes.Reader
}

func newSnappyFrameReader(r io.Reader) (*snappyFrameReader, error) {
	s := &snappyFrameReader{r: r}
	err := s.Reset(r)
	return s, err
}

func (s *snappyFrameReader) Read(b []byte) (int, error) {
	// If anything is left over from a previous partial read, return that.
	if s.currentBlock != nil && s.currentBlock.Len() > 0 {
		return s.currentBlock.Read(b)
	} else {
		s.currentBlock = nil
	}

	sizeBytes := make([]byte, 4)
	_, err := io.ReadFull(s.r, sizeBytes)
	if err != nil {
		return 0, err
	}

	compressedLength := int(binary.BigEndian.Uint32(sizeBytes))
	if compressedLength == 0 {
		if s.remaining != 0 {
			return 0, errors.New("sequencefile: snappy: partial block")
		}

		return 0, io.EOF
	}

	s.compressed.Reset()
	_, err = io.CopyN(&s.compressed, s.r, int64(compressedLength))
	if err != nil {
		return 0, err
	}

	compressed := s.compressed.Bytes()
	uncompressedLength, err := snappy.DecodedLen(compressed)
	if err != nil {
		return 0, err
	}

	s.remaining -= uncompressedLength
	if s.remaining < 0 {
		return 0, errors.New("sequencefile: snappy: partial block")
	}

	// If the amount asked for is greater than the uncompressed size, we can read
	// off the uncompressed data directly. Otherwise, we have to spill into a
	// buffer.
	if len(b) >= uncompressedLength {
		return s.decodeBlock(b[:uncompressedLength], compressed)
	} else {
		s.uncompressed.Reset()
		s.uncompressed.Grow(uncompressedLength)
		uncompressed := s.uncompressed.Bytes()[:uncompressedLength]
		_, err := s.decodeBlock(uncompressed, compressed)
		if err != nil {
			return 0, err
		}

		s.currentBlock = bytes.NewReader(uncompressed)
		return s.currentBlock.Read(b)
	}
}

func (s *snappyFrameReader) decodeBlock(b, compressed []byte) (int, error) {
	uncompressed, err := snappy.Decode(b, compressed)
	if err != nil {
		return 0, err
	}

	// If we're doing this correctly, b and uncompressed should be the same, since
	// snappy uses the passed-in slice if it's big enough.
	if len(uncompressed) != len(b) {
		panic("sequencefile: snappy: input buffer was sized incorrectly")
	}

	return len(b), nil
}

// Reset prepares the snappyFrameReader to read a new stream.
func (s *snappyFrameReader) Reset(r io.Reader) error {
	s.r = r
	s.uncompressed.Reset()
	s.compressed.Reset()

	b := make([]byte, 4)
	_, err := io.ReadFull(s.r, b)
	if err != nil {
		return err
	}

	s.remaining = int(binary.BigEndian.Uint32(b))
	if s.remaining < 0 {
		panic("sequencefile: snappy: stream size overflows int32")
	}

	return nil
}

// Close is a noop; it only exists to satisfy the decompressor interface.
func (s *snappyFrameReader) Close() error {
	return nil
}
