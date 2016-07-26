package sequencefile

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// A Reader reads key/value pairs from a SequenceFile input stream.
//
// A reader is valid at any key or block offset; it's safe to start in the
// middle of a file or seek the underlying input stream if the location was
// recorded between calls to Scan, and as long as you call Reset after seeking.
// Note, however, that with a block-compressed file (Header.Compression set to
// BlockCompression), the position will be at the beginning of the block that
// holds the key, not right before the key itself.
type Reader struct {
	Header          Header
	syncMarkerBytes []byte

	reader io.Reader
	closed bool
	err    error

	compression  Compression
	codec        CompressionCodec
	decompressor decompressor

	buf   bytes.Buffer
	block blockReader
	key   []byte
	value []byte
}

// Open opens a SequenceFile on disk and immediately reads the header.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := NewReader(bufio.NewReader(f))
	err = r.ReadHeader()
	if err != nil {
		return nil, err
	}

	return r, nil
}

// New returns a new Reader for a SequenceFile, reading data from r. If the
// io.Reader is positioned at the start of a file, you should immediately call
// ReadHeader to read through the header.
func NewReader(r io.Reader) *Reader {
	return &Reader{reader: r}
}

// New returns a new Reader for a SequenceFile, reading data from r. Normally,
// compression options are inferred from the header of a file, but if the header
// is unavailable (because you're starting mid-stream) you can call this method
// with the compression options set explicitly.
func NewReaderCompression(r io.Reader, compression Compression, codec CompressionCodec) *Reader {
	rd := NewReader(r)
	rd.compression = compression
	rd.codec = codec

	return rd
}

// Scan advances the reader to the start of the next record, reading the key
// and value into memory. These can then be obtained by calling Key and Value.
// If the end of the file is reached, or there is an error, Scan will return
// false.
func (r *Reader) Scan() bool {
	if r.compression == BlockCompression {
		return r.scanBlock()
	} else {
		return r.scanRecord()
	}
}

// Reset resets the internal state of the reader, but maintains compression
// settings and header information. You should call Reset if you seek the
// underlying reader, but should create an entirely new Reader if you are
// starting a different file.
func (r *Reader) Reset() {
	r.clear()
	r.block = blockReader{}
}

// Err returns the first non-EOF error reached while scanning.
func (r *Reader) Err() error {
	return r.err
}

// Key returns the key for the current record. The byte slice will be reused
// after the next call to Scan.
func (r *Reader) Key() []byte {
	return r.key
}

// Value returns the value for the current record. The byte slice will be
// reused after the next call to Scan.
func (r *Reader) Value() []byte {
	return r.value
}

func (r *Reader) scanRecord() bool {
	if r.closed {
		return false
	}

	r.clear()
	b, err := r.consume(4)
	if err == io.EOF {
		return false
	} else if err != nil {
		r.close(err)
		return false
	}

	// Length -1 means a sync marker (the length is obnoxiously encoded as a cast
	// uint32 just for this).
	totalLength := int(int32(binary.BigEndian.Uint32(b)))
	if totalLength == -1 {
		err = r.checkSync()
		if err != nil {
			r.close(err)
			return false
		}

		return r.scanRecord()
	} else if totalLength < 8 {
		r.close(fmt.Errorf("sequencefile: invalid record length: %d", totalLength))
		return false
	}

	r.clear()
	b, err = r.consume(4)
	if err != nil {
		r.close(err)
		return false
	}

	keyLength := int(int32(binary.BigEndian.Uint32(b)))
	valueLength := totalLength - keyLength
	if keyLength < 4 {
		r.close(fmt.Errorf("sequencefile: invalid key length: %d", keyLength))
		return false
	}

	r.clear()
	r.key, err = r.consume(keyLength)
	if err != nil {
		r.close(err)
		return false
	}

	if r.compression == RecordCompression {
		r.value, err = r.consumeCompressed(valueLength)
		if err != nil {
			r.close(err)
			return false
		}
	} else {
		r.value, err = r.consume(valueLength)
		if err != nil {
			r.close(err)
			return false
		}
	}

	return true
}

func (r *Reader) checkSync() error {
	r.clear()
	b, err := r.consume(SyncSize)
	if err != nil {
		return err
	}

	// If we never read the Header, infer the sync marker from the first time we
	// see it.
	if r.syncMarkerBytes == nil {
		r.syncMarkerBytes = make([]byte, SyncSize)
		copy(r.syncMarkerBytes, b)
	} else if !bytes.Equal(b, r.syncMarkerBytes) {
		return errors.New("sequencefile: invalid sync marker")
	}

	return nil
}

// consume reads some bytes off the input stream, and returns a bite slice that
// is only valid until the next call to clear.
func (r *Reader) consume(n int) ([]byte, error) {
	off := r.buf.Len()
	_, err := io.CopyN(&r.buf, r.reader, int64(n))
	if err != nil {
		return nil, err
	}

	return r.buf.Bytes()[off:r.buf.Len()], nil
}

func (r *Reader) consumeCompressed(n int) ([]byte, error) {
	lr := &io.LimitedReader{R: r.reader, N: int64(n)}
	d, err := r.getDecompressor(lr)
	if err != nil {
		return nil, err
	}

	off := r.buf.Len()
	r.buf.Grow(n)
	_, err = r.buf.ReadFrom(d)
	if err != nil {
		return nil, err
	} else if lr.N > 0 {
		return nil, io.ErrUnexpectedEOF
	}

	return r.buf.Bytes()[off:r.buf.Len()], nil
}

func (r *Reader) clear() {
	r.buf.Reset()
}

func (r *Reader) getDecompressor(src io.Reader) (io.Reader, error) {
	var err error
	if r.decompressor != nil {
		err = r.decompressor.Reset(src)
	} else {
		switch r.codec {
		case GzipCompression:
			r.decompressor, err = gzip.NewReader(src)
		case SnappyCompression:
			r.decompressor, err = newSnappyFrameReader(src)
		default:
			panic("compression set without codec")
		}
	}

	return r.decompressor, err
}

func (r *Reader) close(err error) {
	r.closed = true
	r.err = err
	r.clear()
	if r.decompressor != nil {
		r.decompressor.Close()
	}
}
