package sequencefile

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// A Reader reads key/value pairs from an input stream.
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
	key   []byte
	value []byte
}

// New returns a new Reader for sequencefiles, reading data from r. If the
// io.Reader is positioned at the start of a file, you should immediately call
// ReadHeader to read through the header.
func New(r io.Reader) *Reader {
	return &Reader{reader: r}
}

// New returns a new Reader for sequencefiles, reading data from r. Normally,
// compression options are inferred from the header of a file, but if the header
// is unavailable (because you're starting mid-stream) you can call this method
// with the compression options set explicitly.
func NewCompressed(r io.Reader, compression Compression, codec CompressionCodec) *Reader {
	rd := New(r)
	rd.compression = compression
	rd.codec = codec

	return rd
}

// Scan advances the reader to the start of the next record, reading the key
// and value into memory. These can then be obtained by calling Key and Value.
// If the end of the file is reached, or there is an error, Scan will return
// false.
func (r *Reader) Scan() bool {
	return r.scan(true)
}

// ScanKey works exactly like Scan, but only reads the key of the record into
// memory, not the value. Subsequent calls to Value will return nil.
func (r *Reader) ScanKey() bool {
	return r.scan(false)
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

func (r *Reader) scan(readValues bool) bool {
	if r.closed {
		return false
	}

	b, err := r.consume(4)
	if err == io.EOF {
		return false
	} else if err != nil {
		r.close(err)
		return false
	}

	// Length -1 means a sync marker (obnoxiously encoded as a cast uint32).
	totalLength := int(int32(binary.BigEndian.Uint32(b)))
	if totalLength == -1 {
		return r.checkSyncAndScan(readValues)
	} else if totalLength < 8 {
		r.close(fmt.Errorf("Invalid record length: %d", totalLength))
		return false
	}

	b, err = r.consume(4)
	if err != nil {
		r.close(err)
		return false
	}

	keyLength := int(int32(binary.BigEndian.Uint32(b)))
	valueLength := totalLength - keyLength
	if keyLength < 4 {
		r.close(fmt.Errorf("Invalid key length: %d", keyLength))
		return false
	}

	b, err = r.consume(keyLength)
	if err != nil {
		r.close(err)
		return false
	}

	// Both the key and value have an extra 4 bytes of junk at the beginning for
	// BytesWritable length.
	r.key = b[4:]

	if readValues {
		// We'll reuse the key slice for the value, so copy the key out.
		k := make([]byte, len(r.key))
		copy(k, r.key)
		r.key = k

		if r.compression == RecordCompressed {
			b, err = r.consumeCompressedValue(valueLength)
			if err != nil {
				r.close(err)
				return false
			}

			r.value = b[4:]
		} else {
			b, err = r.consume(valueLength)
			if err != nil {
				r.close(err)
				return false
			}

			r.value = b[4:]
		}
	} else {
		// If the underlying reader is a seeker, we can seek forward over the value.
		// If it's a BufferedReader, we can Discard. If all else fails, we just copy
		// to ioutil.Discard.
		if seeker, ok := r.reader.(io.Seeker); ok {
			_, err = seeker.Seek(int64(valueLength), os.SEEK_CUR)
		} else if buffered, ok := r.reader.(*bufio.Reader); ok {
			_, err = buffered.Discard(valueLength)
		} else {
			_, err = io.CopyN(ioutil.Discard, r.reader, int64(valueLength))
		}

		if err != nil {
			r.close(err)
			return false
		}

		r.value = nil
	}

	return true
}

func (r *Reader) checkSyncAndScan(readValues bool) bool {
	b, err := r.consume(SyncSize)
	if err != nil {
		r.close(err)
		return false
	}

	// If we never read the Header, infer the sync marker from the first time we
	// see it.
	if r.syncMarkerBytes == nil {
		r.syncMarkerBytes = make([]byte, SyncSize)
		copy(r.syncMarkerBytes, b)
	} else if !bytes.Equal(b, r.syncMarkerBytes) {
		r.close(fmt.Errorf("Invalid sync marker: %x vs %x", b, r.syncMarkerBytes))
		return false
	}

	return r.scan(readValues)
}

func (r *Reader) consume(n int) ([]byte, error) {
	r.buf.Reset()
	r.buf.Grow(n)
	b := r.buf.Bytes()[:n]

	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (r *Reader) consumeCompressedValue(n int) ([]byte, error) {
	d, err := r.getDecompressor(io.LimitReader(r.reader, int64(n)))
	if err != nil {
		return nil, err
	}

	r.buf.Reset()
	_, err = r.buf.ReadFrom(d)
	if err != nil {
		return nil, err
	}

	return r.buf.Bytes(), nil
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
			r.decompressor = newSnappyFrameReader(src)
		default:
			panic("compression set without codec")
		}
	}

	return r.decompressor, err
}

func (r *Reader) close(err error) {
	r.closed = true
	r.err = err
	if r.decompressor != nil {
		r.decompressor.Close()
	}
}
