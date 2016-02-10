package sequencefile

import (
	"bytes"
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

	buf   bytes.Buffer
	key   []byte
	value []byte
}

// New returns a new Reader for sequencefiles, reading input from r. If the
// io.Reader is positioned at the start of a file, you should immediately call
// ReadHeader to read through the header.
func New(r io.Reader) *Reader {
	return &Reader{reader: r}
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

	var b []byte

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

	totalKeyLength := int(int32(binary.BigEndian.Uint32(b)))
	if totalKeyLength < 4 {
		r.close(fmt.Errorf("Invalid key length: %d", totalKeyLength))
		return false
	}

	// Both the key and value have an extra 4 bytes of junk at the beginning for
	// BytesWritable length.
	keyStart := 4
	keyEnd := keyStart + (totalKeyLength - 4)
	valueStart := keyEnd + 4
	valueEnd := valueStart + (totalLength - totalKeyLength - 4)

	if readValues {
		b, err = r.consume(totalLength)
		if err != nil {
			r.close(err)
			return false
		}

		r.key = b[keyStart:keyEnd]
		r.value = b[valueStart:valueEnd]
	} else {
		b, err = r.consume(totalKeyLength)
		if err != nil {
			r.close(err)
			return false
		}

		// To discard the value, first we try seeking. If this isn't a file, then we
		// just copy to /dev/null.
		valueLength := int64(totalLength - totalKeyLength)
		if seeker, ok := r.reader.(io.Seeker); ok {
			_, err = seeker.Seek(valueLength, os.SEEK_CUR)
		} else {
			_, err = io.CopyN(ioutil.Discard, r.reader, valueLength)
		}

		if err != nil {
			r.close(err)
			return false
		}

		r.key = b[keyStart:keyEnd]
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

func (r *Reader) close(err error) {
	r.closed = true
	r.err = err
}
