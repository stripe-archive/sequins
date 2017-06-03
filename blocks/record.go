package blocks

import (
	"bytes"
	"io"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
)

// A Record is one key/value pair loaded from a block.
type Record struct {
	ValueLen uint64

	value  []byte
	reader io.Reader
	closed bool
}

func (b *Block) get(key []byte) (*Record, error) {
	r := &Record{}

	b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Name))
		value := bucket.Get(key)
		if b.Compression == SnappyCompression {
			var err error
			value, err = snappy.Decode(nil, value)
			return err
		}
		r.value = value
		r.ValueLen = uint64(len(value))
		r.reader = bytes.NewReader(value)
		return nil
	})

	if r.value == nil {
		return nil, nil
	}
	return r, nil
}

func (r *Record) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *Record) WriteTo(w io.Writer) (n int64, err error) {
	return r.reader.(io.WriterTo).WriteTo(w)
}

func (r *Record) Close() error {
	return nil
}
