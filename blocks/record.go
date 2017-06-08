package blocks

import (
	"bytes"
	"io"

	"io/ioutil"
	"log"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
)

// A Record is one key/value pair loaded from a block.
type Record struct {
	ValueLen uint64
	compression Compression
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
			var decodeBuff *bytes.Buffer
			readerBuff := bytes.NewBuffer(value)
			decodeBuff = bytes.NewBuffer(value)
			reader := snappy.NewReader(decodeBuff)
			decompressed_value, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Println(err)
				return err
			}
			r.reader = snappy.NewReader(readerBuff)
			r.value = decompressed_value
			r.ValueLen = uint64(len(decompressed_value))

			r.compression = b.Compression

		} else {
			r.reader = bytes.NewReader(value)
			r.compression = NoCompression
			r.value = value
			r.ValueLen = uint64(len(value))

		}
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
	if r.compression == SnappyCompression {
		data , err:= ioutil.ReadAll(r.reader)
		if err != nil {
			return int64(0), err
		}
		n, err := w.Write(data)
		if err != nil {
			return int64(n), err
		}
		return int64(n), nil

	} else {
		return r.reader.(io.WriterTo).WriteTo(w)
	}

}

func (r *Record) Close() error {
	return nil
}
