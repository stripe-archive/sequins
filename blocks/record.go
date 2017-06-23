package blocks

import (
	"bytes"
	"io"

	"log"

	"github.com/boltdb/bolt"
	pb "github.com/stripe/sequins/rpc"
	"golang.org/x/net/context"
)

// A Record is one key/value pair loaded from a block.
type Record struct {
	ValueLen    uint64
	compression Compression
	Value       []byte
	reader      io.Reader
	closed      bool
}

// Return on channel?!
func (b *Block) getRange(ctx context.Context, lowKey, highKey []byte, response chan *pb.Record) error {
	// TODO Return channel?
	log.Println("b getRange")

	b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Name)).Cursor()

		for k, v := bucket.Seek(lowKey); k != nil && bytes.Compare(k, highKey) <= 0; k, v = bucket.Next() {
			log.Println(b.ID, string(k), string(v))
			response <- &pb.Record{
				Key:   k,
				Value: v,
				DB: b.Name,
			}
		}
		return nil
	})

	return nil
}

func (b *Block) getRangeWithLimit(ctx context.Context, rng *pb.RangeWithLimit, responseChan chan *pb.Record) error {
	startKey := b.minKey
	if bytes.Compare(startKey, rng.StartKey) < 0 {
		startKey = rng.StartKey
	}
	b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Name)).Cursor()

		for k, v := bucket.Seek(startKey); k != nil && bytes.Compare(k, b.maxKey) <= 0; k, v = bucket.Next() {
			responseChan <- &pb.Record{
				Key:   k,
				Value: v,
				DB: b.Name,
			}
		}
		return nil
	})
	return nil
}

func (b *Block) get(key []byte) (*Record, error) {
	r := &Record{}

	b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Name))
		value := bucket.Get(key)
		/*if b.Compression == SnappyCompression {
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
			r.Value = decompressed_value
			r.ValueLen = uint64(len(decompressed_value))

			r.compression = b.Compression

		} else {*/
		r.reader = bytes.NewReader(value)
		r.compression = NoCompression
		r.Value = value
		r.ValueLen = uint64(len(value))

		//}
		return nil
	})

	if r.Value == nil {
		return nil, nil
	}
	return r, nil
}

func (r *Record) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *Record) WriteTo(w io.Writer) (n int64, err error) {
	/*if r.compression == SnappyCompression {
		data, err := ioutil.ReadAll(r.reader)
		if err != nil {
			return int64(0), err
		}
		n, err := w.Write(data)
		if err != nil {
			return int64(n), err
		}
		return int64(n), nil

	} else {*/
	return r.reader.(io.WriterTo).WriteTo(w)
	//}

}

func (r *Record) Close() error {
	return nil
}
