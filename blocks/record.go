package blocks

import (
	"io"

	"github.com/bsm/go-sparkey"
)

// A Record is one key/value pair loaded from a block.
type Record struct {
	ValueLen uint64

	iterPool iterPool
	iter     *sparkey.HashIter
	reader   io.Reader
	closed   bool
}

func (b *Block) get(key []byte) (*Record, error) {
	iter, err := b.iterPool.getIter()
	if err != nil {
		// In the case of an error, the iter is no longer considered valid.
		return nil, err
	}

	if err := iter.Seek(key); err != nil {
		return nil, err
	}

	if iter.State() != sparkey.ITERATOR_ACTIVE {
		// The key doesn't exist, so put the iterator back in the pool.
		b.iterPool.Put(iter)
		return nil, nil
	}

	return &Record{
		ValueLen: iter.ValueLen(),
		iterPool: b.iterPool,
		iter:     iter,
		reader:   iter.ValueReader(),
	}, nil
}

func (r *Record) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *Record) WriteTo(w io.Writer) (n int64, err error) {
	return r.reader.(io.WriterTo).WriteTo(w)
}

func (r *Record) Close() error {
	if r.iter != nil {
		r.iterPool.Put(r.iter)
	}

	return nil
}
