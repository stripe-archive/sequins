package sequencefile

import (
	"bytes"
	"errors"
	"io"
)

// a blockReader represents an iterator over a single compressed block, for
// block-compressed SequenceFiles.
type blockReader struct {
	n     int
	i     int
	key   []byte
	value []byte
	err   error

	keys       []byte
	keyLengths []int
	keyOffset  int

	values       []byte
	valueLengths []int
	valueOffset  int
}

func (b *blockReader) next() bool {
	if b.i >= b.n {
		return false
	}

	keyLength := b.keyLengths[b.i]
	b.key = b.keys[b.keyOffset : b.keyOffset+keyLength]
	b.keyOffset += keyLength

	if b.values != nil {
		valueLength := b.valueLengths[b.i]
		b.value = b.values[b.valueOffset : b.valueOffset+valueLength]
		b.valueOffset += valueLength
	}

	b.i++
	return true
}

func (r *Reader) scanBlock() bool {
	for !r.block.next() {
		err := r.startBlock()
		if err == io.EOF {
			return false
		} else if err != nil {
			r.close(err)
			return false
		}
	}

	r.key = r.block.key
	r.value = r.block.value
	return true
}

func (r *Reader) startBlock() error {
	// The sync appears at the start of every block, but it still has the -1
	// length prefix in front, just for funsies.
	r.clear()
	_, err := r.consume(4)
	if err != nil {
		return err
	}

	err = r.checkSync()
	if err != nil {
		return err
	}

	n, err := ReadVInt(r.reader)
	if err != nil {
		return err
	}

	block := blockReader{n: int(n)}
	r.clear()

	keyLengthsBytes, err := r.consumeSection()
	if err != nil {
		return err
	}

	block.keyLengths, err = readLengths(keyLengthsBytes, int(n))
	if err != nil {
		return err
	}

	block.keys, err = r.consumeSection()
	if err != nil {
		return err
	}

	valueLengthsBytes, err := r.consumeSection()
	if err != nil {
		return err
	}

	block.valueLengths, err = readLengths(valueLengthsBytes, int(n))
	if err != nil {
		return err
	}

	block.values, err = r.consumeSection()
	if err != nil {
		return err
	}

	r.block = block
	return nil
}

func (r *Reader) consumeSection() ([]byte, error) {
	length, err := ReadVInt(r.reader)
	if err != nil {
		return nil, err
	}

	return r.consumeCompressed(int(length))
}

func readLengths(b []byte, n int) ([]int, error) {
	buf := bytes.NewBuffer(b)
	res := make([]int, 0, n)

	for i := 0; i < n; i++ {
		vint, err := ReadVInt(buf)
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		} else if err != nil {
			return nil, err
		}

		res = append(res, int(vint))
	}

	if buf.Len() != 0 {
		return nil, errors.New("sequencefile: invalid lengths for block")
	}

	return res, nil
}
