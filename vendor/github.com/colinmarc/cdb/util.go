package cdb

import (
	"encoding/binary"
	"io"
)

func readTuple(r io.ReaderAt, offset uint32) (uint32, uint32, error) {
	tuple := make([]byte, 8)
	_, err := r.ReadAt(tuple, int64(offset))
	if err != nil {
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint32(tuple[:4])
	second := binary.LittleEndian.Uint32(tuple[4:])
	return first, second, nil
}

func writeTuple(w io.Writer, first, second uint32) error {
	tuple := make([]byte, 8)
	binary.LittleEndian.PutUint32(tuple[:4], first)
	binary.LittleEndian.PutUint32(tuple[4:], second)

	_, err := w.Write(tuple)
	return err
}
