package cdb

// Iterator represents a sequential iterator over a CDB database.
type Iterator struct {
	db     *CDB
	pos    uint32
	endPos uint32
	err    error
	key    []byte
	value  []byte
}

// Iter creates an Iterator that can be used to iterate the database.
func (cdb *CDB) Iter() *Iterator {
	return &Iterator{
		db:     cdb,
		pos:    uint32(indexSize),
		endPos: cdb.index[0].offset,
	}
}

// Next reads the next key/value pair and advances the iterator one record.
// It returns false when the scan stops, either by reaching the end of the
// database or an error. After Next returns false, the Err method will return
// any error that occurred while iterating.
func (iter *Iterator) Next() bool {
	if iter.pos >= iter.endPos {
		return false
	}

	keyLength, valueLength, err := readTuple(iter.db.reader, iter.pos)
	if err != nil {
		iter.err = err
		return false
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = iter.db.reader.ReadAt(buf, int64(iter.pos+8))
	if err != nil {
		iter.err = err
		return false
	}

	// Update iterator state
	iter.key = buf[:keyLength]
	iter.value = buf[keyLength:]
	iter.pos += 8 + keyLength + valueLength

	return true
}

// Key returns the current key.
func (iter *Iterator) Key() []byte {
	return iter.key
}

// Value returns the current value.
func (iter *Iterator) Value() []byte {
	return iter.value
}

// Err returns the current error.
func (iter *Iterator) Err() error {
	return iter.err
}
