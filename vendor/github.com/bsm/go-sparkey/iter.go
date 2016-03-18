package sparkey

//#include <stdlib.h>
//#include <sparkey/sparkey.h>
import "C"
import (
	"io"
	"io/ioutil"
	"unsafe"
)

/* Log iterator */

// LogIter is a sequential log iterator.
// Iterators are not threadsafe, do not share them
// across multiple goroutines.
//
//  Example usage:
//
//     reader, _  := OpenLogReader("test.spl")
//     iter, _ := reader.Iterator()
//     for iter.Next(); iter.Valid(); iter.Next() {
//         key, _ := iter.Key()
//         val, _ := iter.Value()
//         fmt.Println("K/V", key, value)
//     }
//     if err := iter.Err(); err != nil {
//         fmt.Println("ERROR", err.Error())
//     }
//
type LogIter struct {
	iter *C.sparkey_logiter
	log  *C.sparkey_logreader
	err  error
}

// Err returns an error if one has occurred during iteration.
func (i *LogIter) Err() error {
	return i.err
}

// Closes a log iterator.
// This is a failsafe operation.
func (i *LogIter) Close() {
	if i.iter != nil {
		C.sparkey_logiter_close(&i.iter)
	}
	i.iter = nil
}

// Skip skips a number of entries.
// This is equivalent to calling Next count number of times.
func (i *LogIter) Skip(count int) error {
	rc := C.sparkey_logiter_skip(i.iter, i.log, C.int(count))
	if rc != rc_SUCCESS && rc != rc_ITERINACTIVE-205 {
		i.err = Error(rc)
	}
	return errorOrNil(rc)
}

// Next prepares the iterator to start reading from the next entry.
// The value of State() will be:
//   ITERATOR_CLOSED if the last entry has been passed.
//   ITERATOR_INVALID if anything goes wrong.
//   ITERATOR_ACTIVE if it successfully reached the next entry.
func (i *LogIter) Next() error {
	rc := C.sparkey_logiter_next(i.iter, i.log)
	if rc != rc_SUCCESS && rc != rc_ITERINACTIVE {
		i.err = Error(rc)
	}
	return errorOrNil(rc)
}

// Reset resets the iterator to the start of the current entry. This is only valid if
// state is ITERATOR_ACTIVE.
func (i *LogIter) Reset() error {
	rc := C.sparkey_logiter_reset(i.iter, i.log)
	return errorOrNil(rc)
}

// Valid returns true if iterator is at a valid position
func (i *LogIter) Valid() bool {
	return i.State() == ITERATOR_ACTIVE
}

// State gets the state for an iterator.
func (i *LogIter) State() IteratorState {
	return IteratorState(C.sparkey_logiter_state(i.iter))
}

// EntryType returns the type of the current entry.
func (i *LogIter) EntryType() EntryType {
	return EntryType(C.sparkey_logiter_type(i.iter))
}

// KeyLen returns the key length of the current entry.
func (i *LogIter) KeyLen() uint64 {
	return uint64(C.sparkey_logiter_keylen(i.iter))
}

// ValueLen returns the value length of the current entry.
func (i *LogIter) ValueLen() uint64 {
	return uint64(C.sparkey_logiter_valuelen(i.iter))
}

// Key returns the full key at the current position.
// This method will return a result only once per iteration.
func (i *LogIter) Key() ([]byte, error) {
	return ioutil.ReadAll(i.KeyReader())
}

// KeyReader returns an io.Reader for the key. The reader also implements
// io.WriterTo. The reader is no longer valid once the iterator has proceeded.
func (i *LogIter) KeyReader() io.Reader {
	return &keyReader{i}
}

// Value returns the full values at the current position.
// This method will return a result only once per iteration.
func (i *LogIter) Value() ([]byte, error) {
	return ioutil.ReadAll(i.ValueReader())
}

// ValueReader returns an io.Reader for the value. The reader also implements
// io.WriterTo. The reader is no longer valid once the iterator has proceeded.
func (i *LogIter) ValueReader() io.Reader {
	return &valueReader{i}
}

// Compare compares the keys of two iterators pointing to the same log.
// It assumes that the iterators are both clean, i.e. nothing has been consumed from the current entry.
// It will return zero if the keys are equal, negative if key1 is smaller than key2 and positive if key1 is larger than key2.
func (i *LogIter) Compare(other *LogIter) (int, error) {
	var res C.int
	rc := C.sparkey_logiter_keycmp(i.iter, other.iter, i.log, &res)
	if rc != rc_SUCCESS {
		return 0, Error(rc)
	}
	return int(res), nil
}

/* Hash iterator */

// A hash iterator is an extension to the log iterator and implements
// additional methods for iteration and retrieval
type HashIter struct {
	*LogIter
	reader *HashReader
}

// Seek positions the cursor on the given key.
// Sets the iterator state to ITERATOR_INVALID when key cannot be found.
func (i *HashIter) Seek(key []byte) error {
	var k *C.uint8_t

	lk := len(key)
	if lk > 0 {
		k = (*C.uint8_t)(&key[0])
	}
	rc := C.sparkey_hash_get(i.reader.hash, k, C.uint64_t(lk), i.iter)
	return errorOrNil(rc)
}

// Get retrieves a value for a given key
// Returns nil when a value cannot be found.
func (i *HashIter) Get(key []byte) ([]byte, error) {
	if err := i.Seek(key); err != nil {
		return nil, err
	} else if i.State() == ITERATOR_ACTIVE {
		return i.Value()
	}
	return nil, nil
}

// NextLive positions the cursor at the next non-deleted "live" key
func (i *HashIter) NextLive() error {
	rc := C.sparkey_logiter_hashnext(i.iter, i.reader.hash)
	if rc != rc_SUCCESS && rc != rc_ITERINACTIVE {
		i.err = Error(rc)
	}
	return errorOrNil(rc)
}

/* Key/value reader */

type keyReader struct {
	*LogIter
}

func (k *keyReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	var max, size C.uint64_t
	var ptr *C.uint8_t
	var err error

	max = C.uint64_t(len(b))
	ptr = (*C.uint8_t)(&b[0])
	rc := C.sparkey_logiter_fill_key(k.iter, k.log, max, ptr, &size)
	if rc != rc_SUCCESS {
		err = Error(rc)
	} else if size == 0 {
		err = io.EOF
	}

	return int(size), err
}

func (k *keyReader) WriteTo(w io.Writer) (int64, error) {
	if k.State() != ITERATOR_ACTIVE {
		return 0, ERROR_LOG_ITERATOR_INACTIVE
	}

	var size C.uint64_t
	var ptr *C.uint8_t

	remaining := k.KeyLen()
	var written int64
	for remaining > 0 {
		rc := C.sparkey_logiter_keychunk(k.iter, k.log, C.uint64_t(remaining), &ptr, &size)
		if rc != rc_SUCCESS {
			return written, Error(rc)
		} else if size == 0 {
			return written, nil
		}

		buf := C.GoBytes(unsafe.Pointer(ptr), C.int(size))
		n, err := w.Write(buf)
		written += int64(n)
		remaining -= uint64(n)
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

type valueReader struct {
	*LogIter
}

func (v *valueReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	var max, size C.uint64_t
	var ptr *C.uint8_t
	var err error

	max = C.uint64_t(len(b))
	ptr = (*C.uint8_t)(&b[0])

	rc := C.sparkey_logiter_fill_value(v.iter, v.log, max, ptr, &size)
	if rc != rc_SUCCESS {
		err = Error(rc)
	} else if size == 0 {
		err = io.EOF
	}

	return int(size), err
}

func (v *valueReader) WriteTo(w io.Writer) (int64, error) {
	if v.State() != ITERATOR_ACTIVE {
		return 0, ERROR_LOG_ITERATOR_INACTIVE
	}

	var size C.uint64_t
	var ptr *C.uint8_t

	remaining := v.ValueLen()
	var written int64
	for remaining > 0 {
		rc := C.sparkey_logiter_valuechunk(v.iter, v.log, C.uint64_t(remaining), &ptr, &size)
		if rc != rc_SUCCESS {
			return written, Error(rc)
		} else if size == 0 {
			return written, nil
		}

		buf := C.GoBytes(unsafe.Pointer(ptr), C.int(size))
		n, err := w.Write(buf)
		written += int64(n)
		remaining -= uint64(n)
		if err != nil {
			return written, err
		}
	}

	return written, nil
}
