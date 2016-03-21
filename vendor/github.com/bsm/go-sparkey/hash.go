package sparkey

//#include <stdlib.h>
//#include <sparkey/sparkey.h>
import "C"
import "unsafe"

// WriteHashFile creates a hash table for a specific log file.
// It's safe and efficient to run this multiple times.
// If the hash file already exists, it will be used to speed up the creation of the new file
// by reusing the existing entries, and only update the new hash table based on
// the entries in the log that are new since the last hash was built.
// Note that the hash file is never overwritten, instead the old file is unlinked from
// the filesystem and the new one is created. Thus, it's safe to rewrite the hash table while
// other processes are reading from it.
func WriteHashFile(fname string, size HashSize) error {
	return WriteCustomHashFile(HashFileName(fname), LogFileName(fname), size)
}

// WriteCustomHashFile writes hash files at custom locations.
// This is in case you want to keep your files separate for any reason.
func WriteCustomHashFile(hashname, logname string, size HashSize) error {
	hname := C.CString(hashname)
	defer C.free(unsafe.Pointer(hname))
	lname := C.CString(logname)
	defer C.free(unsafe.Pointer(lname))

	rc := C.sparkey_hash_write(hname, lname, C.int(size))
	return errorOrNil(rc)
}

type HashReader struct {
	name, logname string
	hash          *C.sparkey_hashreader
}

// Open opens a hash/log pair for reading.
// The reader is threadsafe, except during opening or closing.
func Open(fname string) (*HashReader, error) {
	return OpenCustomHashReader(HashFileName(fname), LogFileName(fname))
}

// OpenCustomHashReader opens a hash for reading, using custom file-names.
// This is in case you want to keep your files separate for any reason.
func OpenCustomHashReader(hashname string, logname string) (*HashReader, error) {
	reader := HashReader{name: hashname, logname: logname}
	hname := C.CString(hashname)
	defer C.free(unsafe.Pointer(hname))
	lname := C.CString(logname)
	defer C.free(unsafe.Pointer(lname))

	rc := C.sparkey_hash_open(&reader.hash, hname, lname)
	if rc == rc_SUCCESS {
		return &reader, nil
	}
	return nil, Error(rc)
}

// Name returns the hash file name
func (r *HashReader) Name() string { return r.name }

// LogName returns the associated log-file name
func (r *HashReader) LogName() string { return r.logname }

// NumSlots returns the number of slote entries
func (r *HashReader) NumSlots() uint64 { return uint64(C.sparkey_hash_numentries(r.hash)) }

// NumCollisions returns the number of collisions
func (r *HashReader) NumCollisions() uint64 { return uint64(C.sparkey_hash_numcollisions(r.hash)) }

// Log gets the LogReader that is referenced by the HashReader
func (r *HashReader) Log() *LogReader {
	return &LogReader{name: r.logname, log: C.sparkey_hash_getreader(r.hash)}
}

// Iterator creates a hash iterator for data retrieval.
// Please note that iterators are not threadsafe and must not be shared
// across goroutines.
func (r *HashReader) Iterator() (*HashIter, error) {
	iter, err := r.Log().Iterator()
	if err != nil {
		return nil, err
	}
	return &HashIter{LogIter: iter, reader: r}, nil
}

// Get is a (theadsafe) convenience accessor for keys, which allocates a new
// iterator instance. For single-threaded batch access, allocate
// a new iterator yourself and use iter.Get().
// This method will return nil when a key doesn't exist.
func (r *HashReader) Get(key []byte) ([]byte, error) {
	iter, err := r.Iterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	return iter.Get(key)
}

// Close closes a reader.
// It's allowed to close a HashReader while there are open log iterators associated with it.
// Further operations on such logiterators will fail.
// This is a failsafe operation.
func (r *HashReader) Close() {
	if r.hash != nil {
		C.sparkey_hash_close(&r.hash)
	}
	r.hash = nil
}
