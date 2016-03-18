package sparkey

//#include <stdlib.h>
//#include <sparkey/sparkey.h>
import "C"
import "unsafe"

/* LogWriter */

type LogWriter struct {
	name string
	log  *C.sparkey_logwriter
}

// CreateLogWriter creates a new Sparkey log file, possibly overwriting an already existing.
func CreateLogWriter(fname string, opts *Options) (*LogWriter, error) {
	writer := LogWriter{name: LogFileName(fname)}
	blockSize := C.int(opts.GetCompressionBlockSize())
	compression := C.sparkey_compression_type(opts.GetCompression())
	filename := C.CString(writer.name)
	defer C.free(unsafe.Pointer(filename))

	rc := C.sparkey_logwriter_create(&writer.log, filename, compression, blockSize)
	if rc == rc_SUCCESS {
		return &writer, nil
	}
	return nil, Error(rc)
}

// OpenLogWriter opens an existing Sparkey log file.
func OpenLogWriter(fname string) (*LogWriter, error) {
	writer := LogWriter{name: LogFileName(fname)}
	filename := C.CString(writer.name)
	defer C.free(unsafe.Pointer(filename))

	rc := C.sparkey_logwriter_append(&writer.log, filename)
	if rc == rc_SUCCESS {
		return &writer, nil
	}
	return nil, Error(rc)
}

// Name returns the associated file name
func (w *LogWriter) Name() string { return w.name }

// Put appends a key/value pair to the log file
func (w *LogWriter) Put(key, value []byte) error {
	var ck, cv *C.uint8_t
	lk, lv := len(key), len(value)

	if lk > 0 {
		ck = (*C.uint8_t)(&key[0])
	}
	if lv > 0 {
		cv = (*C.uint8_t)(&value[0])
	}

	rc := C.sparkey_logwriter_put(w.log, C.uint64_t(lk), ck, C.uint64_t(lv), cv)
	return errorOrNil(rc)
}

// Delete appends a delete operation for a key to the log file
func (w *LogWriter) Delete(key []byte) error {
	var k *C.uint8_t
	if len(key) != 0 {
		k = (*C.uint8_t)(&key[0])
	}

	rc := C.sparkey_logwriter_delete(w.log, C.uint64_t(len(key)), k)
	return errorOrNil(rc)
}

// Flush flushes any open compression block to file buffer
func (w *LogWriter) Flush() error {
	rc := C.sparkey_logwriter_flush(w.log)
	return errorOrNil(rc)
}

// WriteHashFile will (re-)write a hashfile for the current log file
func (w *LogWriter) WriteHashFile(size HashSize) error {
	w.Flush() // Try to flush
	return WriteHashFile(w.name, size)
}

// Close closes an open log-writer
func (w *LogWriter) Close() error {
	if w.log == nil {
		return nil
	}
	rc := C.sparkey_logwriter_close(&w.log)
	w.log = nil
	return errorOrNil(rc)
}

/* LogReader */

type LogReader struct {
	name string
	log  *C.sparkey_logreader
}

// OpenLogReader opens an existing Sparkey log file for reading
// The reader is threadsafe, except during opening or closing.
func OpenLogReader(fname string) (*LogReader, error) {
	reader := LogReader{name: LogFileName(fname)}
	filename := C.CString(reader.name)
	defer C.free(unsafe.Pointer(filename))

	rc := C.sparkey_logreader_open(&reader.log, filename)
	if rc == rc_SUCCESS {
		return &reader, nil
	}
	return nil, Error(rc)
}

// Close closes a reader
// It's allowed to close a logreader while there are open logiterators.
// Further operations on such logiterators will fail.
// This is a failsafe operation.
func (r *LogReader) Close() error {
	if r.log != nil {
		C.sparkey_logreader_close(&r.log)
	}
	r.log = nil
	return nil
}

// Name returns the hash file name
func (r *LogReader) Name() string { return r.name }

// MaxKeyLen gets the size of the largest key in the log.
func (r *LogReader) MaxKeyLen() uint64 {
	return uint64(C.sparkey_logreader_maxkeylen(r.log))
}

// MaxValueLen gets the size of the largest value in the log.
func (r *LogReader) MaxValueLen() uint64 {
	return uint64(C.sparkey_logreader_maxvaluelen(r.log))
}

// Compression returns the compression type.
func (r *LogReader) Compression() CompressionType {
	return CompressionType(C.sparkey_logreader_get_compression_type(r.log))
}

// CompressionBlockSize returns the compression block size.
func (r *LogReader) CompressionBlockSize() int {
	return int(C.sparkey_logreader_get_compression_blocksize(r.log))
}

// Iterator initializes an iterator and associates it with the reader.
// The reader must be open. The iterator is not threadsafe.
func (r *LogReader) Iterator() (*LogIter, error) {
	iter := LogIter{log: r.log}
	rc := C.sparkey_logiter_create(&iter.iter, r.log)
	if rc == rc_SUCCESS {
		return &iter, nil
	}
	return nil, Error(rc)
}
