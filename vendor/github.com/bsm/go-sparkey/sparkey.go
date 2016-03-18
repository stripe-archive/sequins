package sparkey

//#include <stdlib.h>
//#include <sparkey/sparkey.h>
import "C"
import

// ** Constants **

"path/filepath"

type CompressionType uint8
type IteratorState uint8
type EntryType uint8
type HashSize int

const (
	KiB = 1024
	MiB = KiB * 1024
)

const (
	COMPRESSION_NONE   = CompressionType(C.SPARKEY_COMPRESSION_NONE)
	COMPRESSION_SNAPPY = CompressionType(C.SPARKEY_COMPRESSION_SNAPPY)
)

const (
	ITERATOR_NEW     = IteratorState(C.SPARKEY_ITER_NEW)
	ITERATOR_ACTIVE  = IteratorState(C.SPARKEY_ITER_ACTIVE)
	ITERATOR_CLOSED  = IteratorState(C.SPARKEY_ITER_CLOSED)
	ITERATOR_INVALID = IteratorState(C.SPARKEY_ITER_INVALID)
)

const (
	ENTRY_PUT    = EntryType(C.SPARKEY_ENTRY_PUT)
	ENTRY_DELETE = EntryType(C.SPARKEY_ENTRY_DELETE)
)

const (
	HASH_SIZE_AUTO  = HashSize(0)
	HASH_SIZE_32BIT = HashSize(4)
	HASH_SIZE_64BIT = HashSize(8)
)

const maxInt = int(^uint(0) >> 1)

// ** Options **

type Options struct {
	// Specifies if block compression should be used
	Compression CompressionType
	// Only relevant if compression type is not COMPRESSION_NONE. Default: 4k
	CompressionBlockSize int
}

func (o *Options) GetCompression() CompressionType {
	if o == nil {
		return COMPRESSION_NONE
	}
	return o.Compression
}

func (o *Options) GetCompressionBlockSize() int {
	if o == nil || (o.Compression == COMPRESSION_SNAPPY && o.CompressionBlockSize < 1) {
		return 4 * KiB
	}
	return o.CompressionBlockSize
}

// ** File name helpers **

// HashFileName generates a file name with an spi extension
func HashFileName(fname string) string { return fileName(fname, ".spi") }

// HashFileName generates a file name with an spl extension
func LogFileName(fname string) string { return fileName(fname, ".spl") }

func fileName(name, target string) string {
	ext := filepath.Ext(name)
	if ext == target {
		return name
	} else if ext == ".spl" || ext == ".spi" {
		name = name[:len(name)-4]
	}
	return name + target
}
