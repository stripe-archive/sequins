package sparkey

//#include <sparkey/sparkey.h>
import "C"
import "strconv"

const (
	rc_SUCCESS      C.sparkey_returncode = 0
	rc_ITERINACTIVE C.sparkey_returncode = 205
)

const (
	ERROR_INTERNAL_ERROR Error = -1

	ERROR_FILE_NOT_FOUND      Error = -100
	ERROR_PERMISSION_DENIED   Error = -101
	ERROR_TOO_MANY_OPEN_FILES Error = -102
	ERROR_FILE_TOO_LARGE      Error = -103
	ERROR_FILE_ALREADY_EXISTS Error = -104
	ERROR_FILE_BUSY           Error = -105
	ERROR_FILE_IS_DIRECTORY   Error = -106
	ERROR_FILE_SIZE_EXCEEDED  Error = -107
	ERROR_FILE_CLOSED         Error = -108
	ERROR_OUT_OF_DISK         Error = -109
	ERROR_UNEXPECTED_EOF      Error = -110
	ERROR_MMAP_FAILED         Error = -111

	ERROR_WRONG_LOG_MAGIC_NUMBER         Error = -200
	ERROR_WRONG_LOG_MAJOR_VERSION        Error = -201
	ERROR_UNSUPPORTED_LOG_MINOR_VERSION  Error = -202
	ERROR_LOG_TOO_SMALL                  Error = -203
	ERROR_LOG_CLOSED                     Error = -204
	ERROR_LOG_ITERATOR_INACTIVE          Error = -205
	ERROR_LOG_ITERATOR_MISMATCH          Error = -206
	ERROR_LOG_ITERATOR_CLOSED            Error = -207
	ERROR_LOG_HEADER_CORRUPT             Error = -208
	ERROR_INVALID_COMPRESSION_BLOCK_SIZE Error = -209
	ERROR_INVALID_COMPRESSION_TYPE       Error = -210

	ERROR_WRONG_HASH_MAGIC_NUMBER        Error = -300
	ERROR_WRONG_HASH_MAJOR_VERSION       Error = -301
	ERROR_UNSUPPORTED_HASH_MINOR_VERSION Error = -302
	ERROR_HASH_TOO_SMALL                 Error = -303
	ERROR_HASH_CLOSED                    Error = -304
	ERROR_FILE_IDENTIFIER_MISMATCH       Error = -305
	ERROR_HASH_HEADER_CORRUPT            Error = -306
	ERROR_HASH_SIZE_INVALID              Error = -307
)

type Error int

// Error implements the error interface
func (e Error) Error() string {
	code := int(e)
	if msg, ok := errorMessages[code]; ok {
		return "sparkey: " + msg
	}
	return "sparkey: unknown error (" + strconv.Itoa(code) + ")"
}

func errorOrNil(rc C.sparkey_returncode) error {
	if rc == rc_SUCCESS {
		return nil
	}
	return Error(rc)
}

var errorMessages = map[int]string{
	0:  "success",
	-1: "internal error",

	-100: "file not found",
	-102: "too many open files",
	-103: "file too large",
	-104: "file already exists",
	-105: "file is busy",
	-106: "file is directory",
	-107: "file size exceeded",
	-108: "file closed",
	-109: "out of disk",
	-110: "unexpected EOF",
	-111: "mmap failed",

	-200: "wrong log magic number",
	-201: "wrong log magic number",
	-202: "unsupported log minor version",
	-203: "log too small",
	-204: "log closed",
	-205: "log interator inactive",
	-206: "log interator mismatch",
	-207: "log interator closed",
	-208: "log header corrupt",
	-209: "invalid compression block size",
	-210: "invalid compression type",

	-300: "wrong hash magic number",
	-301: "wrong hash magic number",
	-302: "unsupported hash minor version",
	-303: "hash too small",
	-304: "hash closed",
	-305: "file identifier mismatch",
	-306: "hash header corrupt",
	-307: "hash size invalid",
}
