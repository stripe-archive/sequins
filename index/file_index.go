package index

import (
	"encoding/binary"
)

// A file index represents an index over a specific file, and the ability
// to retrieve a value from that file.
type fileIndex interface {
	// get a value from the file, or nil if it doesn't exist.
	get([]byte) ([]byte, error)

	// load loads an existing index from when sequins ran previously
	load(manifestEntry manifestEntry) error

	// create creates a new index, overwriting any that exist
	build() error

	// close closes any open files
	close() error

	// cleanup does a best-effort cleanup of extra files (like stored indexes)
	cleanup() error

	// manifestEntry generates a manifestEntry for the file
	manifestEntry() (manifestEntry, error)
}

func serializeIndexEntry(b []byte, offset int64) {
	binary.BigEndian.PutUint64(b, uint64(offset))
}

func deserializeIndexEntry(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
