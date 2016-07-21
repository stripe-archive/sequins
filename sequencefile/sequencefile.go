// Package sequencefile provides functionality for reading Hadoop's SequenceFile
// format.
package sequencefile

import "io"

type Compression int
type CompressionCodec int

const (
	SyncSize = 16

	NotCompressed Compression = iota + 1
	RecordCompressed
	BlockCompressed

	GzipCompression CompressionCodec = iota + 1
	SnappyCompression
)

type decompressor interface {
	Read(p []byte) (n int, err error)
	Reset(r io.Reader) error
	Close() error
}
