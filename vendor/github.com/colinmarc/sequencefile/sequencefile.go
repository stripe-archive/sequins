// Package sequencefile provides functionality for reading and writing Hadoop's
// SequenceFile format, documented here: http://goo.gl/sOSJmJ
package sequencefile

import "io"

type Compression int
type CompressionCodec int

const (
	SyncSize = 16

	GzipClassName   = "org.apache.hadoop.io.compress.GzipCodec"
	SnappyClassName = "org.apache.hadoop.io.compress.SnappyCodec"
)

const (
	NoCompression Compression = iota + 1
	RecordCompression
	BlockCompression
)

const (
	GzipCompression CompressionCodec = iota + 1
	SnappyCompression
)

type decompressor interface {
	Read(p []byte) (n int, err error)
	Reset(r io.Reader) error
	Close() error
}
