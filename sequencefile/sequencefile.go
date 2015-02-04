// Package sequencefile provides functionality for reading Hadoop's SequenceFile
// format.
package sequencefile

type compression int

const (
	SyncSize = 16

	NotCompressed compression = iota + 1
	ValueCompressed
	BlockCompressed
)
