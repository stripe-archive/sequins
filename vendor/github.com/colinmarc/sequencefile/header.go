package sequencefile

import (
	"encoding/binary"
	"fmt"
)

// A Header represents the information contained in the header of the
// SequenceFile.
type Header struct {
	Version                   int
	Compression               Compression
	CompressionCodec          CompressionCodec
	CompressionCodecClassName string
	KeyClassName              string
	ValueClassName            string
	Metadata                  map[string]string
	SyncMarker                string
}

// ReadHeader parses the SequenceFile header from the input stream, and fills
// in the Header struct with the values. This should be called when the reader
// is positioned at the start of the file or input stream, before any records
// are read.
//
// ReadHeader will also validate that the settings of the SequenceFile
// (version, compression, key/value serialization, etc) are compatible.
func (r *Reader) ReadHeader() error {
	magic, err := r.consume(4)
	if err != nil {
		return fmt.Errorf("sequencefile: reading magic number: %s", err)
	} else if string(magic[:3]) != "SEQ" {
		return fmt.Errorf("sequencefile: invalid magic number: %s", magic)
	}

	r.Header.Version = int(magic[3])
	if r.Header.Version < 5 {
		return fmt.Errorf("sequencefile: unsupported version: %d", r.Header.Version)
	}

	keyClassName, err := r.readString()
	if err != nil {
		return err
	}

	valueClassName, err := r.readString()
	if err != nil {
		return err
	}

	r.Header.KeyClassName = keyClassName
	r.Header.ValueClassName = valueClassName

	r.clear()
	flags, err := r.consume(2)
	if err != nil {
		return err
	}

	valueCompression := uint8(flags[0])
	blockCompression := uint8(flags[1])
	if blockCompression > 0 {
		r.Header.Compression = BlockCompression
	} else if valueCompression > 0 {
		r.Header.Compression = RecordCompression
	} else {
		r.Header.Compression = NoCompression
	}

	if r.Header.Compression != NoCompression {
		compressionCodecClassName, err := r.readString()
		if err != nil {
			return err
		}

		r.Header.CompressionCodecClassName = compressionCodecClassName
		switch r.Header.CompressionCodecClassName {
		case "org.apache.hadoop.io.compress.GzipCodec":
			r.Header.CompressionCodec = GzipCompression
		case "org.apache.hadoop.io.compress.SnappyCodec":
			r.Header.CompressionCodec = SnappyCompression
		default:
			return fmt.Errorf("sequencefile: unsupported compression codec: %s", r.Header.CompressionCodecClassName)
		}
	}

	r.compression = r.Header.Compression
	r.codec = r.Header.CompressionCodec

	err = r.readMetadata()
	if err != nil {
		return err
	}

	r.clear()
	marker, err := r.consume(SyncSize)
	if err != nil {
		return err
	}

	r.Header.SyncMarker = string(marker)
	r.syncMarkerBytes = make([]byte, SyncSize)
	copy(r.syncMarkerBytes, marker)

	return nil
}

func (r *Reader) readMetadata() error {
	r.clear()
	b, err := r.consume(4)
	if err != nil {
		return err
	}

	pairs := int(binary.BigEndian.Uint32(b))
	if pairs < 0 || pairs > 1024 {
		return fmt.Errorf("sequencefile: invalid metadata pair count: %d", pairs)
	}

	metadata := make(map[string]string, pairs)
	for i := 0; i < pairs; i++ {
		key, err := r.readString()
		if err != nil {
			return err
		}

		value, err := r.readString()
		if err != nil {
			return err
		}

		metadata[key] = value
	}

	r.Header.Metadata = metadata
	return nil
}

func (r *Reader) readString() (string, error) {
	r.clear()
	b, err := r.consume(1)
	if err != nil {
		return "", err
	}

	length := int(b[0])
	r.clear()
	b, err = r.consume(length)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
