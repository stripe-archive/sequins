package sequencefile

import (
	"encoding/binary"
	"fmt"
)

// A Header represents the information contained in the header of the
// sequencefile.
type Header struct {
	Version                   int
	Compression               compression
	CompressionCodecClassName string
	KeyClassName              string
	ValueClassName            string
	Metadata                  map[string]string
	SyncMarker                string
}

// ReadHeader parses the sequencefile header from the input stream, and fills
// in the Header struct with the values. This should be called when the reader
// is positioned at the start of the file or input stream, before any records
// are read.
//
// ReadHeader will also validate that the settings of the sequencefile
// (version, compression, key/value serialization, etc) are compatible.
func (r *Reader) ReadHeader() error {
	magic, err := r.consume(4)
	if err != nil {
		return fmt.Errorf("Error reading magic number: %s", err)
	} else if string(magic[:3]) != "SEQ" {
		return fmt.Errorf("Invalid magic number: %s", magic)
	}

	r.Header.Version = int(magic[3])
	if r.Header.Version < 5 {
		return fmt.Errorf("Unsupported sequence file version: %d", r.Header.Version)
	}

	keyClassName, err := r.readString()
	if err != nil {
		return err
	}

	valueClassName, err := r.readString()
	if err != nil {
		return err
	}

	if keyClassName != "org.apache.hadoop.io.BytesWritable" {
		return fmt.Errorf("Unsupported key serialization: %s", keyClassName)
	}

	if valueClassName != "org.apache.hadoop.io.BytesWritable" {
		return fmt.Errorf("Unsupported value serialization: %s", valueClassName)
	}

	r.Header.KeyClassName = keyClassName
	r.Header.ValueClassName = valueClassName

	flags, err := r.consume(2)
	if err != nil {
		return err
	}

	valueCompression := uint8(flags[0])
	blockCompression := uint8(flags[1])
	if blockCompression > 0 {
		r.Header.Compression = BlockCompressed
	} else if valueCompression > 0 {
		r.Header.Compression = ValueCompressed
	} else {
		r.Header.Compression = NotCompressed
	}

	if r.Header.Compression != NotCompressed {
		compressionCodecClassName, err := r.readString()
		if err != nil {
			return err
		}

		r.Header.CompressionCodecClassName = compressionCodecClassName
	}

	if r.Header.Compression != NotCompressed {
		return fmt.Errorf("Unsupported compression codec: %s", r.Header.CompressionCodecClassName)
	}

	err = r.readMetadata()
	if err != nil {
		return err
	}

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
	b, err := r.consume(4)
	if err != nil {
		return err
	}

	pairs := int(binary.BigEndian.Uint32(b))
	if pairs < 0 || pairs > 1024 {
		return fmt.Errorf("Invalid metadata pair count: %d", pairs)
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
	b, err := r.consume(1)
	if err != nil {
		return "", err
	}

	length := int(b[0])
	b, err = r.consume(length)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
