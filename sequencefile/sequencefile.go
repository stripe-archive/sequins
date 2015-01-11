package sequencefile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type compression int

const (
	NotCompressed compression = iota + 1
	ValueCompressed
	BlockCompressed

	SyncSize = 16
)

var ErrSync = errors.New("Found sync marker")

type sequenceFileHeader struct {
	Version                   int
	Compression               compression
	CompressionCodecClassName string
	KeyClassName              string
	ValueClassName            string
	Metadata                  map[string]string
	SyncMarker                string
}

type SequenceFile struct {
	Path   string
	Header sequenceFileHeader

	file   *os.File
	buf    bytes.Buffer
	offset int64
}

type Record struct {
	Key   *io.SectionReader
	Value *io.SectionReader

	Offset      int64
	TotalLength int64
}

type ReadError struct {
	path   string
	error  error
	offset int64
}

func (e *ReadError) Error() string {
	return fmt.Sprintf("Error reading file %s at %d: %s", e.path, e.offset, e.error)
}

func New(path string) (*SequenceFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	sf := &SequenceFile{file: file, Path: path}

	err = sf.readHeader()
	if err != nil {
		return nil, sf.newReadError(err)
	}

	return sf, err
}

func (sf *SequenceFile) ReadNextRecord() (*Record, error) {
	record, err := sf.readRecord()
	if err == ErrSync {
		err := sf.readAndCheckSync()
		if err != nil {
			return nil, sf.newReadError(err)
		}

		return sf.ReadNextRecord()
	} else if err == io.EOF {
		return nil, err
	} else if err != nil {
		return nil, sf.newReadError(err)
	}

	return record, nil
}

func (sf *SequenceFile) ReadRecordAtOffset(offset int64) (*Record, error) {
	sf.offset = offset
	sf.file.Seek(offset, 0)

	record, err := sf.readRecord()
	if err == ErrSync {
		return nil, sf.newReadError(errors.New("Unexpected sync marker"))
	} else if err != nil {
		return nil, sf.newReadError(err)
	}

	return record, nil
}

func (sf *SequenceFile) readHeader() error {
	magic, err := sf.readString(3)
	if err != nil || magic != "SEQ" {
		return fmt.Errorf("Error reading magic number: %s", magic)
	}

	version, err := sf.readUint8()
	if err != nil {
		return err
	}

	if version < 5 {
		return fmt.Errorf("Unsupported sequence file version: %d", version)
	}

	sf.Header.Version = int(version)

	_, keyClassName, err := sf.readLengthPrefixedString()
	if err != nil {
		return err
	}

	_, valueClassName, err := sf.readLengthPrefixedString()
	if err != nil {
		return err
	}

	if keyClassName != "org.apache.hadoop.io.BytesWritable" {
		return fmt.Errorf("Unsupported key serialization: %s", keyClassName)
	}

	if valueClassName != "org.apache.hadoop.io.BytesWritable" {
		return fmt.Errorf("Unsupported value serialization: %s", valueClassName)
	}

	sf.Header.KeyClassName = keyClassName
	sf.Header.ValueClassName = valueClassName

	valueCompression, err := sf.readUint8()
	if err != nil {
		return err
	}

	blockCompression, err := sf.readUint8()
	if err != nil {
		return err
	}

	if blockCompression > 0 {
		sf.Header.Compression = BlockCompressed
	} else if valueCompression > 0 {
		sf.Header.Compression = ValueCompressed
	} else {
		sf.Header.Compression = NotCompressed
	}

	if sf.Header.Compression != NotCompressed {
		_, compressionCodecClassName, err := sf.readLengthPrefixedString()
		if err != nil {
			return err
		}

		sf.Header.CompressionCodecClassName = compressionCodecClassName
	}

	if sf.Header.Compression != NotCompressed {
		return fmt.Errorf("Unsupported compression codec: %s", sf.Header.CompressionCodecClassName)
	}

	err = sf.readMetadata()
	if err != nil {
		return err
	}

	marker, err := sf.readString(SyncSize)
	if err != nil {
		return fmt.Errorf("Invalid sync marker: %s", marker)
	}

	sf.Header.SyncMarker = marker

	return nil
}

func (sf *SequenceFile) readMetadata() error {
	pairs, err := sf.readInt32()
	if err != nil {
		return err
	}

	numPairs := int(pairs)

	if numPairs < 0 || numPairs > 1024 {
		return fmt.Errorf("Invalid metadata pair count length: %d", numPairs)
	}

	metadata := make(map[string]string, numPairs)
	for i := 0; i < numPairs; i++ {
		_, key, err := sf.readLengthPrefixedString()
		if err != nil {
			return err
		}

		_, value, err := sf.readLengthPrefixedString()
		if err != nil {
			return err
		}

		metadata[key] = value
	}

	sf.Header.Metadata = metadata
	return nil
}

func (sf *SequenceFile) readRecord() (*Record, error) {
	record := Record{Offset: sf.offset}

	totalLength, err := sf.readInt32()
	if err != nil {
		return nil, err
	}

	if totalLength == -1 {
		return nil, ErrSync
	} else if totalLength < 0 {
		return nil, fmt.Errorf("Invalid record length: %d", totalLength)
	}

	totalKeyLength, err := sf.readInt32()
	if err != nil {
		return nil, err
	}

	keyOffset := sf.offset + 4 // skip four for BytesWritable length
	keyLength := int64(totalKeyLength) - 4

	if keyLength < 0 {
		return nil, fmt.Errorf("Invalid key length: %d", keyLength)
	}

	valueOffset := sf.offset + int64(totalKeyLength) + 4
	valueLength := int64(totalLength - totalKeyLength - 4)

	record.Key = io.NewSectionReader(sf.file, keyOffset, keyLength)
	record.Value = io.NewSectionReader(sf.file, valueOffset, valueLength)
	record.TotalLength = int64(totalLength) + 8 // add 4 bytes each for key length and value length

	off, err := sf.file.Seek(int64(totalLength), 1)
	if err != nil {
		return nil, err
	}

	sf.offset = off

	return &record, nil
}

func (sf *SequenceFile) readAndCheckSync() error {
	b, err := sf.readBytes(SyncSize)

	if err != nil {
		return err
	} else if !bytes.Equal(b, []byte(sf.Header.SyncMarker)) {
		return fmt.Errorf("Invalid sync marker: %x", b)
	}

	return nil
}

func (sf *SequenceFile) readUint8() (uint8, error) {
	var i uint8
	err := binary.Read(sf.file, binary.BigEndian, &i)
	if err != nil {
		return 0, err
	}

	sf.offset++
	return i, nil
}

func (sf *SequenceFile) readInt32() (int32, error) {
	var i int32
	err := binary.Read(sf.file, binary.BigEndian, &i)
	if err != nil {
		return 0, err
	}

	sf.offset += 4
	return i, nil
}

func (sf *SequenceFile) readBytes(length int64) ([]byte, error) {
	sf.buf.Reset()
	n, err := sf.buf.ReadFrom(io.LimitReader(sf.file, length))

	sf.offset += n
	if err != nil {
		return nil, err
	}

	return sf.buf.Bytes(), nil
}

func (sf *SequenceFile) readString(length uint8) (string, error) {
	if length == 0 {
		return "", nil
	}

	b, err := sf.readBytes(int64(length))
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (sf *SequenceFile) readLengthPrefixedString() (uint8, string, error) {
	length, err := sf.readUint8()
	if err != nil {
		return 0, "", err
	}

	string, err := sf.readString(length)
	if err != nil {
		return 0, "", err
	}

	return length, string, nil
}

func (sf *SequenceFile) newReadError(e error) *ReadError {
	return &ReadError{sf.Path, e, sf.offset}
}
