package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/ncw/directio"
)

type noopCloser struct {
	io.Writer
}

func (noopCloser) Close() error {
	return nil
}

func TestWriteAlignedNeedsAlignment(t *testing.T) {
	bufSize := 4096
	buf := make([]byte, 2*bufSize+3)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i % 256)
	}
	b := &bytes.Buffer{}
	err := writeAligned(b, bytes.NewReader(buf), bufSize)
	if err != nil {
		t.Fatal(err)
	}

	if directio.AlignSize > 3 {
		if b.Len() != (2*bufSize + directio.AlignSize) {
			t.Fatal(b.Len())
		}
	} else {
		if b.Len() != len(buf) {
			t.Fatal(b.Len())
		}
	}
	for i := 0; i < len(buf); i++ {
		if b.Bytes()[i] != buf[i] {
			t.Fatalf("%d: %d != %d", i, b.Bytes()[i], buf[i])
		}
	}
	for i := len(buf); i < bufSize*2+directio.AlignSize; i++ {
		if b.Bytes()[i] != 0 {
			t.Fatalf("%d: %d", i, b.Bytes()[i])
		}
	}
}

func TestWriteAlignedNoAlignment(t *testing.T) {
	bufSize := 4096
	buf := make([]byte, 2*bufSize+bufSize+directio.AlignSize)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i % 256)
	}
	b := &bytes.Buffer{}
	err := writeAligned(b, bytes.NewReader(buf), bufSize)
	if err != nil {
		t.Fatal(err)
	}

	if b.Len() != len(buf) {
		t.Fatal(b.Len())
	}
	for i := 0; i < len(buf); i++ {
		if b.Bytes()[i] != buf[i] {
			t.Fatalf("%d: %d != %d", i, b.Bytes()[i], buf[i])
		}
	}
}
