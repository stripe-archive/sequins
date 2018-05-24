package main

import (
	"io"
	"os"

	"github.com/ncw/directio"
)

func WriteFileAligned(dst string, r io.Reader, bufSize int) (err error) {
	var w *os.File
	w, err = directio.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return
	}
	defer func() {
		err2 := w.Close()
		if err == nil {
			err = err2
		}
	}()
	return writeAligned(w, r, bufSize)
}

func writeAligned(w io.Writer, r io.Reader, bufSize int) error {
	buf := directio.AlignedBlock(bufSize)
	n := 0
	for {
		n2, err := r.Read(buf[n:])
		n += n2
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if n == bufSize {
			_, err := w.Write(buf)
			if err != nil {
				return err
			}
			n = 0
		}
	}

	if n > 0 {
		if directio.AlignSize > 0 {
			for i := n & (directio.AlignSize - 1); i > 0 && i < directio.AlignSize; i++ {
				buf[n] = 0
				n++
			}
		}
		_, err := w.Write(buf[:n])
		if err != nil {
			return err
		}
	}
	return nil
}
