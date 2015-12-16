package index

import (
	"bufio"
	"io"
	"os"
)

type bufferedReadSeeker struct {
	*bufio.Reader
	r io.ReadSeeker
}

func newBufferedReadSeeker(r io.ReadSeeker) *bufferedReadSeeker {
	return &bufferedReadSeeker{bufio.NewReader(r), r}
}

func newBufferedReadSeekerSize(r io.ReadSeeker, size int) *bufferedReadSeeker {
	return &bufferedReadSeeker{bufio.NewReaderSize(r, size), r}
}

// Seek normally just seeks the underlying ReadSeeker and resets the buffer;
// as a special case, it can 'tell' the current virtual offset while remaining
// buffered.
func (brs *bufferedReadSeeker) Seek(offset int64, whence int) (int64, error) {
	buffered := int64(brs.Buffered())

	if whence == os.SEEK_CUR {
		current, err := brs.r.Seek(0, os.SEEK_CUR)
		virtual := current - buffered

		// If we're jumping forward within the buffered area, we can do that by just
		// discarding bytes from the buffer. Otherwise, we have to jump from the
		// virtual point, then reset.
		if offset >= 0 && offset <= buffered {
			_, err = brs.Discard(int(offset))
			if err != nil {
				return virtual, err
			}

			return (current - buffered + offset), nil
		} else {
			jump := offset - (current - virtual)
			res, err := brs.r.Seek(jump, whence)
			brs.Reset(brs.r)
			return res, err
		}
	}

	// For other types of seeks, just seek the underlying reader and then reset
	// the buffer.
	res, err := brs.r.Seek(offset, whence)
	brs.Reset(brs.r)
	return res, err
}
