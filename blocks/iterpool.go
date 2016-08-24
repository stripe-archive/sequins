package blocks

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/bsm/go-sparkey"
)

var errOpenFailed = errors.New("failed to open block iter")

// iterPool is a sync.Pool for *sparkey.HashIter objects. Since each one
// represents a sparkey buffer in cgo, it's important to reuse them where
// possible. Since cgo file descriptors don't get closed on GC, we use a
// finalizer to do that.
type iterPool struct {
	sparkeyReader *sparkey.HashReader
	*sync.Pool
}

// newIterPool creates a sync.Pool for *sparkey.HashIter objects.
//
// The new func returns nil if the sparkey.HashIter couldn't be created.
func newIterPool(sparkeyReader *sparkey.HashReader) iterPool {
	return iterPool{
		sparkeyReader: sparkeyReader,
		Pool:          new(sync.Pool),
	}
}

func (ip iterPool) getIter() (*sparkey.HashIter, error) {
	iter := ip.Get()
	if iter == nil {
		iter, err := ip.sparkeyReader.Iterator()
		if err != nil {
			return nil, fmt.Errorf("opening block iter: %s", err)
		}

		runtime.SetFinalizer(iter, (*sparkey.HashIter).Close)
		return iter, nil
	}

	return iter.(*sparkey.HashIter), nil
}
