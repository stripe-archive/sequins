package cdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	h := newCDBHash()
	h.Write([]byte("foo bar baz"))
	assert.EqualValues(t, 776976811, h.Sum32())

	h = newCDBHash()
	h.Write([]byte("The quick brown fox jumped over the lazy dog"))
	assert.EqualValues(t, 3538394712, h.Sum32())
}
