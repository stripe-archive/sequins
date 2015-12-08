package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test values generated with:
//    scala> "foo bar baz".hashCOde

func TestJavaHashCode(t *testing.T) {
	assert.EqualValues(t, 1892173108, hashCode([]byte("foo bar baz")))
	assert.EqualValues(t, -1165917330, hashCode([]byte("foobarbaz")))
	assert.EqualValues(t, 1953395701, hashCode([]byte("The quick brown fox jumped over the lazy dog")))
}
