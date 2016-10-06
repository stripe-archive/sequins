package blocks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test values generated with:
//    scala> "foo bar baz".hashCode

func TestJavaHashCode(t *testing.T) {
	assert.EqualValues(t, 1892173108, hashCode([]byte("foo bar baz")))
	assert.EqualValues(t, -1165917330, hashCode([]byte("foobarbaz")))
	assert.EqualValues(t, 1953395701, hashCode([]byte("The quick brown fox jumped over the lazy dog")))
}

// Test values generated with:
//    scala> var pathological = List(112, 23, 173, 120, 140, 94, 133, 241, 67, 39, 248, 103).map { _.toChar }.mkString
//    pathological: String = p?­x^ñC'øg
//
//    scala> val normal1 = "bar"
//    normal1: String = bar
//
//    scala> val normal2 = "fol"
//    normal2: String = fol
//
//    scala> def tupleHashCode(s: String) = (new Tuple(s).hashCode & Integer.MAX_VALUE) % 20
//    tupleHashCode: (s: String)Int
//
//    scala> def regularHashCode(s: String) = (s.hashCode & Integer.MAX_VALUE) % 20
//    regularHashCode: (s: String)Int
//
//    scala> tupleHashCode(normal1)
//    res5: Int = 10
//
//    scala> tupleHashCode(normal2)
//    res6: Int = 2
//
//    scala> tupleHashCode(pathological)
//    res7: Int = 2
//
//    scala> regularHashCode(normal1)
//    res8: Int = 19
//
//    scala> regularHashCode(normal2)
//    res9: Int = 11
//
//    scala> regularHashCode(pathological)
//    res10: Int = 19
//
// This means that "bar" and the pathological key would be sorted together
// under normal partitioning, but "fol" and the pathological key would be sorted
// together under cascading partitioning.
func TestKeyPartition(t *testing.T) {
	partition, alternate := KeyPartition([]byte("bar"), 20)
	assert.Equal(t, 19, partition)
	assert.Equal(t, 19, alternate)

	partition, alternate = KeyPartition([]byte("fol"), 20)
	assert.Equal(t, 11, partition)
	assert.Equal(t, 11, alternate)

	partition, alternate = KeyPartition([]byte{0x70, 0x17, 0xad, 0x78, 0x8c, 0x5e, 0x85, 0xf1, 0x43, 0x27, 0xf8, 0x67}, 20)
	assert.Equal(t, 19, partition)
	assert.Equal(t, 11, alternate)
}
