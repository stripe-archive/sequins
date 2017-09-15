package blocks

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
)

var (
	data = [][]byte{
		[]byte("Bob"),
		[]byte("Linda"),
		[]byte("Tina"),
		[]byte("Gene"),
		[]byte("Louise"),
		[]byte("Gale"),
		[]byte("Teddy"),
		[]byte("Mort"),
		[]byte("Jimmy Jr"),
		[]byte("Ollie"),
		[]byte("Andy"),
		[]byte("Regular-Sized Rudy"),
		[]byte("Zeke"),
	}
)

func BenchmarkBlockStoreNoCompression(b *testing.B) {
	tmpDir, _ := ioutil.TempDir("", "sequins-benchmark-")

	dataSize := len(data)
	s := rand.NewSource(time.Now().Unix())

	r := rand.New(s)

	bs := New(tmpDir, 8, NoCompression, 8192)
	for n := 0; n < b.N; n++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(n))
		bs.Add(key, data[r.Intn(dataSize)])
	}
	bs.Save(nil)
}

func BenchmarkBlockStoreCompression(b *testing.B) {
	tmpDir, _ := ioutil.TempDir("", "sequins-benchmark-")

	dataSize := len(data)
	s := rand.NewSource(time.Now().Unix())

	r := rand.New(s)

	bs := New(tmpDir, 8, SnappyCompression, 8192)
	for n := 0; n < b.N; n++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(n))
		bs.Add(key, data[r.Intn(dataSize)])
	}
	bs.Save(nil)
}
