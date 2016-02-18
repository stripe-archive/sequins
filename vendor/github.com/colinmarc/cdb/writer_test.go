package cdb_test

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/colinmarc/cdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testWritesReadable(t *testing.T, writer *cdb.Writer) {
	expected := make([][][]byte, 0, 100)
	for i := 0; i < cap(expected); i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(randomdata.SillyName())
		err := writer.Put(key, value)
		require.NoError(t, err)

		expected = append(expected, [][]byte{key, value})
	}

	db, err := writer.Freeze()
	require.NoError(t, err)

	for _, record := range expected {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		require.Nil(t, err)
		assert.Equal(t, string(record[1]), string(val), msg)
	}
}

func TestWritesReadable(t *testing.T) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	testWritesReadable(t, writer)
}

func TestWritesReadableFnv(t *testing.T) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, fnv.New32a())
	require.NoError(t, err)
	require.NotNil(t, writer)

	testWritesReadable(t, writer)
}

func testWritesRandom(t *testing.T, writer *cdb.Writer) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	records := make([][][]byte, 0, 1000)
	seenKeys := make(map[string]bool)
	stringType := reflect.TypeOf("")

	// Make sure we don't end up with duplicate keys, since that makes testing
	// hard.
	for len(records) < cap(records) {
		key, _ := quick.Value(stringType, random)
		if !seenKeys[key.String()] {
			value, _ := quick.Value(stringType, random)
			keyBytes := []byte(key.String())
			valueBytes := []byte(value.String())
			records = append(records, [][]byte{keyBytes, valueBytes})
			seenKeys[key.String()] = true
		}
	}

	for _, record := range records {
		err := writer.Put(record[0], record[1])
		require.NoError(t, err)
	}

	db, err := writer.Freeze()
	require.NoError(t, err)

	for _, record := range records {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		require.Nil(t, err)
		assert.Equal(t, string(record[1]), string(val), msg)
	}
}

func TestWritesRandom(t *testing.T) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	testWritesRandom(t, writer)
}

func TestWritesRandomFnv(t *testing.T) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, fnv.New32a())
	require.NoError(t, err)
	require.NotNil(t, writer)

	testWritesRandom(t, writer)
}

func benchmarkPut(b *testing.B, writer *cdb.Writer) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	stringType := reflect.TypeOf("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := quick.Value(stringType, random)
		value, _ := quick.Value(stringType, random)
		keyBytes := []byte(key.String())
		valueBytes := []byte(value.String())

		writer.Put(keyBytes, valueBytes)
	}
}

func BenchmarkPut(b *testing.B) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter(f, nil)
	require.NoError(b, err)

	benchmarkPut(b, writer)
}

func BenchmarkPutFnv(b *testing.B) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter(f, fnv.New32a())
	require.NoError(b, err)

	benchmarkPut(b, writer)
}

func ExampleWriter() {
	writer, err := cdb.Create("/tmp/example.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Write some key/value pairs to the database.
	writer.Put([]byte("Alice"), []byte("Practice"))
	writer.Put([]byte("Bob"), []byte("Hope"))
	writer.Put([]byte("Charlie"), []byte("Horse"))

	// It's important to call Close or Freeze when you're finished writing
	// records.
	writer.Close()
}
