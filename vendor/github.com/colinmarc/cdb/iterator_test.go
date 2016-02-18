package cdb_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/colinmarc/cdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	db, err := cdb.Open("./test/test.cdb")
	require.NoError(t, err)
	require.NotNil(t, db)

	n := 0
	iter := db.Iter()
	for iter.Next() {
		assert.Equal(t, string(expectedRecords[n][0]), string(iter.Key()))
		assert.Equal(t, string(expectedRecords[n][1]), string(iter.Value()))
		require.NoError(t, iter.Err())
		n++
	}

	assert.Equal(t, len(expectedRecords)-1, n)

	require.NoError(t, iter.Err())
}

func BenchmarkIterator(b *testing.B) {
	db, _ := cdb.Open("./test/test.cdb")
	iter := db.Iter()
	b.ResetTimer()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		for iter.Next() {
		}
	}
}

func ExampleIterator() {
	db, err := cdb.Open("./test/test.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Create an iterator for the database.
	iter := db.Iter()
	for iter.Next() {
		// Do something with iter.Key()/iter.Value()
	}

	if err := iter.Err(); err != nil {
		log.Fatal(err)
	}
}
