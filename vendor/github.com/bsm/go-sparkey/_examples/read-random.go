package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/bsm/go-sparkey"
)

const dbfile = "/tmp/sparkeydata"

func main() {

	// Seed values
	writer, err := sparkey.CreateLogWriter(dbfile, nil)
	abortOn(err)

	for i := 0; i < 67; i++ {
		key := fmt.Sprintf("%012d", rand.Intn(100))
		abortOn(writer.Put([]byte(key), []byte("value")))
	}
	abortOn(writer.WriteHashFile(sparkey.HASH_SIZE_AUTO))
	writer.Close()

	// Reading randomly
	db, err := sparkey.Open(dbfile)
	abortOn(err)
	defer db.Close()

	iter, err := db.Iterator()
	abortOn(err)
	defer iter.Close()

	for {
		hits, misses := 0, 0

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%012d", rand.Intn(100))
			val, err := iter.Get([]byte(key))
			abortOn(err)

			if val == nil {
				misses++
			} else {
				hits++
			}
		}
		log.Printf("hits:%d misses:%d", hits, misses)
		time.Sleep(500 * time.Millisecond)
	}
}

func abortOn(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
