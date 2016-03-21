package main

import (
	"log"

	"github.com/bsm/go-sparkey"
)

const dbfile = "/tmp/sparkeydata"

func abortOn(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	// Write a data file

	writer, err := sparkey.CreateLogWriter(dbfile, nil)
	abortOn(err)

	err = writer.Put([]byte("key1"), []byte("value"))
	abortOn(err)
	err = writer.Put([]byte("key2"), []byte("value"))
	abortOn(err)
	err = writer.Put([]byte("key3"), []byte("value"))
	abortOn(err)

	// Close the writer, write a hash index
	err = sparkey.WriteHashFile(dbfile, sparkey.HASH_SIZE_AUTO)
	abortOn(err)
	writer.Close()

	// Re-open data file, append data

	writer, err = sparkey.OpenLogWriter(dbfile)
	abortOn(err)
	err = writer.Put([]byte("key4"), []byte("value"))
	abortOn(err)
	err = writer.Put([]byte("key1"), []byte("new value"))
	abortOn(err)
	err = writer.Delete([]byte("key3"))
	abortOn(err)

	// Close the writer, re-write the hash index
	writer.Close()
	err = sparkey.WriteHashFile(dbfile, sparkey.HASH_SIZE_AUTO)
	abortOn(err)

	// Open the db for reading
	reader, err := sparkey.Open(dbfile)
	abortOn(err)
	defer reader.Close()

	// Get a value from the db
	val, err := reader.Get([]byte("key1"))
	abortOn(err)
	log.Println("key1", ":", string(val))

	// Create an iterator
	iter, err := reader.Iterator()
	abortOn(err)
	defer iter.Close()

	// Get a value via iterator
	val, err = reader.Get([]byte("key4"))
	abortOn(err)
	log.Println("key4", ":", string(val))

	// Iterate over everything
	for iter.NextLive(); iter.Valid(); iter.NextLive() {
		key, err := iter.Key()
		abortOn(err)
		val, err := iter.Value()
		abortOn(err)
		log.Println(string(key), ":", string(val))
	}
}
