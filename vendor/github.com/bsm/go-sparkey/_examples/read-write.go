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
	writer, err := sparkey.CreateLogWriter(dbfile, nil)
	abortOn(err)
	defer writer.Close()

	abortOn(writer.WriteHashFile(sparkey.HASH_SIZE_AUTO))
	go write(writer)

	for {
		readAll()
		time.Sleep(time.Second)
	}
	select {}
}

func abortOn(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func write(writer *sparkey.LogWriter) {
	cycle := 0
	for {
		cycle++
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("%012d", rand.Intn(20))
			val := fmt.Sprintf("%010d:%d", cycle, i)
			abortOn(writer.Put([]byte(key), []byte(val)))
			time.Sleep(10 * time.Millisecond)
		}

		abortOn(writer.Flush())
		abortOn(writer.WriteHashFile(sparkey.HASH_SIZE_AUTO))
	}
}

func readAll() {
	reader, err := sparkey.Open(dbfile)
	abortOn(err)
	defer reader.Close()

	log.Println("---")
	iter, err := reader.Iterator()
	abortOn(err)
	defer iter.Close()

	for iter.NextLive(); iter.Valid(); iter.NextLive() {
		key, err := iter.Key()
		abortOn(err)
		val, err := iter.Value()
		abortOn(err)
		log.Println(string(key), "->", string(val))
	}
	abortOn(iter.Err())
}
