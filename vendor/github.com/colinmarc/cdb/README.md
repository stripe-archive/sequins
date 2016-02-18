CDB
===

[![GoDoc](https://godoc.org/github.com/colinmarc/cdb/web?status.svg)](https://godoc.org/github.com/colinmarc/cdb) [![build](https://travis-ci.org/colinmarc/cdb.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

This is a native Go implementation of [cdb][1], a constant key/value database
with some very nice properties. From the [design doc][1]:

> cdb is a fast, reliable, simple package for creating and reading constant databases. Its database structure provides several features:
> - Fast lookups: A successful lookup in a large database normally takes just two disk accesses. An unsuccessful lookup takes only one.
> - Low overhead: A database uses 2048 bytes, plus 24 bytes per record, plus the space for keys and data.
> - No random limits: cdb can handle any database up to 4 gigabytes. There are no other restrictions; records don't even have to fit into memory. Databases are stored in a machine-independent format.

[1]: http://cr.yp.to/cdb.html

Usage
-----

```go
writer, err := cdb.Create("/tmp/example.cdb")
if err != nil {
  log.Fatal(err)
}

// Write some key/value pairs to the database.
writer.Put([]byte("Alice"), []byte("Practice"))
writer.Put([]byte("Bob"), []byte("Hope"))
writer.Put([]byte("Charlie"), []byte("Horse"))

// Freeze the database, and open it for reads.
db, err := writer.Freeze()
if err != nil {
  log.Fatal(err)
}

// Fetch a value.
v, err := db.Get([]byte("Alice"))
if err != nil {
  log.Fatal(err)
}

log.Println(string(v))
// => Practice

// Iterate over the database
iter := db.Iter()
for iter.Next() {
    log.Printf("The key %s has a value of length %d\n", string(iter.Key()), len(iter.Value()))
}

if err := iter.Err(); err != nil {
    log.Fatal(err)
}
```
