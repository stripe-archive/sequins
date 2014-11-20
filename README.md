![sequins!!!!!!!](http://i.imgur.com/M67cs6V.jpg)

Sequins!!!
==========

[![build](https://travis-ci.org/colinmarc/sequins.svg?branch=master)](https://travis-ci.org/colinmarc/sequins)

Sequins is a dead-simple static database. It indexes and serves [SequenceFiles][1]
over HTTP, so it's perfect for serving data created with Hadoop.

Building
--------

To create a `sequins` binary (you'll need `go` on your path):

```sh
$ git clone https://github.com/colinmarc/sequins
$ cd sequins
$ make
```

Or, to install a binary to `$GOPATH/bin`:

```sh
$ make install
```

There's also a [docker image](https://registry.hub.docker.com/u/colinmarc/sequins/).

Usage
-----

```sh
$ sequins -b ':9599' -cr 1m hdfs://namenode:8020/path/to/mydata
```

That tells sequins to load your data from HDFS, and check every minute for new
versions. The URL can point to HDFS, or s3, or just be a local path.

Sequins expects your data to be versioned. Inside the top-level directory you
you specify, you should have subdirectories, like this:

```
/mydata/
  version0/
    part-00000
    part-00001
    ...
  version1/
    ...
```

The versions can be timestamps, dates, or anything - sequins will automatically
choose whichever version is the greatest, in **lexicographical** order.

This may seem a little weird, but it works really well for aggregates that you
produce perodically, and it allows sequins to easily hotload new data (see the
corresponding section, below).

Once sequins has started and built the index, you can get the value for a given
key over HTTP. The body of the response will be the result, or if the key
doesn't exist, it'll give you a 404. For example:

```
$ http localhost:9599/foo
HTTP/1.1 200 OK
Accept-Ranges: bytes
Content-Length: 3
Content-Type: text/plain; charset=utf-8
Date: Thu, 04 Sep 2014 11:42:01 GMT
Last-Modified: Thu, 04 Sep 2014 11:39:38 GMT

bar
```

```
$ http localhost:9599/baz
HTTP/1.1 404 Not Found
Content-Length: 0
Content-Type: text/plain; charset=utf-8
Date: Thu, 04 Sep 2014 11:42:20 GMT


```

Note the `Last-Modified` header: this corresponds to the last time sequins was
given new data (see 'hotloading', below). Sequins will also happily
(and correctly) respond to requests with a `Range` header.

Hotloading
----------

Sequins knows how to hotload new data without dropping any requests. After
you've dropped a new, lexicographically-greater version into your top-level
directory, just send SIGHUP to the running process:

`kill -HUP <pid>`

and it'll download the files (if necessary), build an index in the background,
then switch when it's done. If it fails while building the new index for some
reason, it'll continue to serve the current one.

You can also tell sequins to automatically look for new versions with the
`--refresh-period` option.

If you're working with hadoop output, hotloading might accidentally load a
partial result, because Hadoop creates directories when it starts a job. To
mitigate this, you can pass in `--check-for-success`, which will tell sequins to
only load versions with a _SUCCESS file in them (Hadoop creates these files
automatically when it's done running a job).

Status
------

Sending a plain GET request to `/` will make sequins dump out its current
status, like so:

```sh
$ http localhost:9599/ | python -m json.tool
{
    "count": 3,
    "path": "path/to/stuff/1401490544",
    "started": 1409830778,
    "updated": 1409830778
}
```

Miscellany, Caveats
-------------------

 - [Here's a Scalding sink][2] for generating sequins-compatible SequenceFiles.
 It works for anything that can be converted to a JSON value.
 - The HDFS code uses [this hdfs library][3], which currently only supports
 Hadoop 2.0.0 and up (including CDH5).
 - SequenceFiles don't strictly enforce that you have only one value for each
 key; if your data has multiple values for a key, sequins will load it without
 complaint, but only index one value for the key (probably
 nondeterministically).
 - Currently, there's no support for compressed SequenceFiles, or for key/value
 serializations other than `BytesWritable`.

[1]: http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/SequenceFile.html
[2]: https://gist.github.com/colinmarc/ca4a54b9ae9364471b8d
[3]: https://github.com/colinmarc/hdfs
