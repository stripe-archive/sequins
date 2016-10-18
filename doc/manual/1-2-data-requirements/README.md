# Data Requirements

Currently sequins only supports one input file format:
[SequenceFile][sequencefile]. There're a few specifics to keep in mind. These
instructions are specific to Hadoop Map/Reduce, but should be adaptable to
other tools that use the same paradigms.

[sequencefile]: http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/SequenceFile.html

### Compression

You can use either `BLOCK` or `RECORD` compression for SequenceFiles, or leave
them uncompressed. Both snappy and gzip are supported.

### Key and Value Serialization

Hadoop often writes SequenceFiles with individual key and value serializations;
in this way, SequenceFile is generic.

Sequins supports any key and value serialization, but has some exceptions for
the commons ones. In particular,
[org.apache.hadoop.io.BytesWritable][byteswritable] and
[org.apache.hadoop.io.Text][text] are unwrapped, and the actual underlying
bytes are used. For example, if you have a `SequenceFile[BytesWritable, Text]`
and a record is saved as:

    context.write(new BytesWritable("foo".getBytes("ASCII")), new Text("bar"))

Then you can query the value at `/mydata/foo`, as you'd expect.

However, with other serializations, sequins doesn't do any converting for you,
and you'll need to consider how the data actually looks on disk. This can be a
bit tricky. Let's say you have a `SequenceFile[IntWritable, IntWritable]` and
you write the tuple `(42, 100)`. Hadoop serializes a IntWritable as an unsigned
int32[^1], so you'd need to query it as such:

    $ curl localhost:9599/mydata/%00%00%00%2A | hexdump
    0000000 00 00 00 64

[byteswritable]: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/BytesWritable.html
[text]: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/Text.html

[^1]: IntWritable represents a signed int, but it's cast first; so -42 would be `%FF%FF%FF%D6`.
