# Getting Started

### Installing and Running Sequins

You can download a recent release from the [github releases page][releases].

Unzip it wherever you like. Then you can run it to see the usage:

    $ ./sequins --help
    usage: sequins [<flags>]

    Flags:
          --help                Show context-sensitive help (also try
                                --help-long and --help-man).
      -b, --bind=ADDRESS        Address to bind to. Overrides the config
                                option of the same name.
      -r, --source=URI          Where the sequencefiles are. Overrides the
                                config option of the same name.
      -l, --local-store=PATH    Where to store local data. Overrides the
                                config option of the same name.
          --config=PATH         The config file to use. By default, either
                                sequins.conf in the local directory or /etc/
                                sequins.conf will be used.
          --debug-bind=ADDRESS  Address to bind to for pprof and expvars.
                                Overrides the config option of the same name.
          --version             Show application version.

First, start up sequins and point it to wherever you intend to keep your data.
This can be in HDFS:

    $ hadoop fs -mkdir /sequins
    $ ./sequins --local-store /tmp/sequins --bind localhost:9599 \
      --source hdfs://namenode:8020/sequins

Or, if you put it in S3, use a S3 URI[^1]:

    $ ./sequins --local-store /tmp/sequins --bind localhost:9599 \
      --source s3://my-bucket/sequins

If you're just playing around and don't have HDFS or S3 available, you can give
sequins a local path:

    $ mkdir /tmp/foobar
    $ ./sequins --local-store /tmp/sequins --bind localhost:9599 \
      --source /tmp/foobar

Now you can query it (I'm using [httpie][httpie] here, but
curl works just as well):

    $ http localhost:9599/foo/bar
    HTTP/1.1 404 Not Found
    Content-Length: 0
    Content-Type: text/plain; charset=utf-8
    Date: Mon, 01 Aug 2016 11:57:53 GMT

However, you'll only get 404s, since sequins hasn't loaded any data yet. We need
to give it some.

[releases]: https://github.com/stripe/sequins/releases
[httpie]: http://httpie.org

### Writing some data

Sequins works by asynchronously mirroring data at rest in HDFS, S3, or another
filesystem. It's particularly suited for ingesting data written from
[Hadoop][hadoop] or similar tools.

You'll want to write a job that dumps out some key/value-oriented data in the
[SequenceFile][sequencefile] format. This is a commonly-used format in the
Hadoop ecosystem, so tools like Pig, Scalding or Spark should all be able to
write it out of the box. More info on the supported formats can be found in the
[Data Requirements](1-2-data-requirements/README.md) section.

Once you have your data ready, you need to arrange it in a particular way in
S3, HDFS, or on local disk[^2]:

    /path/to/data
    └── mydata
        └── version0
            ├── part-00000  # These names are irrelevant, but this is how Hadoop
            ├── part-00001  # names files.
            ├── part-00002
            └── ...

`mydata` is a name for your dataset, which we'll henceforth call a **database**.
`version0` is the **version** of the database; that comes into play when we want
to update it. Both are arbitrary strings.

Once your data is written, tell sequins to reload the data by HUPing it:

    $ pkill -HUP -f sequins

Sequins should start automatically  downloading and mirroring your data. If it's
a large dataset, this can take a while. (If it's a really big dataset, you'll
want to read about [sharding over multiple
machines](1-4-running-a-distributed-cluster/README.md).)

You can check the progress by opening http://localhost:9599 in a browser, or
simply watching the logs.

[hadoop]: http://hadoop.apache.org
[sequencefile]: http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/SequenceFile.html

### Querying it

Once your data is finished loading, you can query it by issuing an HTTP GET to
`/<database>/<key>`:

    $ http localhost:9599/<database>/<key>
    HTTP/1.1 200 OK
    Content-Length: 7
    Date: Mon, 01 Aug 2016 11:57:53 GMT
    Last-Modified: Mon, 01 Aug 2016 11:56:27 GMT
    X-Sequins-Version: version0

    <value>

**Note**: The database names `healthz` and `healthcheck` cannot be used as they
are reserved for the healthcheck endpoints.

### Further Configuration

Sequins usually reads its config from a configuration file called
`sequins.conf`, either in the local directory or at `/etc/sequins.conf`.

The release tarballs contain a `sequins.conf.example`, which you can copy and
modify as you please; you can also check out the [Configuration
Reference](x-1-configuration-reference/README.md) for more details.

[^1]: You may need S3 credentials in your environment or in a config file. See the [Configuration Reference](x-1-configuration-reference#s3).
[^2]: Yes, S3 doesn't have directories. We do our best to pretend that it does, though. More information can be found in the [next section](1-1-basic-concepts/README.md#the-source-root).
