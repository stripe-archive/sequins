# Getting Started

### Installing Sequins

You can download a recent release from the [github releases page][releases].

Unzip it wherever you like. Then you can run it to see the usage:

    $ ./sequins --help
    usage: sequins [<flags>]

    Flags:
          --help                Show context-sensitive help (also try
                                --help-long and --help-man).
      -b, --bind=ADDRESS        Address to bind to. Overrides the config
                                option of the same name.
      -r, --source=URI            Where the sequencefiles are. Overrides the
                                config option of the same name.
      -l, --local-store=PATH    Where to store local data. Overrides the
                                config option of the same name.
          --config=PATH         The config file to use. By default, either
                                sequins.conf in the local directory or /etc/
                                sequins.conf will be used.
          --debug-bind=ADDRESS  Address to bind to for pprof and expvars.
                                Overrides the config option of the same name.
          --version             Show application version.

To use it, however, we need to have some data prepared.

### Preparing some data

Sequins works by asynchronously mirroring data at rest in HDFS, S3, or another
filesystem. It's particularly suited for ingesting data written from
[Hadoop][hadoop] or similar tools.

You'll want to write a job that dumps out some key/value-oriented data in the
[SequenceFile][sequencefile] format. This is a commonly-used format in the
Hadoop ecosystem, so tools like Pig, Scalding or Spark should all be able to
write it out of the box. More info on the supported formats can be found in the
[Data Requirements](1-2-data-requirements/README.md) section.

Once you have your data ready, you need to arrange it in a particular way in
S3, HDFS, or on local disk[^1]:

    /path/to/data
    └── mydata
        └── version0
            ├── part-00000  # These names are irrelevant, but this is how Hadoop
            ├── part-00001  # names files.
            ├── part-00002
            └── ...

`mydata` is a name for your dataset, which we'll henceforth call a **database**.
`version0` is the **version** of the database; that comes into play when we want to
update it. Both are arbitrary strings.

[^1]: Yes, S3 doesn't have directories. We do our best to pretend that it does, though. More information can be found in the [next section](1-1-basic-concepts/README.md#the-source-root).

### Querying it

First, start up sequins and tell it to load up your data. If you saved it to
HDFS, then you can point it at the namenode:

    $ ./sequins --local-store /tmp/sequins --bind localhost:9599 \
      --source hdfs://namenode:8020/path/to/data

Or, if you put it in S3, use a S3 URI[^2]:

    $ ./sequins --local-store /tmp/sequins --bind localhost:9599 \
      --source s3://my-bucket/path/to/data

Sequins should start downloading and mirroring your data. If it's a large
dataset, this can take a while. (If it's a really big dataset, you'll want to
read about [sharding over multiple
machines](1-4-running-a-distributed-cluster/README.md)).

Finally, you can fetch keys over HTTP (I'm using [httpie][httpie] here, but
curl works just as well):

    $ http localhost:9599/mydata/<key>
    HTTP/1.1 200 OK
    Accept-Ranges: bytes
    Content-Length: 7
    Date: Mon, 01 Aug 2016 11:57:53 GMT
    Last-Modified: Mon, 01 Aug 2016 11:56:27 GMT
    X-Sequins-Version: version0

    <value>

[releases]: https://github.com/stripe/sequins/releases
[hadoop]: http://hadoop.apache.org
[sequencefile]: http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/SequenceFile.html
[httpie]: http://httpie.org

[^2]: You may need S3 credentials in your environment or in a config file. See the [Configuration Reference](x-1-configuration-reference).

### Further Configuration

Sequins usually reads its config from a configuration file called
`sequins.conf`, either in the local directory or at `/etc/sequins.conf`.

The release tarballs contain a `sequins.conf.example`, which you can copy and
modify as you please; you can also check out the [Configuration
Reference](x-1-configuration-reference/README.md) for more details.
