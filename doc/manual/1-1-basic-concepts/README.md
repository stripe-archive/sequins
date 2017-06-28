# Basic Concepts

### The Source Root

Sequins is generally _stateless_; it works by mirroring data at rest somewhere
else. When sequins starts up, and whenever it is told to refresh its local data,
it will do its best to mirror the organizational structure of the source root.

This source root can be on local disk, [HDFS][hdfs], or [Amazon S3][s3]. You can
set it be setting the [source root](../x-1-configuration-reference#source)
configuration property to a URI:

 - Data in local disk can be referred to by just the path, or with a `file://`
   URI:

        file:///path/to/data


 - Data in HDFS can be referred to with an `hdfs://` URI, using the namenode
   address and port as the hostname:

        hdfs://namenode:8020/path/to/data


 - Data in S3 can be referred to by an `s3://` URI, using the bucket name as
   the host:

        s3://my-bucket/path/to/data

In the later case, the "path" is really a key prefix; S3 does not have real
directories. Sequins treats prefix components separated by `/` as directories,
just like awscli or other tools.

Under the source root, the data should be organized into **databases** and below
that into **versions**:

    /path/to/data
    ├── <database>
    │   ├── <version>
    │   │   ├── <data file>
    │   │   ├── <data file>
    │   │   ├── <data file>
    │   │   └── ...
    │   └── <version>
    │       └── ...
    └── <database>
        └── <version>
            └── ...

Both the database and version can be arbitrary strings, but URI-friendly strings
are recommended.

[hdfs]: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html
[s3]: https://aws.amazon.com/s3/

### Databases and Versions

A single sequins instance can hold multiple **databases**. This is the first layer of
directories in the [source root](#the-source-root).

Sequins doesn't support individual writes, like other object stores would.
Instead, databases are versioned. To update a database, you present it with an
entirely new copy of the dataset, called a **version**, by dropping it into the
database folder in the source root.

Database and version names must consist of alphanumeric characters,
hyphens, and underscores (path components are ignored if they include
other characters). But otherwise, they are just arbitrary strings, and
are compared lexicographically - so to update a database, you need to
give it a version that is lexicographically greater than the current
one. At Stripe we use `date +%Y%m%d` (eg `20160901`) and timestamps.

Sequins will load this in the background and hotswap it in atomically.
Additionally, Sequins returns an `X-Sequins-Version` header [on
responses](../1-3-querying-sequins/README.md#response-and-request-headers) if
you need to track what version you're getting.

### Loading New Data

You can tell sequins to check for new data in two ways. The first is the
[refresh_period](../x-1-configuration-reference/README.md#refreshperiod)
configuration property, which instructs sequins to continually check for new
data. You can also send a SIGHUP to the process and it will reload a single
time.

Either way, sequins will perform three operations:

 - For any new databases that weren't there before, load the latest version and
   start serving it.

 - For each database which is no longer in the source root, stop serving it and
   delete it locally.

 - For each existing database, load whichever version is lexicographically
   greatest, if it is not the current local version.
