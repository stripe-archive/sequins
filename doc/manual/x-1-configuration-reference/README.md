# Configuration Reference

The sequins configuration is in the [toml format][toml]. Sequins will look for
a `sequins.conf` file in the local directory, and then `/etc/sequins.conf` if
that doesn't exist.

Below is a full list of the configuration properties. Some configuration
properties are nested under headers, like `[s3]`. See the `sequins.conf.example`
file that ships with releases (also on [github][confexample]) for an example of
the file layout.

A few properties below are durations. These are strings with a shorthand unit,
like `"1s"` or `"20m"`. Valid units are `ns`, `us` (or `µs`), `ms`, `s`, `m`,
and `h`.

## Top Level Properties

### source

 Type  | Default
:----: | ------
string | _unset_ (eg `"hdfs://<namenode>:<port>/path/to/stuff"`)

The url or directory where the sequencefiles are. This can be a local directory,
an HDFS url of the form `hdfs://<namenode>:<port>/path/to/stuff`, or an S3 url
of the form `s3://<bucket>/path/to/stuff`. This should be a a directory of
directories of directories; each first level represents a 'database', and each
subdirectory therein represents a 'version' of that database. This must be set,
but can be overriden from the command line with `--source`.

### bind

Type   | Default
:----: | -------
string | `"0.0.0.0:9599"`

The address to bind on. This can be overridden from the command line with
`--bind`.

### local_store

Type   | Default
:----: | ------
string | `"/var/sequins"`

This is where sequins will store its internal copy of all the data it ingests.
This can be overriden from the command line with `--local-store.`

### max_parallel_loads

Type   | Default
:----: | ------
string | _unset_ (eg `4`)

If this flag is set, sequins will only update this many databases at a time,
minimizing disk usage while new data is being loaded. If you set this to 1, then
loads will be completely serialized.

### throttle_loads

Type   | Default
:----: | -------
string | _unset_ (eg `"800μs"`)

If this flag is set, sequins will sleep this long between writes while loading
data, artificially slowing down loads and reducing disk i/o. If you are using
disks where the latency is extremely sensitive to activity, then loading large
amounts of data can negatively impact your latency, and you may want to
experiment with this setting.

### refresh_period

Type   | Default
:----: | -------
string | _unset_ (eg `"10m"`)

If this flag is set, sequins will periodically download new data this often. If
you enable this, you should also enable `require_success_file`, or sequins may
start automatically downloading a partially-created set of files.

### require_success_file

Type | Default
:--: | -------
bool | `false`

If this flag is set, sequins will only ingest data from directories that have a
_SUCCESS file (which is produced by hadoop when it completes a job).

### content_type

Type   | Default
:----: | -------
string | _unset_ (eg `"application/json"`)

If this is set, sequins will set this Content-Type header on responses.

### goforit_flag_json_path

Type   | Default
:----: | -------
string | _unset_

If this path is specified, sequins will start [Goforit][goforit] backed by the
JSON file located at that path.  Goforit enables feature flagging
within sequins and can be used to easily toggle remote refresh on
and off.  Before each remote refresh, sequins will inspect this file for
the state of the flag sequins.prevent_download (or sequins.prevent_download
.<CLUSTER NAME> if the instance is part of a cluster).  If the flag is
not defined or is set to FALSE, remote refresh will proceed as expected.
If it's set to TRUE, sequins will not refresh from remote.

If the flag is TRUE on initial startup, sequins will backfill versions
from the local store and serve the most recent version available there.

## [storage]

### compression

Type   | Default
:----: | -------
string | `"snappy"`

This can be either 'snappy' or 'none', and defines how data is compressed on
disk.

### block_size

Type | Default
:--: | -------
int  | 4096

This controls the block size for on-disk compression.

### [s3]

### region

Type   | Default
:----: | -------
string | _unset_ (eg `"us-west-2"`)

The S3 region for the bucket where your data is. If unset, and sequins is
running on EC2, this will be set to the instance region.

### access_key_id

Type   | Default
:----: | -------
string | _see below_ (eg `"AKIAIOSFODNN7EXAMPLE"`)

### secret_access_key

Type   | Default
:----: | -------
string | _see below_ (eg `"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"`)

The access key and secret to use for S3. If unset, the env variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` will be used, or IAM instance
role credentials if they are available.

## [sharding]

### enabled

Type | Default
:--: | -------
bool | `false`

If true, sequins will attempt to connect to zookeeper at the specified addresses
(see [zk.servers](#servers)), and coordinate with peer instances to shard datasets. For a
complete description of the sharding algorithm, see the manual.

### replication

Type | Default
:--: | -------
int  | 2

This is the number of replicas responsible for each partition.

### min_replication

Type | Default
:--: | -------
int  | 1

This is the minimum number of replicas required for sequins to switch to
a new version. Set this to a higher value to ensure data redundancy before
upgrading.

You probably don't want this to be equal to `replication`,
or sequins will never upgrade versions if any node at all is down.

### max_replication
Type | Default
:--: | -------
int  | 0 (disabled)

This is the maximum number of replicas that sequins will allow to exist for a
given partition. This can come into play if ZK starts flapping while nodes
are coming online, causing partition assignments to be inconsistent. You
probably don't want this to be equal to `replication` as it will make it hard
to replace a node that is having issues (since the replacement node will see
all partitions as already properly replicated and refuse to fetch them
again).

A value of less than `replication` means that `max_replication` is
ignored and no limit is imposed.

### time_to_converge

Type   | Default
:----: | -------
string | `"10s"`

Upon startup, sequins will wait this long for the set of known peers to
stabilize.

### proxy_timeout

Type   | Default
:----: | -------
string | `"100ms"`

This is the total timeout (connect + request) for proxied requests to peers in a
sequins cluster. You may want to increase this if you're running on particularly
cold storage, or if there are other factors significantly increasing request
time.

### proxy_stage_timeout

Type   | Default
:----: | -------
string | _see below_ (eg `"50ms"`)

After this interval, sequins will try another peer concurrently with the first,
as long as there are other peers available and the total time is less than
`proxy_timeout`. If left unset, this defaults to the `proxy_timeout` divided by
`replication_factor` - enough time for all peers to be tried within the total
timeout.

### cluster_name

Type   | Default
:----: | -------
string | `"sequins"`

This defines the root prefix to use for zookeeper state. If you are running
multiple sequins clusters using the same zookeeper for coordination, you should
change this so they can't conflict.

### advertised_hostname

Type   | Default
:----: | -------
string | _see below_ (eg `"sequins1.example.com"`)

This is the hostname sequins uses to advertise itself to peers in a cluster. It
should be resolvable by those peers. If left unset, it will be set to the
hostname of the server.

### shard_id

Type   | Default
:----: | -------
string | _see below_ (eg `"sequins1"`)

The shard ID is used to determine which partitions the node is responsible for.
By default, it is the same as `advertised_hostname`. Unlike the hostname,
however, it doesn't have to be unique; two nodes can have the same shard_id, in
which case they will download the same partitions. This can be useful if you
don't have stable hostnames, but want to be able to rebuild a server to take the
place of a dead or decomissioning one.

## [zk]

### servers

Type             | Default
:--------------: | -------
array of string  | `["localhost:2181"]`

If set and 'sharding.enabled' is true, sequins will connect to zookeeper at the
given addresses.

### connect_timeout

Type   | Default
:----: | -------
string | `"1s"`


This specifies how long to wait while connecting to zookeeper.

### session_timeout

Type   | Default
:----: | -------
string | `"10s"`

This specifies the session timeout to use with zookeeper. The actual timeout is
negotiated between server and client, but will never be lower than this number.

## [datadog]

### url

Type    | Default
:-----: | -------
string  | `"localhost:8200"`

If set, sequins will send metrics concerning S3 file downloads using the
[DogStatsD][dogstatsd] protocol to this address.

## [debug]

### bind

Type   | Default
:----: | -------
string | _unset_ (eg `"localhost:6060"`)

If set, binds the golang debug http server, which can serve expvars and
profiling information, to the specified address.

### expvars

Type | Default
:--: | -------
bool | `true`

If set, this adds expvars to the debug HTTP server, including the default ones
and a few sequins-specific ones.

### pprof

Type | Default
:--: | -------
bool | `false`

If set, this adds the default pprof handlers to the debug HTTP server.

[toml]: https://github.com/toml-lang/toml
[confexample]: https://github.com/stripe/sequins/blob/master/sequins.conf.example
[dogstatsd]: https://docs.datadoghq.com/guides/dogstatsd/
[goforit]: https://github.com/stripe/goforit
