# Sharding

This is an in-depth description of how sharding works. For an overview of
running a sequins cluster, including notes on how it should behave, see [Running
A Distributed Cluster](../1-4-running-a-distributed-cluster/README.md).

The algorithm described here is effectively isolated between databases.
Therefore, this document speaks to the operations on a single sequins database,
rather than everything at once.

# Design Principles

 - Sequins should be able to scale horizontally by just adding nodes to an
   existing cluster. This should require as little shared state (in Zookeeper)
   as possible, and that shared state should be considered ephemeral.
   Specifically, sequins should absolutely not require shared state in the read
   path; meaning that any node must be able to continue serving requests without
   any shared state available.

 - To minimize implementation complexity, sharding should be as static as
   possible. A cluster must reshard a database once at startup, and once
   whenever a new version is available, but should not dynamically correct over-
   or underreplication.

 - Since sequins is a more-or-less static, stateless store, and since writes are
   async and affect the entire database at once, we don't have to think too hard
   about consistency. But one important property to try to guarantee is
   monotonic reads; if you talk to a single sequins instance during a version
   upgrade, you get only records from version (N-1) until the node upgrades, and
   then only records from version N.

# Shared State in Zookeeper

Zookeeper is used for shared state. To minimize complexity and stay robust to
Zookeeper failure, the code only supports two operations:

 - Publishing ephemeral keys into a znode
 - Listing a znode's children and caching the state locally

All state must be considered possibly minutes, hours or years stale. All read
operations read the cache; the syncing process happens separately.

All znodes described below are prefixed under a root which includes the [cluster
name](../x-1-configuration-reference#clustername) and a protocol number
(currently `v1`).

# Properties of a Version

A version is an immutable map of keys to values, existing in the source root as
a collection of N files. Keys can be duplicated in the map, but only one value
will be loaded (which one is picked is undefined).

In addition, each version is sharded into N partitions. Each partition K
contains the keyspace where `hashCode(key) % N == K`.

We use Java's String hashCode algorithm as the hashing function, and the number of
files as N, because that allows us to treat many datasets as pre-sharded by
Hadoop's shuffle step. In particular, any Hadoop job with the [default
Partitioner][partitioner] will produce a version where each file has the
keyspace of one and only one partition.

However, this isn't a guarantee, so we treat this only as a possible
optimization.

[partitioner]: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/lib/partition/HashPartitioner.html

# Upgrades

This is the meatiest part of the distributed algorithm: how nodes discover and
load new versions.

**On startup** (or when reconnecting to zookeeper), a node

 1. Creates an ephemeral znode under `/nodes` for itself

 2. Starts watching `/nodes`. On startup, it waits for these to remain stable
    for [some period](../x-1-configuration-reference#timetoconverge) before
    continuing.

**When it sees a new version** (this also occurs at startup, and applies to both
existing and new databases), it

 1. Watches the children of `partitions/<version>`, and continually mirrors that
    state locally. This is the map of where partitions live on the cluster.

 2. Waits until every partition represented in the cluster at least once; in
    other words, until the minimum replication factor is at least
    1.

In the background, it decides which partitions it is responsible for and
backfills them. To do that, it

 1. Decides which partitions it is responsible for offline, by:

    a. Creating an array of all partitions in sorted order * the replication
       factor. (e.g. [1, 1, 1, 2, 2, 2, ...])

    b. Creating an array of all shard ids it knows about in sorted order. If
       multiple nodes share the same shard id, the shard id will only show up
       once in the array.

    c. For all of the partitions at index `idx`, if `idx % len(nodes)` is equal
       to its position in the node array, we assign it that partition.

 2. Starts loading and preparing those partitions. As they become available, it
    writes an ephemeral node to the partition map, at
    `/partitions/<version>/<partition>@<hostname>`.

Note that the ring is only used as a way to stably, fairly and deterministically
pick partitions without actually needing to read  current state of the cluster
(which could be racy). Once the partition map is built, that is used as the
actual state of the cluster going forward.

Once a node has the data locally, it can respond to peers that specifically ask
that version, but it won't upgrade _to clients_ until it sees that a version is
complete across the cluster. Note that this switch to clients, which is the
actual upgrade, can happen before or after the local partitions are finished
loading; it's decoupled from that process. This is important if, for example, a
node is starting up to join an existing cluster, and wants to be able to service
requests immediately.

Even once it does upgrade (again, to clients), it keeps the old version around
for a period of time. Specifically, it starts a 10 minute timer, and every time
it receives a request for the old version, it resets the timer. Only at the end
of the timer does it clear the old version.

That means that a cluster effectively has two versions during an upgrade. In
this way, individual nodes can lag behind and still present a consistent picture
to the clients they talk to.

**Finally, when responding to a request**, the node

 - Looks to see if it has the partition of the key locally. If so, it responds immediately
 - If not, determines a list of peers by consulting the cache of
   `/partitions/<version>/`
 - Picks a node at random and tries it (`?proxy=true` is added to the
   querystring to indicate that it shouldn't be proxied further).
 - Every duration of
   [proxy_stage_timeout](../x-1-configuration-reference#proxystagetimeout), it
   starts a request to a new node in parallel.
 - Returns the first request that succeeds, or bails after
   [proxy_timeout](../x-1-configuration-reference#proxy_timeout).
