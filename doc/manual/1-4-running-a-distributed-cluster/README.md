# Running a Distributed Cluster

Multiple sequins nodes can run as a cluster, to distribute load and disk usage,
and to provide redundancy against single node failure.

To avoid the complexity this usually entails, sequins is designed in a way that
optimizes for implementation simplicity over flexibility. Sequins will
automatically and transparently divvy up and replicate your data, but this
comes with two important caveats:

 - Sequins sharding is static. A cluster will reshard a database once at
   startup, and once whenever a new version is available.

 - New nodes will happily overreplicate partitions, but if a node disappears
   the cluster will never automatically re-replicate partitions (you can,
   however, [replace the node](#node-failure)).

Sequins requires a running [Zookeeper][zk] cluster for coordination, but not to
serve requests (see [Zookeeper Failure](#zookeeper-failure) for more
information on how this dependency works, and what the failure modes are).

[zk]: https://zookeeper.apache.org/

### Setting up

You'll need the following properties in `sequins.conf` on all the nodes in the
cluster, at a minimum:

 - `sharding.enabled`: This should be set to `true`.

 - `zk.servers`: This should be the address(es) of the zookeeper quorum, eg
   `["zk1:2181"]`

There's lots of other ways to tweak your distributed setup; see the
[Configuration Reference](../x-1-configuration-reference#sharding) for details.

Then start up the nodes. Because sharding is static, sequins will wait for the
list of peers to stabilize before proceeding; you should **start the nodes at
roughly the same time**, or some partitions will be overreplicated.

That's it! Once the data is ingested, you can query any node in the cluster for
any key; peers will transparently proxy to eachother.

### Node Failure

By default, sequins has a `sharding.replication` setting of 2. That means that
in the face of a single node going down, the cluster should still be able to
respond to all requests[^1].

However, as mentioned earlier, sequins sharding is static; it will not
automatically correct the underreplication that results from a node
disappearing. If a node is permanently down, this can leave you in a precarious
position.

Sequins provides a mechanism to deal with this in the form of the
[shard_id](../x-1-configuration-reference/README.md#shardid) configuration
property. The `shard_id`, unlike hostname, does not have to be unique per node.
If you spin up a node with the same `shard_id`, it will replicate the exact same
partitions. You can use this to replace an exploded node, or to spin up a new
node in advance of decommissioning an old one.

[^1]: Of course, it's still important for clients to retry requests (and have timeouts).

### Zookeeper Failure

Sequins depends on Zookeeper for the coordination of sharding, but only ever
asynchronously. That means that if Zookeeper becomes unavailable,

 - New nodes will refuse to start up.

 - Sequins will not download new databases or versions of databases.

 - In the case of a split, where some nodes can talk to Zookeeper and others
   can not, the partition map may become stale, and you may receive different
   versions of a given database from different nodes.

Crucially, however, Zookeeper going down should **never impact an existing
cluster's ability to service requests**.

### Version upgrades

Sequins uses Zookeeper to check which nodes have which partitions. It will
upgrade to a new version when that version is minimally replicated—when every
partition is available somewhere in the cluster.

Note that this occurs before the version is fully replicated, and
possibly even before the local node has any partitions.

#### Replication

During the window between an upgrade and full replication, Sequins is
more vulnerable to node failure. You can mitigate this vulnerability by
setting `sharding.min_replication`, so that Sequins waits for a certain
amount of redundancy before triggering the upgrade.

However, a high `min_replication` will also make upgrades take longer.
In particular, if `min_replication` is the same as `replication`, a
successful upgrade requires every single node to be up and running.

Recommended settings for a stable cluster are therefore
1 < `min_replication` < `replication` <= count(`shard_id`).

#### Version consistency

Nodes in a sequins cluster will make a best-effort attempt to upgrade a given
database at more or less the same time. It is, of course, physically impossible
to ensure that individual nodes upgrade exactly at the same time, and you should
not rely on this property.

Individual nodes will also attempt to be individually consistent. If you query a
node in sequence, you should only ever see the version monotonically increase;
in other words, once a node has upgraded to a new version of a given database,
it should never return values from an older version, _even if that request was
proxied to another node_. This, however, is also best-effort, and can fail in
the case of extended coordination failure (because of a Zookeeper outage or
something else).

In these cases, Sequins will always optimize for availability, and choose to
return a value from a different version if that is the only value available.
Sequins returns an `X-Sequins-Version` header [on
responses](../1-3-querying-sequins/README.md#response-and-request-headers) if you
need to track what version you're getting.
