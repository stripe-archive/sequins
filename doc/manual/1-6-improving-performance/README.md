# Improving Performance

Sequins is written to have sane defaults out of the box, and should have very
good performance at rest. In production at Stripe, on AWS instances with SSDs,
we see a 50th percentile latency of about 500µs and a 99th percentile latency of
about 2-3ms, and we've run loadtests up to about 20k requests a second. If
you're getting significantly worse performance than that, [please let us
know](https://github.com/stripe/sequins/issues/new).

That said, there are a few tweaks available to improve load times and reduce
latency in some specific circumstances.

### Input File Optimization

Sequins will load your data faster if your input SequenceFiles are compressed
and [pre-sharded](../1-2-data-requirements#sharding).

Data will load even faster, and CPU usage will be lower, if you provide your
input data as [Sparkey format](../1-2-data-requirements#sparkey-files).

### Throttle Loads to Reduce the Impact on Latency

If you're constantly loading new data to a cluster, you may see that adversely
impact latency. On small instances, data loads can thrash the disk or network,
causing requests to drop or get timed out.

Sequins has a couple configuration settings designed to mitigate this. First,
setting [max_parallel_loads](../x-1-configuration-reference#maxparallelloads) to
a low number will effectively queue loads for different databases.

Second, the [throttle_loads](../x-1-configuration-reference#throttleloads)
property lets you significantly slow down every database load, by injecting
sleeps into the process. Used carefully, this can let you amortize the loading
cost over the period until your next write is ready.

### Tweak Proxy Timeouts

[proxy_timeout](../x-1-configuration-reference#proxytimeout) and
[proxy_stage_timeout](../x-1-configuration-reference#proxystagetimeout) let
you tweak the "backup request" strategy for proxied requests in a distributed
cluster.

The defaults, however, are intentionally set high. If you know that
your p99 is consistently under 10ms, for example, then you can adjust the latter
property down, which should reduce variability in latency significantly.
