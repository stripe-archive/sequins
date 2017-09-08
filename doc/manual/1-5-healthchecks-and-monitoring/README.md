# Healthchecks and Monitoring

### The Status Page and JSON

Visiting the host and port that a sequins node is running on with your browser
will give you a simple status page with sharding and version information for
each database:

![](status.png)

The information presented represents the whole cluster in the distributed case,
and should be the same no matter which node you ask. You can also ask for a
specific db, by visiting `/<db>` (in this example: `localhost:9599/flights`).

Finally, you can get a JSON representation of the same information by fetching
with `Accept: application/json`:

    $ http localhost:9599 'Accept:application/json'
    {
        "dbs": {
            "flights": {
              ...

A simplified healthcheck interface is available at the `/healthz` and
`/healthcheck` endpoints which will return a JSON representation of the state
of each node. The status code will either be `200` if they all have the status
`AVAILABLE` or `404` if at least one node does not:

    $ http localhost:9599/healthz
    HTTP/1.1 200 OK
    Content-Length: 46
    Content-Type: application/json
    Date: Wed, 26 Jul 2017 21:49:52 GMT
 
    {
        "baby-names": {
            "1": {
                "localhost": "AVAILABLE"
            }
        }
    }

### Expvars

You can bind the sequins ["debug" HTTP
server](../x-1-configuration-reference/README.md#debug) to a different port, and
it'll publish go "expvars" at `/debug/vars`.

In addition to the [built in expvars][goexpvar], sequins publishes the following
sequins-specific ones:

 - `sequins.Qps.ByStatus`: A map of HTTP status to a count of requests in the.
   past second

 - `sequins.Qps.Total`: A total of the above.

 - `sequins.Latency`: A histogram of latency values for the last second, with
   `Max`, `Mean`, and `PXX` keys.

 - `sequins.DiskUsed`: The amount of local storage used by sequins.

[goexpvar]: https://golang.org/pkg/expvar/

### Datadog

At Stripe, we use [Datadog][datadog] for statsd-like monitoring with lots of
bells and whistles. We've open-sourced our Datadog plugin for sequins on
[github][ddcheck].

Sequins can also report metrics concerning file downloads from S3 using the
[DogStatsD][dogstatsd] protocol if the `datadog.url` (by default
`localhost:8200`) is set.

[datadog]: https://www.datadoghq.com/
[ddcheck]: https://github.com/stripe/datadog-checks/blob/master/checks.d/sequins.py
[dogstatsd]: https://docs.datadoghq.com/guides/dogstatsd/
