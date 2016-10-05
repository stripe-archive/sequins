# Querying Sequins

Sequins has a single interface for fetching values, HTTP GET:

    $ http localhost:9599/mydata/<key>
    HTTP/1.1 200 OK
    Accept-Ranges: bytes
    Content-Length: 7
    Date: Mon, 01 Aug 2016 11:57:53 GMT
    Last-Modified: Mon, 01 Aug 2016 11:56:27 GMT
    X-Sequins-Version: version0

    <value>

For this reason, Sequins has no client library; you can use whatever HTTP client
is available in your language.

### Response and Request Headers

Sequins supports a couple advanced HTTP features and customizations:

 - `Last-Modified` is set to the time that the database was last updated or
   created.

 - `Content-Length` is set on responses, and you should ensure that your HTTP
   client verifies that the response body is the correct length.

 - `X-Sequins-Version` is set on responses, and holds the current version of the
   database.

 - If the request was proxied to a peer in a distributed cluster,
   'X-Sequins-Proxied-to' will hold the hostname of the peer.

### Response Codes

Sequins will sometimes return non-200 response codes:

 - `400 Bad Request`: This is returned for requests with an HTTP method other
   than GET, and for requests with only a single path component (and therefore
   no key), like `GET /foo`.

 - `404 Not Found`: This indicates that either the key or database does not
   exist. If you need to differentiate, check for the presence of an
   `X-Sequins-Version` header; if one is set, then you have reached a valid
   database, but the key is not present in it.

 - `502 Bad Gateway`: This indicates that the node attempted to proxy the
   request to a peer in a distributed cluster, but that no peers were available
   for the given partition. This could be the case if the cluster is partially
   down.

 - `504 Gateway Timeout`: Like a `502`, this indicates that the node attempted
   to proxy the request to a peer or peers in a distributed cluster, but that
   all peers timed out.

If sequins responds with an HTTP status code not listed here, please [file an
issue](https://github.com/stripe/sequins/issues/new).
