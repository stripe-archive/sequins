<div align="center"><img src="doc/sequins.png" width="400px"/></div>

Sequins is a static key/value database for serving batch data. It's built to be

 - Horizontally scalable: replicate a hot dataset over a few nodes, or partition a large one over many.
 - Reliable: serve your data offline, far away from Hadoop. Sequins is built to stay up during multi-node failures.
 - Interoperable: read data from HDFS or S3 in Hadoop's SequenceFile format.
 - Accessible: access values with HTTP GET; no client library required.

See [the manual](https://stripe.github.io/sequins/manual) to get started.

[![build](https://travis-ci.org/stripe/sequins.svg?branch=master)](https://travis-ci.org/stripe/sequins)
