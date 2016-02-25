#!/bin/sh

set -eux

cd /go/src/github.com/stripe/sequins
make

./sequins --help 2>&1 | grep usage && echo 'binary looks good'

mkdir -p /build/bin/
cp -a sequins sequins-dump /build/bin/
echo "DONE"
