#!/bin/sh

set -eux

cd /go/src/github.com/stripe/sequins
make sequins vet

./sequins --help 2>&1 | grep usage && echo 'binary looks good'

mkdir -p /build/bin/
cp -a sequins /build/bin/
echo "DONE"
