#!/bin/sh

set -eux

sh install_protoc.sh /build/proto

cd /go/src/github.com/stripe/sequins
PATH=/build/proto/bin:$PATH make sequins vet

./sequins --help 2>&1 | grep usage && echo 'binary looks good'

mkdir -p /build/bin/
cp -a sequins /build/bin/
echo "DONE"
