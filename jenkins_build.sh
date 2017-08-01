#!/bin/sh

set -eux

# Install protoc
mkdir -p /build/proto
cd /build/proto
wget https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-linux-x86_64.zip
echo '939fece2ccef965864e193abba0650c597f7b0e7bedc618c5f2bf13871f7cbb3a655df25ea72cb8a10d23f698c3bc2b1541b2a29fe8f3b0b98cd47b056e62d0f  protoc-3.3.0-linux-x86_64.zip' > checksum
sha512sum -c checksum
unzip protoc-3.3.0-linux-x86_64.zip

# Install proto-gen-go
mkdir -p /go/src/github.com/golang
cd /go/src/github.com/golang
git clone https://github.com/golang/protobuf.git
cd protobuf
git checkout 748d386b5c1ea99658fd69fe9f03991ce86a90c1
GOBIN=/build/proto/bin make

ls /build/proto/bin

cd /go/src/github.com/stripe/sequins
PATH=/build/proto/bin:$PATH make sequins vet

./sequins --help 2>&1 | grep usage && echo 'binary looks good'

mkdir -p /build/bin/
cp -a sequins /build/bin/
echo "DONE"
