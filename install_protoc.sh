#!/bin/sh

set -eux

INSTALL_ROOT=$1

# Install protoc
mkdir -p $INSTALL_ROOT
cd $INSTALL_ROOT
wget https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-linux-x86_64.zip
echo '939fece2ccef965864e193abba0650c597f7b0e7bedc618c5f2bf13871f7cbb3a655df25ea72cb8a10d23f698c3bc2b1541b2a29fe8f3b0b98cd47b056e62d0f  protoc-3.3.0-linux-x86_64.zip' > checksum
sha512sum -c checksum
unzip protoc-3.3.0-linux-x86_64.zip

# Install proto-gen-go
mkdir -p $GOPATH/src/github.com/golang
cd $GOPATH/src/github.com/golang
git clone https://github.com/golang/protobuf.git
cd protobuf
git checkout 748d386b5c1ea99658fd69fe9f03991ce86a90c1
GOBIN=$INSTALL_ROOT/bin make
