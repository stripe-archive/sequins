#!/bin/sh

set -eux

# Get absolute path
mkdir -p $1
INSTALL_ROOT=$(cd $1 && pwd)

# Install protoc
cd $INSTALL_ROOT
UNAME=$(uname)
if [[ $UNAME == Linux* ]]; then
  wget https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-linux-x86_64.zip
  echo '939fece2ccef965864e193abba0650c597f7b0e7bedc618c5f2bf13871f7cbb3a655df25ea72cb8a10d23f698c3bc2b1541b2a29fe8f3b0b98cd47b056e62d0f  protoc-3.3.0-linux-x86_64.zip' > checksum
elif [[ $UNAME == Darwin* ]]; then
  wget https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-osx-x86_64.zip
  echo '4b26dc9a38413de6843379cfe1d6d143dc7b0732d199f5a529120abffa6df912556d184f409d257bef7099828d04c88c405d3dcb1ae1be95358538567bfeb9e7  protoc-3.3.0-osx-x86_64.zip' > checksum
else
  echo "Unrecognized OS $UNAME"
  exit 1
fi

shasum -a 512 -c checksum
unzip $(awk '{print $2}' < checksum)

# Install proto-gen-go
mkdir -p $GOPATH/src/github.com/golang
cd $GOPATH/src/github.com/golang
git clone https://github.com/golang/protobuf.git
cd protobuf
git checkout 748d386b5c1ea99658fd69fe9f03991ce86a90c1
GOBIN=$INSTALL_ROOT/bin make
