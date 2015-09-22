TRAVIS_TAG ?= $(shell git rev-parse HEAD)
ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(TRAVIS_TAG)-$(ARCH)

all: sequins sequins-dump

sequins: get-deps
	go build -ldflags "-X main.version=$(TRAVIS_TAG)"

sequins-dump:
	go build -ldflags "-X main.version=$(TRAVIS_TAG)"  ./cmd/sequins-dump

install: get-deps
	go install

release: sequins sequins-dump
	./sequins --version
	./sequins-dump --version
	mkdir -p $(RELEASE_NAME)
	cp sequins sequins-dump README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

test: get-deps
	go test ./...

get-deps:
	go get gopkg.in/alecthomas/kingpin.v2
	go get github.com/syndtr/goleveldb/leveldb
	go get github.com/colinmarc/hdfs
	go get github.com/crowdmob/goamz/aws
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/require
	go get github.com/NYTimes/gziphandler

clean:
	rm -f sequins sequins-dump sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: install release test get-deps
