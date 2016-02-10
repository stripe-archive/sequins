TRAVIS_TAG ?= $(shell git rev-parse HEAD)
ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(TRAVIS_TAG)-$(ARCH)

all: sequins sequins-dump

sequins: get-deps
	go build -x -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"

sequins-dump:
	go build -x -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"  ./cmd/sequins-dump

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
	go get github.com/colinmarc/hdfs
	go get github.com/colinmarc/cdb
	go get github.com/pborman/uuid
	go get github.com/spaolacci/murmur3
	go get github.com/crowdmob/goamz/aws
	go get github.com/samuel/go-zookeeper/zk
	go get stathat.com/c/consistent
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/require

clean:
	rm -f sequins sequins-dump sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: install release test get-deps
