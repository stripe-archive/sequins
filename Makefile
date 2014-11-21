TRAVIS_TAG ?= HEAD
SEQUINS_VERSION ?= $(TRAVIS_TAG)-$(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(SEQUINS_VERSION)

all: sequins

sequins: get-deps
	go build

install: get-deps
	go install

release: sequins
	mkdir -p $(RELEASE_NAME)
	cp sequins README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

test: get-deps
	go test ./...

get-deps:
	go get gopkg.in/alecthomas/kingpin.v1
	go get github.com/syndtr/goleveldb/leveldb
	go get github.com/colinmarc/hdfs
	go get github.com/crowdmob/goamz/aws
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/require

clean:
	rm -f sequins sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: install release test get-deps
