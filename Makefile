TRAVIS_TAG ?= $(shell git rev-parse HEAD)
ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(TRAVIS_TAG)-$(ARCH)

SOURCES = $(shell find . -name '*.go')
BUILD = $(shell pwd)/build
CGO_PREAMBLE = CGO_CFLAGS="-I$(BUILD)/include -I$(BUILD)/include/zookeeper" CGO_LDFLAGS="$(BUILD)/lib/libsparkey.a $(BUILD)/lib/libsnappy.a $(BUILD)/lib/libzookeeper_mt.a -lrt -lm -lstdc++"

all: sequins sequins-dump

vendor/snappy/configure:
	cd vendor/snappy && ./autogen.sh

vendor/snappy/Makefile: vendor/snappy/configure
	cd vendor/snappy && ./configure --prefix=$(BUILD)

$(BUILD)/lib/libsnappy.a: vendor/snappy/Makefile
	cd vendor/snappy && make install

vendor/sparkey/configure:
	cd vendor/sparkey && autoreconf --install

vendor/sparkey/Makefile: vendor/sparkey/configure $(BUILD)/lib/libsnappy.a
	cd vendor/sparkey && ./configure --prefix=$(BUILD) LDFLAGS="-L$(BUILD)/lib" CPPFLAGS="-I$(BUILD)/include"

$(BUILD)/lib/libsparkey.a: vendor/sparkey/Makefile
	cd vendor/sparkey && make install

vendor/zookeeper/configure:
	cd vendor/zookeeper && autoreconf --install -v -I $(CURDIR)/vendor/zookeeper/

vendor/zookeeper/Makefile: vendor/zookeeper/configure
	cd vendor/zookeeper && ./configure --prefix=$(BUILD)

$(BUILD)/lib/libzookeeper_mt.a: vendor/zookeeper/Makefile
	cd vendor/zookeeper && make install

sequins: $(SOURCES) $(BUILD)/lib/libsparkey.a $(BUILD)/lib/libsnappy.a $(BUILD)/lib/libzookeeper_mt.a
	$(CGO_PREAMBLE) go build -x -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"

sequins-dump: $(SOURCES)
	go build -x -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"  ./cmd/sequins-dump

release: sequins sequins-dump
	./sequins --version
	./sequins-dump --version
	mkdir -p $(RELEASE_NAME)
	cp sequins sequins-dump README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

test: sequins
	$(CGO_PREAMBLE) go test -short -race -timeout 30s $(shell go list ./... | grep -v vendor )
	$(CGO_PREAMBLE) go test -timeout 10m -run "^TestCluster"

clean:
	rm -rf $(BUILD)
	rm -f vendor/snappy/configure
	cd vendor/snappy && make distclean; true
	rm -f vendor/sparkey/configure
	cd vendor/sparkey && make distclean; true
	rm -f vendor/zookeeper/configure
	cd vendor/zookeeper && make distclean; true
	rm -f sequins sequins-dump sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: release test clean
