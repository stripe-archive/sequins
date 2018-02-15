TRAVIS_TAG ?= $(shell git rev-parse HEAD)
ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(TRAVIS_TAG)-$(ARCH)

SOURCES = $(shell find . -name '*.go' -not -name '*_test.go')
TEST_SOURCES = $(shell find . -name '*_test.go')
BUILD = $(shell pwd)/build

VENDORED_LIBS = $(BUILD)/lib/libsparkey.a $(BUILD)/lib/libsnappy.a $(BUILD)/lib/libzookeeper_mt.a

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	CGO_PREAMBLE_LDFLAGS = -lstdc++
else
	CGO_PREAMBLE_LDFLAGS = -lrt -lm -lstdc++
endif

ifneq ($(VERBOSE),)
  VERBOSITY=-v
endif

CGO_PREAMBLE = CGO_CFLAGS="-I$(BUILD)/include -I$(BUILD)/include/zookeeper" CGO_LDFLAGS="$(VENDORED_LIBS) $(CGO_PREAMBLE_LDFLAGS)"


all: sequins

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

$(BUILD)/bin/go-bindata:
	go build -o $(BUILD)/bin/go-bindata ./vendor/github.com/jteeuwen/go-bindata/go-bindata/

status.tmpl.go: status.tmpl $(BUILD)/bin/go-bindata
	$(BUILD)/bin/go-bindata -o status.tmpl.go status.tmpl

sequins: $(SOURCES) status.tmpl.go $(BUILD)/lib/libsparkey.a $(BUILD)/lib/libsnappy.a $(BUILD)/lib/libzookeeper_mt.a
	$(CGO_PREAMBLE) go build -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"

release: sequins
	./sequins --version
	mkdir -p $(RELEASE_NAME)
	cp sequins sequins.conf.example README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

test: $(TEST_SOURCES)
	$(CGO_PREAMBLE) go test $(VERBOSITY) -short -race -timeout 2m $(shell go list ./... | grep -v vendor)
	# This test exercises some sync.Pool code, so it should be run without -race
	# as well (sync.Pool doesn't ever share objects under -race).
	$(CGO_PREAMBLE) go test $(VERBOSITY) -timeout 30s ./blocks -run TestBlockParallelReads

vet:
	$(CGO_PREAMBLE) go vet $(shell go list ./... | grep -v vendor)

test_functional: sequins $(TEST_SOURCES)
	$(CGO_PREAMBLE) go test $(VERBOSITY) -timeout 10m -run "^TestCluster"

clean:
	rm -rf $(BUILD)
	rm -f vendor/snappy/configure
	cd vendor/snappy && make distclean; true
	rm -f vendor/sparkey/configure
	cd vendor/sparkey && make distclean; true
	rm -f vendor/zookeeper/configure
	cd vendor/zookeeper && make distclean; true
	rm -f sequins sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: release test test_functional clean
