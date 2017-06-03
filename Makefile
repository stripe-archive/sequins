TRAVIS_TAG ?= $(shell git rev-parse HEAD)
ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(TRAVIS_TAG)-$(ARCH)

SOURCES = $(shell find . -name '*.go' -not -name '*_test.go')
TEST_SOURCES = $(shell find . -name '*_test.go')
BUILD = $(shell pwd)/build


all: sequins

$(BUILD)/bin/go-bindata:
	go build -o $(BUILD)/bin/go-bindata ./vendor/github.com/jteeuwen/go-bindata/go-bindata/

status.tmpl.go: status.tmpl $(BUILD)/bin/go-bindata
	$(BUILD)/bin/go-bindata -o status.tmpl.go status.tmpl

sequins: $(SOURCES) status.tmpl.go
	go build -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"

release: sequins
	./sequins --version
	mkdir -p $(RELEASE_NAME)
	cp sequins sequins.conf.example README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

test: $(TEST_SOURCES)
	go test -short -race -timeout 2m $(shell go list ./... | grep -v vendor)
	# This test exercises some sync.Pool code, so it should be run without -race
	# as well (sync.Pool doesn't ever share objects under -race).
	go test -timeout 30s ./blocks -run TestBlockParallelReads

test_functional: sequins $(TEST_SOURCES)
	go test -timeout 10m -run "^TestCluster"

clean:
	rm -rf $(BUILD)
	rm -f vendor/zookeeper/configure
	cd vendor/zookeeper && make distclean; true
	rm -f sequins sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: release test test_functional clean
