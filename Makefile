TRAVIS_TAG ?= $(shell git rev-parse HEAD)
ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = sequins-$(TRAVIS_TAG)-$(ARCH)

all: sequins sequins-dump

sequins:
	go build -x -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"

sequins-dump:
	go build -x -ldflags "-X main.sequinsVersion=$(TRAVIS_TAG)"  ./cmd/sequins-dump

install:
	go install

release: sequins sequins-dump
	./sequins --version
	./sequins-dump --version
	mkdir -p $(RELEASE_NAME)
	cp sequins sequins-dump README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

test: sequins
	go test -short -race -timeout 30s $(shell go list ./... | grep -v vendor )
	go test -timeout 10m -run "^TestCluster"


clean:
	rm -f sequins sequins-dump sequins-*.tar.gz
	rm -rf $(RELEASE_NAME)

.PHONY: install release test
