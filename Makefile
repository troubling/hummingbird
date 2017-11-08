HUMMINGBIRD_VERSION?=$(shell git describe --tags)
HUMMINGBIRD_VERSION_NO_V?=$(shell git describe --tags | cut -d v -f 2)

all: bin/hummingbird

bin/hummingbird: */*.go */*/*.go
	mkdir -p bin
	go build -o bin/hummingbird -ldflags "-X github.com/troubling/hummingbird/common.Version=$(HUMMINGBIRD_VERSION)" github.com/troubling/hummingbird/cmd/hummingbird

get:
	go get -t $(shell go list ./... | grep -v /vendor/)

fmt:
	gofmt -l -w -s $(shell find . -mindepth 1 -maxdepth 1 -type d -print | grep -v vendor)

test:
	@test -z "$(shell find . -name '*.go' | grep -v ./vendor/ | xargs gofmt -l -s)" || (echo "You need to run 'make fmt'"; exit 1)
	go vet $(shell go list ./... | grep -v /vendor/)
	go test -cover $(shell go list ./... | grep -v /vendor/)

functional-test:
	$(MAKE) -C functional

haio: all
	if hash hball 2>/dev/null ; then hball stop ; fi
	sudo rm -f /usr/bin/hummingbird
	sudo cp bin/hummingbird /usr/bin/hummingbird
	sudo chmod 0755 /usr/bin/hummingbird
