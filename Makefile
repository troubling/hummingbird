HUMMINGBIRD_VERSION?=$(shell git describe --tags)

all: bin/hummingbird

bin/hummingbird: */*.go */*/*.go
	mkdir -p bin
	go build -o bin/hummingbird -ldflags "-X common.Version=$(HUMMINGBIRD_VERSION)" cmd/hummingbird/main.go

get:
	go get -t $(shell go list ./... | grep -v /vendor/)

fmt:
	gofmt -l -w -s $(shell find . -mindepth 1 -maxdepth 1 -type d -print | grep -v vendor)

test:
	@test -z "$(shell find . -name '*.go' | grep -v ./vendor/ | xargs gofmt -l -s)" || (echo "You need to run 'make fmt'"; exit 1)
	go vet $(shell go list ./... | grep -v /vendor/)
	go test -cover $(shell go list ./... | grep -v /vendor/)

install: bin/hummingbird
	cp bin/hummingbird $(DESTDIR)/usr/bin/hummingbird

develop: bin/hummingbird
	ln -f -s bin/hummingbird /usr/local/bin/hummingbird

