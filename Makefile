HUMMINGBIRD_VERSION?=$(shell git describe --tags)

all: bin/hummingbird

bin/hummingbird: */*.go */*/*.go
	mkdir -p bin
	go build -o bin/hummingbird -ldflags "-X common.Version=$(HUMMINGBIRD_VERSION)" cmd/hummingbird/main.go

get:
	go get -t ./...

fmt:
	gofmt -l -w -s  .

test:
	@test -z "$(shell find . -name '*.go' | xargs gofmt -l -s)" || (echo "You need to run 'make fmt'"; exit 1)
	go vet ./...
	go test -cover ./...

install: bin/hummingbird
	cp bin/hummingbird $(DESTDIR)/usr/bin/hummingbird

develop: bin/hummingbird
	ln -f -s bin/hummingbird /usr/local/bin/hummingbird

