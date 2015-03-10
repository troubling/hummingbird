all: bin bin/hummingbird

bin:
	mkdir -p bin

bin/hummingbird: main.go */*.go
	go build -o bin/hummingbird -ldflags "-X main.Version '`git describe --tags`'"

get:
	go get ./...

fmt:
	go fmt ./...

install: all
	cp bin/* $(DESTDIR)/usr/bin

develop: all
	ln -f -s `pwd`/bin/* -t /usr/local/bin/

test:
	go test hummingbird/tests

