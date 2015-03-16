all: bin/hummingbird

bin:
	mkdir -p bin

bin/hummingbird: bin main.go */*.go
	go build -o bin/hummingbird -ldflags "-X main.Version '`git describe --tags`'"

get:
	go get -t ./...

fmt:
	go fmt ./...

install: all
	cp bin/* $(DESTDIR)/usr/bin

develop: all
	ln -f -s `pwd`/bin/* -t /usr/local/bin/

test:
	@test -z "$(shell find . -name '*.go' | xargs gofmt -l)" || (echo "Need to run 'go fmt ./...'"; exit 1)
	go vet ./...
	go test -cover ./...

