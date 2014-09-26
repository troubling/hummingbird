all: bin bin/hummingbird

bin:
	mkdir -p bin

bin/hummingbird: main.go */*.go
	go build -o bin/hummingbird

get:
	go get hummingbird

fmt:
	go fmt hummingbird
	go fmt hummingbird/common
	go fmt hummingbird/objectserver
	go fmt hummingbird/proxyserver
	go fmt hummingbird/containerserver
	go fmt hummingbird/bench

install: all
	cp bin/* $(DESTDIR)/usr/bin

develop: all
	ln -f -s `pwd`/bin/* -t /usr/local/bin/
