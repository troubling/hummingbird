all: bin bin/hummingbird-container-server bin/hummingbird-proxy-server bin/hummingbird-object-server bin/hummingbird bin/hummingbird-bench

bin:
	mkdir -p bin

bin/hummingbird-container-server: container-server/*.go common/*.go
	go build -o bin/hummingbird-container-server hummingbird/container-server

bin/hummingbird-proxy-server: proxy-server/*.go common/*.go
	go build -o bin/hummingbird-proxy-server hummingbird/proxy-server

bin/hummingbird-object-server: object-server/*.go common/*.go
	go build -o bin/hummingbird-object-server hummingbird/object-server

bin/hummingbird: init/*.go
	go build -o bin/hummingbird hummingbird/init

bin/hummingbird-bench: bench/*.go
	go build -o bin/hummingbird-bench hummingbird/bench

get:
	go get hummingbird/common
	go get hummingbird/object-server
	go get hummingbird/proxy-server
	go get hummingbird/container-server
	go get hummingbird/init
	go get hummingbird/bench

fmt:
	go fmt hummingbird/common
	go fmt hummingbird/object-server
	go fmt hummingbird/proxy-server
	go fmt hummingbird/container-server
	go fmt hummingbird/init
	go fmt hummingbird/bench

install: all
	cp bin/* $(DESTDIR)/usr/bin

develop: all
	ln -f -s `pwd`/bin/* -t /usr/local/bin/
