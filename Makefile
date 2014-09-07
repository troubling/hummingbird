all: bin bin/hummingbird-container-server bin/hummingbird-proxy-server bin/hummingbird-obj-server

bin:
	mkdir -p bin

bin/hummingbird-container-server: container-server/*.go
	go build -o bin/hummingbird-container-server hummingbird/container-server

bin/hummingbird-proxy-server: proxy-server/*.go
	go build -o bin/hummingbird-proxy-server hummingbird/proxy-server

bin/hummingbird-obj-server: obj-server/*.go
	go build -o bin/hummingbird-obj-server hummingbird/obj-server
