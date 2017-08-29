HUMMINGBIRD_VERSION?=$(shell git describe --tags)
HUMMINGBIRD_VERSION_NO_V?=$(shell git describe --tags | cut -d v -f 2)

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
	chmod a+rx $(DESTDIR)/usr/bin/hummingbird

installsystemd: bin/hummingbird
	sudo cp bin/hummingbird $(DESTDIR)/usr/bin/hummingbird
	sudo chmod a+rx $(DESTDIR)/usr/bin/hummingbird
	sudo cp systemd/lib/systemd/system/*.service $(DESTDIR)/lib/systemd/system/
	# Example commands:
	# sudo systemctl enable hummingbird-proxy
	# sudo systemctl start hummingbird-proxy
	# journalctl -u hummingbird-proxy -f

develop: bin/hummingbird
	ln -f -s bin/hummingbird /usr/local/bin/hummingbird

functionaltest:
	cd functional && make

package: all
	# Started this from https://medium.com/@newhouseb/hassle-free-go-in-production-528af8ee1a58
	sudo rm -rf build/usr
	mkdir -p build/usr/local/bin
	cp bin/hummingbird build/usr/local/bin/hummingbird
	chmod -R 0755 build
	sudo chown -R root: build/usr
	echo 2.0 > build/debian-binary
	echo "Package: hummingbird" > build/control
	echo "Version:" ${HUMMINGBIRD_VERSION_NO_V} >> build/control
	echo "Architecture: amd64" >> build/control
	echo "Section: net" >> build/control
	echo "Maintainer: Rackspace <gholt@rackspace.com>" >> build/control
	echo "Priority: optional" >> build/control
	echo "Description: Hummingbird Object Storage Software" >> build/control
	tar cvzf build/data.tar.gz -C build usr
	tar cvzf build/control.tar.gz -C build control
	cd build && ar rc hummingbird.deb debian-binary control.tar.gz data.tar.gz && cd ..

clean:
	rm -f bin/hummingbird
	sudo rm -rf build
