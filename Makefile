HUMMINGBIRD_VERSION?=$(shell git describe --tags)
HUMMINGBIRD_VERSION_NO_V?=$(shell git describe --tags | cut -d v -f 2)

quick:
	go build cmd/...

clean:
	sudo rm -rf build

fmt:
	gofmt -l -w -s $(shell find . -mindepth 1 -maxdepth 1 -type d -print | grep -v vendor)

test:
	@test -z "$(shell find . -name '*.go' | grep -v ./vendor/ | xargs gofmt -l -s)" || (echo "You need to run 'make fmt'"; exit 1)
	go vet $(shell go list ./... | grep -v /vendor/)
	go test -cover $(shell go list ./... | grep -v /vendor/)

functional-test:
	$(MAKE) -C functional

build:
	sudo rm -rf build
	mkdir -p build/usr/bin
	go build -o build/usr/bin/hummingbird -ldflags "-X common.Version=$(HUMMINGBIRD_VERSION)" cmd/hummingbird/main.go
	go build -o build/make-hummingbird cmd/make-hummingbird/main.go
	build/make-hummingbird ${MAKE_HUMMINGBIRD_ARGS}
	mkdir -p build/var/run/hummingbird
	mkdir -p build/var/log/hummingbird
	mkdir -p build/var/cache/swift

buildperms:
	find build -type d -exec chmod 0755 {} \;
	find build -type f -exec chmod 0644 {} \;
	find build/usr/bin -type f -exec chmod 0755 {} \;
	sudo chown -R root: build/etc build/lib build/usr build/var

package: clean build buildperms
	# Started this from https://medium.com/@newhouseb/hassle-free-go-in-production-528af8ee1a58
	echo 2.0 > build/debian-binary
	echo "Package: hummingbird" > build/control
	echo "Version:" ${HUMMINGBIRD_VERSION_NO_V} >> build/control
	echo "Architecture: amd64" >> build/control
	echo "Section: net" >> build/control
	echo "Maintainer: Rackspace <gholt@rackspace.com>" >> build/control
	echo "Priority: optional" >> build/control
	echo "Description: Hummingbird Object Storage Software" >> build/control
	tar cvzf build/data.tar.gz -C build etc lib usr var
	tar cvzf build/control.tar.gz -C build control
	cd build && (ar rc hummingbird.deb debian-binary control.tar.gz data.tar.gz ; cd ..)

haio-init: clean
	$(MAKE) MAKE_HUMMINGBIRD_ARGS=haio build
	go build -o build/usr/bin/nectar cmd/nectar/main.go
	$(MAKE) MAKE_HUMMINGBIRD_ARGS=haio buildperms
	cd build && (sudo find etc lib usr var -type d -exec test \! -e /{} \; -exec mkdir -p /{} \; -print && sudo find etc lib usr var -type f -exec rm -f /{} \; -exec cp -a {} /{} \; -print ; cd ..)
	sudo chown -R $${USER}: /etc/hummingbird

haio:
	hball stop
	sudo rm -rf build
	mkdir -p build/usr/bin
	go build -o build/usr/bin/hummingbird -ldflags "-X common.Version=$(HUMMINGBIRD_VERSION)" cmd/hummingbird/main.go
	chmod 0755 build/usr/bin/hummingbird
	sudo rm -f /usr/bin/hummingbird
	sudo cp build/usr/bin/hummingbird /usr/bin/hummingbird
	go build -o build/usr/bin/nectar cmd/nectar/main.go
	chmod 0755 build/usr/bin/nectar
	sudo rm -f /usr/bin/nectar
	sudo cp build/usr/bin/nectar /usr/bin/nectar
