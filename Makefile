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

get:
	# Travis uses this
	go get -t $(shell go list ./... | grep -v /vendor/)

build:
	sudo rm -rf build
	mkdir -p build/usr/bin
	go build -o build/usr/bin/hummingbird -ldflags "-X common.Version=$(HUMMINGBIRD_VERSION)" cmd/hummingbird/main.go
	go build -o build/make-hummingbird cmd/make-hummingbird/main.go
	build/make-hummingbird ${MAKE_HUMMINGBIRD_ARGS}
	mkdir -p build/var/run/hummingbird
	mkdir -p build/var/log/hummingbird
	mkdir -p build/var/cache/swift
	mkdir -p build/srv/hummingbird

build-ring:
	hummingbird ring build/etc/hummingbird/object.builder create 10 1 1
	hummingbird ring build/etc/hummingbird/object.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/hummingbird 1
	hummingbird ring build/etc/hummingbird/object.builder rebalance
	hummingbird ring build/etc/hummingbird/container.builder create 10 1 1
	hummingbird ring build/etc/hummingbird/container.builder add r1z1-127.0.0.1:6011/hummingbird 1
	hummingbird ring build/etc/hummingbird/container.builder rebalance
	hummingbird ring build/etc/hummingbird/account.builder create 10 1 1
	hummingbird ring build/etc/hummingbird/account.builder add r1z1-127.0.0.1:6012/hummingbird 1
	hummingbird ring build/etc/hummingbird/account.builder rebalance
	rm -rf build/etc/hummingbird/backups

build-perms:
	find build -type d -exec chmod 0755 {} \;
	find build -type f -exec chmod 0644 {} \;
	find build/usr/bin -type f -exec chmod 0755 {} \;
	sudo chown -R root: build/etc build/lib build/usr build/var

package: clean build build-ring build-perms
	# Started this from https://medium.com/@newhouseb/hassle-free-go-in-production-528af8ee1a58
	echo 2.0 > build/debian-binary
	echo "Package: hummingbird" > build/control
	echo "Version:" ${HUMMINGBIRD_VERSION_NO_V} >> build/control
	echo "Section: net" >> build/control
	echo "Priority: optional" >> build/control
	echo "Architecture: amd64" >> build/control
	echo "Depends: adduser, memcached" >> build/control
	echo "Maintainer: Rackspace <gholt@rackspace.com>" >> build/control
	echo "Description: Hummingbird Object Storage Software" >> build/control
	echo '#!/bin/sh' > build/postinst
	echo "adduser --system --group --no-create-home hummingbird" >> build/postinst
	echo "chown hummingbird: /srv/hummingbird" >> build/postinst
	tar cvzf build/data.tar.gz -C build etc lib srv usr var
	tar cvzf build/control.tar.gz -C build control postinst
	cd build && (ar rc hummingbird.deb debian-binary control.tar.gz data.tar.gz ; cd ..)

haio-init: clean
	$(MAKE) MAKE_HUMMINGBIRD_ARGS=haio build
	go build -o build/usr/bin/nectar cmd/nectar/main.go
	$(MAKE) MAKE_HUMMINGBIRD_ARGS=haio build-perms
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
