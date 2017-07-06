

1. Install dependencies:

        sudo apt-get update
        sudo apt-get install memcached sqlite3 xfsprogs git-core

1. Make sure Memcache is running:

        sudo service memcached start

Prerequisites: Go 1.8
---------------------


1. [Download Go binaries for your platform](https://golang.org/doc/install).

1. Now, export three environment variables to inform both Go and your shell
   where to find what you'll need to run both Go and Hummingbird binaries.

   * `GOROOT` -- The `go` subdirectory of the location where your Go system
     binaries and libraries are, usually `/usr/local/go`. This is not the
     location of any code you write.

   * `GOPATH` -- The folder where you intend to keep Go code, binaries, etc.
     For simplicity, we'll do this in your home directory, so we'll use
     `~/go`.

   * `PATH` -- Append the bin dirs within both of the above onto your PATH,
     with your normal path first, then $GOROOT/bin, and your $GOPATH/bin:

          export GOROOT=/usr/local/go
          export GOPATH=~/go
          export PATH=$PATH:$GOROOT/bin:$GOPATH/bin

1. It's also recommended to add these three lines either to the end of the
   `/etc/profile` on the system, or at least to your own user's profile.

1. Ensure you can now use the new `go` binary:

        $ go version
        go version go1.8.1 linux/amd64

Installing Hummingbird
----------------------

1.  Cretate a directory for the Hummingbird code in GOPATH.  It's easiest to
symlink it into a folder structure within GOPATH:

        mkdir -p $GOPATH/src/github.com/troubling
        ln -s ~/hummingbird $GOPATH/src/github.com/troubling/hummingbird

1.  Check out the source code:

        cd $GOPATH/src/github.com/troubling
        git clone git@github.com:troubling/hummingbird.git

1. Compile, test, and install Hummingbird. A Makefile is
provided to make this simpler:

        cd $GOPATH/src/github.com/troubling/hummingbird
        make get test all

    Once complete, your output will look similar like this:

        go get -t ./...
        go vet ./...
        go test -cover ./...
        ?       github.com/openstack/swift/go/bench [no test files]
        ?       github.com/openstack/swift/go/client    [no test files]
        ?       github.com/openstack/swift/go/cmd   [no test files]
        ok      github.com/openstack/swift/go/hummingbird   4.344s  coverage: 62.1% of statements
        ok      github.com/openstack/swift/go/middleware    0.017s  coverage: 82.7% of statements
        ok      github.com/openstack/swift/go/objectserver  3.391s  coverage: 71.0% of statements
        ok      github.com/openstack/swift/go/probe 3.126s  coverage: 88.7% of statements
        ?       github.com/openstack/swift/go/proxyserver   [no test files]
        go build -o bin/hummingbird -ldflags "-X main.Version=2.6.0-344-g12276d7" cmd/hummingbird.go

1.  To install the hummingbird executable to /usr/bin, run this last command:

        $ sudo GOPATH=$GOPATH GOROOT=$GOROOT PATH=$PATH:$GOROOT/bin make develop

Storage
-------

The following instructions will setup a loopback device and format it as XFS.  You could also adapt this to use local storage as well.

1.  Create the file for the loopback device and setup xfs:

        sudo mkdir /srv
        sudo truncate -s 1GB /srv/swift-disk
        sudo mkfs.xfs /srv/swift-disk

1. Edit `/etc/fstab` and add `/srv/swift-disk /mnt/sdb1 xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0` to the end of it.

1. Create the mountpoint and the individualized links:

        sudo mkdir /mnt/sdb1
        sudo mount /mnt/sdb1
        sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
        sudo chown ${USER}:${USER} /mnt/sdb1/*
        for x in {1..4}; do sudo ln -s /mnt/sdb1/$x /srv/$x; done
        sudo mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5 \
            /srv/2/node/sdb2 /srv/2/node/sdb6 \
            /srv/3/node/sdb3 /srv/3/node/sdb7 \
            /srv/4/node/sdb4 /srv/4/node/sdb8 \
        for x in {1..4}; do sudo chown -R ${USER}:${USER} /srv/$x/; done

Configuration
-------------

1.  If you will be running Hummingbird as a user other than root, then you will need to create `/var/run/hummingbird` with proper permissions.  Hummingbird's PID files will be stored in this directory:

        sudo mkdir -p /var/run/hummingbird
        sudo chown -R ${USER}:${USER} /var/run/hummingbird

1.  Create the log, config, cache directories and copy over the provided aio config files:

        sudo mkdir -p /var/log/hummingbird
        sudo chown -R ${USER}:${USER} /var/log/hummingbird
        sudo mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
        sudo chown -R ${USER}:${USER} /var/cache/swift*
        sudo mkdir -p /etc/hummingbird
        sudo chown -R ${USER}:${USER} /etc/hummingbird
        cp -r ~/hummingbird/aio/etc/hummingbird /etc/hummingbird
        find /etc/hummingbird -name \*.conf | xargs sed -i "s/<user-name>/${USER}/"

1.  Create a local bin directory with the remake rings script and prepare the rings.  It would also be useful to make sure that `~/bin` is in your PATH:

        mkdir ~/bin
        cp -r ~/hummingbird/aio/bin/* ~/bin
        remakerings
    
Running
-------

Now that you've successfully installed Hummingbird, you can run the standard
functional tests to validate things are working.

The `hummingbird` command handles starting services, managing pid files, etc.:

    hummingbird <start|reload|restart|shutdown|stop> <all|object|container|account|proxy|object-replicator|object-auditor>

You may also run daemons interactively: 

    hummingbird object [-c /etc/swift/object-server.conf]

Running Openstack Swift's Functional Tests
------------------------------------------

All that is required to run Swift's functional tests is to download the swift code, install dependencies, and create `/etc/swift/test.conf` as described in the [SAIO Instructions](https://docs.openstack.org/swift/latest/development_saio.html).
