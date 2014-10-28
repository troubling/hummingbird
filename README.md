Hummingbird
=========

Hummingbird is a golang implementation of some parts of Swift.  The idea is to keep the same protocols and on-disk layout, but improve performance dramatically (there may be some benchmarks in the [wiki](https://github.com/rackerlabs/hummingbird/wiki)).


Completeness
------------

The object and container servers are able to pass Swift's functional test suite, though some functionality like replication is absent or incomplete.  The proxy server is currently only complete enough to run simple GET, PUT, and DELETE benchmarks.


Installation
--------------

First, you should have a working [SAIO](http://docs.openstack.org/developer/swift/development_saio.html). (With no storage policies)

Next, you must have a working [Go development environment](https://golang.org/doc/install).

Then, clone this repo.  If you don't have $GOPATH set, replace it with wherever your Go code lives:

```sh
sudo apt-get install pkg-config libsqlite3-dev
git clone 'https://github.com/rackerlabs/hummingbird.git' $GOPATH/src/hummingbird
cd $GOPATH/src/hummingbird
go get
make
sudo make develop # this symlinks local binaries into /usr/local/bin
```


Running
-------

The "hummingbird" executable handles starting services, reading and writing pid files, etc., similar to swift-init.

```sh
hummingbird [start/stop/restart] [object/container/proxy/all]
```


TODO
----

* Object functionality - probably a priority
  * complete???
* Container functionality
  * Replication
  * Although it passes functional tests, I'm pretty sure there are still some bugs around prefix/path/delimiter stuff
* Proxy functionality - A complete proxy is unlikely, but we'd like to be able to serve some traffic (e.g. CDN origin requests, cloud servers traffic, CBS backups).  This will involve:
  * Rackauth support
  * Better error handling
  * Staticweb?
  * Static and dynamic large object support?
* Actually, we should sit down and make a complete list of necessary features
* Lots of Testing

