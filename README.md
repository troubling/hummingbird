Hummingbird
=========

Hummingbird is a golang implementation of some parts of [Openstack Swift](http://swift.openstack.org/).  The idea is to keep the same protocols and on-disk layout, but improve performance dramatically (there may be some benchmarks in the [wiki](https://github.com/rackerlabs/hummingbird/wiki)).

[![Build Status](http://104.239.166.47/api/badge/github.com/rackerlabs/hummingbird/status.svg?branch=master)](http://104.239.166.47/github.com/rackerlabs/hummingbird)


Completeness
------------

The object server is considered feature complete and testing is ongoing.  The proxy server is currently only complete enough to run simple GET, PUT, and DELETE benchmarks.


Installation
--------------

First, you should have a working [SAIO](http://docs.openstack.org/developer/swift/development_saio.html). (With no storage policies)

You will also need to configure your syslog to listen for UDP packets:

For a SAIO, change these lines in /etc/rsyslog.conf:
```
# provides UDP syslog reception
$ModLoad imudp
$UDPServerRun 514
```

and then sudo service rsyslog restart.

Next, you must have a working [Go development environment](https://golang.org/doc/install).

Then, clone this repo.  If you don't have $GOPATH set, replace it with wherever your Go code lives:

```sh
git clone 'https://github.com/rackerlabs/hummingbird.git' $GOPATH/src/hummingbird
cd $GOPATH/src/hummingbird
make get test all
sudo make develop # this symlinks local binaries into /usr/local/bin
```


Running
-------

The "hummingbird" executable handles starting services, reading and writing pid files, etc., similar to swift-init.

```sh
hummingbird [start/stop/restart] [object/proxy/all]
```

If you'd like to start it from a user other than root, you'll probably need to create /var/run/hummingbird with the correct permissions.
