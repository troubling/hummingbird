# Hummingbird All In One (HAIO)

These instructions assume Ubuntu 16.04, but other distributions and versions should work with some translation.

You may want to setup sudo to allow you to switch without entering a password; less secure, but maybe fine for a privately contained virtual machine:

```sh
echo "$USER ALL=(ALL) NOPASSWD: ALL" | sudo tee -a /etc/sudoers
```

Install OS dependencies:

```sh
sudo apt install gcc git-core make memcached sqlite3 xfsprogs
```

Install Go for your platform by following https://golang.org/doc/install  
This will be something like:

```sh
wget https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.9.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee -a /etc/profile
source /etc/profile
```

Get Hummingbird and install as an HAIO:

```sh
go get -t github.com/troubling/hummingbird/...
cd ~/go/src/github.com/troubling/hummingbird
make haio-init
```

Start everything up and do a quick test:

```sh
hbrings
hbreset
hbmain start
nectar -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing head
```

If you're going to be actively developing Hummingbird, you should fork the repository on GitHub and:

```sh
cd ~/go/src/github.com/troubling/hummingbird
git remote rename origin upstream
git remote add origin git@github.com:YOU/hummingbird
```

A normal development loop would be to write code and then:

```sh
make haio
hbreset        # optionally, throws away all previously stored data
hbmain start   # or hball start
# test changes
```

There a few other make targets as well:

```sh
make                 # just does quick compile, mostly just a syntax check
make fmt             # runs go fmt with the options we like
make test            # runs go vet and the unit tests with coverage
make functional-test # runs the functional tests; the haio cluster must be running already
```

Logs will be going through the standard systemd log system, so if you're used to journalctl you can just use that.  
But, hblog is also provided in case that's simpler:

```sh
hblog proxy    # shows all the log lines from the proxy server
hblog proxy -f # shows recent log lines and follows, like a tail -f
hblog object1  # shows logs for just the first object server
hblog object\* # shows logs for all the object servers (can also have -f)
```

If you want to run Openstack Swift's functional tests against Hummingbird:

```sh
cd ~
git clone git@github.com:openstack/swift
sudo mkdir /etc/swift/
sudo chown $USER: /etc/swift
cp ~/swift/test/sample.conf /etc/swift/test.conf
sudo apt install python-eventlet python-mock python-netifaces python-nose python-pastedeploy python-pbr python-pyeclib python-setuptools python-swiftclient python-unittest2 python-xattr
cd ~/swift/test/functional
nosetests
```
