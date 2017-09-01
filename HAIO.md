# Hummingbird All In One (HAIO)

To get started developing Hummingbird, you need to have Ubuntu 16.04, copy the [hummingbird executable](https://troubling.github.io/hummingbird/bin/hummingbird) to the system, and run `hummingbird init haio`. This will install, download, and configure everything needed to start writing and testing Hummingbird code.

You may want to setup sudo to allow you to switch without entering a password; less secure, but maybe fine for a privately contained virtual machine:

```sh
echo "$USER ALL=(ALL) NOPASSWD: ALL" | sudo tee -a /etc/sudoers
```

Once done, `hummingbird init haio` will have placed all the Hummingbird code at ~/go/src/github.com/troubling/hummingbird -- the git upstream remote will be set to the official fork and the origin will be set to your username, just assuming you have forked the code on GitHub and use the same username. You can patch this up with `git remote set-url origin git@github.com:YOU/hummingbird` if needed.

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
make functional-test # runs the functional tests; cluster must be running
```

Logs will be going through the standard systemd log system, so if you're used to journalctl you can just use that.  
But, hblog is also provided in case that's simpler:

```sh
hblog proxy    # shows all the log lines from the proxy server
hblog proxy -f # shows recent log lines and follows, like a tail -f
hblog object1  # shows logs for just the first object server
hblog object\* # shows logs for all the object servers (can also have -f)
```

You can run Openstack Swift's functional tests against Hummingbird if you want:

```sh
cd ~/swift/test/functional
nosetests
```

If you want to stop/start individual services, you would do it much like a "real" user would:

```
sudo systemctl stop hummingbird-proxy
```

Although, since an HAIO pretends to be a cluster of 4 machines, the account, container, and object services all have 4 each:

```
sudo systemctl restart hummingbird-object1
```
