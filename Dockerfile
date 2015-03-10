# DOCKER-VERSION 0.6.4

# This is probably only useful if you're setting up a CI environment.

# Adding this line to /etc/default/docker will convince Docker to use XFS:
# DOCKER_OPTS="--storage-driver=devicemapper --storage-opt dm.fs=xfs"

FROM   ubuntu:14.04

RUN     apt-get update
RUN     apt-get -y upgrade
RUN     apt-get install -y curl build-essential memcached rsync sqlite3 git-core libffi-dev python-setuptools python-coverage python-dev python-nose python-simplejson python-xattr python-eventlet python-greenlet python-pastedeploy python-netifaces python-pip python-dnspython python-mock wget pkg-config libsqlite3-dev sudo

RUN     /usr/sbin/useradd -m -d /home/swift -U swift

RUN     cd /home/swift; git clone https://github.com/openstack/swift.git
RUN     cd /home/swift/swift; python setup.py develop
RUN     cd /home/swift/swift; pip install -r test-requirements.txt

RUN     cd /home/swift; git clone https://github.com/openstack/python-swiftclient.git
RUN     cd /home/swift/python-swiftclient; python setup.py develop

RUN     cd /home/swift; git clone https://github.com/gholt/swiftly.git
RUN     cd /home/swift/swiftly; python setup.py develop

RUN     cp /home/swift/swift/doc/saio/bin/* /usr/local/bin

RUN     mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4 /var/run/swift /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4 /var/run/hummingbird /etc/hummingbird

RUN     cp -r /home/swift/swift/doc/saio/swift /etc/swift
RUN     find /etc/swift/ -name \*.conf | xargs sed -i "s/<your-user-name>/swift/"
RUN     cp /home/swift/swift/test/sample.conf /etc/swift/test.conf
RUN     printf "[swift-hash]\nswift_hash_path_prefix = changeme\nswift_hash_path_suffix = changeme\n" > /etc/swift/swift.conf
RUN     remakerings
RUN     sed -ri 's/60([0-9]{2})/70\1/' /usr/local/bin/remakerings
RUN     sed -ri 's@/etc/swift@/etc/hummingbird@' /usr/local/bin/remakerings
RUN     remakerings

RUN     chown -R swift:swift /etc/swift /etc/hummingbird /srv/* /var/cache/swift* /var/run/swift /var/run/hummingbird

RUN     cd /usr/local; curl 'https://storage.googleapis.com/golang/go1.4.2.linux-amd64.tar.gz' | tar -xz
RUN     echo 'PATH=$PATH:/usr/local/go/bin' >> /etc/bash.bashrc

ADD     id_saddle_bird /root/id_saddle_bird
