Debugging a single account, container, or object
================================================

At times you will want to investigate a single account, container, or object to ensure it exists, or was deleted, etc. There are tools that can be used to investigate within the cluster itself, useful when normal external client queries aren't giving the full picture.

As an example, we'll walk through the information available for a single object. First, we need to know the account, container, and object names for the object we want to investigate. The container and object names should be easily given to the user, in this case we'll use "thecontainer" and "theobject". The account name may be a bit more difficult to determine, as the name we want is the name the Hummingbird cluster uses, not the one the authentication system uses. You can retrieve this information from the nectar command line client, for example:

```
$ nectar auth
Account URL: http://127.0.0.1:8080/v1/AUTH_test
Token: AUTH_f1b5ef55-c668-29ff-aac8-2e0684f8144c
```

This tells us that the account name, according to the cluster, is `AUTH_test`.

We can now use this full object name to gather cluster information about the object:

```
$ hummingbird nodes AUTH_test/thecontainer/theobject

Account         AUTH_test
Container       thecontainer
Object          theobject
Partition       365
Hash            5b43443c7b6302922d25350ffa47d583

Server:Port Device      127.0.0.1:6030 sdb3
Server:Port Device      127.0.0.1:6020 sdb2
Server:Port Device      127.0.0.1:6040 sdb4
Server:Port Device      127.0.0.1:6010 sdb1      [Handoff]


curl -g -I -XHEAD "http://127.0.0.1:6030/sdb3/365/AUTH_test/thecontainer/theobject"
curl -g -I -XHEAD "http://127.0.0.1:6020/sdb2/365/AUTH_test/thecontainer/theobject"
curl -g -I -XHEAD "http://127.0.0.1:6040/sdb4/365/AUTH_test/thecontainer/theobject"
curl -g -I -XHEAD "http://127.0.0.1:6010/sdb1/365/AUTH_test/thecontainer/theobject" # [Handoff]


Use your own device location of servers:
such as "export DEVICE=/srv/node"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb3/objects/365/583/5b43443c7b6302922d25350ffa47d583"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb2/objects/365/583/5b43443c7b6302922d25350ffa47d583"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb4/objects/365/583/5b43443c7b6302922d25350ffa47d583"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb1/objects/365/583/5b43443c7b6302922d25350ffa47d583" # [Handoff]

note: `/srv/node*` is used as default value of `devices`, the real value is set in the config file on each storage node.
```

In this example, we have a cluster using a standard 3 replica storage policy. The Server:Port Device lines show where each copy of the object should reside. The ssh lines show a more specific on disk location of each object copy, assuming the standard storage policy. The curl lines can be useful if you want to remotely check if each server has the copy it should.

With non-standard policies, the information may not be exact but should give generally good information that can be translated to work for the policy in use. You can use the `-P policy_name` option with the hummingbird nodes command to use the non-default policy for a cluster.

You can also gather information about standard policy's object data file on disk, to determine what the account, container, and object names are, etc. For example, let's say I want to know what a "random" .data file represents on a backend server:

```
$ hummingbird oinfo /srv/hb/3/sdb3/objects/365/583/5b43443c7b6302922d25350ffa47d583/1515701148.27933.data 
Path: /AUTH_test/thecontainer/theobject
  Account: AUTH_test
  Container: thecontainer
  Object: theobject
  Object hash: 5b43443c7b6302922d25350ffa47d583
Content-Type: application/octet-stream
Timestamp: 2018-01-11T20:05:48Z (1515701148.27933)
System Metadata:
  No metadata found
Transient System Metadata:
  No metadata found
User Metadata:
  No metadata found
Other Metadata:
  X-Backend-Data-Timestamp: 1515701148.27933
ETag: 764efa883dda1e11db47671c4a3bbd9e (valid)
Content-Length: 3 (valid)
Partition       365
Hash            5b43443c7b6302922d25350ffa47d583

Server:Port Device      127.0.0.1:6030 sdb3
Server:Port Device      127.0.0.1:6020 sdb2
Server:Port Device      127.0.0.1:6040 sdb4
Server:Port Device      127.0.0.1:6010 sdb1      [Handoff]


curl -g -I -XHEAD "http://127.0.0.1:6030/sdb3/365/AUTH_test/thecontainer/theobject"
curl -g -I -XHEAD "http://127.0.0.1:6020/sdb2/365/AUTH_test/thecontainer/theobject"
curl -g -I -XHEAD "http://127.0.0.1:6040/sdb4/365/AUTH_test/thecontainer/theobject"
curl -g -I -XHEAD "http://127.0.0.1:6010/sdb1/365/AUTH_test/thecontainer/theobject" # [Handoff]


Use your own device location of servers:
such as "export DEVICE=/srv/node"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb3/objects/365/583/5b43443c7b6302922d25350ffa47d583"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb2/objects/365/583/5b43443c7b6302922d25350ffa47d583"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb4/objects/365/583/5b43443c7b6302922d25350ffa47d583"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb1/objects/365/583/5b43443c7b6302922d25350ffa47d583" # [Handoff]

note: `/srv/node*` is used as default value of `devices`, the real value is set in the config file on each storage node.
```
