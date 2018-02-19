Hummingbird Priority Replication Tools
======================================

## hummingbird nodes -p &lt;partition&gt;

If you'd like to know where the cluster is storing a given partition, you can use the `hummingbird nodes -p <partition>` command. For example:

```
$ hummingbird nodes -p 365

Account
Container
Object
Partition       365
Hash

Server:Port Device      127.0.0.1:6030 sdb3
Server:Port Device      127.0.0.1:6020 sdb2
Server:Port Device      127.0.0.1:6040 sdb4
Server:Port Device      127.0.0.1:6010 sdb1      [Handoff]


curl -g -I -XHEAD "http://127.0.0.1:6030/sdb3/365/"
curl -g -I -XHEAD "http://127.0.0.1:6020/sdb2/365/"
curl -g -I -XHEAD "http://127.0.0.1:6040/sdb4/365/"
curl -g -I -XHEAD "http://127.0.0.1:6010/sdb1/365/" # [Handoff]


Use your own device location of servers:
such as "export DEVICE=/srv/node"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb3/objects/365"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb2/objects/365"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb4/objects/365"
ssh 127.0.0.1 "ls -lah ${DEVICE:-/srv/node*}/sdb1/objects/365" # [Handoff]

note: `/srv/node*` is used as default value of `devices`, the real value is set in the config file on each storage node.
```

This shows that the partition #365 was primarily stored on the 127.0.0.1:6030/sdb3, 127.0.0.1:6020/sdb2, and 127.0.0.1:6040/sdb4 devices (this was an all-in-one development cluster.) Using this information, you can directly check if the devices actually have that partition's information, or double-check the devices are online and receiving writes etc.


## moveparts

After a ring rebalance / deployment, the swift cluster will heal itself / fill
up new capacity via the replicator. What will happen is as the replicator (in
the rest of the cluster) walks its partitions normally, some of them will
belong to the new capacity and will be moved over there. This can take a while.
**moveparts** will speed this up. It compares the previous ring to the current
ring, finds the partitions that need to be moved, and prioritizes those
movements. Say partition 78 was living:

```
1.1.1.4/sdb1/78
1.1.1.5/sdb2/78
1.1.1.6/sdb3/78
```

and after a rebalance was changed to:

```
1.1.1.4/sdb1/78
1.1.1.5/sdb2/78
1.1.1.8/sdb4/78
```


**moveparts** would send a priority replication call to 1.1.1.6 and tell it to move
*sdb3/78* to *1.1.1.8/sdb4/78*.

This helps in 2 ways:
  *  *1.1.1.8/sdb4* will fill up quicker.
  *  *1.1.1.6/sdb3* will drain faster. Once partition 78 is moved to the new node it
will be removed from *1.1.1.6/sdb3* even if *1.1.1.4/sdb1* (another primary) is
temporarily unavailible.  Normal replication requires all primaries to respond
with success before it removes out-of-place partitions. This can be very
helpful in clusters under heavy load and adding capacity to handle the strain.

If 1.1.1.6/sdb3 was set to zero weight, the priority replication call would
still be made to that node. This will allow for draining nodes when slowly
decommissioning gear.

If 1.1.1.6/sdb3 was completely removed from the new ring then priority replication
calls would be made to the "next" primary. In the above case from
1.1.1.4/sdb1/78 -> 1.1.1.8/sdb4/78.

It also does this in a coordinated fashion to avoid stampeding *1.1.1.8/sdb3*
with a ton of replication calls.

Run it like:

```
hummingbird moveparts /path/to/old/object.ring.gz
```

the current ring must be in the *normal* spot. (i.e. */etc/hummingbird/object.ring.gz*)
and it is important that that is the deployed ring in the cluster.

If you are using storage policies, use *-p policy_id* so it can find the correct
current ring.

This should be run after every ring change to heal your cluster asap.

## restoredevice

When a device fails in the cluster and has to be replaced, and can be replaced
quickly, it is best to just remove / replace it without removing it from the
ring. When the new empty device is back online, replication will fill it back
up with partitions. This happens as the replicator walks the rest of the drives
in the cluster and notices that a partition is missing from the new device.
This process can be sped up using **restoredevice**. This will query the ring,
find the partitions that belong to the given device, and send priority
replication commands to the other primary devices for each partition to push
their data to the new device.

After the new device is online run:
```
hummingbird restoredevice 1.1.1.9 sdb2
```

The IP and device name must match what is stored in the ring. The current ring
must be in the *normal* spot. (i.e. */etc/hummingbird/object.ring.gz*) and it
is important that that is the deployed ring in the cluster.

If you are using storage policies, use *-p policy_id* so it can find the correct
current ring.

This can be run after every device replacement to heal your cluster asap.
