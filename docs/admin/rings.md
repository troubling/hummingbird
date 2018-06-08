# Ring Managment 101

## Ring Overview

The hummingbird rings are responsible for mapping data to devices.  There is a separate ring for the objects, containers, and accounts. Rings are created when you build a new cluster and updated when changes to the cluster need to be made, for example, adding or removing nodes or devices to the storage cluster.  It is important that all servers in the cluster have the same and most current version of the rings.

### Zones

A Zone is an abstract concept in the ring that allows you to define failure domains within the cluster.  Each copy of an object will be stored in a device in a different zone.  It is recommended to choose the highest level of isolation that makes sense for the cluster size.  For example for a small cluster, with a handful of machines, each server might be a zone.  For a larger cluster, it might be each rack of servers.

### Partitions

When an object is stored in the cluster, the ring maps the object to a partition using a hash.  The partitions are evenly distributed across all of the devices in the cluster.

## Ring Creation

Before a ring can be created, there are a few parameters that need to be decided upon.

### Ring Replicas

The replica count determines how many copies of each object will be stored in the cluster.  A typical cluster will have 3 replicas, which means that the cluster will store 3 copies of the object on 3 different devices.

### Partition Power

The Partition Power is the number of partitions used by the ring.  A typical cluster will have a 4,194,304 (2^22) partitions and thus a partition power of 22.

### Min Partition Hours

When the state of the cluster changes, and data needs to be moved, it will try to only move 1 replica at a time (in a 3 replica cluster) to ensure that objects in the cluster remain available.  When a ring change is issued, the ring will not move partitions that have moved more recently than Min Partition Hours.  This helps prevent the cluster from getting into a bad state through a series of ring changes.  A typical cluster will set the Min Partition Hours to 168 (1 week).

A ring can be generated with the command `hummingbird ring <builder_file> create <part_power> <replicas> <min_part_hours>` and an example object ring with the typical settings can be created with `hummingbird ring object.builder create 22 3 168`.

The builder file contains extra information needed for building the rings and care should be taken to backup and not lose these files.

## Adding Devices

Each individual storage device is stored in the ring.  Before adding the device, you will need to know the zone, server ip address and port, device name, and weight of the device.  The server ip address and port should be the address and port that the server for that ring is running on.  The device name should be the name of the partitioned device.  The weight should be the number of Gigabytes the device can store.

A device can be added with the command `hummingbird ring <builder_file> add add z<zone>-<ip>:<port>/<device_name>` and an example device added to the object ring might look like `hummingbird ring object.builder add z1-10.0.0.1:6000/xvdd 1000` for a 1TB device.

Note:  As devices are added to the ring, the actual mappings do not change until you run the rebalance command.  It is best to make all the changes you want to make before rebalancing.

## Rebalancing the Ring

The `hummingbird ring <builder_file> rebalance` command will take the information in the ring builder file and build a compressed ring file that can be used by the servers.  When done, the comannd will return how many partitions moved and the balance of the new ring.

# Ring Best Practices

## Creating the Initial Rings

It is a good idea to create a bash script that creates your initial rings.  An example `make_rings.sh` script could look something like:

```
#!/bin/bash

cd /etc/hummingbird

hummingbird ring object.builder create 22 3 168
hummingbird ring object.builder add r1z1-10.1.1.10:6000/sdd1 2000
hummingbird ring object.builder add r1z1-10.1.1.10:6000/sde1 2000
hummingbird ring object.builder add r1z1-10.1.1.10:6000/sdf1 2000
hummingbird ring object.builder add r1z1-10.1.1.10:6000/sdg1 2000
hummingbird ring object.builder add r1z2-10.1.1.11:6000/sdd1 2000
hummingbird ring object.builder add r1z2-10.1.1.11:6000/sde1 2000
hummingbird ring object.builder add r1z2-10.1.1.11:6000/sdf1 2000
hummingbird ring object.builder add r1z2-10.1.1.11:6000/sdg1 2000
hummingbird ring object.builder add r1z3-10.1.1.12:6000/sdd1 2000
hummingbird ring object.builder add r1z3-10.1.1.12:6000/sde1 2000
hummingbird ring object.builder add r1z3-10.1.1.12:6000/sdf1 2000
hummingbird ring object.builder add r1z3-10.1.1.12:6000/sdg1 2000
hummingbird ring object.builder rebalance

hummingbird ring container.builder create 22 3 168
hummingbird ring container.builder add r1z1-10.1.1.10:6001/sdb1 500
hummingbird ring container.builder add r1z1-10.1.1.10:6001/sdc1 500
hummingbird ring container.builder add r1z2-10.1.1.11:6001/sdb1 500
hummingbird ring container.builder add r1z2-10.1.1.11:6001/sdc1 500
hummingbird ring container.builder add r1z3-10.1.1.12:6001/sdb1 500
hummingbird ring container.builder add r1z3-10.1.1.12:6001/sdc1 500
hummingbird ring container.builder rebalance

hummingbird ring account.builder create 22 3 168
hummingbird ring account.builder add r1z1-10.1.1.10:6002/sdb1 500
hummingbird ring account.builder add r1z1-10.1.1.10:6002/sdc1 500
hummingbird ring account.builder add r1z2-10.1.1.11:6002/sdb1 500
hummingbird ring account.builder add r1z2-10.1.1.11:6002/sdc1 500
hummingbird ring account.builder add r1z3-10.1.1.12:6002/sdb1 500
hummingbird ring account.builder add r1z3-10.1.1.12:6002/sdc1 500
hummingbird ring account.builder rebalance
```

It is *very* important that you add all nodes and devices to the ring before running the rebalance command.

## Andrewd Device Failure Monitoring

Andrewd will continuously scan for unmounted devices and unreachable servers and automatically modify the rings as needed. If a server is unreachable for over four hours, Andrewd will remove all its devices from the rings, rebalance, and push those rings out to the remaining servers. If a device is unmounted for over one hour, that specific device will be removed from the rings, rebalanced, and pushed out.

## Manually Handling a Device failure

If a device has failed and will be replaced in the near future, then no ring changes are neccesary.  The system will work around the failure and replication will copy the data over when the drive is replaced.  Note that there will be some degradation in performance until the device is replaced and fully replicated.

If a device has failed and it will either not be replaced or will be a while before being replaced, then it would be a good idea to set the device's weight to 0.  The ring will spread the partitions that the device was responsible for to other devices in the cluster.  For example if device sde1 failed on node 10.1.1.11, the following commands would be issued:

```
hummingbird ring object.builder set_weight -ip 10.1.1.11 -device sde1 0
hummingbird ring object.builder rebalance
```

When the device is replaced, you can set the weight back to its original value.  The ring will reassign partitions to the device, and Hummingbird will replicate data back to the device.  For example:

```
hummingbird ring object.builder set_weight -ip 10.1.1.11 -device sde1 2000
hummingbird ring object.builder rebalance
```
## Adding or Removing Large Numbers of Devices

If a large number of devices are added or removed in a cluster at full weight, the cluster could get overwhelmed trying to replicate a lot of data at once.  If the device changes are made with a fraction of the final intended weight, then it is easier to control how much data is moved around the cluster.  For example if the size of the cluster is being expanded, add the new devices with a weight of 20% their intended final weight, rebalance and wait for replication to move most of that data.  Then, adjust the weight to 40%, and wait again.  Continue repeating this until the weight is at 100%.  Do the reverse if you intend on removing a large number of devices from the cluster at the same time.  

Note: It is important that you do all of your ring changes before running the rebalance command.

## Andrewd Ring Distribution

Andrewd will continuously scan the cluster and push out new rings as needed. A regular "idle" scan will try to hit every service in the cluster once every 10 minutes. This idle scan is mostly in case a older server comes back online with an older ring, or an on disk ring get corrupted somehow, etc. When the ring is actively changed by Andrewd, such as with a detected device failure, the ring scan will run at full speed to push the new ring out as quickly as possible.

## Ring Action Report

Andrewd will record each ring action it takes in its database and you can retrieve this information as a report:

```
$ hummingbird recon -rar
[2018-06-08 20:12:36] Ring Action Report
Account Ring:
    2018-06-08 16:12 rebalanced due to schedule; now settled
    2018-06-08 20:11 removed unmounted device sdb1 id:0 on server 127.0.0.1:6012
    2018-06-08 20:11 rebalanced due to downed device sdb1 on 127.0.0.1:6012
Container Ring:
    2018-06-08 16:12 rebalanced due to schedule; now settled
    2018-06-08 20:09 removed unmounted device sdb1 id:0 on server 127.0.0.1:6011
    2018-06-08 20:09 rebalanced due to downed device sdb1 on 127.0.0.1:6011
Object Ring 0:
    2018-06-08 16:12 rebalanced due to schedule; now settled
    2018-06-08 20:11 removed unmounted device sdb1 id:0 on server 127.0.0.1:6010
    2018-06-08 20:11 rebalanced due to downed device sdb1 on 127.0.0.1:6010
Object Ring 1:
    2018-06-08 16:12 rebalanced due to schedule; now settled
    2018-06-08 20:11 removed unmounted device sdb1 id:0 on server 127.0.0.1:6010
    2018-06-08 20:11 rebalanced due to downed device sdb1 on 127.0.0.1:6010
Object Ring 2:
    2018-06-08 16:12 rebalanced due to schedule; now settled
    2018-06-08 20:11 removed unmounted device sdb1 id:0 on server 127.0.0.1:6010
    2018-06-08 20:11 rebalanced due to downed device sdb1 on 127.0.0.1:6010
```

Once again, the JSON output is more detailed and more machine parseable:

```
$ hummingbird recon -rar -json
{
    "Name": "Ring Action Report",
    "Time": "2018-06-08T20:11:37.598220043Z",
    "Pass": true,
    "Errors": null,
    "Warnings": null,
    "AccountReport": [
        {
            "Time": "2018-06-08T16:12:45Z",
            "Reason": "rebalanced due to schedule; now settled"
        },
        {
            "Time": "2018-06-08T20:11:24Z",
            "Reason": "removed unmounted device sdb1 id:0 on server 127.0.0.1:6012"
        },
        {
            "Time": "2018-06-08T20:11:24Z",
            "Reason": "rebalanced due to downed device sdb1 on 127.0.0.1:6012"
        }
    ],
    "ContainerReport": [
        {
            "Time": "2018-06-08T16:12:46Z",
            "Reason": "rebalanced due to schedule; now settled"
        },
        {
            "Time": "2018-06-08T20:09:44Z",
            "Reason": "removed unmounted device sdb1 id:0 on server 127.0.0.1:6011"
        },
        {
            "Time": "2018-06-08T20:09:44Z",
            "Reason": "rebalanced due to downed device sdb1 on 127.0.0.1:6011"
        }
    ],
    "ObjectReports": {
        "0": [
            {
                "Time": "2018-06-08T16:12:47Z",
                "Reason": "rebalanced due to schedule; now settled"
            },
            {
                "Time": "2018-06-08T20:11:04Z",
                "Reason": "removed unmounted device sdb1 id:0 on server 127.0.0.1:6010"
            },
            {
                "Time": "2018-06-08T20:11:04Z",
                "Reason": "rebalanced due to downed device sdb1 on 127.0.0.1:6010"
            }
        ],
        "1": [
            {
                "Time": "2018-06-08T16:12:51Z",
                "Reason": "rebalanced due to schedule; now settled"
            },
            {
                "Time": "2018-06-08T20:11:04Z",
                "Reason": "removed unmounted device sdb1 id:0 on server 127.0.0.1:6010"
            },
            {
                "Time": "2018-06-08T20:11:04Z",
                "Reason": "rebalanced due to downed device sdb1 on 127.0.0.1:6010"
            }
        ],
        "2": [
            {
                "Time": "2018-06-08T16:12:52Z",
                "Reason": "rebalanced due to schedule; now settled"
            },
            {
                "Time": "2018-06-08T20:11:04Z",
                "Reason": "removed unmounted device sdb1 id:0 on server 127.0.0.1:6010"
            },
            {
                "Time": "2018-06-08T20:11:04Z",
                "Reason": "rebalanced due to downed device sdb1 on 127.0.0.1:6010"
            }
        ]
    }
}
```
