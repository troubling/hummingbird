Cluster Health
==============

The andrewd service as well as the recon tool monitor the overall cluster health. Andrewd is more of a background, automated process whereas the recon tool is interactive.

Andrewd will initially populate "dispersion objects" and containers into the clusters within a hidden .admin account. These dispersion objects land, one each, on all ring partitions throughout the cluster. After these dispersion objects are in place, andrewd can check for their presence on a recurring schedule and make note of when they are out of place, missing replicas, etc. For example, 1 of 3 replicas being out of place is a common occurrence as a cluster rebalances itself, but 2 of 3 or worse all 3 replicas being out of place can cause temporary errors until rebalancing is performed. Andrewd can prioritize replication based on this information.

## Async Pending Report

The Async Pending Report shows how many updates are waiting to be sent to container servers from object servers. A burst of traffic can cause these updates to saved to disk to be resent later when the traffic isn't as high. These saved requests are known as Async Pendings. An ever increasing number of these indicate a cluster that is overwhelmed or possible failures that need to be addressed.

Note that the JSON output will give counts for each server. This can be useful in tracking down an issue. If all servers are relatively equally backed up, this can indicate trouble with the container server layer. If just one server has nearly all the pending requests, it is probably an issue with that server, say, a flakey network interface or route.

```
$ hummingbird recon -a
[2018-01-16 17:19:59] Async Pending Report
[async_pending] low: 0, high: 0, avg: 0.0, total: 0, Failed: 0.0%, no_result: 0, reported: 4
```

```
$ hummingbird recon -a -json
{
    "Name": "Async Pending Report",
    "Time": "2018-01-16T17:20:09.460534783Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null,
    "Stats": {
        "127.0.0.1:6010": 0,
        "127.0.0.1:6020": 0,
        "127.0.0.1:6030": 0,
        "127.0.0.1:6040": 0
    }
}
```

## Dispersion Report

As noted at the top of this document, andrewd will populate and monitor dispersion objects to ensure data is reachable at all times. The recon tool can show you the last dispersion report andrewd created, when run on the same server as andrewd runs.

Note that the JSON output will give specific detail on which partitions are missing replicas to help with any investigation.

```
$ hummingbird recon -d
[2018-01-16 17:25:16] Dispersion Report

Container Dispersion Report at 2018-01-15 20:59:31 +0000 UTC
For container dispersion.
There were 1024 partitions missing 0 copies.
100.00% of container copies found (3072 of 3072)
Sample represents 100% of the container partition space.

Object Dispersion Report at 2018-01-15 21:01:17 +0000 UTC for Policy 0
Using storage policy 0
There were 1023 partitions missing 0 copies.
There were 1 partitions missing 1 copies.
99.97% of object copies found (3071 of 3072)
Sample represents 100% of the object partition space.

Object Dispersion Report at 2018-01-15 19:21:00 +0000 UTC for Policy 1
Using storage policy 1
There were 1024 partitions missing 0 copies.
100.00% of object copies found (2048 of 2048)
Sample represents 100% of the object partition space.
```

```
$ hummingbird recon -d -json
{
    "Name": "Dispersion Report",
    "Time": "2018-01-16T17:25:36.58866905Z",
    "Pass": true,
    "Errors": null,
    "ContainerReport": {
        "Time": "2018-01-15T20:59:31Z",
        "Report": "For container dispersion.\nThere were 1024 partitions missing 0 copies.\n100.00% of container copies found (3072 of 3072)\nSample represents 100% of the container partition space.\n",
        "Stats": null
    },
    "ObjectReports": [
        {
            "Time": "2018-01-15T21:01:17Z",
            "Policy": 0,
            "Report": "Using storage policy 0\nThere were 1023 partitions missing 0 copies.\nThere were 1 partitions missing 1 copies.\n99.97% of object copies found (3071 of 3072)\nSample represents 100% of the object partition space.\n",
            "Stats": [
                {
                    "Time": "2018-01-15T21:01:17Z",
                    "Partition": 689,
                    "PartitionItemPath": "disp-objs-0/689-44",
                    "ItemsFound": 2,
                    "ItemsNeed": 3
                }
            ]
        },
        {
            "Time": "2018-01-15T19:21:00Z",
            "Policy": 1,
            "Report": "Using storage policy 1\nThere were 1024 partitions missing 0 copies.\n100.00% of object copies found (2048 of 2048)\nSample represents 100% of the object partition space.\n",
            "Stats": null
        }
    ]
}
```

## Drive Status Report

The Drive Status Report is gathered from information recorded by andrewd, and so this recon report needs to be run where andrewd is running. It lists the drives that are unmounted or unreachable, usually indicating drive or server failures that need to be corrected.

```
$ hummingbird recon -ds
[2018-01-16 17:31:05] Drive Report
Weighted / Unmounted / Unreachable devices report for Policy 1
Last successful recon run was 0.19 hours ago.
No weighted drives are currently reported as unmounted or unreachable.
Weighted / Unmounted / Unreachable devices report for Policy 0
Last successful recon run was 0.19 hours ago.
No weighted drives are currently reported as unmounted or unreachable.
```

```
$ hummingbird recon -ds -json
{
    "Name": "Drive Report",
    "Time": "2018-01-16T17:31:09.66558933Z",
    "Pass": true,
    "Errors": null,
    "PolicyReports": [
        {
            "Policy": 0,
            "Time": "2018-01-16T17:19:53Z",
            "SingleReports": null
        },
        {
            "Policy": 1,
            "Time": "2018-01-16T17:19:53Z",
            "SingleReports": null
        }
    ],
    "MaxBadDevAge": 604800000000000
}
```

## MD5 Report

The MD5 Report verifies that all of the servers having the same ring, the same general hummingbird.conf, and the same hummingbird executable.

```
$ hummingbird recon -md5
[2018-01-16 17:36:21] Ring MD5 Report
4/4 hosts matched, 0 error[s] while checking hosts.
[2018-01-16 17:36:21] hummingbird.conf MD5 Report
4/4 hosts matched, 0 error[s] while checking hosts.
[2018-01-16 17:36:21] hummingbird MD5 Report
4/4 hosts matched, 0 error[s] while checking hosts.
```

```
$ hummingbird recon -md5 -json
{
    "Name": "Ring MD5 Report",
    "Time": "2018-01-16T17:36:37.703977395Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null
}
{
    "Name": "hummingbird.conf MD5 Report",
    "Time": "2018-01-16T17:36:37.708930063Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null
}
{
    "Name": "hummingbird MD5 Report",
    "Time": "2018-01-16T17:36:37.71483282Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null
}
```

## Quarantine Report

Occasionally, corrupted files will be found and moved to a quarantine area. Such corruption can occur due to hardware errors or software bugs, though usually they indicate failing hardware. If you notice a large increase in quarantined files, it should be investigated as soon as possible.

Note that the JSON output will give specific counts for each server.

```
$ hummingbird recon -q
[2018-01-16 17:37:22] Quarantine Report
[quarantined_account] low: 0, high: 0, avg: 0.0, total: 0, Failed: 0.0%, no_result: 0, reported: 4
[quarantined_container] low: 0, high: 1, avg: 0.2, total: 1, Failed: 0.0%, no_result: 0, reported: 4
[quarantined_objects] low: 0, high: 0, avg: 0.0, total: 0, Failed: 0.0%, no_result: 0, reported: 4
```

```
$ hummingbird recon -q -json
{
    "Name": "Quarantine Report",
    "Time": "2018-01-16T17:37:24.913043473Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null,
    "Stats": {
        "Accounts": {
            "127.0.0.1:6010": 0,
            "127.0.0.1:6020": 0,
            "127.0.0.1:6030": 0,
            "127.0.0.1:6040": 0
        },
        "Containers": {
            "127.0.0.1:6010": 0,
            "127.0.0.1:6020": 0,
            "127.0.0.1:6030": 0,
            "127.0.0.1:6040": 1
        },
        "Objects": {
            "127.0.0.1:6010": 0,
            "127.0.0.1:6020": 0,
            "127.0.0.1:6030": 0,
            "127.0.0.1:6040": 0
        },
        "Policies": {}
    }
}
```

## Stalled Replicators Report

The Stalled Replicators Report gives counts on how many times a replication process (one per drive per server) had to be restarted due to apparent inactivity. These situations can occur for various reasons, one being that the cluster is so overloaded it cannot complete replication tasks across the whole cluster. But, if the counts are high just on one server, it can indicate an imminent hardware failure on that server or drive.

Note that the JSON output gives specific counts for each server.

```
$ hummingbird recon -rc
[2018-01-16 17:41:13] Stalled Replicators Report
[replication_device_cancelations] low: 0, high: 0, avg: 0.0, total: 0, Failed: 0.0%, no_result: 0, reported: 8
```

```
$ hummingbird recon -rc -json
{   
    "Name": "Stalled Replicators Report",
    "Time": "2018-01-16T17:41:16.726968278Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null,
    "Warnings": null,
    "Stats": {
        "127.0.0.1:6010/sdb1": 0,
        "127.0.0.1:6010/sdb1-1": 0,
        "127.0.0.1:6020/sdb2": 0,
        "127.0.0.1:6020/sdb2-1": 0,
        "127.0.0.1:6030/sdb3": 0,
        "127.0.0.1:6030/sdb3-1": 0,
        "127.0.0.1:6040/sdb4": 0,
        "127.0.0.1:6040/sdb4-1": 0
    }
}
```

## Replication Duration Report

The Replication Duration Report is straightforward in that it gives the average amount of time it takes to run a full replication pass. The JSON output will give averages for each server whereas the plain text report just gives the overall average. If this duration starts dramatically increasing it can indicate an overloaded cluster. For a normal cluster, the high value plus some margin should be used for the min-part-hours of the rings.

```
$ hummingbird recon -rd
[2018-01-16 17:47:33] Replication Duration Report
[replication_duration_secs] low: 10.606, high: 10.850, avg: 10.746, Failed: 0.0%, no_result: 0, reported: 4
Number of drives not completed a pass: 0
```

```
$ hummingbird recon -rd -json
{
    "Name": "Replication Duration Report",
    "Time": "2018-01-16T17:47:35.645741121Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null,
    "Stats": {
        "127.0.0.1:6010": 10.784847003,
        "127.0.0.1:6020": 10.742181572,
        "127.0.0.1:6030": 10.6057160435,
        "127.0.0.1:6040": 10.849519436
    },
    "TotalDriveZero": 0
}
```

## Replication Partitions Per Second Report

The Replication Partitions Per Second Report is related to the Replication Duration Report in that it shows the speed that replication is occurring throughout the cluster. Cluster-wide low counts can indicate an overloaded cluster, whereas low counts on specific servers can indicate hardware failures on those servers such as a slow reading drive or a faulty network interface.

Note that the JSON output gives specific drive and server speeds whereas the plain text report gives just the overall values.

```
$ hummingbird recon -rp
[2018-01-16 17:58:33] Replication Partitions Per Second Report
[replication_srv_parts_per_sec] low: 59.460, high: 60.627, avg: 60.175, Failed: 0.0%, no_result: 0, reported: 4
Number drives with no partitions completed: 0
Cluster wide parts/sec: 60.171
```

```
$ hummingbird recon -rp -json
{
    "Name": "Replication Partitions Per Second Report",
    "Time": "2018-01-16T17:58:35.53897395Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null,
    "Warnings": null,
    "Stats": {
        "127.0.0.1:6010": 67.42422752621235,
        "127.0.0.1:6020": 66.88050956069733,
        "127.0.0.1:6030": 59.460068432757204,
        "127.0.0.1:6040": 67.55173310057371
    },
    "DriveSpeeds": {
        "127.0.0.1:6010/sdb1": 73.92206417606499,
        "127.0.0.1:6010/sdb1-1": 67.17052419992405,
        "127.0.0.1:6020/sdb2": 77.25698292390624,
        "127.0.0.1:6020/sdb2-1": 66.72298074078145,
        "127.0.0.1:6030/sdb3": 55.19717872902748,
        "127.0.0.1:6030/sdb3-1": 67.2507531089707,
        "127.0.0.1:6040/sdb4": 77.77384551774584,
        "127.0.0.1:6040/sdb4-1": 67.34428040540625
    },
    "OverallAverage": 63.54168655592484,
    "TotalDriveZero": 0
}
```

## Time Sync Report

The Time Sync Report ensures that all the servers have relatively close time values. If the server times drift quite far from each other, it will cause "odd" intermittent write errors where some servers accept a write as newer and some feel the write is too old. Server times should be corrected immediately and a time sync service like ntp installed.

```
$ hummingbird recon -time
[2018-01-16 18:00:13] Time Sync Report
4/4 hosts matched, 0 error[s] while checking hosts.
```

```
$ hummingbird recon -time -json
{
    "Name": "Time Sync Report",
    "Time": "2018-01-16T18:00:17.428365173Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null
}
```
