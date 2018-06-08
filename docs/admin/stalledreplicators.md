## Andrewd Replication

Andrewd will orchestrate replication across the cluster as needed. For example, if Andrewd notices a missing dispersion object, it will start replication of that partition. If a device fails and is removed from the ring, Andrewd will start up replication to rebalance the cluster according to ring changes. This works for automated ring changes that Andrewd makes itself (via the unmounted monitor for example) and for ring changes made via `hummingbird ring` commands. Andrewd constantly monitors on the on disk ring files and activates the required replication when those ring files change.

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
