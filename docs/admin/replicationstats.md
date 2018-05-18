## Replication Partitions Per Second Report

The Replication Partitions Per Second Report shows the speed that replication is occurring throughout the cluster. Cluster-wide low counts can indicate an overloaded cluster, whereas low counts on specific servers can indicate hardware failures on those servers such as a slow reading drive or a faulty network interface.

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
