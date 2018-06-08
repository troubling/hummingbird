## Dispersion Report

Andrewd will populate and monitor dispersion objects to ensure data is reachable at all times. For each policy and for containers, Andrewd will place a single item into the cluster on each ring partition. Once in place, Andrewd will monitor these items and record their statuses. For example, 1 of 3 replicas being out of place is a common occurrence as a cluster rebalances itself, but 2 of 3 or worse all 3 replicas being out of place can cause temporary errors until rebalancing is performed. Andrewd can prioritize replication based on this information. The recon tool can show you the last dispersion report andrewd created, when run on the same server as andrewd runs.

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
