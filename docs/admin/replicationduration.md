## Replication Duration Report

The Replication Duration Report gives the average amount of time it takes to run a replication pass of each drive. The JSON output will give averages for each server whereas the plain text report just gives the overall average. If this duration starts dramatically increasing it can indicate an overloaded cluster. For a normal cluster, twice the high value should be used for the min-part-hours of the rings.

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
