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
