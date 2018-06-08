## Quarantine Report

Occasionally, corrupted files will be found and moved to a quarantine area. Such corruption can occur due to hardware errors or software bugs, though usually they indicate failing hardware. If you notice a large increase in quarantined files, it should be investigated as soon as possible.

Andrewd will scan the cluster for quarantined items and attempt to replace them with copies from elsewhere in the cluster. It will try to do this as efficiently as possible, e.g. copying a single item if it knows exactly what got corrupted or falling back to replicating a whole partition if has no further information.

After Andrewd repairs a quarantined item, it moves that corrupted item into the quarantine history directory structure on the device. After some time (30 days by default) Andrewd will then clear it out of the history area completely. This gives time to research corruption if desired while keeping the cluster clean over time.

The first report you can obtain is the general quarantine report, indicating how many quarantined items there are at the time of the report. Note that the JSON output will give specific counts for each server.

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

The second report is more detailed:

```
$ hummingbird recon -qd
[2018-06-08 16:57:25] Quarantine Detail Report
4/4 hosts matched, 0 error[s] while checking hosts.
objects-2
  127.0.0.1:6020
    sdb2
      8c92b619123b4cef80885156b55f59b5-f0467a28-cc0f-73cc-6e70-97f07dfc07f9 /AUTH_test/container2/image.png
```

```
$ hummingbird recon -qd -json
{
    "Name": "Quarantine Detail Report",
    "Time": "2018-06-08T16:57:27.812390228Z",
    "Pass": true,
    "Servers": 4,
    "Successes": 4,
    "Errors": null,
    "TypeToServerToDeviceToItems": {
        "objects-2": {
            "127.0.0.1:6020": {
                "sdb2": [
                    {
                        "NameOnDevice": "8c92b619123b4cef80885156b55f59b5-f0467a28-cc0f-73cc-6e70-97f07dfc07f9",
                        "NameInURL": "/AUTH_test/container2/image.png"
                    }
                ]
            }
        }
    }
}
```
