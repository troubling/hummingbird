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
