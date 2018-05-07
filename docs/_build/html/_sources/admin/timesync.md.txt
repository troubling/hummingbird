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
