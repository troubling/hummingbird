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
