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

Drive Failures
==============
The most common failure you will get running a hummingbird cluster is drive
failures. Here are a couple tools / methods to consider on how to fix these. 

```
hummingbird recon -ds
Weighted / Unmounted / Unreachable devices report for Policy 0
Last successful recon run was 0.50 hours ago.
+--------+------------+------+--------+--------+---------+-----------+------------------------------+------------------+
| POLICY | IP ADDRESS | PORT | DEVICE | WEIGHT | MOUNTED | REACHABLE | LAST STATE CHANGE            | SHOULD BE ZEROED |
| 0      | 10.0.0.1   | 6000 | sdb1   | 100.00 | false   | true      | Wed Jan 17 21:26:09 UTC 2018 | false            |
+--------+------------+------+--------+--------+---------+-----------+------------------------------+------------------+
```

shows that sdb1 on 10.0.0.1 has been unmounted since Wed Jan 17 21:26. 

(If reachable was false that means the the recon request to that object server
failed- every drive on the server would have reachable false.)

You should investigate what is wrong with that drive.

  * If the drive can be recovered/remounted then do that and you're done.
Replication will resend all the writes that should have gone to that drive
while it was down.

  * If the drive can not be recovered but can be replaced in a relatively short
period of time (see below)- then just do that. Once the (new) drive is
remounted, replication will fill that drive with all the data that should be
there from other servers in the cluster. In the mean time writes will be
redirected to other nodes in the cluster.

  * If the drive can not be replaced within a short period of time it should be
removed from the ring. All the data that used to belong on the drive will be
remapped to other drives in the cluster by the replicator. No new requests will
be directed to that drive. The main problem with removing drives from your ring
is it lowers the amount of capacity in your cluster, and it causes extra
backend replication traffic.

What is a "relatively short period of time"? It mostly depends on how long your
replication pass times are (which you can see with `hummingbird recon -rd`). If
you have replication pass times of 1 day and you know you won't get new drives
for atleast a week then you might want to change the ring. If your replication
passes are a week and you'll get a new drive in a day- then just wait.
