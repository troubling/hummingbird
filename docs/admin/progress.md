## Andrewd Progress Report

Andrewd runs several background processes continuously to monitor the health of the cluster and to make repairs when necessary. You can use `hummingbird recon -progress` to see the state of these background processes:

```
$ hummingbird recon -progress
[2018-06-08 16:14:29] Progress Report
╔═══════════════════════════════╦═══════════╤═══════════╤══════════════════════════════════════════════╗
║                       Process ║    Ago    │ Status    │ Detail                                       ║
╠═══════════════════════════════╬═══════════╪═══════════╪══════════════════════════════════════════════╣
║ dispersion populate container ║    18s    │ Running   │                                              ║
║                               ║           │           │ Previous pass completed 45s ago: 1023        ║
║                               ║           │           │ successes, 1 errors                          ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║    dispersion populate object ║    18s    │ Completed │ 3 policies                                   ║
║                               ║           │           │ Previous pass completed 1m3s ago: 3 policies ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║  dispersion populate object-0 ║ 19h36m24s │ Completed │ 1024 successes, 0 errors                     ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║  dispersion populate object-1 ║   1m3s    │ Completed │ 1024 successes, 0 errors                     ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║  dispersion populate object-2 ║ 19h36m36s │ Completed │ 1024 successes, 0 errors                     ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║     dispersion scan container ║    18s    │ Completed │ container-init does not yet exist            ║
║                               ║           │           │ Previous pass completed 1m41s ago:           ║
║                               ║           │           │ container-init does not yet exist            ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║        dispersion scan object ║    15s    │ Completed │ 3 policies                                   ║
║                               ║           │           │ Previous pass completed 1m37s ago: 3         ║
║                               ║           │           │ policies                                     ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║      dispersion scan object-0 ║    15s    │ Completed │ 1024 of 1024 partitions, 0 not found, 0      ║
║                               ║           │           │ errored                                      ║
║                               ║           │           │ Previous pass completed 1m37s ago: 1024 of   ║
║                               ║           │           │ 1024 partitions, 1369 not found, 0 errored   ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║      dispersion scan object-1 ║    17s    │ Completed │ 1024 of 1024 partitions, 0 not found, 0      ║
║                               ║           │           │ errored                                      ║
║                               ║           │           │ Previous pass completed 1m41s ago:           ║
║                               ║           │           │ object-init does not yet exist               ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║      dispersion scan object-2 ║    16s    │ Completed │ 1024 of 1024 partitions, 0 not found, 0      ║
║                               ║           │           │ errored                                      ║
║                               ║           │           │ Previous pass completed 1m40s ago: 1024 of   ║
║                               ║           │           │ 1024 partitions, 0 not found, 0 errored      ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║            quarantine history ║    18s    │ Running   │                                              ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║             quarantine repair ║    6s     │ Completed │ 12 of 12 urls, 0 repairs made, 0 partition   ║
║                               ║           │           │ replications queued                          ║
║                               ║           │           │ Previous pass completed 1m29s ago: 12 of 12  ║
║                               ║           │           │ urls, 0 repairs made, 0 partition            ║
║                               ║           │           │ replications queued                          ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║                   replication ║    18s    │ Running   │                                              ║
║                               ║           │           │ Previous pass completed 1m45s ago: 0 jobs    ║
║                               ║           │           │ done                                         ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║                  ring monitor ║    13s    │ Completed │ 5 of 5 rings, 0 errors, 0 partition copies   ║
║                               ║           │           │ changed                                      ║
║                               ║           │           │ Previous pass completed 1m36s ago: 5 of 5    ║
║                               ║           │           │ rings, 0 errors, 0 partition copies changed  ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║                     ring scan ║    18s    │ Completed │ 1 of 1 urls, 0 errors                        ║
║                               ║           │           │ Previous pass completed 1m41s ago: 1 of 1    ║
║                               ║           │           │ urls, 0 errors                               ║
╟───────────────────────────────╫───────────┼───────────┼──────────────────────────────────────────────╢
║             unmounted monitor ║    18s    │ Running   │                                              ║
║                               ║           │           │ Previous pass did not complete: 6 of 12      ║
║                               ║           │           │ endpoints, 0 errors, 5/0 servers up/down,    ║
║                               ║           │           │ 10/0 devices mounted/unmounted, eta          ║
║                               ║           │           │ 1m0.001234614s                               ║
╚═══════════════════════════════╩═══════════╧═══════════╧══════════════════════════════════════════════╝
```
