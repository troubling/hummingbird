Hummingbird
===========

NOTE: Hummingbird is no longer under active development.

Hummingbird is a scalable, performant distributed object storage system written in Go
that is also a drop in replacement for [OpenStack Swift](http://swift.openstack.org) with minimal migration.
The goal is to keep the same protocols and on-disk layout while dramatically
improving performance.

Documentation
-------------

Development, deployment and administration documentation can be found [here](./docs/README.md)

Performance
-----------

The following tests were run on 5 nodes with 128GB of RAM, 10 960GB SSD, and 45 10T drives each and 10Gb networking.  The systems were configured to use an EC scheme of 4 data and 2 parity shards.  Each run was for 1,000,000 objects at 250 concurency through a single `nectar` client.

## 4 KB objects

| 4 KB Results | Hummingbird | Ceph | Swift |
|--------------|-------------|------|-------|
| PUT/sec      | 10395       | 3525 | 890   |
| GET/sec      | 16207       | 8576 | 4208  |
| DELETE/sec   | 9225        | 5440 | 656   |


## 1 MB objects

| 1MB Results | Hummingbird | Ceph | Swift |
|-------------|-------------|------|-------|
| PUT/sec     | 820         | 778  | 477   |
| GET/sec     | 818         | 619  | 602   |
| DELETE/sec  | 9434        | 6211 | 512   |
