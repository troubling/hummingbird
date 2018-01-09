So You Want to Deploy Hummingbird
=================================

## Concepts

### The Ring

The ring is a datastructure that keeps track of the which devices are in the cluster, and is used to determine which of those devices an object should be stored on.

#### Replicas

The replica count in the ring defines the number of copies of an object and is typlicaly set to 3.

#### Partitions

Partitions are abstract buckets that objects are assigned to.  The ring evenly distributes these partitions to each storage device in the cluster.  The number of partitions is typically represented by a power of 2 and is typically set to 22 which would mean that there are 2^22 (4,194,304) * (# of replicas) partitions in the cluster.

#### Minimum Partition Hours

When changes to the ring are made, it will only move 1 of the replicas at a time.  This helps ensure that data remains available in the cluster.  Once a partition has been moved, it can not move again until Minimum Partition Hours has elapsed.  This value should be set at a minimum of the maximum time a successful replication pass takes in the cluster.  It is typically set to something higher like 168 hours (1 week) to be safe.

### Zones

Hummingbird uses zones as an abstract concept that allows the deployer to define failure domains.  When objects are stored in Hummingbird it will store each replica in a different zone.  In a small deployment, a zone could be each server, and in a larger deployment, it could be each cabinet.

## Hardware Considerations

The recommendations made below are made based on average use cases, and should be considered as a good starting point, but should be tailored for each cluster's use case.

### Storage Nodes

All hummingbird services run on each storage node.  SATA drives are recommended to store object data and SSD drives are recommended for account/container data.  Typically 5% of the total storage should be SSD drives.  It is not recommended to use parity based RAID (like RAID 5) with the storage drives.  A RAID or HBA card with battery backed caching will improve performance though.  Modern Intel Xeon CPUs are recommended and typical servers will have 2 multi core processors.  Each server should have at least two 10Gb Ethernet ports, and 64GB is recommended as a minimum.

### Load Balancing

Each storage node is capable of serving API requests, and it is recommended to have a load balancer in front of the cluster.  HA Proxy is a capable open source load ballancer that works well with hummingbird.

### Networking

When designing your network, here are some general considerations:

  *  Each storage node needs full connectivity to each other storage node.
  *  Each incoming object sent will fan out to (# of replicas) storage servers, so intra cluster bandwith will be (# of replicas)x the overall throughput for incoming requests.
  *  All other requests only require a single storage node.
  *  For larger/busier clusters, consideration will need to be made for bandwidth between zones.
