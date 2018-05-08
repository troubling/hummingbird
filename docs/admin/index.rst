:deconsttitle: Hummingbird Cluster Health

==========================
Hummingbird Cluster Health
==========================

The andrewd service as well as the recon tool monitor the overall cluster health. Andrewd is more of a background, automated process that should be run on one server (often called the admin server) whereas the recon tool is more interactive, often also run on the admin server since it can read information from the andrewd database.

Andrewd will initially populate "dispersion objects" and containers into the clusters within a hidden .admin account. These dispersion objects land, one each, on all ring partitions throughout the cluster. After these dispersion objects are in place, andrewd can check for their presence on a recurring schedule and make note of when they are out of place, missing replicas, etc. For example, 1 of 3 replicas being out of place is a common occurrence as a cluster rebalances itself, but 2 of 3 or worse all 3 replicas being out of place can cause temporary errors until rebalancing is performed. Andrewd can prioritize replication based on this information.

.. toctree::
   :maxdepth: 2

   rings.md
   monitoring.md
   drivestatus.md
   async.md
   dispersion.md
   ringmd5.md
   quarantine.md
   stalledreplicators.md
   replicationstats.md
   replicationduration.md
   timesync.md
   replication-tools.md
   debug-single.md
