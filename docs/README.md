Hummingbird
===========

Hummingbird is a scalable, performant distributed object storage system written in Go
that is also a drop in replacement for [OpenStack Swift](http://swift.openstack.org/) with minimal migration.
The goal is to keep the same protocols and on-disk layout while dramatically
improving performance.

Development
-----------

* Follow [These Instructions](HAIO.md) to install Hummingbird in a development environment.
* [CLI and SDK Support](./dev/clisdk.md)
* [Set up Tempest](./dev/tempest.md) to run Openstack Swift tests on Hummingbird.

Deployment
----------

* [Things to consider](deployment.md) before deploying hummingbird.
* [Hansible](https://github.com/troubling/hansible) is a collection of Ansible playbooks and roles for production Hummingbird deployments.

Administration
--------------

* [Monitoring Hummingbird](./admin/monitoring.md)
* [Debugging account, container or object issues](./admin/debug-single.md)
* [Replication tools](./admin/replication-tools.md)
* [Ring Management](./admin/rings.md)
* [Configuration Tuning](./admin/tuning.md)
* [TLS Support](./dev/tls.md)
* Cluster health and reporting with `hummingbird recon`
    * [Async pending reports](./admin/async.md)
    * [Dispersion report](./admin/dispersion.md)
    * [Drive status](./admin/drivestatus.md)
    * [Cluster health](./admin/progress.md)
    * [Quarantine reports](./admin/quarantine.md)
    * [Replication duration](./admin/replicationduration.md)
    * [Replication stats](./admin/replicationstats.md)
    * [Check for stalled replication](./admin/stalledreplicators.md)
    * [Verify ring hashes](./admin/ringmd5.md)
    * [Check for synchronized system times](./admin/timesync.md)
