Hummingbird Monitoring
======================

Each of the hummingbird services uses Prometheus to instrument the application code.
All the Hummingbird services except proxy server expose their metrics at `/metrics` endpoint. Just doing a curl request against the /metrics path for them will give you all the data for that moment.

For privacy reason we don't expose metrics from proxy server by default. If you want to expose metrics from proxy server, you will need add the obfuscated_prefix in Hummingbird proxy config (/etc/hummingbird/proxy-server.conf).

```
[DEFAULT]
obfuscated_prefix = <prefix_of_your_choice>
```
After this you can access the proxy server metrics at `<prefix_of_your_choice>/metrics` endpoint.

# Metrics exposed by Hummingbird services

| Golang related Metrics                | Metrics Type | Description                                                              |
|---------------------------------------|--------------|--------------------------------------------------------------------------|
| go_gc_duration_seconds                | summary      | A summary of the GC invocation durations.                                |
| go_goroutines                         | gauge        | Number of goroutines that currently exist.                               |
| go_memstats_alloc_bytes               | gauge        | Number of bytes allocated and still in use.                              |
| go_memstats_alloc_bytes_total         | counter      | Total number of bytes allocated, even if freed.                          |
| go_memstats_buck_hash_sys_bytes       | gauge        | Number of bytes used by the profiling bucket hash table.                 |
| go_memstats_frees_total               | counter      | Total number of frees.                                                   |
| go_memstats_gc_sys_bytes              | gauge        | Number of bytes used for garbage collection system metadata.             |
| go_memstats_heap_alloc_bytes          | gauge        | Number of heap bytes allocated and still in use.                         |
| go_memstats_heap_idle_bytes           | gauge        | Number of heap bytes waiting to be used.                                 |
| go_memstats_heap_inuse_bytes          | gauge        | Number of heap bytes that are in use.                                    |
| go_memstats_heap_objects              | gauge        | Number of allocated objects.                                             |
| go_memstats_heap_released_bytes_total | counter      | Total number of heap bytes released to OS.                               |
| go_memstats_heap_sys_bytes            | gauge        | Number of heap bytes obtained from system.                               |
| go_memstats_last_gc_time_seconds      | gauge        | Number of seconds since 1970 of last garbage collection.                 |
| go_memstats_lookups_total             | counter      | Total number of pointer lookups.                                         |
| go_memstats_mallocs_total             | counter      | Total number of mallocs.                                                 |
| go_memstats_mcache_inuse_bytes        | gauge        | Number of bytes in use by mcache structures.                             |
| go_memstats_mcache_sys_bytes          | gauge        | Number of bytes used for mcache structures obtained from system.         |
| go_memstats_mspan_inuse_bytes         | gauge        | Number of bytes in use by mspan structures.                              |
| go_memstats_mspan_sys_bytes           | gauge        | Number of bytes used for mspan structures obtained from system.          |
| go_memstats_next_gc_bytes             | gauge        | Number of heap bytes when next garbage collection will take place.       |
| go_memstats_other_sys_bytes           | gauge        | Number of bytes used for other system allocations.                       |
| go_memstats_stack_inuse_bytes         | gauge        | Number of bytes in use by the stack allocator.                           |
| go_memstats_stack_sys_bytes           | gauge        | Number of bytes obtained from system for stack allocator.                |
| go_memstats_sys_bytes                 | gauge        | Number of bytes obtained by system. Sum of all system allocations.       |
| http_request_duration_microseconds    | summary      | The HTTP request latencies in microseconds.                              |
| http_request_size_bytes               | summary      | The HTTP request sizes in bytes.                                         |
| http_requests_total                   | counter      | Total number of HTTP requests made.                                      |
| http_response_size_bytes              | summary      | The HTTP response sizes in bytes.                                        |
| process_cpu_seconds_total             | counter      | Total user and system CPU time spent in seconds.                         |
| process_max_fds                       | gauge        | Maximum number of open file descriptors.                                 |
| process_open_fds                      | gauge        | Number of open file descriptors.                                         |
| process_resident_memory_bytes         | gauge        | Resident memory size in bytes                                            |
| process_start_time_seconds            | gauge        | Start time of the process since unix epoch in seconds.                   |
| process_virtual_memory_bytes          | gauge        | Virtual memory size in bytes.                                            |


## Hummingbird specific metrics
Each hummingbird service expose its metrics using separate prefix(hb_<service-type>).


| Object Server Metrics                 | Metrics Type | Description                                                              |
|---------------------------------------|--------------|--------------------------------------------------------------------------|
| hb_object_201_responses               | counter      | Total number of 201 response by object server                            |
| hb_object_204_responses               | counter      | Total number of 204 response by object server                            |
| hb_object_500_responses               | counter      | Total number of 500 response by object server                            |
| hb_object_DELETE_requests             | counter      | Total number of DELETE requests received by object server                |
| hb_object_GET_requests                | counter      | Total number of GET requests received by object server                   |
| hb_object_HEAD_requests               | counter      | Total number of HEAD requests received by object server                  |
| hb_object_PUT_requests                | counter      | Total number of PUT requests received by object server                   |
| hb_object_POST_requests               | counter      | Total number of POST requests received by object server                  |
| hb_object_OPTIONS_requests            | counter      | Total number of OPTIONS requests received by object server.              |
| hb_object_requests                    | counter      | Total number of requests received by object server                       |


| Object Replicator Metrics               | Metrics Type | Description                                                            |
|-----------------------------------------|--------------|------------------------------------------------------------------------|
| hb_object_replicator_200_responses      | counter      | Total number of 200 response by object replicator                      |
| hb_object_replicator_404_responses      | counter      | Total number of 404 response by object replicator                      |
| hb_object_replicator_500_responses      | counter      | Total number of 500 response by object replicator                      |
| hb_object_replicator_GET_requests       | counter      | Total number of GET requests received by object replicator             |
| hb_object_replicator_PUT_requests       | counter      | Total number of PUT requests received by object replicator             |
| hb_object_replicator_POST_requests      | counter      | Total number of POST requests received by object replicator            |
| hb_object_replicator_REPCONN_requests   | counter      | Total number of REPCONN requests received by object replicator         |
| hb_object_replicator_REPLICATE_requests | counter      | Total number of REPLICATE requests received by object replicator       |
| hb_object_replicator_requests           | counter      | Total number of requests received by object replicator                 |


| Container Server Specific Metrics     | Metrics Type | Description                                                              |
|---------------------------------------|--------------|--------------------------------------------------------------------------|
| hb_container_200_responses            | counter      | Total number of 200 response by container server                         |
| hb_container_201_responses            | counter      | Total number of 201 response by container server                         |
| hb_container_204_responses            | counter      | Total number of 204 response by container server                         |
| hb_container_404_responses            | counter      | Total number of 404 response by container server                         |
| hb_container_DELETE_requests          | counter      | Total number of DELETE requests received by container server             |
| hb_container_GET_requests             | counter      | Total number of GET requests received by container server                |
| hb_container_HEAD_requests            | counter      | Total number of HEAD requests received by container server               |
| hb_container_PUT_requests             | counter      | Total number of PUT requests received by container server                |
| hb_container_POST_requests            | counter      | Total number of POST requests received by container server               |
| hb_container_REPLICATE_requests       | counter      | Total number of REPLICATE requests received by container server.         |
| hb_container_OPTIONS_requests         | counter      | Total number of OPTIONS requests received by container server.           |
| hb_container_requests                 | counter      | Total number of requests received by container server                    |

| Account Server Specific Metrics     | Metrics Type | Description                                                                |
|---------------------------------------|--------------|--------------------------------------------------------------------------|
| hb_account_200_responses              | counter      | Total number of 200 response by account server                           |
| hb_account_201_responses              | counter      | Total number of 201 response by account server                           |
| hb_account_DELETE_requests            | counter      | Total number of DELETE requests received by account server               |
| hb_account_GET_requests               | counter      | Total number of GET requests received by account server                  |
| hb_account_HEAD_requests              | counter      | Total number of HEAD requests received by account server                 |
| hb_account_PUT_requests               | counter      | Total number of PUT requests received by account server                  |
| hb_account_POST_requests              | counter      | Total number of POST requests received by account server                 |
| hb_account_REPLICATE_requests         | counter      | Total number of REPLICATE requests received by account server.           |
| hb_account_OPTIONS_requests           | counter      | Total number of OPTIONS requests received by account server.             |

| Proxy Server Specific Metrics         | Metrics Type | Description                                                              |
|---------------------------------------|--------------|--------------------------------------------------------------------------|
| hb_proxy_200_responses                | counter      | Total number of 200 response by proxy server                             |
| hb_proxy_201_responses                | counter      | Total number of 201 response by proxy server                             |
| hb_proxy_204_responses                | counter      | Total number of 204 response by proxy server                             |
| hb_proxy_404_responses                | counter      | Total number of 404 response by proxy server                             |
| hb_proxy_DELETE_requests              | counter      | Total number of DELETE requests received by proxy server                 |
| hb_proxy_GET_requests                 | counter      | Total number of GET requests received by proxy server                    |
| hb_proxy_HEAD_requests                | counter      | Total number of HEAD requests received by proxy server                   |
| hb_proxy_PUT_requests                 | counter      | Total number of PUT requests received by proxy server                    |
| hb_proxy_POST_requests                | counter      | Total number of POST requests received by proxy server                   |
| hb_proxy_OPTIONS_requests             | counter      | Total number of OPTIONS requests received by proxy server.               |
| hb_proxy_requests                     | counter      | Total number of requests received by proxy server                        |
| hb_proxy_tempurl_requests             | counter      | Total number of tempurl requests received by proxy server.               |
| hb_proxy_staticweb_requests           | counter      | Total number of staticweb requests received by proxy server.             |
| hb_proxy_slo_DELETE_requests          | counter      | Total number of SLO DELETE requests received by proxy server.            |
| hb_proxy_slo_GET_requests             | counter      | Total number of SLO GET requests received by proxy server.               |
| hb_proxy_slo_PUT_requests             | counter      | Total number of SLO PUT requests received by proxy server.               |


# Prometheus, Grafana & Alertmanager Installation.

You can follow <https://github.com/troubling/hummingbird-monitoring/blob/master/README.md> to setup Hummingbird monitoring using Docker.
