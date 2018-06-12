# Tunable Configuration Options

## Debug Logging

To turn on verbose debug logging you can set `log_level = DEBUG` under the `[DEFAULT]` section of any config file:

```
[DEFAULT]
log_level = DEBUG
```

## Read Affinity

The proxy server supports Read Affinities which give preference to certain devices. By default the proxy server will read from the appropriate devices in random order until it has success. However, you can set the read affinitity to prefer devices in the same datacenter as the proxy server, for example. For this example, assume the proxy server is in region 1. You can set in its proxy-server.conf:

```
[app:proxy-server]
read_affinity = r1=100
```

And this will prefer reading from devices in region 1, only falling back to other regions if necessary.

You may get even more specific and specify a zone to prefer as well if, for example, the proxy server is within that same zone as well as region:

```
read_affinity = r1z1=100, r1=200
```

The number after the equal sign, 100 and 200 above, are the priority values. Lower means higher priority, or first to be used.

## Rate Limits

You can set rate limits for certain operations to control how many resources are used at once. The `account_db_max_writes_per_sec` controls how many concurrent container write (PUT POST DELETE) operations are allowed per account. The `container_db_max_writes_per_sec` controls how many concurrent object write (PUT POST DELETE COPY) operations are allowed per container. Normally you can just leave these unset and let the cluster manage itself. But, if you'd like, you can tune these settings in your proxy-server.conf like in the following example:

```
[filter:ratelimit]
account_db_max_writes_per_sec = 100
container_db_max_writes_per_sec = 100
```
