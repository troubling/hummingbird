How to enable TLS for Hummingbird
=================================

TODO: Clean this up and move it out of the dev section.

## Generate tls cert

```
openssl req \
    -newkey rsa:2048 \
    -nodes \
    -days 3650 \
    -x509 \
    -keyout ca.key \
    -out ca.crt \
    -subj "/CN=*"
```

```
openssl req \
    -newkey rsa:2048 \
    -nodes \
    -keyout hummingbird.key \
    -out hummingbird.csr \
    -subj "/C=US/ST=San Antonio/L=San Antonio/O=/OU=/CN=*"
```
```
openssl x509 \
    -req \
    -days 365 \
    -sha256 \
    -in hummingbird.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out hummingbird.crt \
    -extfile <(echo -e "subjectAltName = IP:127.0.0.1 \nextendedKeyUsage = serverAuth,clientAuth")
```
## Copy & update the ca.crt to system cert directory

```
cp ca.crt /usr/local/share/ca-certificates/
/usr/sbin/update-ca-certificates -f
```

## Copy hummingbird keys to /etc/hummingbird

```
mv hummingbird.crt /etc/hummingbird
mv hummingbird.key /etc/hummingbird
```

Now add the following two lines in all the configuration files in [Default] section
```
cert_file = /etc/hummingbird/hummingbird.crt
key_file = /etc/hummingbird/hummingbird.key
```

## Create new ring with https Scheme
```
hummingbird ring object.builder create 10 3 1
hummingbird ring object.builder add r1z1shttps-127.0.0.1:6010/sdb1 1
hummingbird ring object.builder add r1z2shttps-127.0.0.2:6020/sdb2 1
hummingbird ring object.builder add r1z3shttps-127.0.0.3:6030/sdb3 1
hummingbird ring object.builder add r1z4shttps-127.0.0.4:6040/sdb4 1
hummingbird ring object.builder rebalance
hummingbird ring object-1.builder create 10 2 1
hummingbird ring object-1.builder add r1z1shttps-127.0.0.1:6010/sdb1 1
hummingbird ring object-1.builder add r1z2shttps-127.0.0.2:6020/sdb2 1
hummingbird ring object-1.builder add r1z3shttps-127.0.0.3:6030/sdb3 1
hummingbird ring object-1.builder add r1z4shttps-127.0.0.4:6040/sdb4 1
hummingbird ring object-1.builder rebalance
hummingbird ring object-2.builder create 10 6 1
hummingbird ring object-2.builder add r1z1shttps-127.0.0.1:6010/sdb1 1
hummingbird ring object-2.builder add r1z1shttps-127.0.0.1:6010/sdb5 1
hummingbird ring object-2.builder add r1z2shttps-127.0.0.2:6020/sdb2 1
hummingbird ring object-2.builder add r1z2shttps-127.0.0.2:6020/sdb6 1
hummingbird ring object-2.builder add r1z3shttps-127.0.0.3:6030/sdb3 1
hummingbird ring object-2.builder add r1z3shttps-127.0.0.3:6030/sdb7 1
hummingbird ring object-2.builder add r1z4shttps-127.0.0.4:6040/sdb4 1
hummingbird ring object-2.builder add r1z4shttps-127.0.0.4:6040/sdb8 1
hummingbird ring object-2.builder rebalance
hummingbird ring container.builder create 10 3 1
hummingbird ring container.builder add r1z1shttps-127.0.0.1:6011/sdb1 1
hummingbird ring container.builder add r1z2shttps-127.0.0.2:6021/sdb2 1
hummingbird ring container.builder add r1z3shttps-127.0.0.3:6031/sdb3 1
hummingbird ring container.builder add r1z4shttps-127.0.0.4:6041/sdb4 1
hummingbird ring container.builder rebalance
hummingbird ring account.builder create 10 3 1
hummingbird ring account.builder add r1z1shttps-127.0.0.1:6012/sdb1 1
hummingbird ring account.builder add r1z2shttps-127.0.0.2:6022/sdb2 1
hummingbird ring account.builder add r1z3shttps-127.0.0.3:6032/sdb3 1
hummingbird ring account.builder add r1z4shttps-127.0.0.4:6042/sdb4 1
hummingbird ring account.builder rebalance
```
