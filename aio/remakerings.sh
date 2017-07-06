#!/bin/bash

cd /etc/hummingbird

rm -f *.builder *.ring.gz backups/*

hummingbird ring-builder create ./object.builder 10 3 1
hummingbird ring-builder add ./object.builder r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1
hummingbird ring-builder add ./object.builder r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1
hummingbird ring-builder add ./object.builder r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1
hummingbird ring-builder add ./object.builder r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1
hummingbird ring-builder rebalance ./object.builder

hummingbird ring-builder create ./container.builder 10 3 1
hummingbird ring-builder add ./container.builder r1z1-127.0.0.1:6011R127.0.0.1:8011/sdb1 1
hummingbird ring-builder add ./container.builder r1z2-127.0.0.1:6021R127.0.0.1:8021/sdb2 1
hummingbird ring-builder add ./container.builder r1z3-127.0.0.1:6031R127.0.0.1:8031/sdb3 1
hummingbird ring-builder add ./container.builder r1z4-127.0.0.1:6041R127.0.0.1:8041/sdb4 1
hummingbird ring-builder rebalance ./container.builder

hummingbird ring-builder create ./account.builder 10 3 1
hummingbird ring-builder add ./account.builder r1z1-127.0.0.1:6012R127.0.0.1:8012/sdb1 1
hummingbird ring-builder add ./account.builder r1z2-127.0.0.1:6022R127.0.0.1:8022/sdb2 1
hummingbird ring-builder add ./account.builder r1z3-127.0.0.1:6032R127.0.0.1:8032/sdb3 1
hummingbird ring-builder add ./account.builder r1z4-127.0.0.1:6042R127.0.0.1:8042/sdb4 1
hummingbird ring-builder rebalance ./account.builder
