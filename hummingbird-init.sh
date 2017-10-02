#!/bin/bash
set -e
set -x

sudo adduser --system --group --no-create-home hummingbird
sudo apt install -y memcached

sudo rm -f /usr/bin/hummingbird
sudo mkdir -p /usr/bin
sudo chmod 0755 /usr
sudo chmod 0755 /usr/bin
if [ -e hummingbird ] ; then
    sudo cp hummingbird /usr/bin/
else
    sudo cp bin/hummingbird /usr/bin/
fi
sudo chown root: /usr/bin/hummingbird
sudo chmod 0755 /usr/bin/hummingbird

sudo mkdir -p /etc/hummingbird
sudo chmod 0755 /etc
sudo chmod 0755 /etc/hummingbird
sudo chown cory: /etc/hummingbird
sudo chmod 0755 /etc/hummingbird

sudo tee /etc/hummingbird/hummingbird.conf >/dev/null << EOF
[swift-hash]
swift_hash_path_prefix = changeme
swift_hash_path_suffix = changeme

[storage-policy:0]
name = gold
policy_type = replication
default = yes

[storage-policy:1]
name = silver
policy_type = replication
EOF
sudo chown cory: /etc/hummingbird/hummingbird.conf
sudo chmod 0644 /etc/hummingbird/hummingbird.conf

sudo tee /etc/hummingbird/proxy-server.conf >/dev/null << EOF
[DEFAULT]
bind_ip = 127.0.0.1
bind_port = 8080

[app:proxy-server]
allow_account_management = true
account_autocreate = true

[filter:tempauth]
user_admin_admin = admin .admin .reseller_admin
user_test_tester = testing .admin
user_test2_tester2 = testing2 .admin
user_test_tester3 = testing3

[filter:catch_errors]

[filter:healthcheck]

[filter:proxy-logging]

[filter:ratelimit]

[filter:dlo]

[filter:slo]

[filter:tempurl]

[filter:staticweb]

[filter:copy]
EOF
sudo chown cory: /etc/hummingbird/proxy-server.conf
sudo chmod 0644 /etc/hummingbird/proxy-server.conf

sudo tee /etc/hummingbird/account-server.conf >/dev/null << EOF
[DEFAULT]
devices = /srv/hummingbird
mount_check = false
bind_ip = 127.0.0.1
bind_port = 6012

[app:account-server]
disk_limit = 0/0

[account-replicator]
EOF
sudo chown cory: /etc/hummingbird/account-server.conf
sudo chmod 0644 /etc/hummingbird/account-server.conf

sudo tee /etc/hummingbird/container-server.conf >/dev/null << EOF
[DEFAULT]
devices = /srv/hummingbird
mount_check = false
bind_ip = 127.0.0.1
bind_port = 6011

[app:container-server]
disk_limit = 0/0

[container-replicator]
EOF
sudo chown cory: /etc/hummingbird/container-server.conf
sudo chmod 0644 /etc/hummingbird/container-server.conf

sudo tee /etc/hummingbird/object-server.conf >/dev/null << EOF
[DEFAULT]
devices = /srv/hummingbird
mount_check = false

[app:object-server]
bind_ip = 127.0.0.1
bind_port = 6010
disk_limit = 0/0
account_rate_limit = 0/0

[object-replicator]
bind_ip = 127.0.0.1
bind_port = 8010

[object-auditor]
EOF
sudo chown cory: /etc/hummingbird/object-server.conf
sudo chmod 0644 /etc/hummingbird/object-server.conf

sudo mkdir -p /lib/systemd/system
sudo chmod 0755 /lib
sudo chmod 0755 /lib/systemd
sudo chmod 0755 /lib/systemd/system
sudo tee /lib/systemd/system/hummingbird-proxy.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-proxy
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird proxy
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-proxy
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-proxy.service

sudo tee /lib/systemd/system/hummingbird-account.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-account
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird account
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-account
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-account.service

sudo tee /lib/systemd/system/hummingbird-account-replicator.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-account-replicator
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird account-replicator
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-account-replicator
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-account-replicator.service

sudo tee /lib/systemd/system/hummingbird-container.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-container
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird container
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-container
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-container.service

sudo tee /lib/systemd/system/hummingbird-container-replicator.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-container-replicator
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird container-replicator
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-container-replicator
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-container-replicator.service

sudo tee /lib/systemd/system/hummingbird-object.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-object
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird object
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-object
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-object.service

sudo tee /lib/systemd/system/hummingbird-object-replicator.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-object-replicator
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird object-replicator
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-object-replicator
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-object-replicator.service

sudo tee /lib/systemd/system/hummingbird-object-auditor.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-object-auditor
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird object-auditor
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-object-auditor
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-object-auditor.service

sudo tee /lib/systemd/system/hummingbird-andrewd.service >/dev/null << EOF
# See these pages for lots of options:
# http://0pointer.de/public/systemd-man/systemd.service.html
# http://0pointer.de/public/systemd-man/systemd.exec.html
[Unit]
Description=hummingbird-andrewd
After=syslog.target network.target
[Service]
Type=simple
User=hummingbird
Group=hummingbird
ExecStart=/usr/bin/hummingbird andrewd
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-andrewd
LimitCPU=infinity
LimitFSIZE=infinity
LimitDATA=infinity
LimitSTACK=infinity
LimitCORE=infinity
LimitRSS=infinity
LimitNOFILE=1048576
LimitAS=infinity
LimitNPROC=infinity
LimitMEMLOCK=infinity
LimitLOCKS=infinity
LimitSIGPENDING=infinity
LimitMSGQUEUE=infinity
LimitNICE=infinity
LimitRTPRIO=infinity
LimitRTTIME=infinity
[Install]
WantedBy=multi-user.target
EOF
sudo chmod 0644 /lib/systemd/system/hummingbird-andrewd.service

sudo mkdir -p /var/cache/swift
sudo chown cory: /var/cache/swift
sudo chmod 0755 /var
sudo chmod 0755 /var/cache
sudo chmod 0755 /var/cache/swift
sudo mkdir -p /var/log/hummingbird
sudo chmod 0755 /var
sudo chmod 0755 /var/log
sudo chmod 0755 /var/log/hummingbird
sudo mkdir -p /var/run/hummingbird
sudo chmod 0755 /var
sudo chmod 0755 /var/run
sudo chmod 0755 /var/run/hummingbird

sudo mkdir -p /srv/hummingbird
sudo chown cory: /srv/hummingbird
sudo chmod 0755 /srv
sudo chmod 0755 /srv/hummingbird
sudo /usr/bin/hummingbird ring /etc/hummingbird/account.builder create 10 1 1
sudo /usr/bin/hummingbird ring /etc/hummingbird/account.builder add r1z1-127.0.0.1:6012/hummingbird 1
sudo /usr/bin/hummingbird ring /etc/hummingbird/account.builder rebalance
sudo /usr/bin/hummingbird ring /etc/hummingbird/container.builder create 10 1 1
sudo /usr/bin/hummingbird ring /etc/hummingbird/container.builder add r1z1-127.0.0.1:6011/hummingbird 1
sudo /usr/bin/hummingbird ring /etc/hummingbird/container.builder rebalance
sudo /usr/bin/hummingbird ring /etc/hummingbird/object.builder create 10 1 1
sudo /usr/bin/hummingbird ring /etc/hummingbird/object.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/hummingbird 1
sudo /usr/bin/hummingbird ring /etc/hummingbird/object.builder rebalance
sudo rm -rf /etc/hummingbird/backups
sudo chown cory: /etc/hummingbird/*.{builder,ring.gz}
sudo chmod 0644 /etc/hummingbird/*.{builder,ring.gz}

tail -17 $0 | cut -f2- -d ' '

# Hummingbird is ready for use.
# 
# For quick local testing:
# 
# sudo systemctl start memcached
# sudo systemctl start hummingbird-proxy
# sudo systemctl start hummingbird-account
# sudo systemctl start hummingbird-container
# sudo systemctl start hummingbird-object
# url=`curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Storage-Url | cut -f2 -d ' ' | tr -d '\r\n'`
# token=`curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Auth-Token | tr -d '\r\n'`
# curl -i -X PUT $url/container -H "$token" ; echo
# curl -i -X PUT $url/container/object -H "$token" -T /usr/bin/hummingbird ; echo
# curl -i "$url/container?format=json" -H "$token" ; echo
# sudo find /srv/hummingbird

