package main

import (
	"fmt"
	"os"
	"os/user"
	"strings"
)

func main() {
	if err := makeHummingbird(os.Args[1:]); err != nil {
		panic(err)
	}
}

func makeHummingbird(args []string) error {
	haio := false
	if len(args) == 1 && args[0] == "haio" {
		haio = true
	}
	ip := "0.0.0.0"
	devices := "/srv"
	if haio {
		ip = "127.0.0.1"
		devices = "/srv/hb"
	}
	username := "hummingbird"
	groupname := "hummingbird"
	if haio {
		usr, err := user.Current()
		if err != nil {
			return err
		}
		username = usr.Username
		grpIDs, err := usr.GroupIds()
		if err != nil {
			return err
		}
		grp, err := user.LookupGroupId(grpIDs[0])
		if err != nil {
			return err
		}
		groupname = grp.Name
	}
	var err error
	if err = os.MkdirAll("build/etc/hummingbird", 0755); err != nil {
		return err
	}
	var f *os.File
	if f, err = os.Create("build/etc/hummingbird/hummingbird.conf"); err != nil {
		return err
	}
	if _, err = f.WriteString(hummingbird_conf); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if f, err = os.Create("build/etc/hummingbird/proxy-server.conf"); err != nil {
		return err
	}
	if _, err = f.WriteString(fmt.Sprintf(proxy_server_conf, ip, 8080)); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if haio {
		if err = os.MkdirAll("build/etc/hummingbird/account-server", 0755); err != nil {
			return err
		}
		for i := 1; i <= 4; i++ {
			if f, err = os.Create(fmt.Sprintf("build/etc/hummingbird/account-server/%d.conf", i)); err != nil {
				return err
			}
			if _, err = f.WriteString(fmt.Sprintf(account_server_conf, fmt.Sprintf("%s/%d", devices, i), ip, 6002+i*10)); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
		}
		if err = os.MkdirAll("build/etc/hummingbird/container-server", 0755); err != nil {
			return err
		}
		for i := 1; i <= 4; i++ {
			if f, err = os.Create(fmt.Sprintf("build/etc/hummingbird/container-server/%d.conf", i)); err != nil {
				return err
			}
			if _, err = f.WriteString(fmt.Sprintf(container_server_conf, fmt.Sprintf("%s/%d", devices, i), ip, 6001+i*10)); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
		}
		if err = os.MkdirAll("build/etc/hummingbird/object-server", 0755); err != nil {
			return err
		}
		for i := 1; i <= 4; i++ {
			if f, err = os.Create(fmt.Sprintf("build/etc/hummingbird/object-server/%d.conf", i)); err != nil {
				return err
			}
			if _, err = f.WriteString(fmt.Sprintf(object_server_conf, fmt.Sprintf("%s/%d", devices, i), ip, 6000+i*10, ip, 8000+i*10)); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
		}
	} else {
		if f, err = os.Create("build/etc/hummingbird/account-server.conf"); err != nil {
			return err
		}
		if _, err = f.WriteString(fmt.Sprintf(account_server_conf, devices, ip, 6012)); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		if f, err = os.Create("build/etc/hummingbird/container-server.conf"); err != nil {
			return err
		}
		if _, err = f.WriteString(fmt.Sprintf(container_server_conf, devices, ip, 6011)); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		if f, err = os.Create("build/etc/hummingbird/object-server.conf"); err != nil {
			return err
		}
		if _, err = f.WriteString(fmt.Sprintf(object_server_conf, devices, ip, 6010, ip, 8010)); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	if err = os.MkdirAll("build/lib/systemd/system", 0755); err != nil {
		return err
	}
	for _, name := range []string{"proxy", "andrewd"} {
		if f, err = os.Create(fmt.Sprintf("build/lib/systemd/system/hummingbird-%s.service", name)); err != nil {
			return err
		}
		if _, err = f.WriteString(fmt.Sprintf(service, name, username, groupname, name, "", name)); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	if haio {
		for _, name := range []string{"account", "account-replicator", "container", "container-replicator", "object", "object-replicator", "object-auditor"} {
			for i := 1; i <= 4; i++ {
				if f, err = os.Create(fmt.Sprintf("build/lib/systemd/system/hummingbird-%s%d.service", name, i)); err != nil {
					return err
				}
				ss := strings.SplitN(name, "-", 2)
				basename := name
				if len(ss) == 2 {
					basename = ss[0]
				}
				if _, err = f.WriteString(fmt.Sprintf(service, name, username, groupname, name, fmt.Sprintf(" -c /etc/hummingbird/%s-server/%d.conf", basename, i), name)); err != nil {
					return err
				}
				if err = f.Close(); err != nil {
					return err
				}
			}
		}
	} else {
		for _, name := range []string{"account", "account-replicator", "container", "container-replicator", "object", "object-replicator", "object-auditor"} {
			if f, err = os.Create(fmt.Sprintf("build/lib/systemd/system/hummingbird-%s.service", name)); err != nil {
				return err
			}
			if _, err = f.WriteString(fmt.Sprintf(service, name, username, groupname, name, name)); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
		}
	}
	if haio {
		if err = os.MkdirAll("build/usr/bin", 0755); err != nil {
			return err
		}
		for _, ss := range [][]string{
			{"hball", hball},
			{"hbmain", hbmain},
			{"hbreset", hbreset},
			{"hbrings", hbrings},
			{"hblog", hblog},
		} {
			if f, err = os.Create("build/usr/bin/" + ss[0]); err != nil {
				return err
			}
			if _, err = f.WriteString(ss[1]); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

var hummingbird_conf = `[swift-hash]
swift_hash_path_prefix = changeme
swift_hash_path_suffix = changeme

[storage-policy:0]
name = gold
policy_type = replication
default = yes

[storage-policy:1]
name = silver
policy_type = replication
`

var proxy_server_conf = `[DEFAULT]
bind_ip = %s
bind_port = %d
log_level = DEBUG

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
`

var account_server_conf = `[DEFAULT]
devices = %s
mount_check = false
bind_ip = %s
bind_port = %d

[app:account-server]

[account-replicator]
`

var container_server_conf = `[DEFAULT]
devices = %s
mount_check = false
bind_ip = %s
bind_port = %d

[app:container-server]

[container-replicator]
`

var object_server_conf = `[DEFAULT]
devices = %s
mount_check = false

[app:object-server]
bind_ip = %s
bind_port = %d

[object-replicator]
bind_ip = %s
bind_port = %d

[object-auditor]
`

// See these pages for lots of options:
// http://0pointer.de/public/systemd-man/systemd.service.html
// http://0pointer.de/public/systemd-man/systemd.exec.html
var service = `[Unit]
Description=hummingbird-%s
After=syslog.target network.target
[Service]
Type=simple
User=%s
Group=%s
ExecStart=/usr/bin/hummingbird %s%s
TimeoutStopSec=60
Restart=on-failure
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=hummingbird-%s
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
`

var hball = `#!/bin/bash

sudo systemctl $@ hummingbird-proxy
sudo systemctl $@ hummingbird-account1
sudo systemctl $@ hummingbird-account-replicator1
sudo systemctl $@ hummingbird-container1
sudo systemctl $@ hummingbird-container-replicator1
sudo systemctl $@ hummingbird-object1
sudo systemctl $@ hummingbird-object-replicator1
sudo systemctl $@ hummingbird-object-auditor1
sudo systemctl $@ hummingbird-account2
sudo systemctl $@ hummingbird-account-replicator2
sudo systemctl $@ hummingbird-container2
sudo systemctl $@ hummingbird-container-replicator2
sudo systemctl $@ hummingbird-object2
sudo systemctl $@ hummingbird-object-replicator2
sudo systemctl $@ hummingbird-object-auditor2
sudo systemctl $@ hummingbird-account3
sudo systemctl $@ hummingbird-account-replicator3
sudo systemctl $@ hummingbird-container3
sudo systemctl $@ hummingbird-container-replicator3
sudo systemctl $@ hummingbird-object3
sudo systemctl $@ hummingbird-object-replicator3
sudo systemctl $@ hummingbird-object-auditor3
sudo systemctl $@ hummingbird-account4
sudo systemctl $@ hummingbird-account-replicator4
sudo systemctl $@ hummingbird-container4
sudo systemctl $@ hummingbird-container-replicator4
sudo systemctl $@ hummingbird-object4
sudo systemctl $@ hummingbird-object-replicator4
sudo systemctl $@ hummingbird-object-auditor4
`

var hbmain = `#!/bin/bash

sudo systemctl $@ hummingbird-proxy
sudo systemctl $@ hummingbird-account1
sudo systemctl $@ hummingbird-container1
sudo systemctl $@ hummingbird-object1
sudo systemctl $@ hummingbird-account2
sudo systemctl $@ hummingbird-container2
sudo systemctl $@ hummingbird-object2
sudo systemctl $@ hummingbird-account3
sudo systemctl $@ hummingbird-container3
sudo systemctl $@ hummingbird-object3
sudo systemctl $@ hummingbird-account4
sudo systemctl $@ hummingbird-container4
sudo systemctl $@ hummingbird-object4
`

var hbreset = `#!/bin/bash

sudo systemctl stop hummingbird-proxy || /bin/true
sudo systemctl stop hummingbird-account1 || /bin/true
sudo systemctl stop hummingbird-account-replicator1 || /bin/true
sudo systemctl stop hummingbird-container1 || /bin/true
sudo systemctl stop hummingbird-container-replicator1 || /bin/true
sudo systemctl stop hummingbird-object1 || /bin/true
sudo systemctl stop hummingbird-object-replicator1 || /bin/true
sudo systemctl stop hummingbird-object-auditor1 || /bin/true
sudo systemctl stop hummingbird-account2 || /bin/true
sudo systemctl stop hummingbird-account-replicator2 || /bin/true
sudo systemctl stop hummingbird-container2 || /bin/true
sudo systemctl stop hummingbird-container-replicator2 || /bin/true
sudo systemctl stop hummingbird-object2 || /bin/true
sudo systemctl stop hummingbird-object-replicator2 || /bin/true
sudo systemctl stop hummingbird-object-auditor2 || /bin/true
sudo systemctl stop hummingbird-account3 || /bin/true
sudo systemctl stop hummingbird-account-replicator3 || /bin/true
sudo systemctl stop hummingbird-container3 || /bin/true
sudo systemctl stop hummingbird-container-replicator3 || /bin/true
sudo systemctl stop hummingbird-object3 || /bin/true
sudo systemctl stop hummingbird-object-replicator3 || /bin/true
sudo systemctl stop hummingbird-object-auditor3 || /bin/true
sudo systemctl stop hummingbird-account4 || /bin/true
sudo systemctl stop hummingbird-account-replicator4 || /bin/true
sudo systemctl stop hummingbird-container4 || /bin/true
sudo systemctl stop hummingbird-container-replicator4 || /bin/true
sudo systemctl stop hummingbird-object4 || /bin/true
sudo systemctl stop hummingbird-object-replicator4 || /bin/true
sudo systemctl stop hummingbird-object-auditor4 || /bin/true
sudo find /var/log/hummingbird /var/cache/swift -type f -exec rm -f {} \;
sudo umount -f /srv/hb || /bin/true
sudo mkdir -p /srv/hb
sudo truncate -s 5GB /srv/hb-disk
sudo mkfs.xfs -f /srv/hb-disk
sudo mount -o loop /srv/hb-disk /srv/hb
sudo mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 \
    /var/cache/swift4 /var/run/swift /srv/hb/1/sdb1 /srv/hb/2/sdb2 \
    /srv/hb/3/sdb3 /srv/hb/4/sdb4 /var/run/hummingbird /var/log/hummingbird
sudo chown -R "${USER}:${USER}" /var/run/hummingbird \
    /var/log/hummingbird /var/cache/swift* /srv/*
sudo systemctl restart memcached
`

var hbrings = `#!/bin/bash
set -e

cd /etc/hummingbird

rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

hummingbird ring object.builder create 10 3 1
hummingbird ring object.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1
hummingbird ring object.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1
hummingbird ring object.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1
hummingbird ring object.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1
hummingbird ring object.builder rebalance

hummingbird ring object-1.builder create 10 2 1
hummingbird ring object-1.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1
hummingbird ring object-1.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1
hummingbird ring object-1.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1
hummingbird ring object-1.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1
hummingbird ring object-1.builder rebalance

hummingbird ring object-2.builder create 10 6 1
hummingbird ring object-2.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1
hummingbird ring object-2.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb5 1
hummingbird ring object-2.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1
hummingbird ring object-2.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb6 1
hummingbird ring object-2.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1
hummingbird ring object-2.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb7 1
hummingbird ring object-2.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1
hummingbird ring object-2.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb8 1
hummingbird ring object-2.builder rebalance

hummingbird ring container.builder create 10 3 1
hummingbird ring container.builder add r1z1-127.0.0.1:6011/sdb1 1
hummingbird ring container.builder add r1z2-127.0.0.1:6021/sdb2 1
hummingbird ring container.builder add r1z3-127.0.0.1:6031/sdb3 1
hummingbird ring container.builder add r1z4-127.0.0.1:6041/sdb4 1
hummingbird ring container.builder rebalance

hummingbird ring account.builder create 10 3 1
hummingbird ring account.builder add r1z1-127.0.0.1:6012/sdb1 1
hummingbird ring account.builder add r1z2-127.0.0.1:6022/sdb2 1
hummingbird ring account.builder add r1z3-127.0.0.1:6032/sdb3 1
hummingbird ring account.builder add r1z4-127.0.0.1:6042/sdb4 1
hummingbird ring account.builder rebalance
`

var hblog = `#!/bin/bash

journalctl -u hummingbird-$@
`
