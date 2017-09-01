package main

import (
	"fmt"
	"os"
	"os/user"

	"github.com/troubling/hummingbird/common"
)

func initCommand(args []string) error {
	subcmd := ""
	if len(args) > 0 {
		subcmd, args = args[0], args[1:]
		if subcmd != "debian" && subcmd != "haio" {
			return fmt.Errorf("unknown subcommand: %q", args[0])
		}
	}
	versionNoV := common.Version
	if versionNoV == "" {
		versionNoV = "0.0.1"
	}
	if versionNoV[0] == 'v' {
		versionNoV = versionNoV[1:]
	}
	username := "hummingbird"
	groupname := "hummingbird"
	if subcmd == "haio" {
		usr, err := user.Current()
		if err != nil {
			return err
		}
		username = usr.Username
		grp, err := user.LookupGroupId(usr.Gid)
		if err != nil {
			return err
		}
		groupname = grp.Name
	}
	scriptName := "hummingbird-init"
	if subcmd != "" {
		scriptName += "-" + subcmd
	}
	scriptName += ".sh"
	script, err := os.Create(scriptName)
	if err != nil {
		return nil
	}
	var printErr error
	// This just lets us run through creating the script, ignoring errors, and
	// then we can return the first error encountered, if any.
	print := func(format string, args ...interface{}) {
		_, innerErr := fmt.Fprintf(script, format+"\n", args...)
		if innerErr != nil && printErr == nil {
			printErr = innerErr
		}
	}

	print(`#!/bin/bash`)
	print(`set -e`)
	print(`set -x`)
	print(``)

	var prefix string
	switch subcmd {
	case "":
		print(`sudo adduser --system --group --no-create-home hummingbird`)
		print(`sudo apt install -y memcached`)
	case "haio":
		print(`sudo apt install -y gcc git-core make memcached sqlite3 tar wget xfsprogs`)
		print(`wget -nc https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz`)
		print(`sudo tar -C /usr/local -xzf go1.9.linux-amd64.tar.gz`)
		print(`export PATH=$PATH:/usr/local/go/bin`)
		print(`grep -q /usr/local/go/bin /etc/profile || echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee -a /etc/profile`)
		print(`echo "Retrieving sources; this may take a while..."`)
		print(`/usr/local/go/bin/go get -t github.com/troubling/hummingbird/...`)
		print(`cd ~/go/src/github.com/troubling/hummingbird`)
		print(`go build -o nectar github.com/troubling/hummingbird/cmd/nectar/...`)
		print(`sudo cp nectar /usr/bin/`)
		print(`sudo chown root: /usr/bin/nectar`)
		print(`sudo chmod 0755 /usr/bin/nectar`)
		print(`cd -`)
	case "debian":
		prefix = "build"
		print(`if [ -e build ] ; then`)
		print(`    echo '"build" exists already; that would be our working directory.'`)
		print(`    exit 1`)
		print(`fi`)
		print(`mkdir build`)
		print(`sudo apt install -y binutils`)
	}
	print(``)

	print(`sudo rm -f %s/usr/bin/hummingbird`, prefix)
	print(`sudo mkdir -p %s/usr/bin`, prefix)
	print(`sudo cp hummingbird %s/usr/bin/`, prefix)
	print(`sudo chown root: %s/usr/bin/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/usr/bin/hummingbird`, prefix)
	print(``)

	print(`sudo mkdir -p %s/etc/hummingbird`, prefix)
	if subcmd != "debian" {
		print(`sudo chown %s: %s/etc/hummingbird`, username, prefix)
	}
	print(`sudo chmod 0755 %s/etc/hummingbird`, prefix)
	print(``)

	print(`sudo tee %s/etc/hummingbird/hummingbird.conf >/dev/null << EOF`, prefix)
	print(`[swift-hash]`)
	print(`swift_hash_path_prefix = changeme`)
	print(`swift_hash_path_suffix = changeme`)
	print(``)
	print(`[storage-policy:0]`)
	print(`name = gold`)
	print(`policy_type = replication`)
	print(`default = yes`)
	print(``)
	print(`[storage-policy:1]`)
	print(`name = silver`)
	print(`policy_type = replication`)
	print(`EOF`)
	if subcmd != "debian" {
		print(`sudo chown %s: %s/etc/hummingbird/hummingbird.conf`, username, prefix)
	}
	print(`sudo chmod 0644 %s/etc/hummingbird/hummingbird.conf`, prefix)
	print(``)

	print(`sudo tee %s/etc/hummingbird/proxy-server.conf >/dev/null << EOF`, prefix)
	print(`[DEFAULT]`)
	print(`bind_ip = 127.0.0.1`)
	print(`bind_port = 8080`)
	print(``)
	print(`[app:proxy-server]`)
	print(`allow_account_management = true`)
	print(`account_autocreate = true`)
	print(``)
	print(`[filter:tempauth]`)
	print(`user_admin_admin = admin .admin .reseller_admin`)
	print(`user_test_tester = testing .admin`)
	print(`user_test2_tester2 = testing2 .admin`)
	print(`user_test_tester3 = testing3`)
	print(``)
	print(`[filter:catch_errors]`)
	print(``)
	print(`[filter:healthcheck]`)
	print(``)
	print(`[filter:proxy-logging]`)
	print(``)
	print(`[filter:ratelimit]`)
	print(``)
	print(`[filter:dlo]`)
	print(``)
	print(`[filter:slo]`)
	print(``)
	print(`[filter:tempurl]`)
	print(``)
	print(`[filter:staticweb]`)
	print(``)
	print(`[filter:copy]`)
	print(`EOF`)
	if subcmd != "debian" {
		print(`sudo chown %s: %s/etc/hummingbird/proxy-server.conf`, username, prefix)
	}
	print(`sudo chmod 0644 %s/etc/hummingbird/proxy-server.conf`, prefix)
	print(``)

	printAccountServerConf := func(index int) {
		var pth string
		var devices string
		var port int
		if index < 1 {
			pth = prefix + "/etc/hummingbird/account-server.conf"
			devices = "/srv/hummingbird"
			port = 6012
		} else {
			print(`sudo mkdir -p %s/etc/hummingbird/account-server`, prefix)
			if subcmd != "debian" {
				print(`sudo chown %s: %s/etc/hummingbird/account-server`, username, prefix)
			}
			print(`sudo chmod 0755 %s/etc/hummingbird/account-server`, prefix)
			pth = fmt.Sprintf("%s/etc/hummingbird/account-server/%d.conf", prefix, index)
			devices = fmt.Sprintf("/srv/hb/%d", index)
			port = 6002 + index*10
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`[DEFAULT]`)
		print(`devices = %s`, devices)
		print(`mount_check = false`)
		print(`bind_ip = 127.0.0.1`)
		print(`bind_port = %d`, port)
		print(``)
		print(`[app:account-server]`)
		print(``)
		print(`[account-replicator]`)
		print(`EOF`)
		if subcmd != "debian" {
			print(`sudo chown %s: %s`, username, pth)
		}
		print(`sudo chmod 0644 %s`, pth)
		print(``)
	}
	printContainerServerConf := func(index int) {
		var pth string
		var devices string
		var port int
		if index < 1 {
			pth = prefix + "/etc/hummingbird/container-server.conf"
			devices = "/srv/hummingbird"
			port = 6011
		} else {
			print(`sudo mkdir -p %s/etc/hummingbird/container-server`, prefix)
			if subcmd != "debian" {
				print(`sudo chown %s: %s/etc/hummingbird/container-server`, username, prefix)
			}
			print(`sudo chmod 0755 %s/etc/hummingbird/container-server`, prefix)
			pth = fmt.Sprintf("%s/etc/hummingbird/container-server/%d.conf", prefix, index)
			devices = fmt.Sprintf("/srv/hb/%d", index)
			port = 6001 + index*10
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`[DEFAULT]`)
		print(`devices = %s`, devices)
		print(`mount_check = false`)
		print(`bind_ip = 127.0.0.1`)
		print(`bind_port = %d`, port)
		print(``)
		print(`[app:container-server]`)
		print(``)
		print(`[container-replicator]`)
		print(`EOF`)
		if subcmd != "debian" {
			print(`sudo chown %s: %s`, username, pth)
		}
		print(`sudo chmod 0644 %s`, pth)
		print(``)
	}
	printObjectServerConf := func(index int) {
		var pth string
		var devices string
		var port int
		if index < 1 {
			pth = prefix + "/etc/hummingbird/object-server.conf"
			devices = "/srv/hummingbird"
			port = 6010
		} else {
			print(`sudo mkdir -p %s/etc/hummingbird/object-server`, prefix)
			if subcmd != "debian" {
				print(`sudo chown %s: %s/etc/hummingbird/object-server`, username, prefix)
			}
			print(`sudo chmod 0755 %s/etc/hummingbird/object-server`, prefix)
			pth = fmt.Sprintf("%s/etc/hummingbird/object-server/%d.conf", prefix, index)
			devices = fmt.Sprintf("/srv/hb/%d", index)
			port = 6000 + index*10
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`[DEFAULT]`)
		print(`devices = %s`, devices)
		print(`mount_check = false`)
		print(``)
		print(`[app:object-server]`)
		print(`bind_ip = 127.0.0.1`)
		print(`bind_port = %d`, port)
		print(``)
		print(`[object-replicator]`)
		print(`bind_ip = 127.0.0.1`)
		print(`bind_port = %d`, port+2000)
		print(``)
		print(`[object-auditor]`)
		print(`EOF`)
		if subcmd != "debian" {
			print(`sudo chown %s: %s`, username, pth)
		}
		print(`sudo chmod 0644 %s`, pth)
		print(``)
	}
	start := 0
	stop := 0
	if subcmd == "haio" {
		start = 1
		stop = 4
	}
	for index := start; index <= stop; index++ {
		printAccountServerConf(index)
		printContainerServerConf(index)
		printObjectServerConf(index)
	}

	printService := func(basename string, index int) {
		var pth string
		var extraArgs string
		if index < 1 {
			pth = fmt.Sprintf(prefix+"/lib/systemd/system/hummingbird-%s.service", basename)
		} else {
			pth = fmt.Sprintf(prefix+"/lib/systemd/system/hummingbird-%s%d.service", basename, index)
			extraArgs = fmt.Sprintf(" -c /etc/hummingbird/%s-server/%d.conf", basename, index)
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`# See these pages for lots of options:`)
		print(`# http://0pointer.de/public/systemd-man/systemd.service.html`)
		print(`# http://0pointer.de/public/systemd-man/systemd.exec.html`)
		print(`[Unit]`)
		print(`Description=hummingbird-%s`, basename)
		print(`After=syslog.target network.target`)
		print(`[Service]`)
		print(`Type=simple`)
		print(`User=%s`, username)
		print(`Group=%s`, groupname)
		print(`ExecStart=/usr/bin/hummingbird %s%s`, basename, extraArgs)
		print(`TimeoutStopSec=60`)
		print(`Restart=on-failure`)
		print(`RestartSec=5`)
		print(`StandardOutput=syslog`)
		print(`StandardError=syslog`)
		print(`SyslogIdentifier=hummingbird-%s`, basename)
		print(`LimitCPU=infinity`)
		print(`LimitFSIZE=infinity`)
		print(`LimitDATA=infinity`)
		print(`LimitSTACK=infinity`)
		print(`LimitCORE=infinity`)
		print(`LimitRSS=infinity`)
		print(`LimitNOFILE=1048576`)
		print(`LimitAS=infinity`)
		print(`LimitNPROC=infinity`)
		print(`LimitMEMLOCK=infinity`)
		print(`LimitLOCKS=infinity`)
		print(`LimitSIGPENDING=infinity`)
		print(`LimitMSGQUEUE=infinity`)
		print(`LimitNICE=infinity`)
		print(`LimitRTPRIO=infinity`)
		print(`LimitRTTIME=infinity`)
		print(`[Install]`)
		print(`WantedBy=multi-user.target`)
		print(`EOF`)
		print(`sudo chmod 0644 %s`, pth)
		print(``)
	}
	print(`sudo mkdir -p %s/lib/systemd/system`, prefix)
	printService("proxy", 0)
	start = 0
	stop = 0
	if subcmd == "haio" {
		start = 1
		stop = 4
	}
	for index := start; index <= stop; index++ {
		printService("account", index)
		printService("account-replicator", index)
		printService("container", index)
		printService("container-replicator", index)
		printService("object", index)
		printService("object-replicator", index)
		printService("object-auditor", index)
	}
	printService("andrewd", 0)

	if subcmd == "haio" {
		print(`sudo tee %s/usr/bin/hball >/dev/null << EOF`, prefix)
		print(`#!/bin/bash`)
		print(``)
		print(`if hash systemctl 2>/dev/null ; then`)
		print(`    sudo systemctl \$@ hummingbird-proxy`)
		print(`    sudo systemctl \$@ hummingbird-account1`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator1`)
		print(`    sudo systemctl \$@ hummingbird-container1`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator1`)
		print(`    sudo systemctl \$@ hummingbird-object1`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator1`)
		print(`    sudo systemctl \$@ hummingbird-object-auditor1`)
		print(`    sudo systemctl \$@ hummingbird-account2`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator2`)
		print(`    sudo systemctl \$@ hummingbird-container2`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator2`)
		print(`    sudo systemctl \$@ hummingbird-object2`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator2`)
		print(`    sudo systemctl \$@ hummingbird-object-auditor2`)
		print(`    sudo systemctl \$@ hummingbird-account3`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator3`)
		print(`    sudo systemctl \$@ hummingbird-container3`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator3`)
		print(`    sudo systemctl \$@ hummingbird-object3`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator3`)
		print(`    sudo systemctl \$@ hummingbird-object-auditor3`)
		print(`    sudo systemctl \$@ hummingbird-account4`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator4`)
		print(`    sudo systemctl \$@ hummingbird-container4`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator4`)
		print(`    sudo systemctl \$@ hummingbird-object4`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator4`)
		print(`    sudo systemctl \$@ hummingbird-object-auditor4`)
		print(`else`)
		print(`    hummingbird \$@ all`)
		print(`fi`)
		print(`EOF`)
		print(`sudo chmod 0755 %s/usr/bin/hball`, prefix)
		print(``)

		print(`sudo tee %s/usr/bin/hbmain >/dev/null << EOF`, prefix)
		print(`#!/bin/bash`)
		print(``)
		print(`if hash systemctl 2>/dev/null ; then`)
		print(`    sudo systemctl \$@ hummingbird-proxy`)
		print(`    sudo systemctl \$@ hummingbird-account1`)
		print(`    sudo systemctl \$@ hummingbird-container1`)
		print(`    sudo systemctl \$@ hummingbird-object1`)
		print(`    sudo systemctl \$@ hummingbird-account2`)
		print(`    sudo systemctl \$@ hummingbird-container2`)
		print(`    sudo systemctl \$@ hummingbird-object2`)
		print(`    sudo systemctl \$@ hummingbird-account3`)
		print(`    sudo systemctl \$@ hummingbird-container3`)
		print(`    sudo systemctl \$@ hummingbird-object3`)
		print(`    sudo systemctl \$@ hummingbird-account4`)
		print(`    sudo systemctl \$@ hummingbird-container4`)
		print(`    sudo systemctl \$@ hummingbird-object4`)
		print(`else`)
		print(`    hummingbird \$@ main`)
		print(`fi`)
		print(`EOF`)
		print(`sudo chmod 0755 %s/usr/bin/hbmain`, prefix)
		print(``)

		print(`sudo tee %s/usr/bin/hbrings >/dev/null << EOF`, prefix)
		print(`#!/bin/bash`)
		print(`set -e`)
		print(``)
		print(`cd /etc/hummingbird`)
		print(``)
		print(`rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz`)
		print(``)
		print(`hummingbird ring object.builder create 10 3 1`)
		print(`hummingbird ring object.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1`)
		print(`hummingbird ring object.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1`)
		print(`hummingbird ring object.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1`)
		print(`hummingbird ring object.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1`)
		print(`hummingbird ring object.builder rebalance`)
		print(``)
		print(`hummingbird ring object-1.builder create 10 2 1`)
		print(`hummingbird ring object-1.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1`)
		print(`hummingbird ring object-1.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1`)
		print(`hummingbird ring object-1.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1`)
		print(`hummingbird ring object-1.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1`)
		print(`hummingbird ring object-1.builder rebalance`)
		print(``)
		print(`hummingbird ring object-2.builder create 10 6 1`)
		print(`hummingbird ring object-2.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb1 1`)
		print(`hummingbird ring object-2.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/sdb5 1`)
		print(`hummingbird ring object-2.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb2 1`)
		print(`hummingbird ring object-2.builder add r1z2-127.0.0.1:6020R127.0.0.1:8020/sdb6 1`)
		print(`hummingbird ring object-2.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb3 1`)
		print(`hummingbird ring object-2.builder add r1z3-127.0.0.1:6030R127.0.0.1:8030/sdb7 1`)
		print(`hummingbird ring object-2.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb4 1`)
		print(`hummingbird ring object-2.builder add r1z4-127.0.0.1:6040R127.0.0.1:8040/sdb8 1`)
		print(`hummingbird ring object-2.builder rebalance`)
		print(``)
		print(`hummingbird ring container.builder create 10 3 1`)
		print(`hummingbird ring container.builder add r1z1-127.0.0.1:6011/sdb1 1`)
		print(`hummingbird ring container.builder add r1z2-127.0.0.1:6021/sdb2 1`)
		print(`hummingbird ring container.builder add r1z3-127.0.0.1:6031/sdb3 1`)
		print(`hummingbird ring container.builder add r1z4-127.0.0.1:6041/sdb4 1`)
		print(`hummingbird ring container.builder rebalance`)
		print(``)
		print(`hummingbird ring account.builder create 10 3 1`)
		print(`hummingbird ring account.builder add r1z1-127.0.0.1:6012/sdb1 1`)
		print(`hummingbird ring account.builder add r1z2-127.0.0.1:6022/sdb2 1`)
		print(`hummingbird ring account.builder add r1z3-127.0.0.1:6032/sdb3 1`)
		print(`hummingbird ring account.builder add r1z4-127.0.0.1:6042/sdb4 1`)
		print(`hummingbird ring account.builder rebalance`)
		print(`EOF`)
		print(`sudo chmod 0755 %s/usr/bin/hbrings`, prefix)
		print(``)

		print(`sudo tee %s/usr/bin/hbreset >/dev/null << EOF`, prefix)
		print(`#!/bin/bash`)
		print(``)
		print(`if hash systemctl 2>/dev/null ; then`)
		print(`    sudo systemctl stop hummingbird-proxy || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account-replicator1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container-replicator1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-replicator1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-auditor1 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account-replicator2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container-replicator2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-replicator2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-auditor2 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account-replicator3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container-replicator3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-replicator3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-auditor3 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account4 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-account-replicator4 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container4 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-container-replicator4 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object4 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-replicator4 || /bin/true`)
		print(`    sudo systemctl stop hummingbird-object-auditor4 || /bin/true`)
		print(`else`)
		print(`    hummingbird stop all`)
		print(`fi`)
		print(`sudo find /var/log/hummingbird /var/cache/swift -type f -exec rm -f {} \;`)
		print(`sudo mountpoint -q /srv/hb && sudo umount -f /srv/hb || /bin/true`)
		print(`sudo mkdir -p /srv/hb`)
		print(`sudo truncate -s 5GB /srv/hb-disk`)
		print(`sudo mkfs.xfs -f /srv/hb-disk`)
		print(`sudo mount -o loop /srv/hb-disk /srv/hb`)
		print(`sudo mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4 /var/run/swift /srv/hb/1/sdb1 /srv/hb/2/sdb2 /srv/hb/3/sdb3 /srv/hb/4/sdb4 /var/run/hummingbird /var/log/hummingbird`)
		print(`sudo chown -R ${USER}: /var/run/hummingbird /var/log/hummingbird /var/cache/swift* /srv/hb*`)
		print(`if hash systemctl 2>/dev/null ; then`)
		print(`    sudo systemctl restart memcached`)
		print(`else`)
		print(`    sudo service memcached restart`)
		print(`fi`)
		print(`EOF`)
		print(`sudo chmod 0755 %s/usr/bin/hbreset`, prefix)
		print(``)

		print(`sudo tee %s/usr/bin/hblog >/dev/null << EOF`, prefix)
		print(`#!/bin/bash`)
		print(``)
		print(`if hash journalctl 2>/dev/null ; then`)
		print(`    journalctl -u hummingbird-\$@`)
		print(`else`)
		print(`    echo "Unsure how to view your logs"`)
		print(`    exit 1`)
		print(`fi`)
		print(`EOF`)
		print(`sudo chmod 0755 %s/usr/bin/hblog`, prefix)
		print(``)
	}

	print(`sudo mkdir -p %s/var/run/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/var/run/hummingbird`, prefix)
	print(`sudo mkdir -p %s/var/log/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/var/log/hummingbird`, prefix)
	print(`sudo mkdir -p %s/var/cache/swift`, prefix)
	if subcmd != "debian" {
		print(`sudo chown %s: %s/var/cache/swift`, username, prefix)
	}
	print(`sudo chmod 0755 %s/var/cache/swift`, prefix)
	print(``)

	if subcmd != "haio" {
		print(`sudo mkdir -p %s/srv/hummingbird`, prefix)
		if subcmd != "debian" {
			print(`sudo chown %s: %s/srv/hummingbird`, username, prefix)
		}
		print(`sudo chmod 0755 %s/srv/hummingbird`, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/account.builder create 10 1 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/account.builder add r1z1-127.0.0.1:6012/hummingbird 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/account.builder rebalance`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/container.builder create 10 1 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/container.builder add r1z1-127.0.0.1:6011/hummingbird 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/container.builder rebalance`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/object.builder create 10 1 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/object.builder add r1z1-127.0.0.1:6010R127.0.0.1:8010/hummingbird 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/object.builder rebalance`, prefix, prefix)
		print(`sudo rm -rf %s/etc/hummingbird/backups`, prefix)
		if subcmd != "debian" {
			print(`sudo chown %s: %s/etc/hummingbird/*.{builder,ring.gz}`, username, prefix)
		}
		print(`sudo chmod 0644 %s/etc/hummingbird/*.{builder,ring.gz}`, prefix)
		print(``)
	}

	if subcmd == "debian" {
		// Started this from https://medium.com/@newhouseb/hassle-free-go-in-production-528af8ee1a58
		print(`echo 2.0 > %s/debian-binary`, prefix)
		print(``)

		print(`cat > %s/control << EOF`, prefix)
		print(`Package: hummingbird`)
		print(`Version: %s`, versionNoV)
		print(`Section: net`)
		print(`Priority: optional`)
		print(`Architecture: amd64`)
		print(`Depends: adduser, memcached`)
		print(`Maintainer: Rackspace <gholt@rackspace.com>`)
		print(`Description: Hummingbird Object Storage Software`)
		print(`EOF`)
		print(``)

		print(`cat > %s/postinst << EOF`, prefix)
		print(`#!/bin/sh`)
		print(``)
		print(`adduser --system --group --no-create-home hummingbird`)
		print(`chown hummingbird: /srv/hummingbird`)
		print(`EOF`)
		print(``)

		print(`cd %s`, prefix)
		print(`tar czf control.tar.gz control postinst`)
		print(`sudo chown -R root: etc lib srv usr var`)
		print(`sudo find etc lib srv usr var -type d -exec chmod 0755 {} \;`)
		print(`sudo find etc lib srv usr var -type f -exec chmod 0644 {} \;`)
		print(`sudo find usr/bin -type f -exec chmod 0755 {} \;`)
		print(`tar czf data.tar.gz etc lib srv usr var`)
		print(`ar rc hummingbird-%s.deb debian-binary control.tar.gz data.tar.gz`, versionNoV)
		print(`mv hummingbird-%s.deb ../`, versionNoV)
		print(`cd -`)
		print(`sudo rm -rf build`) // build type here instead of %s, prefix for safety
		print(``)
	}

	if subcmd == "haio" {
		print(`sudo systemctl daemon-reload`)
		print(`hbrings`)
		print(`hbreset`)
		print(`hbmain start`)
		print(`/usr/bin/nectar -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing head`)
		print(`git clone --depth 1 https://github.com/openstack/swift ~/swift`)
		print(`sudo mkdir -p /etc/swift`)
		print(`sudo chown %s: /etc/swift`, username)
		print(`cp ~/swift/test/sample.conf /etc/swift/test.conf`)
		print(`sudo apt install -y python-eventlet python-mock python-netifaces python-nose python-pastedeploy python-pbr python-pyeclib python-setuptools python-swiftclient python-unittest2 python-xattr`)
		print(`cd ~/swift/test/functional`)
		print(`nosetests || /bin/true # This always fails because we don't pass all of Swift's tests.`)
		print(`cd -`)
		print(`hbmain stop`)
		print(`cd ~/go/src/github.com/troubling/hummingbird`)
		print(`git remote rename origin upstream`)
		print(`git remote add origin git@github.com:%s/hummingbird`, username)
		print(`cd -`)
		print(``)
	}

	switch subcmd {
	case "debian":
		print(`tail -3 $0 | cut -f2- -d ' '

# hummingbird-%s.deb has been built.
`, versionNoV)
	case "haio":
		print(`tail -9 $0 | cut -f2- -d ' '

# HAIO is ready for use.
# 
# You probably want relogin or run:
# 
# source /etc/profile
# 
# See https://github.com/troubling/hummingbird/blob/master/HAIO.md for more info.
`)
	default:
		print(`tail -17 $0 | cut -f2- -d ' '

# Hummingbird is ready for use.
# 
# For quick local testing:
# 
# sudo systemctl start memcached
# sudo systemctl start hummingbird-proxy
# sudo systemctl start hummingbird-account
# sudo systemctl start hummingbird-container
# sudo systemctl start hummingbird-object
# url=` + "`" + `curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Storage-Url | cut -f2 -d ' ' | tr -d '\r\n'` + "`" + `
# token=` + "`" + `curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Auth-Token | tr -d '\r\n'` + "`" + `
# curl -i -X PUT $url/container -H "$token" ; echo
# curl -i -X PUT $url/container/object -H "$token" -T /usr/bin/hummingbird ; echo
# curl -i "$url/container?format=json" -H "$token" ; echo
# sudo find /srv/hummingbird
`)
	}

	err = script.Close()
	if printErr != nil {
		return printErr
	}
	if err != nil {
		return err
	}
	if err = os.Chmod(scriptName, 0755); err != nil {
		return err
	}
	fmt.Println("Created", scriptName)
	return nil
}
