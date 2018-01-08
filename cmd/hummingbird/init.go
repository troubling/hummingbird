package main

import (
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/troubling/hummingbird/common"
)

func initCommand(args []string) error {
	subcmd := ""
	if len(args) > 0 {
		subcmd, args = args[0], args[1:]
		if subcmd != "deb" && subcmd != "haio" {
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
		print(`go get -t github.com/troubling/hummingbird/...`)
		print(`gopath=` + "`" + `go env | grep ^GOPATH= | cut -d '"' -f2` + "`")
		print(`pushd "${gopath}/src/github.com/troubling/hummingbird"`)
		print(`make haio`)
		print(`popd`)
	case "deb":
		prefix = "build"
		print(`if [ -e build ] ; then`)
		print(`    echo '"build" exists already; that would be our working directory.'`)
		print(`    exit 1`)
		print(`fi`)
		print(`mkdir build`)
		print(`sudo chmod 0755 build`)
		print(`sudo apt install -y binutils`)
	}
	print(``)

	print(`sudo rm -f %s/usr/bin/hummingbird`, prefix)
	print(`sudo mkdir -p %s/usr/bin`, prefix)
	print(`sudo chmod 0755 %s/usr`, prefix)
	print(`sudo chmod 0755 %s/usr/bin`, prefix)
	print(`if [ -e hummingbird ] ; then`)
	print(`    sudo cp hummingbird %s/usr/bin/`, prefix)
	print(`else`)
	print(`    sudo cp bin/hummingbird %s/usr/bin/`, prefix)
	print(`fi`)
	print(`sudo chown root: %s/usr/bin/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/usr/bin/hummingbird`, prefix)
	print(``)

	print(`sudo mkdir -p %s/etc/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/etc`, prefix)
	print(`sudo chmod 0755 %s/etc/hummingbird`, prefix)
	if subcmd != "deb" {
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
	if subcmd != "deb" {
		print(`sudo chown %s: %s/etc/hummingbird/hummingbird.conf`, username, prefix)
	}
	print(`sudo chmod 0644 %s/etc/hummingbird/hummingbird.conf`, prefix)
	print(``)

	print(`sudo tee %s/etc/hummingbird/proxy-server.conf >/dev/null << EOF`, prefix)
	print(`[DEFAULT]`)
	print(`bind_ip = 127.0.0.1`)
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
	if subcmd != "deb" {
		print(`sudo chown %s: %s/etc/hummingbird/proxy-server.conf`, username, prefix)
	}
	print(`sudo chmod 0644 %s/etc/hummingbird/proxy-server.conf`, prefix)
	print(``)

	printAccountServerConf := func(index int) {
		var pth string
		var devices string
		var port int
		var repport int
		if index < 1 {
			pth = prefix + "/etc/hummingbird/account-server.conf"
			devices = "/srv/hummingbird"
		} else {
			print(`sudo mkdir -p %s/etc/hummingbird/account-server`, prefix)
			print(`sudo chmod 0755 %s/etc`, prefix)
			print(`sudo chmod 0755 %s/etc/hummingbird`, prefix)
			print(`sudo chmod 0755 %s/etc/hummingbird/account-server`, prefix)
			if subcmd != "deb" {
				print(`sudo chown %s: %s/etc/hummingbird/account-server`, username, prefix)
			}
			print(`sudo chmod 0755 %s/etc/hummingbird/account-server`, prefix)
			pth = fmt.Sprintf("%s/etc/hummingbird/account-server/%d.conf", prefix, index)
			devices = fmt.Sprintf("/srv/hb/%d", index)
			port = common.DefaultAccountServerPort + index*10
			repport = common.DefaultAccountReplicatorPort + index*10
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`[DEFAULT]`)
		print(`devices = %s`, devices)
		print(`mount_check = false`)
		print(`bind_ip = 127.0.0.1`)
		if port != 0 {
			print(`bind_port = %d`, port)
		}
		print(``)
		print(`[app:account-server]`)
		print(`disk_limit = 0/0`)
		print(``)
		print(`[account-replicator]`)
		if repport != 0 {
			print(`bind_port = %d`, repport)
		}
		print(`EOF`)
		if subcmd != "deb" {
			print(`sudo chown %s: %s`, username, pth)
		}
		print(`sudo chmod 0644 %s`, pth)
		print(``)
	}
	printContainerServerConf := func(index int) {
		var pth string
		var devices string
		var port int
		var repport int
		if index < 1 {
			pth = prefix + "/etc/hummingbird/container-server.conf"
			devices = "/srv/hummingbird"
		} else {
			print(`sudo mkdir -p %s/etc/hummingbird/container-server`, prefix)
			print(`sudo chmod 0755 %s/etc`, prefix)
			print(`sudo chmod 0755 %s/etc/hummingbird`, prefix)
			print(`sudo chmod 0755 %s/etc/hummingbird/container-server`, prefix)
			if subcmd != "deb" {
				print(`sudo chown %s: %s/etc/hummingbird/container-server`, username, prefix)
			}
			print(`sudo chmod 0755 %s/etc/hummingbird/container-server`, prefix)
			pth = fmt.Sprintf("%s/etc/hummingbird/container-server/%d.conf", prefix, index)
			devices = fmt.Sprintf("/srv/hb/%d", index)
			port = common.DefaultContainerServerPort + index*10
			repport = common.DefaultContainerReplicatorPort + index*10
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`[DEFAULT]`)
		print(`devices = %s`, devices)
		print(`mount_check = false`)
		print(`bind_ip = 127.0.0.1`)
		if port != 0 {
			print(`bind_port = %d`, port)
		}
		print(``)
		print(`[app:container-server]`)
		print(`disk_limit = 0/0`)
		print(``)
		print(`[container-replicator]`)
		if repport != 0 {
			print(`bind_port = %d`, repport)
		}
		print(`EOF`)
		if subcmd != "deb" {
			print(`sudo chown %s: %s`, username, pth)
		}
		print(`sudo chmod 0644 %s`, pth)
		print(``)
	}
	printObjectServerConf := func(index int) {
		var pth string
		var devices string
		var port int
		var repport int
		if index < 1 {
			pth = prefix + "/etc/hummingbird/object-server.conf"
			devices = "/srv/hummingbird"
		} else {
			print(`sudo mkdir -p %s/etc/hummingbird/object-server`, prefix)
			print(`sudo chmod 0755 %s/etc`, prefix)
			print(`sudo chmod 0755 %s/etc/hummingbird`, prefix)
			print(`sudo chmod 0755 %s/etc/hummingbird/object-server`, prefix)
			if subcmd != "deb" {
				print(`sudo chown %s: %s/etc/hummingbird/object-server`, username, prefix)
			}
			print(`sudo chmod 0755 %s/etc/hummingbird/object-server`, prefix)
			pth = fmt.Sprintf("%s/etc/hummingbird/object-server/%d.conf", prefix, index)
			devices = fmt.Sprintf("/srv/hb/%d", index)
			port = common.DefaultObjectServerPort + index*10
			repport = common.DefaultObjectReplicatorPort + index*10
		}
		print(`sudo tee %s >/dev/null << EOF`, pth)
		print(`[DEFAULT]`)
		print(`devices = %s`, devices)
		print(`mount_check = false`)
		print(``)
		print(`[app:object-server]`)
		print(`bind_ip = 127.0.0.1`)
		if port != 0 {
			print(`bind_port = %d`, port)
		}
		print(`disk_limit = 0/0`)
		print(`account_rate_limit = 0/0`)
		print(``)
		print(`[object-replicator]`)
		if repport != 0 {
			print(`bind_port = %d`, repport)
		}
		print(``)
		print(`[object-auditor]`)
		print(`EOF`)
		if subcmd != "deb" {
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
		servername := basename
		parts := strings.SplitN(basename, "-", 2)
		if len(parts) == 2 {
			servername = parts[0]
		}
		var pth string
		var extraArgs string
		if index < 1 {
			pth = fmt.Sprintf(prefix+"/lib/systemd/system/hummingbird-%s.service", basename)
		} else {
			pth = fmt.Sprintf(prefix+"/lib/systemd/system/hummingbird-%s%d.service", basename, index)
			extraArgs = fmt.Sprintf(" -c /etc/hummingbird/%s-server/%d.conf", servername, index)
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
		print(`ExecStart=/usr/bin/hummingbird systemd start %s%s`, basename, extraArgs)
		print(`ExecReload=/usr/bin/hummingbird systemd reload \$MAINPID`)
		print(`ExecStop=/usr/bin/hummingbird systemd stop \$MAINPID`)
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
	print(`sudo chmod 0755 %s/lib`, prefix)
	print(`sudo chmod 0755 %s/lib/systemd`, prefix)
	print(`sudo chmod 0755 %s/lib/systemd/system`, prefix)
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
	}
	printService("andrewd", 0)

	if subcmd == "haio" {
		print(`sudo tee %s/usr/bin/hball >/dev/null << EOF`, prefix)
		print(`#!/bin/bash`)
		print(``)
		print(`if hash systemctl 2>/dev/null ; then`)
		print(`    sudo systemctl \$@ hummingbird-proxy &`)
		print(`    sudo systemctl \$@ hummingbird-account1 &`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator1 &`)
		print(`    sudo systemctl \$@ hummingbird-container1 &`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator1 &`)
		print(`    sudo systemctl \$@ hummingbird-object1 &`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator1 &`)
		print(`    sudo systemctl \$@ hummingbird-account2 &`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator2 &`)
		print(`    sudo systemctl \$@ hummingbird-container2 &`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator2 &`)
		print(`    sudo systemctl \$@ hummingbird-object2 &`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator2 &`)
		print(`    sudo systemctl \$@ hummingbird-account3 &`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator3 &`)
		print(`    sudo systemctl \$@ hummingbird-container3 &`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator3 &`)
		print(`    sudo systemctl \$@ hummingbird-object3 &`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator3 &`)
		print(`    sudo systemctl \$@ hummingbird-account4 &`)
		print(`    sudo systemctl \$@ hummingbird-account-replicator4 &`)
		print(`    sudo systemctl \$@ hummingbird-container4 &`)
		print(`    sudo systemctl \$@ hummingbird-container-replicator4 &`)
		print(`    sudo systemctl \$@ hummingbird-object4 &`)
		print(`    sudo systemctl \$@ hummingbird-object-replicator4 &`)
		print(`    wait`)
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
		print(`    sudo systemctl \$@ hummingbird-proxy &`)
		print(`    sudo systemctl \$@ hummingbird-account1 &`)
		print(`    sudo systemctl \$@ hummingbird-container1 &`)
		print(`    sudo systemctl \$@ hummingbird-object1 &`)
		print(`    sudo systemctl \$@ hummingbird-account2 &`)
		print(`    sudo systemctl \$@ hummingbird-container2 &`)
		print(`    sudo systemctl \$@ hummingbird-object2 &`)
		print(`    sudo systemctl \$@ hummingbird-account3 &`)
		print(`    sudo systemctl \$@ hummingbird-container3 &`)
		print(`    sudo systemctl \$@ hummingbird-object3 &`)
		print(`    sudo systemctl \$@ hummingbird-account4 &`)
		print(`    sudo systemctl \$@ hummingbird-container4 &`)
		print(`    sudo systemctl \$@ hummingbird-object4 &`)
		print(`    wait`)
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
		print(`hummingbird ring object.builder add r1z1-127.0.0.1:6010/sdb1 1`)
		print(`hummingbird ring object.builder add r1z2-127.0.0.1:6020/sdb2 1`)
		print(`hummingbird ring object.builder add r1z3-127.0.0.1:6030/sdb3 1`)
		print(`hummingbird ring object.builder add r1z4-127.0.0.1:6040/sdb4 1`)
		print(`hummingbird ring object.builder rebalance`)
		print(``)
		print(`hummingbird ring object-1.builder create 10 2 1`)
		print(`hummingbird ring object-1.builder add r1z1-127.0.0.1:6010/sdb1 1`)
		print(`hummingbird ring object-1.builder add r1z2-127.0.0.1:6020/sdb2 1`)
		print(`hummingbird ring object-1.builder add r1z3-127.0.0.1:6030/sdb3 1`)
		print(`hummingbird ring object-1.builder add r1z4-127.0.0.1:6040/sdb4 1`)
		print(`hummingbird ring object-1.builder rebalance`)
		print(``)
		print(`hummingbird ring object-2.builder create 10 6 1`)
		print(`hummingbird ring object-2.builder add r1z1-127.0.0.1:6010/sdb1 1`)
		print(`hummingbird ring object-2.builder add r1z1-127.0.0.1:6010/sdb5 1`)
		print(`hummingbird ring object-2.builder add r1z2-127.0.0.1:6020/sdb2 1`)
		print(`hummingbird ring object-2.builder add r1z2-127.0.0.1:6020/sdb6 1`)
		print(`hummingbird ring object-2.builder add r1z3-127.0.0.1:6030/sdb3 1`)
		print(`hummingbird ring object-2.builder add r1z3-127.0.0.1:6030/sdb7 1`)
		print(`hummingbird ring object-2.builder add r1z4-127.0.0.1:6040/sdb4 1`)
		print(`hummingbird ring object-2.builder add r1z4-127.0.0.1:6040/sdb8 1`)
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
		print(`    sudo systemctl stop hummingbird-proxy &`)
		print(`    sudo systemctl stop hummingbird-account1 &`)
		print(`    sudo systemctl stop hummingbird-account-replicator1 &`)
		print(`    sudo systemctl stop hummingbird-container1 &`)
		print(`    sudo systemctl stop hummingbird-container-replicator1 &`)
		print(`    sudo systemctl stop hummingbird-object1 &`)
		print(`    sudo systemctl stop hummingbird-object-replicator1 &`)
		print(`    sudo systemctl stop hummingbird-account2 &`)
		print(`    sudo systemctl stop hummingbird-account-replicator2 &`)
		print(`    sudo systemctl stop hummingbird-container2 &`)
		print(`    sudo systemctl stop hummingbird-container-replicator2 &`)
		print(`    sudo systemctl stop hummingbird-object2 &`)
		print(`    sudo systemctl stop hummingbird-object-replicator2 &`)
		print(`    sudo systemctl stop hummingbird-account3 &`)
		print(`    sudo systemctl stop hummingbird-account-replicator3 &`)
		print(`    sudo systemctl stop hummingbird-container3 &`)
		print(`    sudo systemctl stop hummingbird-container-replicator3 &`)
		print(`    sudo systemctl stop hummingbird-object3 &`)
		print(`    sudo systemctl stop hummingbird-object-replicator3 &`)
		print(`    sudo systemctl stop hummingbird-account4 &`)
		print(`    sudo systemctl stop hummingbird-account-replicator4 &`)
		print(`    sudo systemctl stop hummingbird-container4 &`)
		print(`    sudo systemctl stop hummingbird-container-replicator4 &`)
		print(`    sudo systemctl stop hummingbird-object4 &`)
		print(`    sudo systemctl stop hummingbird-object-replicator4 &`)
		print(`    wait`)
		print(`else`)
		print(`    hummingbird stop all`)
		print(`fi`)
		print(`sudo find /var/log/hummingbird /var/cache/swift -type f -exec rm -f {} \;`)
		print(`sudo mountpoint -q /srv/hb && sudo umount -f /srv/hb || /bin/true`)
		print(`sudo mkdir -p /srv/hb`)
		print(`sudo chmod 0755 /srv`)
		print(`sudo chmod 0755 /srv/hb`)
		print(`sudo truncate -s 5GB /srv/hb-disk`)
		print(`sudo mkfs.xfs -f /srv/hb-disk`)
		print(`sudo mount -o loop /srv/hb-disk /srv/hb`)
		print(`sudo mkdir -p /srv/hb/1/sdb1 /srv/hb/2/sdb2 /srv/hb/3/sdb3 /srv/hb/4/sdb4 /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4 /var/log/hummingbird /var/run/swift /var/run/hummingbird /var/local/hummingbird`)
		print(`sudo chmod 0755 /srv`)
		print(`sudo chmod 0755 /var`)
		print(`sudo chmod 0755 /var/cache`)
		print(`sudo chmod 0755 /var/log`)
		print(`sudo chmod 0755 /var/run`)
		print(`sudo chown -R ${USER}: /srv/hb /var/cache/swift* /var/log/hummingbird /var/run/swift /var/run/hummingbird /var/local/hummingbird`)
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

	print(`sudo mkdir -p %s/var/cache/swift`, prefix)
	if subcmd != "deb" {
		print(`sudo chown %s: %s/var/cache/swift`, username, prefix)
	}
	print(`sudo chmod 0755 %s/var`, prefix)
	print(`sudo chmod 0755 %s/var/cache`, prefix)
	print(`sudo chmod 0755 %s/var/cache/swift`, prefix)
	print(`sudo mkdir -p %s/var/log/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/var`, prefix)
	print(`sudo chmod 0755 %s/var/log`, prefix)
	print(`sudo chmod 0755 %s/var/log/hummingbird`, prefix)
	print(`sudo mkdir -p %s/var/run/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/var`, prefix)
	print(`sudo chmod 0755 %s/var/run`, prefix)
	print(`sudo chmod 0755 %s/var/run/hummingbird`, prefix)
	print(`sudo mkdir -p %s/var/local/hummingbird`, prefix)
	print(`sudo chmod 0755 %s/var/local/hummingbird`, prefix)
	print(``)

	if subcmd != "haio" {
		print(`sudo mkdir -p %s/srv/hummingbird`, prefix)
		if subcmd != "deb" {
			print(`sudo chown %s: %s/srv/hummingbird`, username, prefix)
		}
		print(`sudo chmod 0755 %s/srv`, prefix)
		print(`sudo chmod 0755 %s/srv/hummingbird`, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/account.builder create 10 1 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/account.builder add r1z1-127.0.0.1:6012/hummingbird 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/account.builder rebalance`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/container.builder create 10 1 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/container.builder add r1z1-127.0.0.1:6011/hummingbird 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/container.builder rebalance`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/object.builder create 10 1 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/object.builder add r1z1-127.0.0.1:6010/hummingbird 1`, prefix, prefix)
		print(`sudo %s/usr/bin/hummingbird ring %s/etc/hummingbird/object.builder rebalance`, prefix, prefix)
		print(`sudo rm -rf %s/etc/hummingbird/backups`, prefix)
		if subcmd != "deb" {
			print(`sudo chown %s: %s/etc/hummingbird/*.{builder,ring.gz}`, username, prefix)
		}
		print(`sudo chmod 0644 %s/etc/hummingbird/*.{builder,ring.gz}`, prefix)
		print(``)
	}

	if subcmd == "deb" {
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

		print(`pushd "%s"`, prefix)
		print(`tar czf control.tar.gz control postinst`)
		print(`sudo chown -R root: etc lib srv usr var`)
		print(`sudo find etc lib srv usr var -type d -exec chmod 0755 {} \;`)
		print(`sudo find etc lib srv usr var -type f -exec chmod 0644 {} \;`)
		print(`sudo find usr/bin -type f -exec chmod 0755 {} \;`)
		print(`tar czf data.tar.gz etc lib srv usr var`)
		print(`ar rc hummingbird-%s.deb debian-binary control.tar.gz data.tar.gz`, versionNoV)
		print(`mv hummingbird-%s.deb ../`, versionNoV)
		print(`popd`)
		print(`sudo rm -rf build`) // build type here instead of %s, prefix for safety
		print(``)
	}

	if subcmd == "haio" {
		print(`if hash systemctl 2>/dev/null ; then`)
		print(`    sudo systemctl daemon-reload`)
		print(`fi`)
		print(`hbrings`)
		print(`hbreset`)
		print(`hbmain start`)
		print(`if [ ! -e ~/swift ] ; then`)
		print(`    git clone https://github.com/openstack/swift ~/swift`)
		print(`fi`)
		print(`sudo mkdir -p /etc/swift`)
		print(`sudo chown %s: /etc/swift`, username)
		print(`sudo chmod 0755 /etc`)
		print(`sudo chmod 0755 /etc/swift`)
		print(`cp ~/swift/test/sample.conf /etc/swift/test.conf`)
		print(`sudo apt install -y python-eventlet python-mock python-netifaces python-nose python-pastedeploy python-pbr python-pyeclib python-setuptools python-swiftclient python-unittest2 python-xattr`)
		print(`pushd ~/swift/test/functional`)
		print(`nosetests || /bin/true # This always fails because we don't pass all of Swift's tests.`)
		print(`popd`)
		print(`hbmain stop`)
		print(``)
	}

	switch subcmd {
	case "deb":
		print(`tail -3 $0 | cut -f2- -d ' '`)
		print(``)
		print(`# hummingbird-%s.deb has been built.`, versionNoV)
		print(``)
	case "haio":
		print(`tail -9 $0 | cut -f2- -d ' '`)
		print(``)
		print(`# HAIO is ready for use.`)
		print(`# `)
		print(`# You probably want relogin or run:`)
		print(`# `)
		print(`# source /etc/profile`)
		print(`# `)
		print(`# See https://github.com/troubling/hummingbird/blob/master/HAIO.md for more info.`)
		print(``)
	default:
		print(`tail -17 $0 | cut -f2- -d ' '`)
		print(``)
		print(`# Hummingbird is ready for use.`)
		print(`# `)
		print(`# For quick local testing:`)
		print(`# `)
		print(`# sudo systemctl start memcached`)
		print(`# sudo systemctl start hummingbird-proxy`)
		print(`# sudo systemctl start hummingbird-account`)
		print(`# sudo systemctl start hummingbird-container`)
		print(`# sudo systemctl start hummingbird-object`)
		print(`# url=` + "`" + `curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Storage-Url | cut -f2 -d ' ' | tr -d '\r\n'` + "`" + ``)
		print(`# token=` + "`" + `curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Auth-Token | tr -d '\r\n'` + "`" + ``)
		print(`# curl -i -X PUT $url/container -H "$token" ; echo`)
		print(`# curl -i -X PUT $url/container/object -H "$token" -T /usr/bin/hummingbird ; echo`)
		print(`# curl -i "$url/container?format=json" -H "$token" ; echo`)
		print(`# sudo find /srv/hummingbird`)
		print(``)
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
