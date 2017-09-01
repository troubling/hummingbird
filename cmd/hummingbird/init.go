package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"

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
	usr, err := user.Current()
	if err != nil {
		return err
	}
	sudo, err := exec.LookPath("sudo")
	if err != nil {
		return err
	}
	if usr.Uid != "0" {
		if subcmd == "" {
			return runCommand(sudo, os.Args[0], "init")
		} else {
			return runCommand(sudo, os.Args[0], "init", subcmd, usr.Username)
		}
	}
	username := "hummingbird"
	groupname := "hummingbird"
	userID := 0
	groupID := 0
	homedir := "."
	dst := "/"
	if subcmd != "" {
		if len(args) < 1 {
			return fmt.Errorf("internal error: init %s but no user", subcmd)
		}
		usr, err := user.Lookup(args[0])
		if err != nil {
			return err
		}
		args = args[1:]
		if subcmd == "haio" {
			username = usr.Username
		}
		userID, err = strconv.Atoi(usr.Uid)
		if err != nil {
			return err
		}
		groupID, err = strconv.Atoi(usr.Gid)
		if err != nil {
			return err
		}
		grp, err := user.LookupGroupId(usr.Gid)
		if err != nil {
			return err
		}
		if subcmd == "haio" {
			groupname = grp.Name
		}
		homedir = usr.HomeDir
	}
	if subcmd == "debian" {
		dst = "build"
		if _, err := os.Stat(dst); !os.IsNotExist(err) {
			return fmt.Errorf("%s exists already; that would be our working directory", dst)
		}
	}
	if subcmd == "haio" {
		apt, err := exec.LookPath("apt")
		if err != nil {
			return err
		}
		if err = runCommand(apt, "install", "-y", "gcc", "git-core", "make", "memcached", "sqlite3", "tar", "wget", "xfsprogs"); err != nil {
			return err
		}
		wget, err := exec.LookPath("wget")
		if err != nil {
			return err
		}
		if err = runCommand(sudo, "-u", username, wget, "-nc", "https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz"); err != nil {
			return err
		}
		tar, err := exec.LookPath("tar")
		if err != nil {
			return err
		}
		if err = runCommand(tar, "-C", "/usr/local", "-xzf", "go1.9.linux-amd64.tar.gz"); err != nil {
			return err
		}
		f, err := os.OpenFile("/etc/profile", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		if _, err = f.WriteString("\nexport PATH=$PATH:/usr/local/go/bin\n"); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		if _, err = fmt.Println("Retrieving sources; this may take a while..."); err != nil {
			return err
		}
		if err := runCommand(sudo, "-u", username, "/usr/local/go/bin/go", "get", "-t", "github.com/troubling/hummingbird/..."); err != nil {
			return err
		}
		if err := runCommand("/usr/local/go/bin/go", "build", "-o", "/usr/bin/nectar", "github.com/troubling/hummingbird/cmd/nectar/..."); err != nil {
			return err
		}
	}
	if err := hummingbirdCreate(dst); err != nil {
		return err
	}
	if err := hummingbirdConfCreate(dst); err != nil {
		return err
	}
	if err := proxyServerConfCreate(dst); err != nil {
		return err
	}
	start := 0
	stop := 0
	if subcmd == "haio" {
		start = 1
		stop = 4
	}
	for index := start; index <= stop; index++ {
		if err := accountServerConfCreate(dst, index); err != nil {
			return err
		}
		if err := containerServerConfCreate(dst, index); err != nil {
			return err
		}
		if err := objectServerConfCreate(dst, index); err != nil {
			return err
		}
	}
	for _, name := range []string{"proxy", "andrewd"} {
		if err := serviceCreate(dst, name, 0, username, groupname); err != nil {
			return err
		}
	}
	for _, name := range []string{"account", "account-replicator", "container", "container-replicator", "object", "object-replicator", "object-auditor"} {
		if subcmd == "haio" {
			for index := 1; index <= 4; index++ {
				if err := serviceCreate(dst, name, index, username, groupname); err != nil {
					return err
				}
			}
		} else {
			if err := serviceCreate(dst, name, 0, username, groupname); err != nil {
				return err
			}
		}
	}
	if subcmd == "haio" {
		hballCreate(dst)
		hbmainCreate(dst)
		hbringsCreate(dst)
		hbresetCreate(dst)
		hblogCreate(dst)
	}
	if err := os.MkdirAll(path.Join(dst, "var/run/hummingbird"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(path.Join(dst, "var/log/hummingbird"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(path.Join(dst, "var/cache/swift"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(path.Join(dst, "srv/hummingbird"), 0755); err != nil {
		return err
	}
	if subcmd != "haio" {
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/account.builder"), "create", "10", "1", "1"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/account.builder"), "add", "r1z1-127.0.0.1:6012/hummingbird", "1"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/account.builder"), "rebalance"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/container.builder"), "create", "10", "1", "1"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/container.builder"), "add", "r1z1-127.0.0.1:6011/hummingbird", "1"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/container.builder"), "rebalance"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/object.builder"), "create", "10", "1", "1"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/object.builder"), "add", "r1z1-127.0.0.1:6010R127.0.0.1:8010/hummingbird", "1"); err != nil {
			return err
		}
		if err := runCommand(os.Args[0], "ring", path.Join(dst, "etc/hummingbird/object.builder"), "rebalance"); err != nil {
			return err
		}
		if err := os.RemoveAll(path.Join(dst, "etc/hummingbird/backups")); err != nil {
			return err
		}
	}
	if subcmd == "" {
		adduser, err := exec.LookPath("adduser")
		if err != nil {
			return err
		}
		if err := runCommand(adduser, "--system", "--group", "--no-create-home", "hummingbird"); err != nil {
			return err
		}
		usr, err := user.Lookup("hummingbird")
		if err != nil {
			return err
		}
		hummingbirdUserID, err := strconv.Atoi(usr.Uid)
		if err != nil {
			return err
		}
		hummingbirdGroupID, err := strconv.Atoi(usr.Gid)
		if err != nil {
			return err
		}
		if err := os.Chown(path.Join(dst, "srv/hummingbird"), hummingbirdUserID, hummingbirdGroupID); err != nil {
			return err
		}
		if _, err = exec.LookPath("memcached"); err != nil {
			apt, err := exec.LookPath("apt")
			if err != nil {
				fmt.Fprintln(os.Stderr, "WARNING: Could not determine if memcached was installed; hummingbird will need at least one memcache server.")
			} else {
				if err = runCommand(apt, "install", "-y", "memcached"); err != nil {
					return err
				}
			}
		}
	}
	var debName string
	if subcmd == "debian" {
		if err := debianBinaryCreate(dst); err != nil {
			return err
		}
		version := common.Version
		if version == "" {
			version = "0.0.1"
		}
		if version[0] == 'v' {
			version = version[1:]
		}
		if err := debianControlCreate(dst, version); err != nil {
			return err
		}
		if err := debianPostInstCreate(dst); err != nil {
			return err
		}
		if err := filepath.Walk(dst, func(pth string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || strings.Contains(pth, "/bin/") {
				if err = os.Chmod(pth, 0755); err != nil {
					return err
				}
			} else {
				if err = os.Chmod(pth, 0644); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		tar, err := exec.LookPath("tar")
		if err != nil {
			return err
		}
		if err := runCommand(tar, "czf", path.Join(dst, "control.tar.gz"), "-C", dst, "control", "postinst"); err != nil {
			return err
		}
		if err := runCommand(tar, "czf", path.Join(dst, "data.tar.gz"), "-C", dst, "etc", "lib", "srv", "usr", "var"); err != nil {
			return err
		}
		ar, err := exec.LookPath("ar")
		if err != nil {
			return err
		}
		prevdir, err := os.Getwd()
		if err != nil {
			return err
		}
		if err = os.Chdir(dst); err != nil {
			return err
		}
		debName = "hummingbird-" + version + ".deb"
		if err = runCommand(ar, "rc", debName, "debian-binary", "control.tar.gz", "data.tar.gz"); err != nil {
			return err
		}
		if err = os.Chdir(prevdir); err != nil {
			return err
		}
		if err = os.Rename(path.Join(dst, debName), debName); err != nil {
			return err
		}
		if err = os.Chown(debName, userID, groupID); err != nil {
			return err
		}
		if err = os.Chmod(debName, 0644); err != nil {
			return err
		}
		if err = os.RemoveAll(dst); err != nil {
			return err
		}
	}
	if subcmd == "haio" {
		if err := filepath.Walk("/etc/hummingbird", func(pth string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if err = os.Chmod(pth, 0755); err != nil {
					return err
				}
			} else {
				if err = os.Chmod(pth, 0644); err != nil {
					return err
				}
			}
			return os.Chown(pth, userID, groupID)
		}); err != nil {
			return err
		}
		systemctl, err := exec.LookPath("systemctl")
		if err == nil {
			if err = runCommand(systemctl, "daemon-reload"); err != nil {
				return err
			}
		}
		if err = runCommand(sudo, "-u", username, "/usr/bin/hbrings"); err != nil {
			return err
		}
		if err = runCommand(sudo, "-u", username, "/usr/bin/hbreset"); err != nil {
			return err
		}
		if err = runCommand(sudo, "-u", username, "/usr/bin/hbmain", "start"); err != nil {
			return err
		}
		if err = runCommand("/usr/bin/nectar", "-A", "http://127.0.0.1:8080/auth/v1.0", "-U", "test:tester", "-K", "testing", "head"); err != nil {
			return err
		}
		if _, err := os.Stat(path.Join(homedir, "swift")); os.IsNotExist(err) {
			git, err := exec.LookPath("git")
			if err != nil {
				return err
			}
			if err = runCommand(sudo, "-u", username, git, "clone", "https://github.com/openstack/swift", path.Join(homedir, "swift")); err != nil {
				return err
			}
		}
		if err = os.MkdirAll("/etc/swift", 0755); err != nil {
			return err
		}
		if err = os.Chown("/etc/swift", userID, groupID); err != nil {
			return err
		}
		sf, err := os.Open(path.Join(homedir, "swift/test/sample.conf"))
		if err != nil {
			return err
		}
		df, err := os.Create("/etc/swift/test.conf")
		if err != nil {
			return err
		}
		if _, err := io.Copy(df, sf); err != nil {
			return nil
		}
		if err = sf.Close(); err != nil {
			return err
		}
		if err = df.Close(); err != nil {
			return err
		}
		apt, err := exec.LookPath("apt")
		if err != nil {
			return err
		}
		if err = runCommand(apt, "install", "-y", "python-eventlet", "python-mock", "python-netifaces", "python-nose", "python-pastedeploy", "python-pbr", "python-pyeclib", "python-setuptools", "python-swiftclient", "python-unittest2", "python-xattr"); err != nil {
			return err
		}
		prevdir, err := os.Getwd()
		if err != nil {
			return err
		}
		if err = os.Chdir(path.Join(homedir, "swift/test/functional")); err != nil {
			return err
		}
		nosetests, err := exec.LookPath("nosetests")
		if err != nil {
			return err
		}
		// This always fails because we don't pass 100% of Swift's tests.
		runCommand(nosetests)
		if err = os.Chdir(prevdir); err != nil {
			return err
		}
		if err = runCommand(sudo, "-u", username, "/usr/bin/hbmain", "stop"); err != nil {
			return err
		}
		if err = os.Chdir(path.Join(homedir, "go/src/github.com/troubling/hummingbird")); err != nil {
			return err
		}
		git, err := exec.LookPath("git")
		if err != nil {
			return err
		}
		if err = runCommand(sudo, "-u", username, git, "remote", "rename", "origin", "upstream"); err != nil {
			return err
		}
		if err = runCommand(sudo, "-u", username, git, "remote", "add", "origin", fmt.Sprintf("git@github.com:%s/hummingbird", username)); err != nil {
			return err
		}
	}
	fmt.Println()
	switch subcmd {
	case "debian":
		fmt.Println(debName, "has been built.")
	case "haio":
		fmt.Println(`HAIO is ready for use.

You probably want relogin or run:

source /etc/profile

See https://github.com/troubling/hummingbird/blob/master/HAIO.md for more info.
`)
	default:
		fmt.Println(`Hummingbird is ready for use.

For quick local testing:

sudo systemctl start memcached
sudo systemctl start hummingbird-proxy
sudo systemctl start hummingbird-account
sudo systemctl start hummingbird-container
sudo systemctl start hummingbird-object
url=` + "`" + `curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Storage-Url | cut -f2 -d ' ' | tr -d '\r\n'` + "`" + `
token=` + "`" + `curl -si http://127.0.0.1:8080/auth/v1.0 -H x-auth-user:test:tester -H x-auth-key:testing | grep ^X-Auth-Token | tr -d '\r\n'` + "`" + `
curl -i -X PUT $url/container -H "$token" ; echo
curl -i -X PUT $url/container/object -H "$token" -T /usr/bin/hummingbird ; echo
curl -i "$url/container?format=json" -H "$token" ; echo
sudo find /srv/hummingbird
`)
	}
	return nil
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func hummingbirdCreate(dst string) error {
	pth := path.Join(dst, "usr/bin/hummingbird")
	if err := os.Remove(pth); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	sf, err := os.Open(os.Args[0])
	if err != nil {
		return err
	}
	df, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = io.Copy(df, sf); err != nil {
		return err
	}
	if err = sf.Close(); err != nil {
		return err
	}
	if err = df.Close(); err != nil {
		return err
	}
	return os.Chmod(pth, 0755)
}

func hummingbirdConfCreate(dst string) error {
	pth := path.Join(dst, "etc/hummingbird/hummingbird.conf")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`[swift-hash]
swift_hash_path_prefix = changeme
swift_hash_path_suffix = changeme

[storage-policy:0]
name = gold
policy_type = replication
default = yes

[storage-policy:1]
name = silver
policy_type = replication
`); err != nil {
		return err
	}
	return f.Close()
}

func proxyServerConfCreate(dst string) error {
	pth := path.Join(dst, "etc/hummingbird/proxy-server.conf")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`[DEFAULT]
bind_ip = 127.0.0.1
bind_port = 8080
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
`); err != nil {
		return err
	}
	return f.Close()
}

func accountServerConfCreate(dst string, index int) error {
	var pth string
	var devices string
	var port int
	if index < 1 {
		pth = path.Join(dst, "etc/hummingbird/account-server.conf")
		devices = "/srv/hummingbird"
		port = 6012
	} else {
		pth = path.Join(dst, fmt.Sprintf("etc/hummingbird/account-server/%d.conf", index))
		devices = fmt.Sprintf("/srv/hb/%d", index)
		port = 6002 + index*10
	}
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(fmt.Sprintf(`[DEFAULT]
devices = %s
mount_check = false
bind_ip = 127.0.0.1
bind_port = %d

[app:account-server]

[account-replicator]
`, devices, port)); err != nil {
		return err
	}
	return f.Close()
}

func containerServerConfCreate(dst string, index int) error {
	var pth string
	var devices string
	var port int
	if index < 1 {
		pth = path.Join(dst, "etc/hummingbird/container-server.conf")
		devices = "/srv/hummingbird"
		port = 6011
	} else {
		pth = path.Join(dst, fmt.Sprintf("etc/hummingbird/container-server/%d.conf", index))
		devices = fmt.Sprintf("/srv/hb/%d", index)
		port = 6001 + index*10
	}
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(fmt.Sprintf(`[DEFAULT]
devices = %s
mount_check = false
bind_ip = 127.0.0.1
bind_port = %d

[app:container-server]

[container-replicator]
`, devices, port)); err != nil {
		return err
	}
	return f.Close()
}

func objectServerConfCreate(dst string, index int) error {
	var pth string
	var devices string
	var port int
	var repPort int
	if index < 1 {
		pth = path.Join(dst, "etc/hummingbird/object-server.conf")
		devices = "/srv/hummingbird"
		port = 6010
		repPort = 8010
	} else {
		pth = path.Join(dst, fmt.Sprintf("etc/hummingbird/object-server/%d.conf", index))
		devices = fmt.Sprintf("/srv/hb/%d", index)
		port = 6000 + index*10
		repPort = 8000 + index*10
	}
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(fmt.Sprintf(`[DEFAULT]
devices = %s
mount_check = false

[app:object-server]
bind_ip = 127.0.0.1
bind_port = %d

[object-replicator]
bind_ip = 127.0.0.1
bind_port = %d

[object-auditor]
`, devices, port, repPort)); err != nil {
		return err
	}
	return f.Close()
}

func serviceCreate(dst, name string, index int, username, groupname string) error {
	var pth string
	var conf string
	if index < 1 {
		pth = path.Join(dst, fmt.Sprintf("lib/systemd/system/hummingbird-%s.service", name))
		conf = ""
	} else {
		pth = path.Join(dst, fmt.Sprintf("lib/systemd/system/hummingbird-%s%d.service", name, index))
		ss := strings.SplitN(name, "-", 2)
		basename := name
		if len(ss) == 2 {
			basename = ss[0]
		}
		conf = fmt.Sprintf(" -c /etc/hummingbird/%s-server/%d.conf", basename, index)
	}
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	// See these pages for lots of options:
	// http://0pointer.de/public/systemd-man/systemd.service.html
	// http://0pointer.de/public/systemd-man/systemd.exec.html
	if _, err = f.WriteString(fmt.Sprintf(`[Unit]
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
`, name, username, groupname, name, conf, name)); err != nil {
		return err
	}
	return f.Close()
}

func hballCreate(dst string) error {
	pth := path.Join(dst, "usr/bin/hball")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`#!/bin/bash

if hash systemctl 2>/dev/null ; then
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
else
    hummingbird $@ all
fi
`); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Chmod(pth, 0755)
}

func hbmainCreate(dst string) error {
	pth := path.Join(dst, "usr/bin/hbmain")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`#!/bin/bash

if hash systemctl 2>/dev/null ; then
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
else
    hummingbird $@ main
fi
`); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Chmod(pth, 0755)
}

func hbringsCreate(dst string) error {
	pth := path.Join(dst, "usr/bin/hbrings")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`#!/bin/bash
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
`); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Chmod(pth, 0755)
}

func hbresetCreate(dst string) error {
	pth := path.Join(dst, "usr/bin/hbreset")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`#!/bin/bash

if hash systemctl 2>/dev/null ; then
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
else
    hummingbird stop all
fi
sudo find /var/log/hummingbird /var/cache/swift -type f -exec rm -f {} \;
sudo mountpoint -q /srv/hb && sudo umount -f /srv/hb || /bin/true
sudo mkdir -p /srv/hb
sudo truncate -s 5GB /srv/hb-disk
sudo mkfs.xfs -f /srv/hb-disk
sudo mount -o loop /srv/hb-disk /srv/hb
sudo mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4 /var/run/swift /srv/hb/1/sdb1 /srv/hb/2/sdb2 /srv/hb/3/sdb3 /srv/hb/4/sdb4 /var/run/hummingbird /var/log/hummingbird
sudo chown -R ${USER}: /var/run/hummingbird /var/log/hummingbird /var/cache/swift* /srv/hb*
if hash systemctl 2>/dev/null ; then
    sudo systemctl restart memcached
else
    sudo service memcached restart
fi
`); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Chmod(pth, 0755)
}

func hblogCreate(dst string) error {
	pth := path.Join(dst, "usr/bin/hblog")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`#!/bin/bash

if hash journalctl 2>/dev/null ; then
    journalctl -u hummingbird-$@
else
    echo "Unsure how to view your logs"
    exit 1
fi
`); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Chmod(pth, 0755)
}

// Started this from https://medium.com/@newhouseb/hassle-free-go-in-production-528af8ee1a58

func debianBinaryCreate(dst string) error {
	pth := path.Join(dst, "debian-binary")
	if err := os.MkdirAll(path.Dir(pth), 0644); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`2.0
`); err != nil {
		return err
	}
	return f.Close()
}

func debianControlCreate(dst, version string) error {
	pth := path.Join(dst, "control")
	if err := os.MkdirAll(path.Dir(pth), 0644); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(fmt.Sprintf(`Package: hummingbird
Version: %s
Section: net
Priority: optional
Architecture: amd64
Depends: adduser, memcached
Maintainer: Rackspace <gholt@rackspace.com>
Description: Hummingbird Object Storage Software
`, version)); err != nil {
		return err
	}
	return f.Close()
}

func debianPostInstCreate(dst string) error {
	pth := path.Join(dst, "postinst")
	if err := os.MkdirAll(path.Dir(pth), 0755); err != nil {
		return err
	}
	f, err := os.Create(pth)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(`#!/bin/sh

adduser --system --group --no-create-home hummingbird
chown hummingbird: /srv/hummingbird
`); err != nil {
		return err
	}
	return f.Close()
}
