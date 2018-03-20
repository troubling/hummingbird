//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/bench"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/containerserver"
	"github.com/troubling/hummingbird/objectserver"
	"github.com/troubling/hummingbird/proxyserver"
	"github.com/troubling/hummingbird/tools"
	"github.com/troubling/nectar"
)

const (
	runPath = "/var/run/hummingbird"
	logPath = "/var/log/hummingbird"
)

func getProcess(name string) (*os.Process, error) {
	var pid int
	file, err := os.Open(filepath.Join(runPath, fmt.Sprintf("%s.pid", name)))
	if err != nil {
		return nil, err
	}
	_, err = fmt.Fscanf(file, "%d", &pid)
	if err != nil {
		return nil, err
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return nil, err
	}
	return process, nil
}

func findConfig(name string) string {
	configName := strings.Split(name, "-")[0]
	configSearch := []string{
		fmt.Sprintf("/etc/hummingbird/%s-server.conf", configName),
		fmt.Sprintf("/etc/hummingbird/%s-server.conf.d", configName),
		fmt.Sprintf("/etc/hummingbird/%s-server", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf.d", configName),
		fmt.Sprintf("/etc/swift/%s-server", configName),
	}
	for _, config := range configSearch {
		if fs.Exists(config) {
			return config
		}
	}
	return ""
}

func startServer(name string, args ...string) error {
	process, err := getProcess(name)
	if err == nil {
		process.Release()
		return errors.New("Found already running " + name + " server")
	}

	serverConf := findConfig(name)
	if serverConf == "" {
		return errors.New("Unable to find config file.")
	}

	serverExecutable, err := exec.LookPath(os.Args[0])
	if err != nil {
		return errors.New("Unable to find hummingbird executable in path.")
	}

	uid, gid, err := conf.UidFromConf(serverConf)
	if err != nil {
		return errors.New("Unable to find uid to execute process:" + err.Error())
	}

	logfile := filepath.Join(logPath, name+".log")
	errfile := filepath.Join(logPath, name+".err")
	cmd := exec.Command(serverExecutable, append([]string{name, "-c", serverConf, "-l", logfile, "-e", errfile}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if uint32(os.Getuid()) != uid { // This is goofy.
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uid, Gid: gid}
	}
	cmd.Stdin = nil

	syscall.Umask(022)
	err = cmd.Start()
	if err != nil {
		return errors.New("Error starting server:" + err.Error())
	}
	file, err := os.Create(filepath.Join(runPath, fmt.Sprintf("%s.pid", name)))
	if err != nil {
		return errors.New("Error creating pidfile:" + err.Error())
	}
	defer file.Close()
	fmt.Fprintf(file, "%d", cmd.Process.Pid)
	fmt.Println(strings.Title(name), "server started.")
	return nil
}

func stopServer(name string, args ...string) error {
	process, err := getProcess(name)
	if err != nil {
		return errors.New(strings.Title(name) + " server not found.")
	}
	process.Signal(os.Kill)
	process.Wait()
	os.Remove(filepath.Join(runPath, fmt.Sprintf("%s.pid", name)))
	fmt.Println(strings.Title(name), "server stopped.")
	return nil
}

func restartServer(name string, args ...string) error {
	process, err := getProcess(name)
	if err == nil {
		process.Signal(os.Kill)
		process.Wait()
		fmt.Println(strings.Title(name), "server stopped.")
	} else {
		fmt.Println(strings.Title(name), "server not found.")
	}
	os.Remove(filepath.Join(runPath, fmt.Sprintf("%s.pid", name)))
	return startServer(name, args...)
}

func gracefulRestartServer(name string, args ...string) error {
	process, err := getProcess(name)
	if err == nil {
		process.Signal(syscall.SIGTERM)
		time.Sleep(time.Second)
		fmt.Println(strings.Title(name), "server graceful shutdown began.")
	} else {
		fmt.Println(strings.Title(name), "server not found.")
	}
	process.Release()
	os.Remove(filepath.Join(runPath, fmt.Sprintf("%s.pid", name)))
	return startServer(name, args...)
}

func gracefulShutdownServer(name string, args ...string) error {
	process, err := getProcess(name)
	if err != nil {
		return errors.New(strings.Title(name) + " server not found.")
	}
	process.Signal(syscall.SIGTERM)
	process.Release()
	os.Remove(filepath.Join(runPath, fmt.Sprintf("%s.pid", name)))
	fmt.Println(strings.Title(name), "server graceful shutdown began.")
	return nil
}

func processControlCommand(serverCommand func(name string, args ...string) error) {
	for _, reqDir := range []string{runPath, logPath} {
		if !fs.Exists(reqDir) {
			err := os.MkdirAll(reqDir, 0600)
			if err != nil {
				fmt.Fprintln(os.Stderr, reqDir, "does not exist, and unable to create it.")
				fmt.Fprintln(os.Stderr, "You should create it, writable by the user you wish to launch servers with.")
				os.Exit(1)
			}
		}
	}

	if flag.NArg() < 2 {
		flag.Usage()
		return
	}

	switch flag.Arg(1) {
	case "proxy", "object", "object-replicator", "container", "container-replicator", "account", "account-replicator", "andrewd":
		if err := serverCommand(flag.Arg(1), flag.Args()[2:]...); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "main":
		exc := 0
		for _, server := range []string{"proxy", "object", "container", "account"} {
			if err := serverCommand(server); err != nil {
				fmt.Fprintln(os.Stderr, server, ":", err)
				exc = 1
			}
		}
		os.Exit(exc)
	case "all":
		exc := 0
		for _, server := range []string{"proxy", "object", "object-replicator",
			"container", "container-replicator", "account",
			"account-replicator"} {
			if err := serverCommand(server); err != nil {
				fmt.Fprintln(os.Stderr, server, ":", err)
				exc = 1
			}
		}
		os.Exit(exc)
	default:
		flag.Usage()
	}
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
	proxyFlags := flag.NewFlagSet("proxy server", flag.ExitOnError)
	proxyFlags.String("c", findConfig("proxy"), "Config file/directory to use")
	proxyFlags.String("l", "stdout", "Log location")
	proxyFlags.String("e", "stderr", "Error log location")
	proxyFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird proxy [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run proxy server")
		proxyFlags.PrintDefaults()
	}

	objectFlags := flag.NewFlagSet("object server", flag.ExitOnError)
	objectFlags.String("c", findConfig("object"), "Config file/directory to use")
	objectFlags.String("l", "stdout", "Log location")
	objectFlags.String("e", "stderr", "Error log location")
	objectFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird object [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run object server")
		objectFlags.PrintDefaults()
	}

	objectReplicatorFlags := flag.NewFlagSet("object replicator", flag.ExitOnError)
	objectReplicatorFlags.Bool("q", false, "Quorum Delete. Will delete handoff node if pushed to #replicas/2 + 1 nodes.")
	objectReplicatorFlags.String("c", findConfig("object"), "Config file/directory to use")
	objectReplicatorFlags.String("l", "stdout", "Log location")
	objectReplicatorFlags.String("e", "stderr", "Error log location")
	objectReplicatorFlags.Bool("once", false, "Run one pass of the replicator")
	objectReplicatorFlags.String("devices", "", "Replicate only given devices. Comma-separated list.")
	objectReplicatorFlags.String("partitions", "", "Replicate only given partitions. Comma-separated list.")
	objectReplicatorFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird object-replicator [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run object replicator")
		objectReplicatorFlags.PrintDefaults()
	}

	containerFlags := flag.NewFlagSet("container server", flag.ExitOnError)
	containerFlags.String("c", findConfig("container"), "Config file/directory to use")
	containerFlags.String("l", "stdout", "Log location")
	containerFlags.String("e", "stderr", "Error log location")
	containerFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird container [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run container server")
		containerFlags.PrintDefaults()
	}

	containerReplicatorFlags := flag.NewFlagSet("container replicator", flag.ExitOnError)
	containerReplicatorFlags.String("c", findConfig("container"), "Config file/directory to use")
	containerReplicatorFlags.String("l", "stdout", "Log location")
	containerReplicatorFlags.String("e", "stderr", "Error log location")
	containerReplicatorFlags.Bool("once", false, "Run one pass of the replicator")
	containerReplicatorFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird container-replicator [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run container replicator")
		containerReplicatorFlags.PrintDefaults()
	}

	accountFlags := flag.NewFlagSet("account server", flag.ExitOnError)
	accountFlags.String("c", findConfig("account"), "Config file/directory to use")
	accountFlags.String("l", "stdout", "Log location")
	accountFlags.String("e", "stderr", "Error log location")
	accountFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird account [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run account server")
		accountFlags.PrintDefaults()
	}

	accountReplicatorFlags := flag.NewFlagSet("account replicator", flag.ExitOnError)
	accountReplicatorFlags.String("c", findConfig("account"), "Config file/directory to use")
	accountReplicatorFlags.String("l", "stdout", "Log location")
	accountReplicatorFlags.String("e", "stderr", "Error log location")
	accountReplicatorFlags.Bool("once", false, "Run one pass of the replicator")
	accountReplicatorFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird account-replicator [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run account replicator")
		accountReplicatorFlags.PrintDefaults()
	}

	ringBuilderFlags := flag.NewFlagSet("ring builder", flag.ExitOnError)
	ringBuilderFlags.Bool("debug", false, "Run in debug mode")
	ringBuilderFlags.Bool("json", false, "Ouput in JSON format")
	ringBuilderFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird ring <builder_file> command\n")
		fmt.Fprintf(os.Stderr, "  Builds a swift style ring.  Commands are:\n")
		fmt.Fprintf(os.Stderr, "    create <part_power> <replicas> <min_part_hours> (create a new ring)\n")
		fmt.Fprintf(os.Stderr, "    add <device> <weight> (add a new device to the ring)\n")
		fmt.Fprintf(os.Stderr, "    rebalance [-dryrun] (rebalance the ring)\n")
		fmt.Fprintf(os.Stderr, "    search <search_flags> (search for devices in the ring)\n")
		fmt.Fprintf(os.Stderr, "    set_weight <search_flags> [-yes] <weight> (change the weight of 1 or more devices)\n")
		fmt.Fprintf(os.Stderr, "    remove <search_flags> [-yes] (remove device from the ring)\n")
		fmt.Fprintf(os.Stderr, "    set_info <search_flags> [-yes] <change_flags> (change device information)\n")
		fmt.Fprintf(os.Stderr, "    info (display ring info)\n")
		fmt.Fprintf(os.Stderr, "    analyze (analyze ring)\n")
		fmt.Fprintf(os.Stderr, "    validate (validate ring)\n")
		fmt.Fprintf(os.Stderr, "    write_ring (write the ring file)\n")
		fmt.Fprintf(os.Stderr, "    pretend_min_part_hours_passed (reset min_part_hours)\n")
		fmt.Fprintf(os.Stderr, "  <device> is of the form: [r<region>]z<zone>[s<scheme>]-<ip>:<port>[R<r_ip>:<r_port>]/<device_name>_<meta>\n")
		fmt.Fprintf(os.Stderr, "  <scheme> can be either http or https\n")
		fmt.Fprintf(os.Stderr, "  <search_flags> is at least one of: -region, -zone, -scheme, -ip, -port, -replication-ip, replication-port, -device, -meta, -weight\n")
		fmt.Fprintf(os.Stderr, "  <change_flags> is at least one of: -change-ip, -change-port, -change-replication-ip, -change-replication-port, -change-device, -change-meta, -change-scheme\n")
		ringBuilderFlags.PrintDefaults()
	}

	nodesFlags := flag.NewFlagSet("", flag.ExitOnError)
	nodesFlags.Bool("a", false, "Show all handoff nodes")
	nodesFlags.String("p", "", "Show nodes for a given partition")
	nodesFlags.String("r", "", "Specify which ring file to use")
	nodesFlags.String("P", "", "Specify which policy to use")
	nodesFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird nodes [-a] <account> [<container> [<object]]\n")
		fmt.Fprintf(os.Stderr, "hummingbird nodes [-a] <account>[/<container[/<object>]]\n")
		fmt.Fprintf(os.Stderr, "hummingbird nodes [-a] -P policy_name <account> <container> <object\n")
		fmt.Fprintf(os.Stderr, "hummingbird nodes [-a] -P policy_name <account>[/<container>[/<object]]\n")
		fmt.Fprintf(os.Stderr, "hummingbird nodes [-a] -p partition -r <ring.gz>\n")
		fmt.Fprintf(os.Stderr, "hummingbird nodes [-a] -p partition -P policy_name\n")
		nodesFlags.PrintDefaults()
	}

	andrewdFlags := flag.NewFlagSet("andrewd", flag.ExitOnError)
	andrewdFlags.String("c", findConfig("andrewd"), "Config file to use")
	andrewdFlags.String("l", "stdout", "Log location")
	andrewdFlags.String("e", "stderr", "Error log location")
	andrewdFlags.Bool("once", false, "Run one pass of the tools")
	andrewdFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird andrewd [ARGS]")
		fmt.Fprintln(os.Stderr, "  An automated-admin daemon. Should be run from a")
		fmt.Fprintln(os.Stderr, "  single admin location with network access to")
		fmt.Fprintln(os.Stderr, "  backend servers and the a/c/o rings.")
		fmt.Fprintln(os.Stderr, "  A sample config is:")
		fmt.Fprintln(os.Stderr, "  [andrewd]")
		fmt.Fprintln(os.Stderr, "  user = hummingbird")
		fmt.Fprintln(os.Stderr, "")
		andrewdFlags.PrintDefaults()
	}

	objectInfoFlags := flag.NewFlagSet("", flag.ExitOnError)
	objectInfoFlags.Bool("n", false, "Don't verify file contents against stored etag")
	objectInfoFlags.String("P", "", "Specify which policy to use")
	objectInfoFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird oinfo [ARGS] OBJECT_FILE\n")
		objectInfoFlags.PrintDefaults()
	}

	reconFlags := flag.NewFlagSet("", flag.ExitOnError)
	reconFlags.Bool("progress", false, "Show andrewd progress report; state of internal processes")
	reconFlags.Bool("md5", false, "Get md5sum of servers ring and compare to local copy")
	reconFlags.Bool("time", false, "Check time synchronization")
	reconFlags.Bool("q", false, "Get cluster quarantine stats")
	reconFlags.Bool("qd", false, "Get cluster quarantine detailed report")
	reconFlags.Bool("a", false, "Get cluster async pending stats")
	reconFlags.Bool("rd", false, "Get cluster replication pass duration stats")
	reconFlags.Bool("rp", false, "Get cluster replication partition/sec stats")
	reconFlags.Bool("rc", false, "List all drives with replicator cancellations")
	reconFlags.Bool("d", false, "Show last dispersion report")
	reconFlags.Bool("ds", false, "Show drive status report")
	reconFlags.Bool("rar", false, "Show andrewd ring action report")
	reconFlags.String("c", findConfig("andrewd"), "Andrewd Config file to use (e.g. for dispersion)")
	reconFlags.Bool("json", false, "Output in json. {\"ok\": true|false, \"msg\": \"text-output\"}")
	reconFlags.String("certfile", "", "Cert file to use for setting up https client")
	reconFlags.String("keyfile", "", "Key file to use for setting up https client")
	reconFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird recon [ARGS] \n")
		reconFlags.PrintDefaults()
	}

	/* main flag parser, which doesn't do much */

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Hummingbird Usage")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "The built-in process control is for entertainment purposes only. Please use a real service manager.")
		fmt.Fprintln(os.Stderr, "     hummingbird start [daemon name]    -- start a server")
		fmt.Fprintln(os.Stderr, "     hummingbird stop [daemon name]     -- stop a server immediately")
		fmt.Fprintln(os.Stderr, "     hummingbird shutdown [daemon name] -- gracefully stop a server")
		fmt.Fprintln(os.Stderr, "     hummingbird reload [daemon name]   -- alias for graceful-restart")
		fmt.Fprintln(os.Stderr, "     hummingbird restart [daemon name]  -- stop then restart a server")
		fmt.Fprintln(os.Stderr, "  The daemons are: object, proxy, object-replicator, andrewd, all, main")
		fmt.Fprintln(os.Stderr)
		objectFlags.Usage()
		fmt.Fprintln(os.Stderr)
		objectReplicatorFlags.Usage()
		fmt.Fprintln(os.Stderr)
		ringBuilderFlags.Usage()
		fmt.Fprintln(os.Stderr)
		proxyFlags.Usage()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird moveparts [old ring.gz]")
		fmt.Fprintln(os.Stderr, "  Prioritize replication for moving partitions after a ring change")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird restoredevice [ip] [device-name]")
		fmt.Fprintln(os.Stderr, "  Reconstruct a device from its peers")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird bench CONFIG")
		fmt.Fprintln(os.Stderr, "  Run bench tool")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird dbench CONFIG")
		fmt.Fprintln(os.Stderr, "  Run direct to object server bench tool")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird thrash CONFIG")
		fmt.Fprintln(os.Stderr, "  Run thrash bench tool")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird grep [ACCOUNT/CONTAINER/PREFIX] [SEARCH-STRING]")
		fmt.Fprintln(os.Stderr, "  Run grep on the edge")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird init [deb | haio]")
		fmt.Fprintln(os.Stderr, "  Creates a script to initialize a system for use with Hummingbird. This ")
		fmt.Fprintln(os.Stderr, "  will create the standard directories, systemd service files, etc.")
		fmt.Fprintln(os.Stderr, "  The deb option will create a script for building a Debian package that,")
		fmt.Fprintln(os.Stderr, "  when installed, will do the same work as the init script.")
		fmt.Fprintln(os.Stderr, "  The haio option will create a script to do similar actions, but for a")
		fmt.Fprintln(os.Stderr, "  Hummingbird All In One developer installation.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "hummingbird nectar ...")
		fmt.Fprintln(os.Stderr, "  Runs an embedded version of the nectar client tool.")
		fmt.Fprintln(os.Stderr, "  Run with no parameters for help.")
		fmt.Fprintln(os.Stderr)
		nodesFlags.Usage()
		fmt.Fprintln(os.Stderr)
		andrewdFlags.Usage()
		fmt.Fprintln(os.Stderr)
		objectInfoFlags.Usage()
		fmt.Fprintln(os.Stderr)
		reconFlags.Usage()
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		return
	}

	switch flag.Arg(0) {
	case "version":
		fmt.Println(common.Version)
	case "start":
		processControlCommand(startServer)
	case "stop":
		processControlCommand(stopServer)
	case "restart":
		processControlCommand(restartServer)
	case "reload", "graceful-restart":
		processControlCommand(gracefulRestartServer)
	case "shutdown", "graceful-shutdown":
		processControlCommand(gracefulShutdownServer)
	case "proxy":
		proxyFlags.Parse(flag.Args()[1:])
		srv.RunServers(proxyserver.NewServer, proxyFlags)
	case "container":
		containerFlags.Parse(flag.Args()[1:])
		srv.RunServers(containerserver.NewServer, containerFlags)
	case "container-replicator":
		containerReplicatorFlags.Parse(flag.Args()[1:])
		srv.RunServers(containerserver.NewReplicator, containerReplicatorFlags)
	case "account":
		accountFlags.Parse(flag.Args()[1:])
		srv.RunServers(accountserver.NewServer, accountFlags)
	case "account-replicator":
		accountReplicatorFlags.Parse(flag.Args()[1:])
		srv.RunServers(accountserver.NewReplicator, accountReplicatorFlags)
	case "object":
		objectFlags.Parse(flag.Args()[1:])
		srv.RunServers(objectserver.NewServer, objectFlags)
	case "object-replicator":
		objectReplicatorFlags.Parse(flag.Args()[1:])
		srv.RunServers(objectserver.NewReplicator, objectReplicatorFlags)
	case "bench":
		bench.RunBench(flag.Args()[1:])
	case "dbench":
		bench.RunDBench(flag.Args()[1:])
	case "cbench":
		bench.RunCBench(flag.Args()[1:])
	case "cgbench":
		bench.RunCGBench(flag.Args()[1:])
	case "thrash":
		bench.RunThrash(flag.Args()[1:])
	case "moveparts":
		objectserver.MoveParts(flag.Args()[1:], srv.DefaultConfigLoader{})
	case "restoredevice":
		objectserver.RestoreDevice(flag.Args()[1:], srv.DefaultConfigLoader{})
	case "ring":
		ringBuilderFlags.Parse(flag.Args()[1:])
		tools.RingBuildCmd(ringBuilderFlags)
	case "nodes":
		nodesFlags.Parse(flag.Args()[1:])
		tools.Nodes(nodesFlags, srv.DefaultConfigLoader{})
	case "andrewd":
		andrewdFlags.Parse(flag.Args()[1:])
		srv.RunServers(tools.NewAdmin, andrewdFlags)
	case "oinfo":
		objectInfoFlags.Parse(flag.Args()[1:])
		tools.ObjectInfo(objectInfoFlags, srv.DefaultConfigLoader{})
	case "recon":
		reconFlags.Parse(flag.Args()[1:])
		if pass := tools.ReconClient(reconFlags, srv.DefaultConfigLoader{}); !pass {
			os.Exit(1)
		}
	case "init":
		if err := initCommand(flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "init error:", err)
			os.Exit(1)
		}
	case "systemd":
		if err := systemdCommand(flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "systemd error:", err)
			os.Exit(1)
		}
	case "nectar":
		nectar.CLI(flag.Args(), nil, nil, nil)
	default:
		flag.Usage()
	}
}
