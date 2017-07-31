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
	case "proxy", "object", "object-replicator", "object-auditor", "container", "container-replicator", "account", "account-replicator":
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
		for _, server := range []string{"proxy", "object", "object-replicator", "object-auditor",
			"container", "container-replicator", "account", "account-replicator"} {
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

	objectAuditorFlags := flag.NewFlagSet("object auditor", flag.ExitOnError)
	objectAuditorFlags.String("c", findConfig("object"), "Config file/directory to use")
	objectAuditorFlags.String("l", "stdout", "Log location")
	objectAuditorFlags.String("e", "stderr", "Error log location")
	objectAuditorFlags.Bool("once", false, "Run one pass of the auditor")
	objectAuditorFlags.Usage = func() {
		fmt.Fprintln(os.Stderr, "hummingbird object-auditor [ARGS]")
		fmt.Fprintln(os.Stderr, "  Run object auditor")
		objectAuditorFlags.PrintDefaults()
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
	ringBuilderFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird ring <builder_file> command\n")
		fmt.Fprintf(os.Stderr, "  Builds a swift style ring.  Commands are:\n")
		fmt.Fprintf(os.Stderr, "    create <part_power> <replicas> <min_part_hours> (create a new ring)\n")
		fmt.Fprintf(os.Stderr, "    add <device> <weight> (add a new device to the ring)\n")
		fmt.Fprintf(os.Stderr, "    rebalance (rebalance the ring)\n")
		fmt.Fprintf(os.Stderr, "    search <search_flags> (search for devices in the ring)\n")
		fmt.Fprintf(os.Stderr, "    set_weight <search_flags> [-yes] <weight> (change the weight of 1 or more devices)\n")
		fmt.Fprintf(os.Stderr, "    remove <search_flags> [-yes] (remove device from the ring)\n")
		fmt.Fprintf(os.Stderr, "    set_info <search_flags> [-yes] <change_flags> (change device information)\n")
		fmt.Fprintf(os.Stderr, "    info (display ring info)\n")
		fmt.Fprintf(os.Stderr, "  <device> is of the form: [r<region>]z<zone>-<ip>:<port>[R<r_ip>:<r_port>]/<device_name>_<meta>\n")
		fmt.Fprintf(os.Stderr, "  <search_flags> is at least one of: -region, -zone, -ip, -port, -replication-ip, replication-port, -device, -meta, -weight\n")
		fmt.Fprintf(os.Stderr, "  <change_flags> is at least one of: -change-ip, -change-port, -change-replication-ip, -change-replication-port, -change-device, -change-meta")
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
		fmt.Fprintln(os.Stderr, "  The daemons are: object, proxy, object-replicator, object-auditor, all, main")
		fmt.Fprintln(os.Stderr)
		objectFlags.Usage()
		fmt.Fprintln(os.Stderr)
		objectReplicatorFlags.Usage()
		fmt.Fprintln(os.Stderr)
		objectAuditorFlags.Usage()
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
		fmt.Fprintln(os.Stderr, "hummingbird rescueparts [partnum1,partnum2,...]")
		fmt.Fprintln(os.Stderr, "  Will send requests to all the object nodes to try to fully replicate given partitions if they have them.")
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
		nodesFlags.Usage()
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
		srv.RunServers(proxyserver.GetServer, proxyFlags)
	case "container":
		containerFlags.Parse(flag.Args()[1:])
		srv.RunServers(containerserver.GetServer, containerFlags)
	case "container-replicator":
		containerReplicatorFlags.Parse(flag.Args()[1:])
		srv.RunDaemon(containerserver.GetReplicator, containerReplicatorFlags)
	case "account":
		accountFlags.Parse(flag.Args()[1:])
		srv.RunServers(accountserver.GetServer, accountFlags)
	case "account-replicator":
		accountReplicatorFlags.Parse(flag.Args()[1:])
		srv.RunDaemon(accountserver.GetReplicator, accountReplicatorFlags)
	case "object":
		objectFlags.Parse(flag.Args()[1:])
		srv.RunServers(objectserver.GetServer, objectFlags)
	case "object-replicator":
		objectReplicatorFlags.Parse(flag.Args()[1:])
		srv.RunDaemon(objectserver.NewReplicator, objectReplicatorFlags)
	case "object-auditor":
		objectAuditorFlags.Parse(flag.Args()[1:])
		srv.RunDaemon(objectserver.NewAuditor, objectAuditorFlags)
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
		objectserver.MoveParts(flag.Args()[1:])
	case "restoredevice":
		objectserver.RestoreDevice(flag.Args()[1:])
	case "rescueparts":
		objectserver.RescueParts(flag.Args()[1:])
	case "ring":
		ringBuilderFlags.Parse(flag.Args()[1:])
		tools.RingBuildCmd(ringBuilderFlags)
	case "nodes":
		nodesFlags.Parse(flag.Args()[1:])
		tools.Nodes(nodesFlags)
	default:
		flag.Usage()
	}
}
