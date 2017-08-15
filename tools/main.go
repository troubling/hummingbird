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

package tools

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"

	"github.com/justinas/alice"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
)

func getAffixes() (string, string) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to get hash prefix and suffix")
		os.Exit(1)
	}
	return hashPathPrefix, hashPathSuffix
}

func inferRingType(account, container, object string) string {
	if object != "" {
		return "object"
	} else if container != "" {
		return "container"
	} else if account != "" {
		return "account"
	}
	return ""
}

func getRing(ringPath, ringType string, policyNum int) (ring.Ring, string) {
	// if you have a direct path to a ring send it as ringPath. otherwise send
	// a ringType ("" defaults to 'object') and optional policy and this'll
	// try to find it in usual spots
	prefix, suffix := getAffixes()
	if ringPath != "" {
		r, err := ring.LoadRing(ringPath, prefix, suffix)
		if err != nil {
			fmt.Println("Unable to load ring ", ringPath)
			os.Exit(1)
		}
		if strings.Contains(ringPath, "account") {
			return r, "account"
		} else if strings.Contains(ringPath, "container") {
			return r, "container"
		} else if strings.Contains(ringPath, "object") {
			return r, "object"
		} else {
			fmt.Println("Unknown ring type", ringPath)
			os.Exit(1)
		}
	}

	if ringType == "" {
		ringType = "object"
	}

	r, err := ring.GetRing(ringType, prefix, suffix, policyNum)
	if err != nil {
		if policyNum > 0 {
			fmt.Printf("Unable to load %v-%v ring\n", ringType, policyNum)
		} else {
			fmt.Printf("Unable to load %v ring\n", ringType)
		}
		os.Exit(1)
	}
	return r, ringType
}

func policyByName(policyName string) *conf.Policy {
	if policyName == "" {
		return nil
	}

	policyList := conf.LoadPolicies()
	for _, v := range policyList {
		if v.Name == policyName {
			return v
		}
	}

	fmt.Println("No policy named ", policyName)
	os.Exit(1)
	return nil
}

func storageDirectory(datadir string, partNum uint64, nameHash string) string {
	partition := fmt.Sprintf("%v", partNum)
	return filepath.Join(datadir, partition, nameHash[len(nameHash)-3:], nameHash)
}

func curlHeadCommand(ipStr string, port int, device string, partNum uint64, target string, policy int) string {
	formatted_ip := ipStr
	ip := net.ParseIP(ipStr)
	if ip != nil && strings.Contains(ipStr, ":") {
		formatted_ip = fmt.Sprintf("[%v]", ipStr)
	}
	policyStr := ""
	if policy > 0 {
		policyStr = fmt.Sprintf(" -H \"X-Backend-Storage-Policy-Index: %v\"", policy)
	}
	return fmt.Sprintf("curl -g -I -XHEAD \"http://%v:%v/%v/%v/%v\"%v", formatted_ip, port, device, partNum, common.Urlencode(target), policyStr)
}

func getPathHash(ringType, account, container, object string) string {
	prefix, suffix := getAffixes()
	if object != "" && container == "" {
		fmt.Println("container is required if object is provided")
		os.Exit(1)
	}
	paths := prefix + "/" + account
	if container != "" {
		paths = paths + "/" + container
	}
	if object != "" {
		paths = paths + "/" + object
	}
	paths = paths + suffix
	h := md5.New()
	fmt.Fprintf(h, "%v", paths)
	return fmt.Sprintf("%032x", h.Sum(nil))
}

func printRingLocations(r ring.Ring, ringType, datadir, account, container, object, partition string, allHandoffs bool, policy int) {
	if r == nil {
		fmt.Println("No ring specified")
		os.Exit(1)
	}
	if datadir == "" {
		fmt.Println("No datadir specified")
		os.Exit(1)
	}
	var target string
	if object != "" {
		target = fmt.Sprintf("%v/%v/%v", account, container, object)
	} else if container != "" {
		target = fmt.Sprintf("%v/%v", account, container)
	} else {
		target = fmt.Sprintf("%v", account)
	}
	var partNum uint64
	if partition != "" {
		var err error
		partNum, err = strconv.ParseUint(partition, 10, 64)
		if err != nil {
			fmt.Println("Invalid partition")
			os.Exit(1)
		}
	} else {
		partNum = r.GetPartition(account, container, object)
	}
	primaries := r.GetNodes(partNum)
	handoffLimit := len(primaries)
	if allHandoffs {
		handoffLimit = -1
	}

	pathHash := ""
	if account != "" && partition == "" {
		pathHash = getPathHash(ringType, account, container, object)
	}
	fmt.Printf("Partition\t%v\n", partNum)
	fmt.Printf("Hash     \t%v\n\n", pathHash)

	for _, v := range primaries {
		fmt.Printf("Server:Port Device\t%v:%v %v\n", v.Ip, v.Port, v.Device)
	}
	handoffs := r.GetMoreNodes(partNum)
	for i, v := 0, handoffs.Next(); v != nil; i, v = i+1, handoffs.Next() {
		if handoffLimit != -1 && i == handoffLimit {
			break
		}
		fmt.Printf("Server:Port Device\t%v:%v %v\t [Handoff]\n", v.Ip, v.Port, v.Device)
	}
	fmt.Printf("\n\n")
	for _, v := range primaries {
		cmd := curlHeadCommand(v.Ip, v.Port, v.Device, partNum, target, policy)
		fmt.Println(cmd)
	}
	handoffs = r.GetMoreNodes(partNum)
	for i, v := 0, handoffs.Next(); v != nil; i, v = i+1, handoffs.Next() {
		if handoffLimit != -1 && i == handoffLimit {
			break
		}
		cmd := curlHeadCommand(v.Ip, v.Port, v.Device, partNum, target, policy)
		fmt.Printf("%v # [Handoff]\n", cmd)
	}

	fmt.Printf("\n\nUse your own device location of servers:\n")
	fmt.Printf("such as \"export DEVICE=/srv/node\"\n")

	if pathHash != "" {
		for _, v := range primaries {
			fmt.Printf("ssh %s \"ls -lah ${DEVICE:-/srv/node*}/%v/%v\"\n", v.Ip, v.Device, storageDirectory(datadir, partNum, pathHash))
		}
		handoffs = r.GetMoreNodes(partNum)
		for i, v := 0, handoffs.Next(); v != nil; i, v = i+1, handoffs.Next() {
			if handoffLimit != -1 && i == handoffLimit {
				break
			}
			fmt.Printf("ssh %s \"ls -lah ${DEVICE:-/srv/node*}/%v/%v\" # [Handoff]\n", v.Ip, v.Device, storageDirectory(datadir, partNum, pathHash))
		}
	} else {
		for _, v := range primaries {
			fmt.Printf("ssh %s \"ls -lah ${DEVICE:-/srv/node*}/%v/%v/%v\"\n", v.Ip, v.Device, datadir, partNum)
		}
		handoffs = r.GetMoreNodes(partNum)
		for i, v := 0, handoffs.Next(); v != nil; i, v = i+1, handoffs.Next() {
			if handoffLimit != -1 && i == handoffLimit {
				break
			}
			fmt.Printf("ssh %s \"ls -lah ${DEVICE:-/srv/node*}/%v/%v/%v\" # [Handoff]\n", v.Ip, v.Device, datadir, partNum)
		}
	}

	fmt.Printf("\nnote: `/srv/node*` is used as default value of `devices`, the real value is set in the config file on each storage node.\n")
}

func printItemLocations(r ring.Ring, ringType, account, container, object, partition string, allHandoffs bool, policy int) {
	location := ""
	if policy > 0 {
		location = fmt.Sprintf("%vs-%d", ringType, policy)
	} else {
		location = fmt.Sprintf("%vs", ringType)
	}
	fmt.Printf("\nAccount  \t%v\n", account)
	fmt.Printf("Container\t%v\n", container)
	fmt.Printf("Object   \t%v\n", object)

	printRingLocations(r, ringType, location, account, container, object, partition, allHandoffs, policy)
}

func parseArg0(arg0 string) (string, string, string) {
	arg0 = strings.TrimPrefix(arg0, "/v1/")
	parts := strings.SplitN(arg0, "/", 3)
	if len(parts) == 1 {
		return parts[0], "", ""
	} else if len(parts) == 2 {
		return parts[0], parts[1], ""
	} else if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	return arg0, "", ""
}

func Nodes(flags *flag.FlagSet) {
	var account, container, object string
	if flags.NArg() == 1 {
		account, container, object = parseArg0(flags.Arg(0))
	} else {
		account = flags.Arg(0)
		container = flags.Arg(1)
		object = flags.Arg(2)
	}
	partition := flags.Lookup("p").Value.(flag.Getter).Get().(string)
	policyName := flags.Lookup("P").Value.(flag.Getter).Get().(string)
	allHandoffs := flags.Lookup("a").Value.(flag.Getter).Get().(bool)
	policy := policyByName(policyName)
	var policyNum int
	if policy != nil {
		policyNum = policy.Index
	} else {
		policyNum = 0
	}

	var r ring.Ring
	var ringType string
	inferredType := inferRingType(account, container, object)
	ringPath := flags.Lookup("r").Value.(flag.Getter).Get().(string)
	if ringPath != "" {
		r, ringType = getRing(ringPath, "", policyNum)
		if inferredType != "" && ringType != inferredType {
			fmt.Printf("Error %v specified but ring type: %v\n", inferredType, ringType)
			os.Exit(1)
		}
		if ringType == "object" && policyNum == 0 {
			_, ringFileName := path.Split(ringPath)
			if strings.HasPrefix(ringFileName, "object") && strings.Contains(ringFileName, "-") {
				polSuff := strings.Split(ringFileName, "-")[1]
				if polN, err := strconv.ParseInt(polSuff[:(len(polSuff)-len(".ring.gz"))], 10, 64); err == nil {
					policyNum = int(polN)
				}
			}
		}
	} else {
		r, ringType = getRing("", inferredType, policyNum)
	}

	if partition != "" {
		account = ""
		container = ""
		object = ""
	} else {
		if account == "" && (container != "" || object != "") {
			fmt.Println("No account specified")
			os.Exit(1)
		}
		if container == "" && object != "" {
			fmt.Println("No container specified")
			os.Exit(1)
		}
		if account == "" {
			fmt.Println("No target specified")
			os.Exit(1)
		}
	}

	printItemLocations(r, ringType, account, container, object, partition, allHandoffs, policyNum)
}

type AutoAdmin struct {
	logger         srv.LowLevelLogger
	port           int
	bindIp         string
	workDir        string
	hClient        client.ProxyClient
	policies       conf.PolicyList
	metricsScope   tally.Scope
	metricsCloser  io.Closer
	bc             *BirdCatcher
	runningForever bool
}

func (a *AutoAdmin) populateDispersion() {
	if !putDispersionAccount(a.hClient, a.logger) {
		return
	}
	if !putDispersionContainers(a.hClient, a.logger) {
		return
	}
	for _, pol := range a.policies {
		if !pol.Deprecated {
			if !putDispersionObjects(a.hClient, pol, a.logger) {
				return
			}
		}
	}
}

func (a *AutoAdmin) Run() {
	a.populateDispersion()
	if !a.runningForever {
		a.bc.runDispersionOnce()
	}
}

func (a *AutoAdmin) RunForever() {
	go a.startWebServer()
	go a.bc.runDispersionForever()
	a.runningForever = true
	for {
		a.Run()
		time.Sleep(60 * time.Second)
	}
}

func (a *AutoAdmin) GetHandler() http.Handler {
	router := srv.NewRouter()
	router.Get("/metrics", prometheus.Handler())

	return alice.New(middleware.Metrics(a.metricsScope)).Then(router)
}

func (a *AutoAdmin) startWebServer() {
	for {
		if sock, err := srv.RetryListen(a.bindIp, a.port); err != nil {
			a.logger.Error("Listen failed", zap.Error(err))
		} else {
			http.Serve(sock, a.GetHandler())
		}
	}
}

func NewAdmin(serverconf conf.Config, flags *flag.FlagSet) (srv.Daemon, srv.LowLevelLogger, error) {

	if !serverconf.HasSection("andrewd") {
		return nil, nil, fmt.Errorf("Unable to find andrewd config section")
	}
	pdc, err := client.NewProxyDirectClient(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("Could not make client: %v", err)
	}
	a := &AutoAdmin{
		hClient:        client.NewProxyClient(pdc, nil, nil),
		port:           int(serverconf.GetInt("andrewd", "bind_port", 7000)),
		bindIp:         serverconf.GetDefault("andrewd", "bind_ip", "0.0.0.0"),
		workDir:        serverconf.GetDefault("andrewd", "work_dir", "/var/cache/swift"),
		policies:       conf.LoadPolicies(),
		runningForever: false,
		//containerDispersionGauge: []tally.Gauge{}, TODO- add container disp
	}

	a.metricsScope, a.metricsCloser = tally.NewRootScope(tally.ScopeOptions{
		Prefix:         "hb_andrewd",
		Tags:           map[string]string{},
		CachedReporter: promreporter.NewReporter(promreporter.Options{}),
		Separator:      promreporter.DefaultSeparator,
	}, time.Second)

	logLevelString := serverconf.GetDefault("andrewd", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	if a.logger, err = srv.SetupLogger("andrewd", &logLevel, flags); err != nil {
		return nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	a.bc = NewBirdCatcher(
		a.logger, client.NewProxyClient(pdc, nil, nil), a.metricsScope)

	return a, a.logger, nil
}
