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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
)

func getAffixes() (string, string) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to get hash prefix and suffix")
		os.Exit(1)
	}
	return hashPathPrefix, hashPathSuffix
}

func getRing(ringPath string, policyNum int) (ring.Ring, string) {
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

	r, err := ring.GetRing("object", prefix, suffix, policyNum)
	if err != nil {
		fmt.Printf("Unable to load object-%v ring\n", policyNum)
		os.Exit(1)
	}
	return r, "object"
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
			fmt.Printf("ssh %s \"ls -lah ${DEVICE:-/srv/node*}/%v/%v\" # [Handoff]", v.Ip, v.Device, storageDirectory(datadir, partNum, pathHash))
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
	if account == "" && (container != "" || object != "") {
		fmt.Println("No account specified")
		os.Exit(1)
	}
	if container == "" && object != "" {
		fmt.Println("No container specified")
		os.Exit(1)
	}
	if account == "" && partition == "" {
		fmt.Println("No target specified")
		os.Exit(1)
	}
	location := ""
	if partition != "" {
		if policy > 0 {
			location = fmt.Sprintf("%v-%d", ringType, policy)
		} else {
			location = fmt.Sprintf("%vs", ringType)
		}
	}
	if account != "" && container != "" && object != "" {
		if policy > 0 {
			location = fmt.Sprintf("objects-%d", policy)
		} else {
			location = "objects"
		}
	}
	if account != "" && container != "" && object == "" {
		location = "containers"
		if ringType != "container" {
			fmt.Println("Warning: container specified but ring not named \"container\"")
		}
	}
	if account != "" && container == "" && object == "" {
		location = "accounts"
		if ringType != "account" {
			fmt.Println("Warning: account specified but ring not named \"account\"")
		}
	}
	fmt.Printf("\nAccount  \t%v\n", account)
	fmt.Printf("Container\t%v\n", container)
	fmt.Printf("Object   \t%v\n", object)

	printRingLocations(r, ringType, location, account, container, object, partition, allHandoffs, policy)
}

func GetNodes(flags *flag.FlagSet) {
	ringPath := flags.Arg(0)
	account := flags.Arg(1)
	container := flags.Arg(2)
	object := flags.Arg(3)
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

	r, ringType := getRing(ringPath, policyNum)
	printItemLocations(r, ringType, account, container, object, partition, allHandoffs, policyNum)
}
