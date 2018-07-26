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

package objectserver

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"golang.org/x/net/http2"
)

type devLimiter struct {
	inUse             map[int]int
	m                 sync.Mutex
	max               int
	somethingFinished chan struct{}
}

func (d *devLimiter) start(j *PriorityRepJob) bool {
	d.m.Lock()
	doable := d.inUse[j.FromDevice.Id] < d.max
	doable = doable && d.inUse[j.ToDevice.Id] < d.max
	if doable {
		d.inUse[j.FromDevice.Id] += 1
		d.inUse[j.ToDevice.Id] += 1
	}
	d.m.Unlock()
	return doable
}

func (d *devLimiter) finished(j *PriorityRepJob) {
	d.m.Lock()
	d.inUse[j.FromDevice.Id] -= 1
	d.inUse[j.ToDevice.Id] -= 1
	d.m.Unlock()
	select {
	case d.somethingFinished <- struct{}{}:
	default:
	}
}

func (d *devLimiter) waitForSomethingToFinish() {
	<-d.somethingFinished
}

func SendPriRepJob(job *PriorityRepJob, client common.HTTPClient, userAgent string) (string, bool) {
	url := fmt.Sprintf("%s://%s:%d/priorityrep", job.FromDevice.Scheme, job.FromDevice.ReplicationIp, job.FromDevice.ReplicationPort)
	jsonned, err := json.Marshal(job)
	if err != nil {
		return fmt.Sprintf("Failed to serialize job for some reason: %s", err), false
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonned))
	if err != nil {
		return fmt.Sprintf("Failed to create request for some reason: %s", err), false
	}
	req.Header.Set("User-Agent", userAgent)
	req.ContentLength = int64(len(jsonned))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Sprintf("Error moving partition %d: %v",
			job.Partition, err), false
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		if data, err := ioutil.ReadAll(resp.Body); err == nil {
			prp := PriorityReplicationResult{}
			if err = json.Unmarshal(data, &prp); err == nil {
				return fmt.Sprintf("Replicating partition %d from %s/%s to %s/%s replicated %d objects with %d errors",
					job.Partition, job.FromDevice.Ip, job.FromDevice.Device, job.ToDevice.Ip, job.ToDevice.Device,
					prp.ObjectsReplicated, prp.ObjectsErrored), prp.Success
			} else {
				return fmt.Sprintf("could not get valid response for partition %d: %v",
					job.Partition, err), false
			}
		} else {
			return fmt.Sprintf("could not  read body forpartition %d: %v",
				job.Partition, err), false
		}
	} else if resp.StatusCode == 404 {
		return fmt.Sprintf("partition %d: not found on %s/%s",
			job.Partition, job.FromDevice.Ip, job.FromDevice.Device), true
	}
	return fmt.Sprintf("Bad status code moving partition %d: %d",
		job.Partition, resp.StatusCode), false
}

// doPriRepJobs executes a list of PriorityRepJobs, limiting concurrent jobs per device to deviceMax.
func doPriRepJobs(jobs []*PriorityRepJob, deviceMax int, client common.HTTPClient, userAgent string) []uint64 {
	limiter := &devLimiter{inUse: make(map[int]int), max: deviceMax, somethingFinished: make(chan struct{}, 1)}
	wg := sync.WaitGroup{}
	badParts := []uint64{}
	var badPartsLock sync.Mutex
	for len(jobs) > 0 {
		foundDoable := false
		for i := range jobs {
			if !limiter.start(jobs[i]) {
				continue
			}
			foundDoable = true
			wg.Add(1)
			go func(job *PriorityRepJob) {
				defer wg.Done()
				defer limiter.finished(job)
				res, ok := SendPriRepJob(job, client, userAgent)
				fmt.Println(res)
				if !ok {
					badPartsLock.Lock()
					badParts = append(badParts, job.Partition)
					badPartsLock.Unlock()
				}
			}(jobs[i])
			jobs = append(jobs[:i], jobs[i+1:]...)
			break
		}
		if !foundDoable {
			limiter.waitForSomethingToFinish()
		}
	}
	wg.Wait()
	return badParts
}

// getPartMoveJobs takes two rings and creates a list of jobs for any partition moves between them.
func getPartMoveJobs(oldRing, newRing ring.Ring, overrideParts []uint64, policy int) []*PriorityRepJob {
	allNewDevices := map[string]bool{}
	for _, dev := range newRing.AllDevices() {
		if dev == nil {
			continue
		}
		allNewDevices[fmt.Sprintf("%s:%d/%s", dev.Ip, dev.Port, dev.Device)] = true
	}
	jobs := make([]*PriorityRepJob, 0)
	for i := uint64(0); true; i++ {
		partition := i
		if len(overrideParts) > 0 {
			if int(partition) < len(overrideParts) {
				partition = overrideParts[partition]
			} else {
				break
			}
		}
		olddevs := oldRing.GetNodes(partition)
		newdevs := newRing.GetNodes(partition)
		if olddevs == nil || newdevs == nil {
			break
		}
		for i := range olddevs {
			if olddevs[i].Id != newdevs[i].Id {
				// TODO: handle if a node just changes positions, which doesn't happen, but isn't against the contract.
				fromDev := olddevs[i]
				if _, ok := allNewDevices[fmt.Sprintf("%s:%d/%s", fromDev.Ip, fromDev.Port, fromDev.Device)]; !ok {
					fromDev = olddevs[(i+1)%len(olddevs)]
				}
				jobs = append(jobs, &PriorityRepJob{
					Partition:  partition,
					FromDevice: fromDev,
					ToDevice:   newdevs[i],
					Policy:     policy,
				})
			}
		}
	}
	return jobs
}

func objectRingPolicyIndex(s string) (int, error) {
	if !strings.Contains(s, "object") {
		return 0, fmt.Errorf("object not in string: %v", s)
	}
	re := regexp.MustCompile(`object-(\d*)`)
	match := re.FindStringSubmatch(s)
	if match == nil {
		return 0, nil
	} else {
		policyIdx, err := strconv.Atoi(match[1])
		if err != nil {
			return 0, fmt.Errorf("invalid policy index: %v\n", match[1])
		}
		return policyIdx, nil
	}
}

func doMoveParts(args []string, cnf srv.ConfigLoader) int {
	flags := flag.NewFlagSet("moveparts", flag.ExitOnError)
	policyName := flags.String("P", "", "policy to use")
	certFile := flags.String("certfile", "", "Cert file to use for setting up https client")
	keyFile := flags.String("keyfile", "", "Key file to use for setting up https client")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird moveparts [old ringfile]\n")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 1 {
		flags.Usage()
		return 1
	}
	policyIndex := 0
	if *policyName != "" {
		policies, err := conf.GetPolicies()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Unable to load policies:", err)
			return 1
		}
		p := policies.NameLookup(*policyName)
		if p == nil {
			fmt.Fprintf(os.Stderr, "Unknown policy named %q\n", *policyName)
			return 1
		}
		policyIndex = p.Index
	}
	oldPolicy, policyErr := objectRingPolicyIndex(flags.Arg(0))
	if policyErr != nil {
		fmt.Println("Invalid ring:", policyErr)
		return 1
	}

	if oldPolicy != policyIndex {
		fmt.Printf("Old policy: %v doesn't match specified policy: %v\n", oldPolicy, policyIndex)
		return 1
	}

	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return 1
	}
	oldRing, err := ring.LoadRing(flags.Arg(0), hashPathPrefix, hashPathSuffix)
	if err != nil {
		fmt.Println("Unable to load old ring:", err)
		return 1
	}
	curRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, policyIndex)
	if err != nil {
		fmt.Println("Unable to load current ring:", err)
		return 1
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
	}
	if *certFile != "" && *keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(*certFile, *keyFile)
		if err != nil {
			fmt.Println("Error getting TLS config:", err)
			return 1
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			fmt.Println("Error setting up http2:", err)
			return 1
		}
	}
	// TODO: Do we want to trace requests with this client?
	client := &http.Client{Timeout: time.Hour,
		Transport: transport,
	}
	badParts := []uint64{}
	for {
		jobs := getPartMoveJobs(oldRing, curRing, badParts, policyIndex)
		lastRun := len(jobs)
		for i := len(jobs) - 1; i > 0; i-- { // shuffle jobs list
			j := rand.Intn(i + 1)
			jobs[j], jobs[i] = jobs[i], jobs[j]
		}
		fmt.Println("Job count:", len(jobs))
		badParts = doPriRepJobs(jobs, 2, client, "doMoveParts")
		if len(badParts) == 0 {
			break
		} else {
			fmt.Printf("Finished run of partitions. retrying %d.\n", len(badParts))
			fmt.Println("NOTE: This will loop on any partitions not found on any primary")
			if lastRun == len(badParts) {
				time.Sleep(time.Minute * 5)
			} else {
				time.Sleep(time.Second * 5)
			}
		}
	}
	fmt.Println("Done sending jobs.")
	return 0
}

// MoveParts takes two object .ring.gz files as []string{oldRing, newRing} and dispatches priority replication jobs to rebalance data in line with any ring changes.
func MoveParts(args []string, cnf srv.ConfigLoader) {
	ret := doMoveParts(args, cnf)
	os.Exit(ret)
}

// getRestoreDeviceJobs takes an ip address and device name, and creates a list of jobs to restore that device's data from peers.
func getRestoreDeviceJobs(theRing ring.Ring, ip string, devName string, srcRegionOnly int, allPeers bool, overrideParts []uint64, policy int) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	for i := uint64(0); true; i++ {
		partition := i
		if len(overrideParts) > 0 {
			if int(partition) < len(overrideParts) {
				partition = overrideParts[partition]
			} else {
				break
			}
		}
		devs := theRing.GetNodes(partition)
		if devs == nil {
			break
		}
		var toDev *ring.Device
		for _, dev := range devs {
			if dev.Device == devName && (dev.Ip == ip || dev.ReplicationIp == ip) {
				toDev = dev
				break
			}
		}
		if toDev != nil {
			foundJob := false
			for len(devs) > 0 {
				rd := rand.Intn(len(devs))
				src := devs[rd]
				devs = append(devs[:rd], devs[rd+1:]...)
				if src.Device == toDev.Device && (src.Ip == toDev.Ip || src.ReplicationIp == toDev.ReplicationIp) {
					continue
				}
				if srcRegionOnly >= 0 && src.Region != srcRegionOnly {
					continue
				}
				jobs = append(jobs, &PriorityRepJob{
					Partition:  partition,
					FromDevice: src,
					ToDevice:   toDev,
					Policy:     policy,
				})
				foundJob = true
				if !allPeers {
					break
				}
			}
			if !foundJob {
				fmt.Printf("Could not find job for partition: %d\n", partition)
			}
		}
	}
	return jobs
}

// RestoreDevice takes an IP address and device name such as []string{"172.24.0.1", "sda1"} and attempts to restores its data from peers.
func RestoreDevice(args []string, cnf srv.ConfigLoader) {
	flags := flag.NewFlagSet("restoredevice", flag.ExitOnError)
	policyName := flags.String("P", "", "policy to use")
	region := flags.Int("region", -1, "restore device only from peers in specified region")
	ringLoc := flags.String("r", "", "Specify which ring file to use")
	conc := flags.Int("c", 2, "limit of per device concurrency priority repl calls")
	full := flags.Bool("f", false, "send priority replicate calls to every qualifying peer primary (slow)")
	certFile := flags.String("certfile", "", "Cert file to use for setting up https client")
	keyFile := flags.String("keyfile", "", "Key file to use for setting up https client")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird restoredevice [ip] [device]\n")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 2 {
		flags.Usage()
		return
	}
	policyIndex := 0
	if *policyName != "" {
		policies, err := conf.GetPolicies()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Unable to load policies:", err)
			return
		}
		p := policies.NameLookup(*policyName)
		if p == nil {
			fmt.Fprintf(os.Stderr, "Unknown policy named %q\n", *policyName)
			return
		}
		policyIndex = p.Index
	}
	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	var objRing ring.Ring
	if *ringLoc == "" {
		objRing, err = ring.GetRing("object", hashPathPrefix, hashPathSuffix, policyIndex)
		if err != nil {
			fmt.Println("Unable to load ring:", err)
			return
		}
	} else {
		objRing, err = ring.LoadRing(*ringLoc, hashPathPrefix, hashPathSuffix)
		if err != nil {
			fmt.Println("Unable to load ring:", err)
			return
		}

	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
	}
	if *certFile != "" && *keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(*certFile, *keyFile)
		if err != nil {
			fmt.Println("Error getting TLS config:", err)
			return
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			fmt.Println("Error setting up http2:", err)
			return
		}
	}
	// TODO: Do we want to trace requests with this client?
	client := &http.Client{
		Timeout:   time.Hour * 4,
		Transport: transport,
	}
	badParts := []uint64{}
	for {
		jobs := getRestoreDeviceJobs(objRing, flags.Arg(0), flags.Arg(1), *region, *full, badParts, policyIndex)
		lastRun := len(jobs)
		fmt.Println("Job count:", len(jobs))
		for i := len(jobs) - 1; i > 0; i-- { // shuffle jobs list
			j := rand.Intn(i + 1)
			jobs[j], jobs[i] = jobs[i], jobs[j]
		}
		badParts = doPriRepJobs(jobs, *conc, client, "RestoreDevice")
		if len(badParts) == 0 {
			break
		} else {
			fmt.Printf("Finished run of partitions. retrying %d.\n", len(badParts))
			fmt.Println("NOTE: This will loop on any partitions not found on any primary")
			if lastRun == len(badParts) {
				time.Sleep(time.Minute * 5)
			} else {
				time.Sleep(time.Second * 5)
			}
		}
	}
	fmt.Println("Done sending jobs.")
}
