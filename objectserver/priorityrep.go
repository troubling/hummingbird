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
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
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
	for _, dev := range j.ToDevices {
		doable = doable && d.inUse[dev.Id] < d.max
	}
	if doable {
		d.inUse[j.FromDevice.Id] += 1
		for _, dev := range j.ToDevices {
			d.inUse[dev.Id] += 1
		}
	}
	d.m.Unlock()
	return doable
}

func (d *devLimiter) finished(j *PriorityRepJob) {
	d.m.Lock()
	d.inUse[j.FromDevice.Id] -= 1
	for _, dev := range j.ToDevices {
		d.inUse[dev.Id] -= 1
	}
	d.m.Unlock()
	select {
	case d.somethingFinished <- struct{}{}:
	default:
	}
}

func (d *devLimiter) waitForSomethingToFinish() {
	<-d.somethingFinished
}

func SendPriRepJob(job *PriorityRepJob, client *http.Client) (string, bool) {
	url := fmt.Sprintf("http://%s:%d/priorityrep", job.FromDevice.ReplicationIp, job.FromDevice.ReplicationPort)
	jsonned, err := json.Marshal(job)
	if err != nil {
		return fmt.Sprintf("Failed to serialize job for some reason: %s", err), false
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonned))
	if err != nil {
		return fmt.Sprintf("Failed to create request for some reason: %s", err), false
	}
	req.ContentLength = int64(len(jsonned))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Sprintf("Error moving partition %d: %v",
			job.Partition, err), false
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Sprintf("Bad status code moving partition %d: %d",
			job.Partition, resp.StatusCode), false
	}
	return fmt.Sprintf("Replicating partition %d from %s/%s",
		job.Partition, job.FromDevice.Ip, job.FromDevice.Device), true
}

// doPriRepJobs executes a list of PriorityRepJobs, limiting concurrent jobs per device to deviceMax.
func doPriRepJobs(jobs []*PriorityRepJob, deviceMax int, client *http.Client) []uint64 {
	limiter := &devLimiter{inUse: make(map[int]int), max: deviceMax, somethingFinished: make(chan struct{}, 1)}
	wg := sync.WaitGroup{}
	badParts := []uint64{}
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
				res, ok := SendPriRepJob(job, client)
				fmt.Println(res)
				if !ok {
					badParts = append(badParts, job.Partition)
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
func getPartMoveJobs(oldRing, newRing ring.Ring) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	for partition := uint64(0); true; partition++ {
		olddevs := oldRing.GetNodes(partition)
		newdevs := newRing.GetNodes(partition)
		if olddevs == nil || newdevs == nil {
			break
		}
		for i := range olddevs {
			if olddevs[i].Id != newdevs[i].Id {
				// TODO: handle if a node just changes positions, which doesn't happen, but isn't against the contract.
				jobs = append(jobs, &PriorityRepJob{
					Partition:  partition,
					FromDevice: olddevs[i],
					ToDevices:  []*ring.Device{newdevs[i]},
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
	policy := flags.Int("p", 0, "policy index to use")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird moveparts [old ringfile]")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 1 {
		flags.Usage()
		return 1
	}

	oldPolicy, policyErr := objectRingPolicyIndex(flags.Arg(0))
	if policyErr != nil {
		fmt.Println("Invalid ring:", policyErr)
		return 1
	}

	if oldPolicy != *policy {
		fmt.Printf("Old policy: %v doesn't match specified policy: %v\n", oldPolicy, *policy)
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
	curRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, *policy)
	if err != nil {
		fmt.Println("Unable to load current ring:", err)
		return 1
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getPartMoveJobs(oldRing, curRing)
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 2, client)
	fmt.Println("Done sending jobs.")
	return 0
}

// MoveParts takes two object .ring.gz files as []string{oldRing, newRing} and dispatches priority replication jobs to rebalance data in line with any ring changes.
func MoveParts(args []string, cnf srv.ConfigLoader) {
	ret := doMoveParts(args, cnf)
	os.Exit(ret)
}

// getRestoreDeviceJobs takes an ip address and device name, and creates a list of jobs to restore that device's data from peers.
func getRestoreDeviceJobs(theRing ring.Ring, ip string, devName string, srcRegionOnly int, allPeers bool, overrideParts []uint64) []*PriorityRepJob {
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
					ToDevices:  []*ring.Device{toDev},
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
	policy := flags.Int("p", 0, "policy index to use")
	region := flags.Int("region", -1, "restore device only from peers in specified region")
	ringLoc := flags.String("r", "", "Specify which ring file to use")
	conc := flags.Int("c", 2, "limit of per device concurrency priority repl calls")
	full := flags.Bool("f", false, "send priority replicate calls to every qualifying peer primary (slow)")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird restoredevice [ip] [device]\n")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 2 {
		flags.Usage()
		return
	}

	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	var objRing ring.Ring
	if *ringLoc == "" {
		objRing, err = ring.GetRing("object", hashPathPrefix, hashPathSuffix, *policy)
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
	client := &http.Client{Timeout: time.Hour * 4}
	badParts := []uint64{}
	for {
		jobs := getRestoreDeviceJobs(objRing, flags.Arg(0), flags.Arg(1), *region, *full, badParts)
		lastRun := len(jobs)
		fmt.Println("Job count:", len(jobs))
		for i := len(jobs) - 1; i > 0; i-- { // shuffle jobs list
			j := rand.Intn(i + 1)
			jobs[j], jobs[i] = jobs[i], jobs[j]
		}
		badParts = doPriRepJobs(jobs, *conc, client)
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

func getRescuePartsJobs(objRing ring.Ring, partitions []uint64) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	allDevices := objRing.AllDevices()
	for d := range allDevices {
		if allDevices[d] != nil {
			for _, p := range partitions {
				nodes, _ := objRing.GetJobNodes(p, allDevices[d].Id)
				jobs = append(jobs, &PriorityRepJob{
					Partition:  p,
					FromDevice: allDevices[d],
					ToDevices:  nodes,
				})
			}
		}
	}
	return jobs
}

func RescueParts(args []string, cnf srv.ConfigLoader) {
	flags := flag.NewFlagSet("rescueparts", flag.ExitOnError)
	policy := flags.Int("p", 0, "policy index to use")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird rescueparts partnum1,partnum2,...\n")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 1 {
		flags.Usage()
		return
	}

	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	objRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, *policy)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return
	}
	partsStr := strings.Split(flags.Arg(0), ",")
	partsInt := make([]uint64, len(partsStr))
	for i, p := range partsStr {
		partsInt[i], err = strconv.ParseUint(p, 10, 64)
		if err != nil {
			fmt.Println("Invalid Partition:", p)
			return
		}
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getRescuePartsJobs(objRing, partsInt)
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 1, client)
	fmt.Println("Done sending jobs.")
}
