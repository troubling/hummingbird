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
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
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
	url := fmt.Sprintf("http://%s:%d/priorityrep", job.FromDevice.ReplicationIp, job.FromDevice.ReplicationPort+500)
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
func doPriRepJobs(jobs []*PriorityRepJob, deviceMax int, client *http.Client) {
	limiter := &devLimiter{inUse: make(map[int]int), max: deviceMax, somethingFinished: make(chan struct{}, 1)}
	wg := sync.WaitGroup{}
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
				res, _ := SendPriRepJob(job, client)
				fmt.Println(res)
			}(jobs[i])
			jobs = append(jobs[:i], jobs[i+1:]...)
			break
		}
		if !foundDoable {
			limiter.waitForSomethingToFinish()
		}
	}
	wg.Wait()
}

// getPartMoveJobs takes two rings and creates a list of jobs for any partition moves between them.
func getPartMoveJobs(oldRing, newRing ring.Ring) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	for partition := uint64(0); true; partition++ {
		olddevs := oldRing.GetNodesInOrder(partition)
		newdevs := newRing.GetNodesInOrder(partition)
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

// MoveParts takes two object .ring.gz files as []string{oldRing, newRing} and dispatches priority replication jobs to rebalance data in line with any ring changes.
func MoveParts(args []string) {
	flags := flag.NewFlagSet("moveparts", flag.ExitOnError)
	policy := flags.Int("p", 0, "policy index to use")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird moveparts [old ringfile]")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 1 {
		flags.Usage()
		return
	}

	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	oldRing, err := ring.LoadRing(flags.Arg(0), hashPathPrefix, hashPathSuffix)
	if err != nil {
		fmt.Println("Unable to load old ring:", err)
		return
	}
	curRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, *policy)
	if err != nil {
		fmt.Println("Unable to load current ring:", err)
		return
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getPartMoveJobs(oldRing, curRing)
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 2, client)
	fmt.Println("Done sending jobs.")
}

// getRestoreDeviceJobs takes an ip address and device name, and creates a list of jobs to restore that device's data from peers.
func getRestoreDeviceJobs(theRing ring.Ring, ip string, devName string) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	for partition := uint64(0); true; partition++ {
		devs := theRing.GetNodesInOrder(partition)
		if devs == nil {
			break
		}
		for i, dev := range devs {
			if dev.Device == devName && (dev.Ip == ip || dev.ReplicationIp == ip) {
				src := devs[(i+1)%len(devs)]
				jobs = append(jobs, &PriorityRepJob{
					Partition:  partition,
					FromDevice: src,
					ToDevices:  []*ring.Device{dev},
				})
			}
		}
	}
	return jobs
}

// RestoreDevice takes an IP address and device name such as []string{"172.24.0.1", "sda1"} and attempts to restores its data from peers.
func RestoreDevice(args []string) {
	flags := flag.NewFlagSet("restoredevice", flag.ExitOnError)
	policy := flags.Int("p", 0, "policy index to use")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "USAGE: hummingbird restoredevice [ip] [device]\n")
		flags.PrintDefaults()
	}
	flags.Parse(args)
	if len(flags.Args()) != 2 {
		flags.Usage()
		return
	}

	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	objRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, *policy)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getRestoreDeviceJobs(objRing, flags.Arg(0), flags.Arg(1))
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 2, client)
	fmt.Println("Done sending jobs.")
}

func getRescuePartsJobs(objRing ring.Ring, partitions []uint64) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	allDevices := objRing.AllDevices()
	for d := range allDevices {
		for _, p := range partitions {
			nodes, _ := objRing.GetJobNodes(p, allDevices[d].Id)
			jobs = append(jobs, &PriorityRepJob{
				Partition:  p,
				FromDevice: &allDevices[d],
				ToDevices:  nodes,
			})
		}
	}
	return jobs
}

func RescueParts(args []string) {
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

	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
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
