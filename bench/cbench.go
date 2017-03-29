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

package bench

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
)

type ContainerObject struct {
	Url string
}

func (obj *ContainerObject) Put() bool {
	req, err := http.NewRequest("PUT", obj.Url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "0")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	return err == nil && resp.StatusCode/100 == 2
}

func (obj *ContainerObject) Delete() bool {
	req, _ := http.NewRequest("DELETE", obj.Url, nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	return err == nil && resp.StatusCode/100 == 2
}

func RunCBench(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: [configuration file]")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [cbench]")
		fmt.Println("    address = http://127.0.0.1:6011/")
		fmt.Println("    containers = 5")
		fmt.Println("    concurrency = 10")
		fmt.Println("    num_objects = 10000")
		fmt.Println("    num_gets = 500")
		fmt.Println("    delete = yes")
		fmt.Println("    check_mounted = false")
		fmt.Println("    #drive_list = sdb1,sdb2")
		os.Exit(1)
	}

	benchconf, err := conf.LoadConfig(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	address := benchconf.GetDefault("cbench", "address", "http://127.0.0.1:6011/")
	if !strings.HasSuffix(address, "/") {
		address = address + "/"
	}
	concurrency := int(benchconf.GetInt("cbench", "concurrency", 10))
	containerCount := int(benchconf.GetInt("cbench", "containers", 5))
	numObjects := benchconf.GetInt("cbench", "num_objects", 10000)
	numGets := int(benchconf.GetInt("cbench", "num_gets", 500))
	checkMounted := benchconf.GetBool("cbench", "check_mounted", false)
	driveList := benchconf.GetDefault("cbench", "drive_list", "")
	numPartitions := int64(100)
	delete := benchconf.GetBool("cbench", "delete", true)

	deviceList := GetDevices(address, checkMounted)
	if driveList != "" {
		deviceList = strings.Split(driveList, ",")
	}

	containers := make([]string, containerCount)
	for i := 0; i < containerCount; i++ {
		device := strings.Trim(deviceList[i%len(deviceList)], " ")
		part := rand.Int63() % numPartitions
		cid := rand.Int63()
		containers[i] = fmt.Sprintf("%s%s/%d/%s/%d", address, device, part, "a", cid)
		req, _ := http.NewRequest("PUT", containers[i], nil)
		req.Header.Set("X-Timestamp", common.GetTimestamp())
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil || resp.StatusCode/100 != 2 {
			fmt.Println("Container PUT failed: ", resp.StatusCode, err)
			os.Exit(1)
		}
	}

	objects := make([]*ContainerObject, numObjects)
	for i, _ := range objects {
		container := containers[i%len(containers)]
		objects[i] = &ContainerObject{
			Url: fmt.Sprintf("%s/%d", container, rand.Int63()),
		}
	}

	work := make([]func() bool, len(objects))
	for i, _ := range objects {
		work[i] = objects[i].Put
	}
	DoJobs("OBJECT PUT", work, concurrency)

	time.Sleep(time.Second * 2)

	getContainer := func() bool {
		container := containers[rand.Int()%len(containers)]
		req, _ := http.NewRequest("GET", container+"?format=json", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
			w, err := io.Copy(ioutil.Discard, resp.Body)
			if err != nil || w < 2 {
				return false
			}
		}
		return err == nil && resp.StatusCode/100 == 2 && resp.Header.Get("Content-Type") == "application/json; charset=utf-8"
	}
	work = make([]func() bool, numGets)
	for i := 0; i < numGets; i++ {
		work[i] = getContainer
	}
	DoJobs("CONTAINER GET", work, concurrency)

	time.Sleep(time.Second * 2)

	if delete {
		work = make([]func() bool, len(objects))
		for i, _ := range objects {
			work[i] = objects[i].Delete
		}
		DoJobs("OBJECT DELETE", work, concurrency)
	}
}

func RunCGBench(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: [configuration file]")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [cgbench]")
		fmt.Println("    address = http://127.0.0.1:6011/")
		fmt.Println("    concurrency = 5")
		fmt.Println("    num_objects = 5000000")
		fmt.Println("    report_interval = 100000")
		fmt.Println("    num_gets = 10")
		fmt.Println("    check_mounted = false")
		fmt.Println("    #drive_list = sdb1,sdb2")
		os.Exit(1)
	}

	benchconf, err := conf.LoadConfig(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	address := benchconf.GetDefault("cgbench", "address", "http://127.0.0.1:6011/")
	if !strings.HasSuffix(address, "/") {
		address = address + "/"
	}
	concurrency := int(benchconf.GetInt("cgbench", "concurrency", 5))
	reportInterval := benchconf.GetInt("cgbench", "report_interval", 100000)
	numObjects := benchconf.GetInt("cgbench", "num_objects", 5000000)
	numGets := int(benchconf.GetInt("cgbench", "num_gets", 10))
	checkMounted := benchconf.GetBool("cgbench", "check_mounted", false)
	driveList := benchconf.GetDefault("cgbench", "drive_list", "")
	numPartitions := int64(100)

	deviceList := GetDevices(address, checkMounted)
	if driveList != "" {
		deviceList = strings.Split(driveList, ",")
	}

	device := strings.Trim(deviceList[0], " ")
	part := rand.Int63() % numPartitions
	cid := rand.Int63()
	container := fmt.Sprintf("%s%s/%d/%s/%d", address, device, part, "a", cid)
	req, _ := http.NewRequest("PUT", container, nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if err != nil || resp.StatusCode/100 != 2 {
		fmt.Println("Container PUT failed: ", resp.StatusCode, err)
		os.Exit(1)
	}

	for totalObjects := int64(0); totalObjects < numObjects; totalObjects += reportInterval {
		fmt.Println("Object count:", totalObjects)
		objects := make([]*ContainerObject, int(reportInterval))
		for i, _ := range objects {
			objects[i] = &ContainerObject{
				Url: fmt.Sprintf("%s/%d", container, rand.Int63()),
			}
		}

		work := make([]func() bool, len(objects))
		for i, _ := range objects {
			work[i] = objects[i].Put
		}
		DoJobs("OBJECT PUT", work, concurrency)

		time.Sleep(time.Second * 2)

		getContainer := func() bool {
			req, _ := http.NewRequest("GET", container+"?format=json&marker=5", nil)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				defer resp.Body.Close()
				w, err := io.Copy(ioutil.Discard, resp.Body)
				if err != nil || w < 2 {
					return false
				}
			}
			return err == nil && resp.StatusCode/100 == 2 && resp.Header.Get("Content-Type") == "application/json; charset=utf-8"
		}
		work = make([]func() bool, numGets)
		for i := 0; i < numGets; i++ {
			work[i] = getContainer
		}
		DoJobs("CONTAINER GET", work, concurrency)

		time.Sleep(time.Second * 2)
	}
}
