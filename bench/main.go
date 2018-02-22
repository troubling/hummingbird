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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/nectar"
)

type Object struct {
	c         nectar.Client
	state     int
	container string
	name      string
	data      []byte
	verbose   bool
}

func (obj *Object) Put() bool {
	resp := obj.c.PutObject(obj.container, obj.name, nil, bytes.NewReader(obj.data))
	resp.Body.Close()
	if obj.verbose && resp.StatusCode/100 != 2 {
		fmt.Printf("object PUT failed: %d to %s/%s (%s)\n", resp.StatusCode, obj.container, obj.name, resp.Header)
	}
	return resp.StatusCode/100 == 2
}

func (obj *Object) Get() bool {
	if resp := obj.c.GetObject(obj.container, obj.name, nil); resp.StatusCode/100 != 2 {
		if obj.verbose {
			fmt.Printf("object GET failed: %d to %s/%s (%s)\n", resp.StatusCode, obj.container, obj.name, resp.Header)
		}
		resp.Body.Close()
		return false
	} else {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		return true
	}
}

func (obj *Object) Delete() bool {
	resp := obj.c.DeleteObject(obj.container, obj.name, nil)
	resp.Body.Close()
	if obj.verbose && resp.StatusCode/100 != 2 {
		fmt.Printf("object DELETE failed: %d to %s/%s (%s)\n", resp.StatusCode, obj.container, obj.name, resp.Header)
	}
	return resp.StatusCode/100 == 2
}

func DoJobs(name string, work []func() bool, concurrency int) {
	wg := sync.WaitGroup{}
	cwg := sync.WaitGroup{}
	errorCount := 0
	jobTimes := make([]float64, 0, len(work))
	times := make(chan float64)
	errors := make(chan int)
	jobqueue := make(chan func() bool)
	cwg.Add(2)
	go func() {
		for n := range errors {
			errorCount += n
		}
		cwg.Done()
	}()
	go func() {
		for t := range times {
			jobTimes = append(jobTimes, t)
		}
		sort.Float64s(jobTimes)
		cwg.Done()
	}()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for job := range jobqueue {
				startJob := time.Now()
				if !job() {
					errors <- 1
				}
				times <- float64(time.Now().Sub(startJob)) / float64(time.Second)
			}
			wg.Done()
		}()
	}
	start := time.Now()
	for _, job := range work {
		jobqueue <- job
	}
	close(jobqueue)
	wg.Wait()
	totalTime := float64(time.Now().Sub(start)) / float64(time.Second)
	close(errors)
	close(times)
	cwg.Wait()
	sum := 0.0
	for _, val := range jobTimes {
		sum += val
	}
	avg := sum / float64(len(work))
	diffsum := 0.0
	for _, val := range jobTimes {
		diffsum += math.Pow(val-avg, 2.0)
	}
	fmt.Printf("%ss: %d @ %.2f/s\n", name, len(work), float64(len(work))/totalTime)
	fmt.Println("  Failures:", errorCount)
	fmt.Printf("  Mean: %.5fs (%.1f%% RSD)\n", avg, math.Sqrt(diffsum/float64(len(work)))*100.0/avg)
	fmt.Printf("  Median: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.5)])
	fmt.Printf("  85%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.85)])
	fmt.Printf("  90%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.90)])
	fmt.Printf("  95%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.95)])
	fmt.Printf("  99%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.99)])
}

func RunBench(args []string) {
	if len(args) < 1 {
		fmt.Print(`Usage: [configuration file]
The configuration file should look something like:
    [bench]
    auth = http://localhost:8080/auth/v1.0
    user = test:tester
    key = testing
    concurrency = 15
    object_size = 131072
    num_objects = 5000
    num_gets = 30000
    delete = yes
    allow_insecure_auth_cert = no
    single_container = false
or
    [bench]
    auth = http://192.168.56.1:5000/v3/
    tenant = test
    user = tester
    password = testing
    concurrency = 15
    object_size = 131072
    num_objects = 5000
    num_gets = 30000
    delete = yes
    allow_insecure_auth_cert = no
    single_container = false
`)
		os.Exit(1)
	}

	benchconf, err := conf.LoadConfig(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	authURL := benchconf.GetDefault("bench", "auth", "https://localhost:8080/auth/v1.0")
	authTenant := benchconf.GetDefault("bench", "tenant", "")
	authUser := benchconf.GetDefault("bench", "user", "test:tester")
	authPassword := benchconf.GetDefault("bench", "password", "")
	authKey := benchconf.GetDefault("bench", "key", "testing")
	authRegion := benchconf.GetDefault("bench", "region", "")
	authPrivateEndpoint := benchconf.GetBool("bench", "private", false)
	concurrency := int(benchconf.GetInt("bench", "concurrency", 16))
	objectSize := benchconf.GetInt("bench", "object_size", 131072)
	numObjects := benchconf.GetInt("bench", "num_objects", 5000)
	numGets := benchconf.GetInt("bench", "num_gets", 30000)
	objDelete := benchconf.GetBool("bench", "delete", true)
	singleContainer := benchconf.GetBool("bench", "single_container", false)
	verbose := benchconf.GetBool("bench", "verbose", false)
	allowInsecureAuthCert := benchconf.GetBool("bench", "allow_insecure_auth_cert", false)
	salt := fmt.Sprintf("%d", rand.Int63())

	fmt.Printf("Hbird Bench. Concurrency: %d. Object size in bytes: %d\n", concurrency, objectSize)
	var cli nectar.Client
	var resp *http.Response
	if allowInsecureAuthCert {
		cli, resp = nectar.NewInsecureClient(authTenant, authUser, authPassword, authKey, authRegion, authURL, authPrivateEndpoint)
	} else {
		cli, resp = nectar.NewClient(authTenant, authUser, authPassword, authKey, authRegion, authURL, authPrivateEndpoint)
	}
	if resp != nil {
		msg, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Println("Error creating client:", string(msg))
		os.Exit(1)
	}
	numContainers := concurrency
	if singleContainer {
		numContainers = 1
	}
	for i := 0; i < numContainers; i++ {
		if resp := cli.PutContainer(fmt.Sprintf("%d-%s", i, salt), nil); resp.StatusCode/100 != 2 {
			fmt.Println("Error putting container:", resp)
			os.Exit(1)
		}
	}

	data := make([]byte, objectSize)
	objects := make([]*Object, numObjects)
	for i := range objects {
		objects[i] = &Object{
			state:     0,
			container: fmt.Sprintf("%d-%s", i%numContainers, salt),
			name:      fmt.Sprintf("%x", rand.Int63()),
			data:      data,
			c:         cli,
			verbose:   verbose,
		}
	}

	work := make([]func() bool, len(objects))
	for i := range objects {
		work[i] = objects[i].Put
	}
	DoJobs("PUT", work, concurrency)

	time.Sleep(time.Second * 2)

	work = make([]func() bool, numGets)
	for i := int64(0); i < numGets; i++ {
		work[i] = objects[int(rand.Int63()%int64(len(objects)))].Get
	}
	DoJobs("GET", work, concurrency)

	if objDelete {
		work = make([]func() bool, len(objects))
		for i := range objects {
			work[i] = objects[i].Delete
		}
		DoJobs("DELETE", work, concurrency)
		for i := 0; i < numContainers; i++ {
			if resp := cli.DeleteContainer(fmt.Sprintf("%d-%s", i, salt), nil); resp.StatusCode/100 != 2 {
				fmt.Println("Error deleting container:", err)
				os.Exit(1)
			}
		}
	}
}

func RunThrash(args []string) {
	rand.Seed(time.Now().UTC().UnixNano())
	if len(args) < 1 {
		fmt.Println("Usage: [configuration file]")
		fmt.Println("Only supports auth 1.0.")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [thrash]")
		fmt.Println("    auth = http://localhost:8080/auth/v1.0")
		fmt.Println("    user = test:tester")
		fmt.Println("    key = testing")
		fmt.Println("    concurrency = 15")
		fmt.Println("    object_size = 131072")
		fmt.Println("    num_objects = 5000")
		fmt.Println("    gets_per_object = 5")
		fmt.Println("    allow_insecure_auth_cert = no")

		os.Exit(1)
	}

	thrashconf, err := conf.LoadConfig(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	authURL := thrashconf.GetDefault("thrash", "auth", "https://localhost:8080/auth/v1.0")
	authUser := thrashconf.GetDefault("thrash", "user", "test:tester")
	authKey := thrashconf.GetDefault("thrash", "key", "testing")
	concurrency := int(thrashconf.GetInt("thrash", "concurrency", 16))
	objectSize := thrashconf.GetInt("thrash", "object_size", 131072)
	numObjects := thrashconf.GetInt("thrash", "num_objects", 5000)
	numGets := int(thrashconf.GetInt("thrash", "gets_per_object", 5))
	allowInsecureAuthCert := thrashconf.GetBool("thrash", "allow_insecure_auth_cert", false)
	salt := fmt.Sprintf("%d", rand.Int63())

	var cli nectar.Client
	var resp *http.Response
	if allowInsecureAuthCert {
		cli, resp = nectar.NewInsecureClient("", authUser, "", authKey, "", authURL, false)
	} else {
		cli, resp = nectar.NewClient("", authUser, "", authKey, "", authURL, false)
	}
	if err != nil {
		msg, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Println("Error creating client:", msg)
		os.Exit(1)
	}

	for i := 0; i < concurrency; i++ {
		if err := cli.PutContainer(fmt.Sprintf("%d-%s", i, salt), nil); err != nil {
			fmt.Println("Error putting container:", err)
			os.Exit(1)
		}
	}

	data := make([]byte, objectSize)
	objects := make([]*Object, numObjects)
	for i := range objects {
		objects[i] = &Object{
			state:     0,
			container: fmt.Sprintf("%d-%s", i%concurrency, salt),
			name:      fmt.Sprintf("%x", rand.Int63()),
			data:      data,
			c:         cli,
		}
	}

	workch := make(chan func() bool)

	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				(<-workch)()
			}
		}()
	}

	for {
		i := int(rand.Int63() % int64(len(objects)))
		if objects[i].state == 1 {
			workch <- objects[i].Put
		} else if objects[i].state < numGets+2 {
			workch <- objects[i].Get
		} else if objects[i].state >= numGets+2 {
			workch <- objects[i].Delete
			objects[i] = &Object{
				container: fmt.Sprintf("%d-%s", i%concurrency, salt),
				name:      fmt.Sprintf("%x", rand.Int63()),
				data:      data,
				c:         cli,
			}
			continue
		}
		objects[i].state += 1
	}
}
