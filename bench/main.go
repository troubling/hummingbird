package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"hummingbird/common"
)

var client = &http.Client{}

var storageURL = ""
var authToken = ""

var devNull, _ = os.OpenFile("/dev/null", os.O_WRONLY, 0666)

func Auth(endpoint string, user string, key string) (string, string) {
	req, err := http.NewRequest("GET", endpoint, nil)
	req.Header.Set("X-Auth-User", user)
	req.Header.Set("X-Auth-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("ERROR MAKING AUTH REQUEST")
		os.Exit(1)
	}
	resp.Body.Close()
	return resp.Header.Get("X-Storage-Url"), resp.Header.Get("X-Auth-Token")
}

func PutContainers(storageURL string, authToken string, count int) {
	for i := 0; i < count; i++ {
		url := fmt.Sprintf("%s/%d", storageURL, i)
		req, _ := http.NewRequest("PUT", url, nil)
		req.Header.Set("X-Auth-Token", authToken)
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode/100 != 2 {
			fmt.Println("ERROR CREATING CONTAINERS", resp.StatusCode)
			os.Exit(1)
		}
		resp.Body.Close()
	}
}

type Object struct {
	Url         string
	PutError    int
	DeleteError int
	GetError    int
	Data        []byte
}

func (obj *Object) Put() {
	req, _ := http.NewRequest("PUT", obj.Url, bytes.NewReader(obj.Data))
	req.Header.Set("X-Auth-Token", authToken)
	req.Header.Set("Content-Length", strconv.FormatInt(int64(len(obj.Data)), 10))
	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if (err != nil) || (resp.StatusCode/100 != 2) {
		obj.PutError += 1
	}
}

func (obj *Object) Get() {
	req, _ := http.NewRequest("GET", obj.Url, nil)
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(req)
	if resp != nil {
		io.Copy(devNull, resp.Body)
	}
	if (err != nil) || (resp.StatusCode/100 != 2) {
		obj.GetError += 1
	}
}

func (obj *Object) Delete() {
	req, _ := http.NewRequest("DELETE", obj.Url, nil)
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if (err != nil) || (resp.StatusCode/100 != 2) {
		obj.DeleteError += 1
	}
}

func DoJobs(work []func(), concurrency int) time.Duration {
	wg := sync.WaitGroup{}
	starterPistol := make(chan int)
	jobId := int32(1)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			_, _ = <-starterPistol
			for {
				job := int(atomic.AddInt32(&jobId, 1))
				if job >= len(work) {
					wg.Done()
					return
				}
				work[job]()
			}
		}()
	}
	start := time.Now()
	close(starterPistol)
	wg.Wait()
	return time.Now().Sub(start)
}

func ParseInt(number string) int {
	val, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		fmt.Println("Error parsing number:", number)
		os.Exit(1)
	}
	return int(val)
}

func main() {
	hummingbird.UseMaxProcs()

	if len(os.Args) < 2 {
		fmt.Println("Usage:", os.Args[0], "[configuration file]")
		fmt.Println("Only supports auth 1.0.")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [bench]")
		fmt.Println("    auth = http://localhost:8080/auth/v1.0")
		fmt.Println("    user = test:tester")
		fmt.Println("    key = testing")
		fmt.Println("    concurrency = 15")
		fmt.Println("    object_size = 131072")
		fmt.Println("    num_objects = 5000")
		fmt.Println("    num_gets = 30000")
		fmt.Println("    delete = yes")
		os.Exit(1)
	}

	benchconf, err := hummingbird.LoadIniFile(os.Args[1])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	authURL := benchconf.GetDefault("bench", "auth", "http://localhost:8080/auth/v1.0")
	authUser := benchconf.GetDefault("bench", "user", "test:tester")
	authKey := benchconf.GetDefault("bench", "key", "testing")
	concurrency := ParseInt(benchconf.GetDefault("bench", "concurrency", "16"))
	objectSize := ParseInt(benchconf.GetDefault("bench", "object_size", "131072"))
	numObjects := ParseInt(benchconf.GetDefault("bench", "num_objects", "5000"))
	numGets := ParseInt(benchconf.GetDefault("bench", "num_gets", "30000"))
	delete := hummingbird.LooksTrue(benchconf.GetDefault("bench", "delete", "yes"))

	storageURL, authToken = Auth(authURL, authUser, authKey)

	PutContainers(storageURL, authToken, concurrency)

	data := make([]byte, objectSize)
	objects := make([]Object, numObjects)
	for i := 0; i < len(objects); i++ {
		objects[i].Url = fmt.Sprintf("%s/%d/%d", storageURL, i%concurrency, rand.Int63())
		objects[i].Data = data
	}

	work := make([]func(), len(objects))
	for i := 0; i < len(objects); i++ {
		work[i] = objects[i].Put
	}
	putTime := DoJobs(work, concurrency)
	fmt.Printf("   PUT %d objects @ %.2f/s\n", numObjects, float64(numObjects)/(float64(putTime)/float64(time.Second)))

	work = make([]func(), numGets)
	for i := 0; i < numGets; i++ {
		work[i] = objects[int(rand.Int63()%int64(len(objects)))].Get
	}
	getTime := DoJobs(work, concurrency)
	fmt.Printf("   GET %d objects @ %.2f/s\n", numGets, float64(numGets)/(float64(getTime)/float64(time.Second)))

	if delete {
		work = make([]func(), len(objects))
		for i := 0; i < len(objects); i++ {
			work[i] = objects[i].Delete
		}
		deleteTime := DoJobs(work, concurrency)
		fmt.Printf("DELETE %d objects @ %.2f/s\n", numObjects, float64(numObjects)/(float64(deleteTime)/float64(time.Second)))
	}

	putErrors := 0
	getErrors := 0
	deleteErrors := 0
	for i := 0; i < len(objects); i++ {
		getErrors += objects[i].GetError
		deleteErrors += objects[i].DeleteError
		putErrors += objects[i].PutError
	}
	if putErrors > 0 {
		fmt.Println("Put errors:", putErrors)
	}
	if getErrors > 0 {
		fmt.Println("Get errors:", getErrors)
	}
	if deleteErrors > 0 {
		fmt.Println("Delete errors:", deleteErrors)
	}
}
