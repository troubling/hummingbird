package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
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

var CONCURRENCY = 16
var OBJ_SIZE = 131072
var PUT_COUNT = 5000
var GET_COUNT = 30000

var storageURL = ""
var authToken = ""

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
		ioutil.ReadAll(resp.Body)
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

func main() {
	hummingbird.UseMaxProcs()
	storageURL, authToken = Auth("http://localhost:8080/auth/v1.0", "test:tester", "testing")

	PutContainers(storageURL, authToken, CONCURRENCY)

	data := make([]byte, OBJ_SIZE)
	objects := make([]Object, PUT_COUNT)
	for i := 0; i < len(objects); i++ {
		objects[i].Url = fmt.Sprintf("%s/%d/%d", storageURL, i%CONCURRENCY, rand.Int63())
		objects[i].Data = data
	}

	work := make([]func(), len(objects))
	for i := 0; i < len(objects); i++ {
		work[i] = objects[i].Put
	}
	putTime := DoJobs(work, CONCURRENCY)
	fmt.Printf("   PUT %d objects @ %.2f/s\n", PUT_COUNT, float64(PUT_COUNT)/float64(putTime/time.Second))

	work = make([]func(), GET_COUNT)
	for i := 0; i < GET_COUNT; i++ {
		work[i] = objects[int(rand.Int63()%int64(len(objects)))].Get
	}
	getTime := DoJobs(work, CONCURRENCY)
	fmt.Printf("   GET %d objects @ %.2f/s\n", GET_COUNT, float64(GET_COUNT)/float64(getTime/time.Second))

	work = make([]func(), len(objects))
	for i := 0; i < len(objects); i++ {
		work[i] = objects[i].Delete
	}
	deleteTime := DoJobs(work, CONCURRENCY)
	fmt.Printf("DELETE %d objects @ %.2f/s\n", PUT_COUNT, float64(PUT_COUNT)/float64(deleteTime/time.Second))

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
