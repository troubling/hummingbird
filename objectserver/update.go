package objectserver

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"hummingbird/common"
)

const deleteAtDivisor = 3600
const deleteAtAccount = ".expiring_objects"

var client = &http.Client{Timeout: time.Second * 10}

func HeaderToMap(headers http.Header) map[string]string {
	ret := make(map[string]string)
	for key, value := range headers {
		if len(value) > 0 {
			ret[key] = value[0]
		}
	}
	return ret
}

func UpdateContainer(metadata map[string]interface{}, request *hummingbird.WebRequest, vars map[string]string, hashDir string) {
	contpartition := request.Header.Get("X-Container-Partition")
	if contpartition == "" {
		return
	}
	conthosts := strings.Split(request.Header.Get("X-Container-Host"), ",")
	contdevices := strings.Split(request.Header.Get("X-Container-Device"), ",")
	for index := range conthosts {
		if conthosts[index] == "" {
			break
		}
		host := conthosts[index]
		device := contdevices[index]
		url := fmt.Sprintf("http://%s/%s/%s/%s/%s/%s", host, device, contpartition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			continue
		}
		req.Header.Add("X-Trans-Id", request.Header.Get("X-Trans-Id"))
		req.Header.Add("X-Timestamp", metadata["X-Timestamp"].(string))
		if request.Method != "DELETE" {
			req.Header.Add("X-Content-Type", metadata["Content-Type"].(string))
			req.Header.Add("X-Size", metadata["Content-Length"].(string))
			req.Header.Add("X-Etag", metadata["ETag"].(string))
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		if err != nil || (resp.StatusCode/100) != 2 {
			request.LogError("Container update failed with %s/%s, saving async", host, device)
			data := map[string]interface{}{
				"op":        request.Method,
				"account":   vars["account"],
				"container": vars["container"],
				"obj":       vars["obj"],
				"headers":   HeaderToMap(req.Header),
			}
			suffDir, hash := filepath.Split(hashDir)
			suffDir = filepath.Dir(hashDir)
			partitionDir, suff := filepath.Split(suffDir)
			partitionDir = filepath.Dir(suffDir)
			objDir := filepath.Dir(partitionDir)
			rootDir := filepath.Dir(objDir)
			asyncDir := filepath.Join(rootDir, "async_pending", suff)
			os.MkdirAll(asyncDir, 0700)
			asyncFile := filepath.Join(asyncDir, fmt.Sprintf("%s-%s", hash, request.XTimestamp))
			hummingbird.WriteFileAtomic(asyncFile, hummingbird.PickleDumps(data), 0600)
		}
	}
}

// TODO: UNTESTED
func UpdateDeleteAt(request *hummingbird.WebRequest, vars map[string]string, metadata map[string]interface{}, hashDir string) {
	if _, ok := metadata["X-Delete-At"]; !ok {
		return
	}
	deleteAt, err := hummingbird.ParseDate(metadata["X-Delete-At"].(string))
	if err != nil {
		return
	}
	partition := request.Header.Get("X-Delete-At-Partition")
	host := request.Header.Get("X-Delete-At-Host")
	device := request.Header.Get("X-Delete-At-Device")

	deleteAtContainer := (deleteAt.Unix() / deleteAtDivisor) * deleteAtDivisor
	url := fmt.Sprintf("http://%s/%s/%s/%s/%d/%d-%s/%s/%s", host, device, partition, deleteAtAccount, deleteAtContainer,
		deleteAt.Unix(), hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
	req, err := http.NewRequest(request.Method, url, nil)
	req.Header.Add("X-Trans-Id", request.Header.Get("X-Trans-Id"))
	req.Header.Add("X-Timestamp", request.Header.Get("X-Timestamp"))
	req.Header.Add("X-Size", "0")
	req.Header.Add("X-Content-Type", "text/plain")
	req.Header.Add("X-Etag", metadata["ETag"].(string))
	resp, err := client.Do(req)
	if err != nil {
		resp.Body.Close()
	}
	if err != nil || (resp.StatusCode/100) != 2 {
		request.LogError("Container update failed with %s/%s, saving async", host, device)
		data := map[string]interface{}{
			"op":        request.Method,
			"account":   vars["account"],
			"container": vars["container"],
			"obj":       vars["obj"],
			"headers":   HeaderToMap(req.Header),
		}
		suffDir, hash := filepath.Split(hashDir)
		suffDir = filepath.Dir(hashDir)
		partitionDir, suff := filepath.Split(suffDir)
		partitionDir = filepath.Dir(suffDir)
		objDir := filepath.Dir(partitionDir)
		rootDir := filepath.Dir(objDir)
		asyncDir := filepath.Join(rootDir, "async_pending", suff)
		os.MkdirAll(asyncDir, 0700)
		asyncFile := filepath.Join(asyncDir, fmt.Sprintf("%s-%s", hash, request.XTimestamp))
		hummingbird.WriteFileAtomic(asyncFile, hummingbird.PickleDumps(data), 0600)
	}
}
