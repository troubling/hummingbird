package main

import (
    "net/http"
	"strings"
	"fmt"
	"io/ioutil"

    "hummingbird/common"
)


const deleteAtDivisor = 3600
const deleteAtAccount = ".expiring_objects"

func UpdateContainer(metadata map[string]interface{}, request *hummingbird.SwiftRequest, vars map[string]string) {
    client := &http.Client{}
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
        defer ioutil.ReadAll(resp.Body)
        if err != nil || (resp.StatusCode/100) != 2 {
            continue
            // TODO: async update files
        }
    }
}

// TODO: UNTESTED
func UpdateDeleteAt(request *hummingbird.SwiftRequest, vars map[string]string, metadata map[string]interface{}) {
    if _, ok := metadata["X-Delete-At"]; !ok {
        return
    }
    deleteAt, err := hummingbird.ParseDate(metadata["X-Delete-At"].(string))
    if err != nil {
        return
    }
    client := &http.Client{}
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
    defer ioutil.ReadAll(resp.Body)
    if err != nil || (resp.StatusCode/100) != 2 {
        // TODO: async update files
    }
}
