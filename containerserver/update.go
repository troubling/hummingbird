package containerserver

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"hummingbird/common"
)

var client = &http.Client{Timeout: time.Second * 10}

func UpdateAccount(info *ContainerInfo, request *hummingbird.WebRequest, vars map[string]string) {
	accpartition := request.Header.Get("X-Account-Partition")
	if accpartition == "" {
		return
	}
	acchosts := strings.Split(request.Header.Get("X-Account-Host"), ",")
	accdevices := strings.Split(request.Header.Get("X-Account-Device"), ",")
	for index := range acchosts {
		if acchosts[index] == "" {
			break
		}
		host := acchosts[index]
		device := accdevices[index]
		url := fmt.Sprintf("http://%s/%s/%s/%s/%s", host, device, accpartition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			continue
		}
		req.Header.Add("X-Trans-Id", request.Header.Get("X-Trans-Id"))
		req.Header.Add("X-Put-Timestamp", info.PutTimestamp)
		req.Header.Add("X-Delete-Timestamp", info.DeleteTimestamp)
		req.Header.Add("X-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
		req.Header.Add("X-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
		req.Header.Add("X-Backend-Storage-Policy-Index", strconv.Itoa(info.StoragePolicyIndex))
		resp, err := client.Do(req)
		resp.Body.Close()
		if err != nil || (resp.StatusCode/100) != 2 {
			// TODO: log error
			continue
		}
	}
}
