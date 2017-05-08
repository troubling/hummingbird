//  Copyright (c) 2016 Rackspace
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

package containerserver

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"go.uber.org/zap"
)

var waitForAccountUpdate = time.Second * 5

func (server *ContainerServer) accountUpdate(writer http.ResponseWriter, request *http.Request, vars map[string]string, info *ContainerInfo, logger srv.LowLevelLogger) {
	firstDone := make(chan struct{}, 1)
	go func() {
		defer func() { firstDone <- struct{}{} }()
		defer middleware.Recover(writer, request, "PANIC WHILE UPDATING ACCOUNT")
		accpartition := request.Header.Get("X-Account-Partition")
		if accpartition == "" {
			logger.Error("Account update failed: bad partition")
			return
		}
		hosts := strings.Split(request.Header.Get("X-Account-Host"), ",")
		devices := strings.Split(request.Header.Get("X-Account-Device"), ",")
		if len(hosts) != len(devices) {
			logger.Error("Account update failed: different numbers of hosts and devices in request")
			return
		}
		for index, host := range hosts {
			url := fmt.Sprintf("http://%s/%s/%s/%s/%s", host, devices[index], accpartition,
				common.Urlencode(vars["account"]), common.Urlencode(vars["container"]))
			req, err := http.NewRequest("PUT", url, nil)
			if err != nil {
				logger.Error("Account update failed: error creating request object")
				continue
			}
			req.Header.Add("X-Put-Timestamp", info.PutTimestamp)
			req.Header.Add("X-Delete-Timestamp", info.DeleteTimestamp)
			req.Header.Add("X-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
			req.Header.Add("X-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
			req.Header.Add("X-Trans-Id", request.Header.Get("X-Trans-Id"))
			req.Header.Add("X-Backend-Storage-Policy-Index", strconv.Itoa(info.StoragePolicyIndex))
			if request.Header.Get("X-Account-Override-Deleted") == "yes" {
				req.Header.Add("X-Account-Override-Deleted", "yes")
			}
			resp, err := server.updateClient.Do(req)
			if err != nil {
				logger.Error("Account update failed: bad response",
					zap.String("hosts[index]", hosts[index]),
					zap.String("devices[index]", devices[index]))
				continue
			}
			defer resp.Body.Close()
			if (resp.StatusCode / 100) != 2 {
				logger.Error("Account update failed: bad response",
					zap.String("hosts[index]", hosts[index]),
					zap.String("devices[index]", devices[index]))
			}
		}
	}()
	select {
	case <-time.After(waitForAccountUpdate):
	case <-firstDone:
	}
}
