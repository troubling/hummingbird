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
	"context"
	"fmt"
	"io"
	"io/ioutil"
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

func splitHeader(header string) []string {
	if header == "" {
		return []string{}
	}
	return strings.Split(header, ",")
}

func (server *ContainerServer) accountUpdate(writer http.ResponseWriter, request *http.Request, vars map[string]string, info *ContainerInfo, logger srv.LowLevelLogger) {
	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		defer middleware.Recover(writer, request, "PANIC WHILE UPDATING ACCOUNT")
		accpartition := request.Header.Get("X-Account-Partition")
		if accpartition == "" {
			logger.Error("Account update failed: bad partition")
			return
		}
		hosts := splitHeader(request.Header.Get("X-Account-Host"))
		devices := splitHeader(request.Header.Get("X-Account-Device"))

		if len(hosts) != len(devices) {
			logger.Error("Account update failed: different numbers of hosts and devices in request")
			return
		}
		schemes := splitHeader(request.Header.Get("X-Account-Scheme"))
		for len(schemes) < len(hosts) {
			schemes = append(schemes, "http")
		}
		for index, host := range hosts {
			if err := accountUpdateHelper(request.Context(), info, schemes[index], host, devices[index], accpartition, vars["account"], vars["container"], request.Header.Get("X-Trans-Id"), request.Header.Get("X-Account-Override-Deleted") == "yes", server.updateClient); err != nil {
				logger.Error(
					"Account update failed:", zap.Error(err),
					zap.String("schemes[index]", schemes[index]),
					zap.String("hosts[index]", hosts[index]),
					zap.String("devices[index]", devices[index]),
				)
			}
		}
	}()
	select {
	case <-time.After(waitForAccountUpdate):
	case <-done:
	}
}

func accountUpdateHelper(ctx context.Context, info *ContainerInfo, scheme, host, device, accpartition, account, container, transID string, accountOverrideDeleted bool, updateClient common.HTTPClient) error {
	url := fmt.Sprintf("%s://%s/%s/%s/%s/%s", scheme, host, device, accpartition,
		common.Urlencode(account), common.Urlencode(container))
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Add("X-Backend-Suppress-2xx-Logging", "t")
	req.Header.Add("X-Put-Timestamp", info.PutTimestamp)
	req.Header.Add("X-Delete-Timestamp", info.DeleteTimestamp)
	req.Header.Add("X-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
	req.Header.Add("X-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
	req.Header.Add("X-Trans-Id", transID)
	req.Header.Add("X-Backend-Storage-Policy-Index", strconv.Itoa(info.StoragePolicyIndex))
	if accountOverrideDeleted {
		req.Header.Add("X-Account-Override-Deleted", "yes")
	}
	resp, err := updateClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%d response status code", resp.StatusCode)
	}
	return nil
}
