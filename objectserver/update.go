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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"context"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"go.uber.org/zap"
)

/*This hash is used to represent a zero byte async file that is
  created for an expiring object*/
const zeroByteHash = "d41d8cd98f00b204e9800998ecf8427e"
const deleteAtAccount = ".expiring_objects"

func splitHeader(header string) []string {
	if header == "" {
		return []string{}
	}
	return strings.Split(header, ",")
}

func (server *ObjectServer) hashPath(account, container, obj string) string {
	h := md5.New()
	io.WriteString(h, server.hashPathPrefix+"/"+account+"/"+container+"/"+obj+server.hashPathSuffix)
	return hex.EncodeToString(h.Sum(nil))
}

func (server *ObjectServer) expirerContainer(deleteAt time.Time, account, container, obj string) string {
	i := new(big.Int)
	fmt.Sscanf(server.hashPath(account, container, obj), "%x", i)
	shardInt := i.Mod(i, big.NewInt(100)).Int64()
	timestamp := (deleteAt.Unix()/server.expiringDivisor)*server.expiringDivisor - shardInt
	if timestamp < 0 {
		timestamp = 0
	} else if timestamp > 9999999999 {
		timestamp = 9999999999
	}
	return fmt.Sprintf("%010d", timestamp)
}

func (server *ObjectServer) sendContainerUpdate(ctx context.Context, scheme, host, device, method, partition, account, container, obj string, headers http.Header) bool {
	obj_url := fmt.Sprintf("%s://%s/%s/%s/%s/%s/%s", scheme, host, device, partition,
		common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
	if req, err := http.NewRequest(method, obj_url, nil); err == nil {
		req = req.WithContext(ctx)
		req.Header = headers
		if resp, err := server.updateClient.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return true
			}
		}
	}
	return false
}

func (server *ObjectServer) saveAsync(method, account, container, obj, localDevice string, headers http.Header, logger srv.LowLevelLogger) {
	hash := server.hashPath(account, container, obj)
	asyncFile := filepath.Join(server.driveRoot, localDevice, "async_pending", hash[29:32], hash+"-"+headers.Get("X-Timestamp"))
	tempDir := TempDirPath(server.driveRoot, localDevice)
	data := map[string]interface{}{
		"op":        method,
		"account":   account,
		"container": container,
		"obj":       obj,
		"headers":   common.Headers2Map(headers),
	}
	var err error
	if err = os.MkdirAll(filepath.Dir(asyncFile), 0755); err == nil {
		var writer fs.AtomicFileWriter
		writer, err = fs.NewAtomicFileWriter(tempDir, filepath.Dir(asyncFile))
		if err == nil {
			defer writer.Abandon()
			writer.Write(pickle.PickleDumps(data))
			writer.Save(asyncFile)
			return
		}
	}
	logger.Error("Error saving obj async", zap.String("objPath", fmt.Sprintf("%s/%s/%s", account, container, obj)), zap.Error(err))
}

func (server *ObjectServer) updateContainer(ctx context.Context, metadata map[string]string, request *http.Request, vars map[string]string, logger srv.LowLevelLogger) {
	partition := request.Header.Get("X-Container-Partition")
	hosts := splitHeader(request.Header.Get("X-Container-Host"))
	devices := splitHeader(request.Header.Get("X-Container-Device"))
	schemes := splitHeader(request.Header.Get("X-Container-Scheme"))
	if partition == "" || len(hosts) == 0 || len(devices) == 0 {
		return
	}
	for len(schemes) < len(hosts) {
		schemes = append(schemes, "http")
	}
	requestHeaders := http.Header{
		"X-Backend-Storage-Policy-Index": {common.GetDefault(request.Header, "X-Backend-Storage-Policy-Index", "0")},
		"Referer":                        {common.GetDefault(request.Header, "Referer", "-")},
		"User-Agent":                     {common.GetDefault(request.Header, "User-Agent", "-")},
		"X-Trans-Id":                     {common.GetDefault(request.Header, "X-Trans-Id", "-")},
		"X-Timestamp":                    request.Header["X-Timestamp"],
		"X-Delete-At":                    request.Header["X-Delete-At"],
	}
	if request.Method != "DELETE" {
		requestHeaders.Add("X-Content-Type", metadata["Content-Type"])
		requestHeaders.Add("X-Size", metadata["Content-Length"])
		requestHeaders.Add("X-Etag", metadata["ETag"])
	}
	failures := 0
	for index := range hosts {
		if !server.sendContainerUpdate(ctx, schemes[index], hosts[index], devices[index], request.Method, partition, vars["account"], vars["container"], vars["obj"], requestHeaders) {
			logger.Error("ERROR container update failed (saving for async update later)",
				zap.String("Host", hosts[index]),
				zap.String("Device", devices[index]))
			failures++
		}
	}
	if failures > 0 {
		server.saveAsync(request.Method, vars["account"], vars["container"], vars["obj"], vars["device"], requestHeaders, logger)
	}
}

func (server *ObjectServer) containerUpdates(writer http.ResponseWriter, request *http.Request, metadata map[string]string, deleteAt string, vars map[string]string, logger srv.LowLevelLogger) {
	defer middleware.Recover(writer, request, "PANIC WHILE UPDATING CONTAINER LISTINGS")

	done := make(chan struct{}, 1)
	go func() {
		server.updateContainer(request.Context(), metadata, request, vars, logger)
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(server.updateTimeout):
	}
}
