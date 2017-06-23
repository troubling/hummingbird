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
	"encoding/json"
	"flag"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/test"
	"go.uber.org/zap"
)

func TestFormatTimestamp(t *testing.T) {
	require.Equal(t, "0000000000.00000", formatTimestamp("something"))
	require.Equal(t, "0000000001.00000", formatTimestamp("1"))
	require.Equal(t, "1000000000.00000", formatTimestamp("1000000000"))
	require.Equal(t, "1000000000.00000", formatTimestamp("1000000000.00000"))
}

func TestContainerGetNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("GET", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestContainerDeleteNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestContainerPostDeleted(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestContainerPostNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("POST", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestContainerPostBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("POST", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", "invalid")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)
}

func TestContainerPutBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)
}

func TestContainerDeleteBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", "invalid")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)
}

func TestContainerPutExisting(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "2000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 202, rsp.Status)
}

func TestContainerPutPolicyConflict(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)
	require.Equal(t, 0, server.containerEngine.OpenCount())

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "2000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 409, rsp.Status)
	require.Equal(t, 0, server.containerEngine.OpenCount())
}

func TestContainerPutHead(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
	require.Equal(t, "0", rsp.Header().Get("X-Container-Object-Count"))
	require.Equal(t, "0", rsp.Header().Get("X-Container-Bytes-Used"))
	require.Equal(t, "2", rsp.Header().Get("X-Backend-Storage-Policy-Index"))
	require.Equal(t, "1000000000.00001", rsp.Header().Get("X-Put-Timestamp"))
}

func TestContainerPutNoPolicy(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
	require.Equal(t, "0", rsp.Header().Get("X-Backend-Storage-Policy-Index"))
}

func TestContainerDeleteNotEmpty(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := test.MakeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", common.GetTimestamp())
		req.Header.Set("X-Content-Type", "application/octet-stream")
		req.Header.Set("X-Size", "2")
		req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.Status)
	}

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 409, rsp.Status)
}

func TestContainerPutObjectsGet(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := test.MakeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", common.GetTimestamp())
		req.Header.Set("X-Content-Type", "application/octet-stream")
		req.Header.Set("X-Size", "2")
		req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.Status)
	}

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c?format=json", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "application/json; charset=utf-8", rsp.Header().Get("Content-Type"))
	require.Equal(t, "3", rsp.Header().Get("X-Container-Object-Count"))
	require.Equal(t, "6", rsp.Header().Get("X-Container-Bytes-Used"))
	var data []ObjectListingRecord
	require.Nil(t, json.Unmarshal(rsp.Body.Bytes(), &data))
	require.Equal(t, 3, len(data))
	require.Equal(t, "1", data[0].Name)
	require.Equal(t, "2", data[1].Name)
	require.Equal(t, "3", data[2].Name)
	require.Equal(t, int64(2), data[2].Size)
	require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", data[2].ETag)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c", nil)
	req.Header.Set("Accept", "application/json")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "application/json; charset=utf-8", rsp.Header().Get("Content-Type"))

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "1\n2\n3\n", rsp.Body.String())

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c?format=xml", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "application/xml; charset=utf-8", rsp.Header().Get("Content-Type"))
	// TODO parse and validate xml.  or maybe we won't do that.
}

func TestContainerPutObjectsFails(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	server.containerEngine = fakeContainerEngine{}

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 500, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 500, rsp.Status)
}

func TestContainerGetTextEmpty(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
}

func TestContainerPutObjectBadRequests(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)
}

func TestContainerPutDeleteObjectsGet(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := test.MakeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", common.GetTimestamp())
		req.Header.Set("X-Content-Type", "application/octet-stream")
		req.Header.Set("X-Size", "2")
		req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.Status)
	}

	for _, object := range []string{"1", "2", "3"} {
		rsp := test.MakeCaptureResponse()
		req, err := http.NewRequest("DELETE", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", common.GetTimestamp())
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 204, rsp.Status)
	}

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
	require.Equal(t, "0", rsp.Header().Get("X-Container-Object-Count"))
	require.Equal(t, "0", rsp.Header().Get("X-Container-Bytes-Used"))
}

func TestContainerMetadata(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Meta-First", "1")
	req.Header.Set("X-Container-Meta-Second", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Container-Meta-First", "!")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Container-Meta-Second", "@")
	req.Header.Set("X-Timestamp", common.CanonicalTimestamp(1))
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
	require.Equal(t, "!", rsp.Header().Get("X-Container-Meta-First"))
	require.Equal(t, "2", rsp.Header().Get("X-Container-Meta-Second"))
}

func TestContainerDelete(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.CanonicalTimestamp(1))
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestHealthcheck(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("GET", "/healthcheck", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "OK", rsp.Body.String())
}

func TestDiskUsage(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()

	server.diskInUse = common.NewKeyedLimit(2, 8)
	server.diskInUse.Acquire("sda", false)
	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("GET", "/diskusage", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	expected, err := server.diskInUse.MarshalJSON()
	require.Nil(t, err)
	require.Equal(t, string(expected), string(rsp.Body.Bytes()))
}

func TestGetServer(t *testing.T) {
	oldgethash := GetHashPrefixAndSuffix
	oldgetsync := GetSyncRealms
	defer func() {
		GetHashPrefixAndSuffix = oldgethash
		GetSyncRealms = oldgetsync
	}()
	GetHashPrefixAndSuffix = func() (string, string, error) {
		return "changeme", "changeme", nil
	}
	GetSyncRealms = func() conf.SyncRealmList {
		return conf.SyncRealmList(map[string]conf.SyncRealm{})
	}

	configString := strings.Join([]string{
		"[app:container-server]\n",
		"devices=whatever\n",
		"mount_check=false\n",
		"bind_ip=127.0.0.2\n",
		"bind_port=1000\n",
		"log_level=INFO\n"}, "")
	configString += test.GetLogPathConfLine("container.log")
	conf, err := conf.StringConfig(configString)
	require.Nil(t, err)
	bindIP, bindPort, s, logger, err := GetServer(conf, &flag.FlagSet{})
	require.Nil(t, err)
	server, ok := s.(*ContainerServer)
	require.True(t, ok)
	require.Equal(t, "127.0.0.2", bindIP)
	require.Equal(t, 1000, bindPort)
	require.NotNil(t, logger)
	require.Equal(t, "whatever", server.driveRoot)
	require.Equal(t, zap.NewAtomicLevelAt(zap.InfoLevel), server.logLevel)
	require.False(t, server.checkMounts)
	require.NotNil(t, server.updateClient)
	require.NotNil(t, server.containerEngine)
}

func TestContainerAutoCreateOnPut(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/.a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/.a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
}

func TestContainerAutoCreateOnDelete(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("DELETE", "/device/1/.a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/.a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
}

func TestContainerNotFoundOnPut(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/cX/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/cY/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestContainerVerifySync(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()
	server.syncRealms = conf.SyncRealmList(map[string]conf.SyncRealm{
		"realm1": {
			Name:     "realm1",
			Key1:     "somekey",
			Key2:     "someotherkey",
			Clusters: map[string]string{"cluster1": "http://some/cluster/url"},
		},
	})

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm2/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm1/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm2/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm1/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
}
