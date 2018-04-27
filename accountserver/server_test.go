//  Copyright (c) 2016-2017 Rackspace
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

package accountserver

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var makeTestServerCounter uint64 = 0

func makeTestServer() (http.Handler, func(), error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, nil, err
	}
	if err := os.Mkdir(filepath.Join(dir, "device"), 0777); err != nil {
		return nil, nil, err
	}
	server := &AccountServer{
		driveRoot:        dir,
		hashPathPrefix:   "changeme",
		hashPathSuffix:   "changeme",
		logLevel:         zap.NewAtomicLevelAt(zapcore.InfoLevel),
		logger:           zap.NewNop(),
		checkMounts:      false,
		accountEngine:    newLRUEngine(dir, "changeme", "changeme", 32),
		diskInUse:        common.NewKeyedLimit(2, 2),
		autoCreatePrefix: ".",
	}
	cleanup := func() {
		os.RemoveAll(dir)
	}
	return server.GetHandler(*new(conf.Config), fmt.Sprintf("test_accountserver_%d", atomic.AddUint64(&makeTestServerCounter, 1))), cleanup, nil
}

func TestAccountGetNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("GET", "/device/1/a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestAccountDeleteNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("DELETE", "/device/1/a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestAccountPostDeleted(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a", nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a", nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestAccountPostNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("POST", "/device/1/a", nil)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.Status)
}

func TestAccountPutBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)
}

func TestAccountDeleteBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a", nil)
	req.Header.Set("X-Timestamp", "invalid")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.Status)
}

func TestAccountPutHead(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
	require.Equal(t, "0", rsp.Header().Get("X-Account-Object-Count"))
	require.Equal(t, "0", rsp.Header().Get("X-Account-Bytes-Used"))
	require.Equal(t, "1000000000.00001", rsp.Header().Get("X-Put-Timestamp"))
}

func TestAccountDeleteNotEmpty(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := test.MakeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Put-Timestamp", common.GetTimestamp())
		req.Header.Set("X-Object-Count", "0")
		req.Header.Set("X-Bytes-Used", "0")
		req.Header.Set("X-Backend-Storage-Policy-Index", "0")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.Status)
	}

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 409, rsp.Status)
}

func TestAccountPutContainersGet(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := test.MakeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Put-Timestamp", common.GetTimestamp())
		req.Header.Set("X-Object-Count", "0")
		req.Header.Set("X-Bytes-Used", "0")
		req.Header.Set("X-Backend-Storage-Policy-Index", "0")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.Status)
	}

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "1\n2\n3\n", rsp.Body.String())

	// TODO parse and validate xml/json.  or maybe we won't do that.
	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a", nil)
	req.Header.Set("Accept", "application/json")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "application/json; charset=utf-8", rsp.Header().Get("Content-Type"))

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a?format=xml", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.Status)
	require.Equal(t, "application/xml; charset=utf-8", rsp.Header().Get("Content-Type"))
}

func TestContainerGetTextEmpty(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
}

func TestAccountMetadata(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Account-Meta-First", "1")
	req.Header.Set("X-Account-Meta-Second", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Account-Meta-First", "!")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a", nil)
	require.Nil(t, err)
	req.Header.Set("X-Account-Meta-Second", "@")
	req.Header.Set("X-Timestamp", common.CanonicalTimestamp(1))
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
	require.Equal(t, "!", rsp.Header().Get("X-Account-Meta-First"))
	require.Equal(t, "2", rsp.Header().Get("X-Account-Meta-Second"))
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

func TestAccountAutoCreateOnPut(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := test.MakeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/.a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Put-Timestamp", common.GetTimestamp())
	req.Header.Set("X-Object-Count", "0")
	req.Header.Set("X-Bytes-Used", "0")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.Status)

	rsp = test.MakeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/.a", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.Status)
}
