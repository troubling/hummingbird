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
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestServer struct {
	*httptest.Server
	host       string
	port       int
	root       string
	objServer  *ObjectServer
	confLoader *srv.TestConfigLoader
}

func (t *TestServer) Close() {
	t.objServer.asyncWG.Wait()
	os.RemoveAll(t.root)
	t.Server.Close()
}

func (t *TestServer) Do(method string, path string, body io.ReadCloser) (*http.Response, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("http://%s:%d%s", t.host, t.port, path), body)
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

var testObjectServers uint64 = 0

func makeObjectServer(confLoader *srv.TestConfigLoader, settings ...string) (*TestServer, error) {
	driveRoot, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(driveRoot)
	configString := fmt.Sprintf("[app:object-server]\ndevices=%s\nmount_check=false\n", driveRoot)
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, err := conf.StringConfig(configString)
	if err != nil {
		return nil, err
	}
	_, server, _, err := NewServer(conf, &flag.FlagSet{}, confLoader)
	if err != nil {
		return nil, err
	}
	ts := httptest.NewServer(server.GetHandler(conf, fmt.Sprintf("test_object_%d", atomic.AddUint64(&testObjectServers, 1))))
	u, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	host, ports, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(ports)
	if err != nil {
		return nil, err
	}
	return &TestServer{Server: ts, host: host, port: port, root: driveRoot, objServer: server.(*ObjectServer), confLoader: confLoader}, nil
}

func TestReplicateRecalculate(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()
	os.MkdirAll(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff"), 0755)
	f, _ := os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.99999.data"))
	f.Close()

	trs1, err := makeReplicatorWebServer(confLoader)
	require.Nil(t, err)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot

	resp, err := trs1.Do("REPLICATE", "/sda/1", nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ := ioutil.ReadAll(resp.Body)
	hashes, err := pickle.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	f, _ = os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753550.00000.meta"))
	f.Close()
	resp, err = trs1.Do("REPLICATE", "/sda/1", nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ = ioutil.ReadAll(resp.Body)
	hashes, err = pickle.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	resp, err = trs1.Do("REPLICATE", "/sda/1/fff", nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ = ioutil.ReadAll(resp.Body)
	hashes, err = pickle.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "f78ade0081b2648499a4395d406e625c")
}

func TestBasicPutGet(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, timestamp, resp.Header.Get("X-Timestamp"))
	assert.Equal(t, "9", resp.Header.Get("Content-Length"))
}

func TestBasicPutDelete(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 404, resp.StatusCode)
}

func TestBasicPutPostGet(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Object-Meta-TestPutPostGet", "Hi!")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 202, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, timestamp, resp.Header.Get("X-Timestamp"))
	assert.Equal(t, "9", resp.Header.Get("Content-Length"))
	assert.Equal(t, "Hi!", resp.Header.Get("X-Object-Meta-TestPutPostGet"))
}

func TestPostXDA(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Object-Meta-TestPutPostGet", "Hi!")
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-Delete-At", common.CanonicalTimestampFromTime(time.Now().Add(time.Second*5)))
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 409, resp.StatusCode)
}

func TestPostContentType(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "any/thing")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 409, resp.StatusCode)
}

func TestPostNotFound(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Object-Meta-TestPostNotFound", "Howdy!")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 404, resp.StatusCode)
}

func TestGetRanges(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	getRanges := func(ranges string) (*http.Response, []byte) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
		assert.Nil(t, err)
		req.Header.Set("Range", ranges)
		resp, err := http.DefaultClient.Do(req)
		assert.Nil(t, err)
		body, err := ioutil.ReadAll(resp.Body)
		assert.Nil(t, err)
		return resp, body
	}

	resp, body := getRanges("bytes=0-5")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "ABCDEF", string(body))

	resp, body = getRanges("bytes=20-")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "UVWXYZ", string(body))

	resp, body = getRanges("bytes=-6")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "UVWXYZ", string(body))

	resp, body = getRanges("bytes=27-28")
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)

	resp, body = getRanges("bytes=20-,-6")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "multipart/byteranges;boundary="))
	assert.Equal(t, "366", resp.Header.Get("Content-Length"))
	assert.Equal(t, 2, strings.Count(string(body), "UVWXYZ"))
}

func TestBadEtag(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("ETag", "11111111111111111111111111111111")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 422, resp.StatusCode)
}

func TestCorrectEtag(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	etag := "437bba8e0bf58337674f4539e75186ac"
	req.Header.Set("ETag", etag)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)
	assert.Equal(t, etag, resp.Header.Get("Etag"))
}

func TestUppercaseEtag(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("ETag", "437BBA8E0BF58337674F4539E75186AC")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)
	assert.Equal(t, "437bba8e0bf58337674f4539e75186ac", resp.Header.Get("Etag"))
}

type shortReader struct{}

func (s *shortReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

type fakeResponse struct {
	status int
}

func (*fakeResponse) Header() http.Header {
	return make(http.Header)
}

func (*fakeResponse) Write(p []byte) (int, error) {
	return len(p), nil
}

func (f *fakeResponse) WriteHeader(s int) {
	f.status = s
}

func TestDisconnectOnPut(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), &shortReader{})
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "10")
	req.Header.Set("X-Timestamp", timestamp)

	resp := &fakeResponse{}

	ts.Server.Config.Handler.ServeHTTP(resp, req)
	assert.Equal(t, resp.status, 499)
}

func TestEmptyDevice(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d//0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestEmptyPartition(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda//a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestEmptyAccount(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0//c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestEmptyContainer(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a//o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestEmptyObject(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestBasicPutDeleteAt(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	time_unix := int(time.Now().Unix())
	time_unix += 30
	time_delete := strconv.Itoa(time_unix)

	//put file with x-delete header
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)

	req.Header.Set("X-Delete-At", time_delete)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-If-Delete-At", time_delete)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 404, resp.StatusCode)

	//put file without x-delete header
	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-If-Delete-At", time_delete)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 412, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

type slowReader struct {
	readChan chan int
	id       int
}

func (s slowReader) Read(p []byte) (i int, err error) {
	data := <-s.readChan
	if data == 1 {
		time.Sleep(100000000)
	}
	if data == 2 {
		return 0, io.EOF
	}
	p[0] = byte('a')
	return 1, nil
}

func (s *slowReader) Close() error {
	return nil
}

func TestAcquireDevice(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader, "disk_limit", "1/10")
	assert.Nil(t, err)
	defer ts.Close()

	sr1 := slowReader{id: 1, readChan: make(chan int)}
	req1, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), sr1)
	assert.Nil(t, err)
	req1.Header.Set("Content-Type", "text")
	req1.Header.Set("Content-Length", "1")
	req1.Header.Set("X-Timestamp", common.GetTimestamp())

	sr2 := slowReader{id: 2, readChan: make(chan int)}
	req2, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o2", ts.host, ts.port), sr2)
	assert.Nil(t, err)
	req2.Header.Set("Content-Type", "text")
	req2.Header.Set("Content-Length", "1")
	req2.Header.Set("X-Timestamp", common.GetTimestamp())

	done1 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req1)
		assert.Equal(t, resp.StatusCode, 201)
		assert.Nil(t, err)
		done1 <- true

	}()
	done2 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req2)
		assert.Nil(t, err)
		assert.Equal(t, resp.StatusCode, 503)
		assert.Equal(t, resp.Header.Get("X-Disk-Usage"), "1")
		done2 <- true

	}()
	//sending good read to 1
	sr1.readChan <- 0
	//sending sleep to 2, let #1 win the race
	sr2.readChan <- 1
	//sending good read to 2
	sr2.readChan <- 0
	//sending EOF to 2
	sr2.readChan <- 2
	//2 exiting goroutine
	<-done2
	//sending EOF to 1
	sr1.readChan <- 2
	//1 exiting goroutine
	<-done1
}

func TestAccountAcquireDevice(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader, "account_rate_limit", "1/0")
	assert.Nil(t, err)
	defer ts.Close()

	sr1 := slowReader{id: 1, readChan: make(chan int)}
	req1, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/α/c/o", ts.host, ts.port), sr1)
	assert.Nil(t, err)
	req1.Header.Set("Content-Type", "text")
	req1.Header.Set("Content-Length", "1")
	req1.Header.Set("X-Timestamp", common.GetTimestamp())

	sr2 := slowReader{id: 2, readChan: make(chan int)}
	req2, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/α/c/o2", ts.host, ts.port), sr2)
	assert.Nil(t, err)
	req2.Header.Set("Content-Type", "text")
	req2.Header.Set("Content-Length", "1")
	req2.Header.Set("X-Timestamp", common.GetTimestamp())

	done1 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req1)
		assert.Equal(t, resp.StatusCode, 201)
		assert.Nil(t, err)
		done1 <- true

	}()
	done2 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req2)
		assert.Nil(t, err)
		assert.Equal(t, resp.StatusCode, 498)
		assert.Equal(t, resp.Header.Get("X-Disk-Usage"), "")
		done2 <- true

	}()
	//sending good read to 1
	sr1.readChan <- 0
	//sending sleep to 2, let #1 win the race
	sr2.readChan <- 1
	//sending good read to 2
	sr2.readChan <- 0
	//sending EOF to 2
	sr2.readChan <- 2
	//2 exiting goroutine
	<-done2
	//sending EOF to 1
	sr1.readChan <- 2
	//1 exiting goroutine
	<-done1
}
