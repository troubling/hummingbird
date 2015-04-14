// //  Copyright (c) 2015 Rackspace
// //
// //  Licensed under the Apache License, Version 2.0 (the "License");
// //  you may not use this file except in compliance with the License.
// //  You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// //  Unless required by applicable law or agreed to in writing, software
// //  distributed under the License is distributed on an "AS IS" BASIS,
// //  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// //  implied.
// //  See the License for the specific language governing permissions and
// //  limitations under the License.

package proxyserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	hummingbird "hummingbird/common"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/stretchr/testify/assert"
)

type ProxyLogSaver struct {
	logged []string
}

func (s *ProxyLogSaver) Err(l string) error {
	s.logged = append(s.logged, l)
	return nil
}

func (s *ProxyLogSaver) Info(l string) error {
	s.logged = append(s.logged, l)
	return nil
}

func (s *ProxyLogSaver) Debug(l string) error {
	s.logged = append(s.logged, l)
	return nil
}

type MockRing struct{}

func (mr *MockRing) GetNodes(partition uint64) (response []*hummingbird.Device) {
	return []*hummingbird.Device{&hummingbird.Device{
		Id:              0,
		Device:          "sdb1",
		Ip:              "127.0.0.1",
		Port:            12345,
		Region:          0,
		ReplicationIp:   "",
		ReplicationPort: 0,
		Weight:          0.0,
		Zone:            0,
	}, &hummingbird.Device{
		Id:              1,
		Device:          "sdb2",
		Ip:              "127.0.0.1",
		Port:            12346,
		Region:          0,
		ReplicationIp:   "",
		ReplicationPort: 0,
		Weight:          0.0,
		Zone:            0,
	}, &hummingbird.Device{
		Id:              2,
		Device:          "sdb3",
		Ip:              "127.0.0.1",
		Port:            12347,
		Region:          0,
		ReplicationIp:   "",
		ReplicationPort: 0,
		Weight:          0.0,
		Zone:            0}}
}
func (mr *MockRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return nil, false
}
func (mr *MockRing) GetPartition(account string, container string, object string) uint64 {
	return 1234
}
func (mr *MockRing) LocalDevices(localPort int) (devs []*hummingbird.Device) {
	return nil
}

func (mr *MockRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes {
	return nil
}

func newRequest(method, path string, body io.Reader) *http.Request {
	r, _ := http.NewRequest(method, path, body)
	u, _ := url.Parse(path)
	r.URL = u
	r.RequestURI = path
	return r
}

func TestHealthCheck(t *testing.T) {
	logger := &ProxyLogSaver{}
	server := &ProxyServer{
		client:        &http.Client{},
		accountRing:   &MockRing{},
		containerRing: &MockRing{},
		objectRing:    &MockRing{},
		mc:            &memcache.Client{},
		logger:        logger,
	}
	expectedBody := "OK"
	handler := server.getHandler()
	recorder := httptest.NewRecorder()
	req := newRequest("GET", "/healthcheck", nil)
	handler.ServeHTTP(recorder, req)
	assert.Equal(t, expectedBody, recorder.Body.String())
	assert.Equal(t, 1, len(server.logger.(*logSaver).logged))
}

// func mockServerRequest(amount *int, headers map[string]string) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		status, err := strconv.ParseInt(r.Header.Get("reqStatus"), 0, 64)
// 		if err != nil {
// 			fmt.Println(err.Error())
// 		}
// 		for key := range r.Header {
// 			headers[key] = r.Header.Get(key)
// 		}
// 		w.WriteHeader(int(status))
// 		*amount = *amount + 1
// 	}
// }

// func TestHandlers(t *testing.T) {
// 	logger, conn := GetLogger()
// 	defer conn.Close()
// 	server := &ProxyServer{
// 		client:        &http.Client{},
// 		accountRing:   &MockRing{},
// 		containerRing: &MockRing{},
// 		objectRing:    &MockRing{},
// 		mc:            &memcache.Client{},
// 		logger:        logger,
// 	}
// 	nodes := server.objectRing.GetNodes(1234)
// 	// servers := []*httptest.Server{}
// 	// s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 	// 	fmt.Printf("I GOT HERE?")
// 	// }))
// 	// s.Listener, _ = net.Listen("tcp", "127.0.0.1:11213")
// 	// s.Start()
// 	// servers = append(servers, s)
// 	// sentHeaders := map[string]string{}
// 	// requests := 0
// 	// rP := &requests
// 	// for _, node := range nodes {
// 	// 	ts := httptest.NewUnstartedServer(mockServerRequest(rP, sentHeaders))
// 	// 	ts.Listener, _ = net.Listen("tcp", fmt.Sprintf("%s:%v", node.Ip, node.Port))
// 	// 	ts.Start()
// 	// 	servers = append(servers, ts)
// 	// }
// 	server.mc = memcache.New("127.0.0.1:11213")

// 	tests := []struct {
// 		matcher               string
// 		path                  string
// 		method                string
// 		body                  string
// 		headers               map[string]string
// 		handler               http.HandlerFunc
// 		requestAmounts        int
// 		requestResponseStatus int
// 		expectedBody          string
// 		expectedHeaders       []string
// 		expectedSentHeaders   []string
// 	}{
// 		{"/healthcheck", "/healthcheck", "GET", "", map[string]string{}, server.HealthCheckHandler(), 0, 200, "OK", []string{}, []string{}},
// 		{"/auth/v1.0", "/auth/v1.0", "GET", "", map[string]string{}, server.AuthHandler(), 0, 401, "Unauthorized\n", []string{}, []string{}},
// 		{"/auth/v1.0", "/auth/v1.0", "GET", "", map[string]string{"X-Auth-User": "Test"}, server.AuthHandler(), 0, 200, "OK\n",
// 			[]string{"X-Storage-Token", "X-Auth-Token", "X-Storage-Url"}, []string{}},
// 		{"/v1/{account}", "/v1/a", "GET", "", map[string]string{}, server.AccountGetHandler(), 0, 501, "Not Implemented\n", []string{}, []string{}},
// 		{"/v1/{account}", "/v1/a", "DELETE", "", map[string]string{}, server.AccountDeleteHandler(), 3, 201, "", []string{}, []string{}},
// 		{"/v1/{account}", "/v1/a", "PUT", "test", map[string]string{}, server.AccountPutHandler(), 3, 201, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}", "/v1/a/c", "GET", "", map[string]string{}, server.ContainerGetHeadHandler(), 1, 200, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}", "/v1/a/c", "HEAD", "", map[string]string{}, server.ContainerGetHeadHandler(), 1, 200, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}", "/v1/a/c", "DELETE", "", map[string]string{}, server.ContainerDeleteHandler(), 3, 201, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}", "/v1/a/c", "PUT", "test", map[string]string{}, server.ContainerPutHandler(), 3, 201, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}/{obj:.*}", "/v1/a/c/o", "GET", "", map[string]string{}, server.ObjectGetHeadHandler(), 1, 200, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}/{obj:.*}", "/v1/a/c/o", "HEAD", "", map[string]string{}, server.ObjectGetHeadHandler(), 1, 200, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}/{obj:.*}", "/v1/a/c/o", "DELETE", "", map[string]string{}, server.ObjectDeleteHandler(), 3, 201, "", []string{}, []string{}},
// 		{"/v1/{account}/{container}/{obj:.*}", "/v1/a/c/o", "PUT", "test", map[string]string{}, server.ObjectPutHandler(), 3, 201, "", []string{}, []string{
// 			"X-Container-Partition",
// 			"X-Container-Host",
// 			"X-Container-Device"}},
// 	}
// 	for _, test := range tests {
// 		*rP = 0
// 		writer := httptest.NewRecorder()
// 		request := &http.Request{}
// 		if test.method == "PUT" {
// 			request, _ = http.NewRequest(test.method, fmt.Sprintf("http://127.0.0.1:1234%s", test.path), bytes.NewBufferString(test.body))
// 		} else {
// 			request, _ = http.NewRequest(test.method, fmt.Sprintf("http://127.0.0.1:1234%s", test.path), nil)
// 		}
// 		strStat := strconv.Itoa(test.requestResponseStatus)
// 		request.Header.Add("reqStatus", strStat)
// 		for key, val := range test.headers {
// 			request.Header.Add(key, val)
// 		}
// 		mux := mux.NewRouter()
// 		mux.HandleFunc(test.matcher, test.handler).Methods(strings.ToTitle(strings.ToLower(test.method)))
// 		mux.ServeHTTP(writer, request)
// 		assert.Equal(t, test.requestResponseStatus, writer.Code, fmt.Sprintf("Test path: %s, Test Method: %s", test.path, test.method))
// 		if test.expectedBody != "" {
// 			assert.Equal(t, test.expectedBody, writer.Body.String())
// 		}
// 		if len(test.expectedSentHeaders) != 0 {
// 			for _, value := range test.expectedSentHeaders {
// 				assert.NotNil(t, sentHeaders[value], fmt.Sprintf("Header: %s was not sent", value))
// 			}
// 		}
// 		if len(test.expectedHeaders) != 0 {
// 			for _, value := range test.expectedHeaders {
// 				assert.NotNil(t, writer.Header()[value], fmt.Sprintf("Header: %s was not returned", value))
// 			}
// 		}
// 		time.Sleep(4 * time.Millisecond)
// 		assert.Equal(t, test.requestAmounts, *rP, fmt.Sprintf("Test path: %s, Test Method: %s", test.path, test.method))
// 	}
// 	for _, ser := range servers {
// 		ser.Close()
// 	}
// }
