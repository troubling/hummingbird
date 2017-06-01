//  Copyright (c) 2017 Rackspace
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

package middleware

import (
	"bytes"
	"context"
	//"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	//"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/troubling/hummingbird/client"
)

var deleteMarkerRegex = regexp.MustCompile(`/v1/a/c_v/001/o\d{10}\.\d{5}`)

var simpleContainerList = `[{"hash":"202cb962ac59075b964b07152d234b70","last_modified":"2017-05-22T17:24:03.00000","bytes":3,"name":"001/o0000012345.12345","content_type":"application/octet-stream"},{"hash":"250cf8b51c773f3f8dc8b4be867a9a02","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"001/o0000012345.12344","content_type":"application/octet-stream"},{"hash":"68053af2923e00204c3ca7c6a3150cf7","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"001/o0000012345.12343","content_type":"application/octet-stream"}]`

var deleteMarkerContainerList = `[{"hash":"202cb962ac59075b964b07152d234b70","last_modified":"2017-05-22T17:24:03.00000","bytes":3,"name":"001/o0000012345.12345","content_type":"application/x-deleted;swift_versions_deleted=1"},{"hash":"250cf8b51c773f3f8dc8b4be867a9a02","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"001/o0000012345.12344","content_type":"application/octet-stream"},{"hash":"68053af2923e00204c3ca7c6a3150cf7","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"001/o0000012345.12343","content_type":"application/octet-stream"}]`

func TestObjectPutHistory(t *testing.T) {
	newObjectContents := "some shiny new updated contents"
	originalObjectContents := "some original contents"
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" && request.URL.Path == "/v1/a/c/o" {
			writer.Header().Set("X-Timestamp", "12345.12345")
			writer.WriteHeader(200)
			writer.Write([]byte(originalObjectContents))
		} else if request.Method == "PUT" && request.URL.Path == "/v1/a/c/o" {
			buf := make([]byte, 1024)
			length, _ := request.Body.Read(buf)
			require.NotNil(t, request.Body)
			require.Equal(t, newObjectContents, string(buf[:length]))
			writer.WriteHeader(201)
		} else if request.Method == "PUT" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12345" {
			buf := make([]byte, 1024)
			length, _ := request.Body.Read(buf)
			require.NotNil(t, request.Body)
			require.Equal(t, originalObjectContents, string(buf[:length]))
			writer.WriteHeader(201)
		} else {
			if request.Body != nil {
				buf := make([]byte, 1024)
				_, _ = request.Body.Read(buf)
			}
			require.FailNow(t, fmt.Sprintf("Unexpected request, method: %s, path: %s", request.Method, request.URL.Path))
		}
	})
	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("PUT", "/v1/a/c/o", bytes.NewBuffer([]byte(newObjectContents)))
	ctx := &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{
			next: next,
		},
		Logger: zap.NewNop(),
		C: client.NewProxyClient(nil, nil, map[string]*client.ContainerInfo{
			"container/a/c": {
				SysMetadata: map[string]string{
					"Versions-Location": "c_v",
					"Versions-Mode":     "history",
				},
			},
			"container/a/c_v": {},
		}),
	}

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))
	require.Nil(t, err)

	vw.ServeHTTP(w, req)
	resp := w.Result()

	require.Equal(t, 201, resp.StatusCode)
}

func TestObjectDeleteHistory(t *testing.T) {
	originalObjectContents := "some original contents"
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" && request.URL.Path == "/v1/a/c/o" {
			writer.Header().Set("X-Timestamp", "12345.12345")
			writer.WriteHeader(200)
			writer.Write([]byte(originalObjectContents))
		} else if request.Method == "DELETE" && request.URL.Path == "/v1/a/c/o" {
			require.Equal(t, int64(0), request.ContentLength)
			writer.WriteHeader(204)
		} else if request.Method == "PUT" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12345" {
			buf := make([]byte, 1024)
			length, _ := request.Body.Read(buf)
			require.NotNil(t, request.Body)
			require.Equal(t, originalObjectContents, string(buf[:length]))
			writer.WriteHeader(201)
		} else if request.Method == "PUT" && deleteMarkerRegex.MatchString(request.URL.Path) {
			require.Equal(t, int64(0), request.ContentLength)
			writer.WriteHeader(201)
		} else {
			if request.Body != nil {
				buf := make([]byte, 1024)
				_, _ = request.Body.Read(buf)
			}
			require.FailNow(t, fmt.Sprintf("Unexpected request, method: %s, path: %s", request.Method, request.URL.Path))
		}
	})

	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("DELETE", "/v1/a/c/o", http.NoBody)
	ctx := &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{
			next: next,
		},
		Logger: zap.NewNop(),
		C: client.NewProxyClient(nil, nil, map[string]*client.ContainerInfo{
			"container/a/c": {
				SysMetadata: map[string]string{
					"Versions-Location": "c_v",
					"Versions-Mode":     "history",
				},
			},
			"container/a/c_v": {},
		}),
	}

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))
	require.Nil(t, err)

	vw.ServeHTTP(w, req)
	resp := w.Result()

	require.Equal(t, 204, resp.StatusCode)
}

func TestObjectDeleteStack(t *testing.T) {
	originalObjectContents := "some original contents"
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "PUT" && request.URL.Path == "/v1/a/c/o" {
			buf := make([]byte, 1024)
			length, _ := request.Body.Read(buf)
			require.NotNil(t, request.Body)
			require.Equal(t, originalObjectContents, string(buf[:length]))
			writer.WriteHeader(201)
		} else if request.Method == "GET" && request.URL.Path == "/v1/a/c_v" {
			writer.Header().Set("X-Timestamp", "12345.12345")
			writer.WriteHeader(200)
			writer.Write([]byte(simpleContainerList))
		} else if request.Method == "DELETE" && request.URL.Path == "/v1/a/c/o" {
			require.Equal(t, int64(0), request.ContentLength)
			writer.WriteHeader(204)
		} else if request.Method == "GET" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12345" {
			writer.Header().Set("X-Timestamp", "12345.12345")
			writer.WriteHeader(200)
			writer.Write([]byte(originalObjectContents))
		} else if request.Method == "DELETE" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12345" {
			writer.WriteHeader(204)
		} else if request.Method == "PUT" && deleteMarkerRegex.MatchString(request.URL.Path) {
			require.Equal(t, int64(0), request.ContentLength)
			writer.WriteHeader(201)
		} else {
			if request.Body != nil {
				buf := make([]byte, 1024)
				_, _ = request.Body.Read(buf)
			}
			require.FailNow(t, fmt.Sprintf("Unexpected request, method: %s, path: %s", request.Method, request.URL.Path))
		}
	})

	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("DELETE", "/v1/a/c/o", http.NoBody)
	ctx := &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{
			next: next,
		},
		Logger: zap.NewNop(),
		C: client.NewProxyClient(nil, nil, map[string]*client.ContainerInfo{
			"container/a/c": {
				SysMetadata: map[string]string{
					"Versions-Location": "c_v",
					"Versions-Mode":     "stack",
				},
			},
			"container/a/c_v": {},
		}),
	}

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))
	require.Nil(t, err)

	vw.ServeHTTP(w, req)
	resp := w.Result()

	require.Equal(t, 204, resp.StatusCode)
}

func TestObjectDeleteStackMarker(t *testing.T) {
	originalObjectContents := "some original contents"
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "PUT" && request.URL.Path == "/v1/a/c/o" {
			buf := make([]byte, 1024)
			length, _ := request.Body.Read(buf)
			require.NotNil(t, request.Body)
			require.Equal(t, originalObjectContents, string(buf[:length]))
			writer.WriteHeader(201)
		} else if request.Method == "GET" && request.URL.Path == "/v1/a/c_v" {
			writer.Header().Set("X-Timestamp", "12345.12345")
			writer.WriteHeader(200)
			writer.Write([]byte(deleteMarkerContainerList))
		} else if request.Method == "HEAD" && request.URL.Path == "/v1/a/c/o" {
			require.Equal(t, int64(0), request.ContentLength)
			writer.WriteHeader(404)
		} else if request.Method == "GET" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12344" {
			writer.Header().Set("X-Timestamp", "12345.12345")
			writer.WriteHeader(200)
			writer.Write([]byte(originalObjectContents))
		} else if request.Method == "DELETE" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12344" {
			writer.WriteHeader(204)
		} else if request.Method == "DELETE" && request.URL.Path == "/v1/a/c_v/001/o0000012345.12345" {
			writer.WriteHeader(204)
		} else if request.Method == "PUT" && deleteMarkerRegex.MatchString(request.URL.Path) {
			require.Equal(t, int64(0), request.ContentLength)
			writer.WriteHeader(201)
		} else {
			if request.Body != nil {
				buf := make([]byte, 1024)
				_, _ = request.Body.Read(buf)
			}
			require.FailNow(t, fmt.Sprintf("Unexpected request, method: %s, path: %s", request.Method, request.URL.Path))
		}
	})

	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("DELETE", "/v1/a/c/o", http.NoBody)
	ctx := &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{
			next: next,
		},
		Logger: zap.NewNop(),
		C: client.NewProxyClient(nil, nil, map[string]*client.ContainerInfo{
			"container/a/c": {
				SysMetadata: map[string]string{
					"Versions-Location": "c_v",
					"Versions-Mode":     "stack",
				},
			},
			"container/a/c_v": {},
		}),
	}

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))
	require.Nil(t, err)

	vw.ServeHTTP(w, req)
	resp := w.Result()

	require.Equal(t, 204, resp.StatusCode)
}

func TestRemoveVersioning(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "", request.Header["X-Versions-Location"][0])
		require.Equal(t, "", request.Header["X-Container-Sysmeta-Versions-Location"][0])
		require.Equal(t, "", request.Header["X-Container-Sysmeta-Versions-Mode"][0])
		writer.WriteHeader(200)
		writer.Write([]byte("HELLO"))
	})
	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("POST", "/v1/a/c", nil)
	fakeContext := NewFakeProxyContext(next)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	req.Header.Set("X-Remove-Versions-Location", "anywhere")
	vw.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "HELLO", string(body))
	require.Equal(t, "", resp.Header.Get("X-Versions-Location"))
}

func TestAddVersioning(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "", request.Header.Get("X-Versions-Location"))
		require.Equal(t, "", request.Header.Get("X-Versions-Mode"))
		require.Equal(t, "anywhere", request.Header.Get("X-Container-Sysmeta-Versions-Location"))
		require.Equal(t, "stack", request.Header.Get("X-Container-Sysmeta-Versions-Mode"))
		writer.Header().Set("X-Container-Sysmeta-Versions-Location", "anywhere")
		writer.Header().Set("X-Container-Sysmeta-Versions-Mode", "stack")
		writer.WriteHeader(200)
		writer.Write([]byte("HELLO"))
	})
	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("POST", "/v1/a/c", nil)
	fakeContext := NewFakeProxyContext(next)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	req.Header.Set("X-Versions-Location", "anywhere")
	vw.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "HELLO", string(body))
	require.Equal(t, "anywhere", resp.Header.Get("X-Versions-Location"))
}

func TestNotBothTypes(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
		writer.Write([]byte("HELLO"))
	})
	vw := versionedWrites{next: next, enabled: true}
	w := httptest.NewRecorder()

	req, err := http.NewRequest("POST", "/v1/a/c", nil)
	fakeContext := NewFakeProxyContext(next)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	req.Header.Set("X-Versions-Location", "anywhere")
	req.Header.Set("X-History-Location", "anywhere")
	vw.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "Only one of X-Versions-Location or X-History-Location may be specified", string(body))
}

func TestDisabled(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
		writer.Write([]byte("HELLO"))
	})
	vw := versionedWrites{next: next, enabled: false}

	req, err := http.NewRequest("POST", "/v1/a/c/o", nil)
	fakeContext := NewFakeProxyContext(next)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	w := httptest.NewRecorder()
	vw.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "HELLO", string(body))

	w2 := httptest.NewRecorder()
	req.Header.Set("X-Versions-Location", "anywhere")
	vw.ServeHTTP(w2, req)
	resp2 := w2.Result()
	body2, _ := ioutil.ReadAll(resp2.Body)

	require.Equal(t, 412, resp2.StatusCode)
	require.Equal(t, "Versioned Writes is disabled", string(body2))

	req.Header.Del("X-Versions-Location")
	req.Header.Set("X-History-Location", "anywhere")
	w3 := httptest.NewRecorder()
	vw.ServeHTTP(w3, req)
	resp3 := w3.Result()
	body3, _ := ioutil.ReadAll(resp3.Body)

	require.Equal(t, 412, resp3.StatusCode)
	require.Equal(t, "Versioned Writes is disabled", string(body3))
}
