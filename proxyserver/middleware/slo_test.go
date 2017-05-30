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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var simplePutManifest = `[{"path":"/hat/a"},{"size_bytes":3,"path":"/hat/b"},{"etag":"68053af2923e00204c3ca7c6a3150cf7","size_bytes":3,"path":"hat/c"}]`

var simpleManifest = `[{"hash":"202cb962ac59075b964b07152d234b70","last_modified":"2017-05-22T17:24:03.00000","bytes":3,"name":"/hat/a","content_type":"application/octet-stream"},{"hash":"250cf8b51c773f3f8dc8b4be867a9a02","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"/hat/b","content_type":"application/octet-stream"},{"hash":"68053af2923e00204c3ca7c6a3150cf7","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"hat/c","content_type":"application/octet-stream"}]`

var rangedManifest = `[{"hash":"202cb962ac59075b964b07152d234b70","last_modified":"2017-05-22T17:24:03.00000","bytes":3,"name":"/hat/a","content_type":"application/octet-stream", "range": "1-2"},{"hash":"250cf8b51c773f3f8dc8b4be867a9a02", "range":"0-1", "last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"/hat/b","content_type":"application/octet-stream"},{"hash":"68053af2923e00204c3ca7c6a3150cf7","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"hat/c","content_type":"application/octet-stream"}]`

var superManifest = `[
{"hash":"202cb962ac59075b964b07152d234b70","range":"2-7","last_modified":"2017-05-22T17:24:03.00000","bytes":9,"name":"/hat/man","sub_slo": true, "content_type":"application/octet-stream"},
{"hash":"250cf8b51c773f3f8dc8b4be867a9a02","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"/hat/b","content_type":"application/octet-stream"},
{"hash":"68053af2923e00204c3ca7c6a3150cf7","last_modified":"2017-05-22T17:24:04.00000","bytes":3,"name":"hat/c","content_type":"application/octet-stream"}]`

func TestGetRegular(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" {
			writer.WriteHeader(200)
			writer.Write([]byte("not a slo"))
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "v/a/c/o", nil)
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, resp.StatusCode, 200)
	require.Equal(t, string(body), "not a slo")
}

func TestGetMultipartManifest(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" {
			writer.Header().Set("X-Static-Large-Object", "True")
			writer.Header().Set("Content-Type", "app/html")
			writer.WriteHeader(200)
			writer.Write([]byte(simpleManifest))
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/v/a/c/o?multipart-manifest=get", nil)
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, resp.Header.Get("Content-Type"), "application/json; charset=utf-8")
	var manifest []sloItem
	err = json.Unmarshal(body, &manifest)
	require.Nil(t, err)
	require.Equal(t, 3, len(manifest))
	require.Equal(t, "/hat/a", manifest[0].Name)

	w = httptest.NewRecorder()
	req, err = http.NewRequest("GET", "/v/a/c/o?multipart-manifest=get&format=raw", nil)
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	nresp := w.Result()
	body, _ = ioutil.ReadAll(nresp.Body)

	require.Equal(t, "app/html", nresp.Header.Get("Content-Type"))
	var pManifest []sloPutManifest
	err = json.Unmarshal(body, &pManifest)
	require.Nil(t, err)
	require.Equal(t, 3, len(pManifest))
	require.Equal(t, "/hat/b", pManifest[1].Path)
}

func TestGetSlo(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" {
			switch request.URL.Path {
			case "/v1/a/c/o":
				writer.Header().Set("X-Static-Large-Object", "True")
				writer.Header().Set("Content-Type", "app/html")
				writer.WriteHeader(200)
				writer.Write([]byte(simpleManifest))
			case "/v1/a/hat/a":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"202cb962ac59075b964b07152d234b70\"")
				writer.WriteHeader(200)
				writer.Write([]byte("123"))
			case "/v1/a/hat/b":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"250cf8b51c773f3f8dc8b4be867a9a02\"")
				writer.WriteHeader(200)
				writer.Write([]byte("456"))
			case "/v1/a/hat/c":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"68053af2923e00204c3ca7c6a3150cf7\"")
				writer.WriteHeader(200)
				writer.Write([]byte("789"))
			}
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/v1/a/c/o", nil)
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, resp.Header.Get("Content-Type"), "app/html")
	require.Equal(t, "123456789", string(body))
}

func TestGetSloRangeRequest(t *testing.T) {
	var paths []string
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		if request.Method == "GET" {
			switch request.URL.Path {
			case "/v1/a/c/o":
				writer.Header().Set("X-Static-Large-Object", "True")
				writer.Header().Set("Content-Type", "app/html")
				writer.WriteHeader(200)
				writer.Write([]byte(simpleManifest))
			case "/v1/a/hat/a":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"202cb962ac59075b964b07152d234b70\"")
				writer.WriteHeader(200)
				writer.Write([]byte("123"))
			case "/v1/a/hat/b":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"250cf8b51c773f3f8dc8b4be867a9a02\"")
				writer.WriteHeader(200)
				require.Equal(t, "bytes=1-2", request.Header.Get("Range"))
				writer.Write([]byte("56"))
			case "/v1/a/hat/c":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"68053af2923e00204c3ca7c6a3150cf7\"")
				require.Equal(t, "bytes=0-1", request.Header.Get("Range"))
				writer.WriteHeader(200)
				writer.Write([]byte("78"))
			}
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("Range", "bytes=4-7")

	sm.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, resp.Header.Get("Content-Type"), "app/html")
	require.Equal(t, "5678", string(body))
	require.Equal(t, "/v1/a/c/o", paths[0])
	require.Equal(t, "/v1/a/c/o", paths[1])
	require.Equal(t, "/v1/a/hat/b", paths[2])
	require.Equal(t, "/v1/a/hat/c", paths[3])
}

func TestGetRangedSlo(t *testing.T) {
	var paths []string
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		if request.Method == "GET" {
			switch request.URL.Path {
			case "/v1/a/c/o":
				writer.Header().Set("X-Static-Large-Object", "True")
				writer.Header().Set("Content-Type", "app/html")
				writer.WriteHeader(200)
				writer.Write([]byte(rangedManifest))
			case "/v1/a/hat/a":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"202cb962ac59075b964b07152d234b70\"")
				writer.WriteHeader(200)
				require.Equal(t, "bytes=1-2", request.Header.Get("Range"))
				writer.Write([]byte("23"))
			case "/v1/a/hat/b":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"250cf8b51c773f3f8dc8b4be867a9a02\"")
				writer.WriteHeader(200)
				require.Equal(t, "bytes=0-1", request.Header.Get("Range"))
				writer.Write([]byte("45"))
			case "/v1/a/hat/c":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"68053af2923e00204c3ca7c6a3150cf7\"")
				writer.WriteHeader(200)
				writer.Write([]byte("789"))
			}
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/v1/a/c/o", nil)
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, resp.Header.Get("Content-Type"), "app/html")
	require.Equal(t, "2345789", string(body))
	require.Equal(t, "/v1/a/c/o", paths[0])
	require.Equal(t, "/v1/a/hat/a", paths[1])
	require.Equal(t, "/v1/a/hat/b", paths[2])
	require.Equal(t, "/v1/a/hat/c", paths[3])
}

func TestGetSuperSlo(t *testing.T) {
	var paths []string
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		if request.Method == "GET" {
			switch request.URL.Path {
			case "/v1/a/c/o":
				writer.Header().Set("X-Static-Large-Object", "True")
				writer.Header().Set("Content-Type", "app/html")
				writer.WriteHeader(200)
				writer.Write([]byte(superManifest))
			case "/v1/a/hat/man":
				writer.Header().Set("X-Static-Large-Object", "True")
				writer.Header().Set("Content-Type", "app/html")
				writer.WriteHeader(200)
				writer.Write([]byte(simpleManifest))
			case "/v1/a/hat/a":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"202cb962ac59075b964b07152d234b70\"")
				writer.WriteHeader(200)
				if request.Header.Get("Range") == "bytes=2-2" {
					writer.Write([]byte("3"))
				} else {
					writer.Write([]byte("123"))
				}
			case "/v1/a/hat/b":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"250cf8b51c773f3f8dc8b4be867a9a02\"")
				writer.WriteHeader(200)
				writer.Write([]byte("456"))
			case "/v1/a/hat/c":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Etag", "\"68053af2923e00204c3ca7c6a3150cf7\"")
				writer.WriteHeader(200)
				if request.Header.Get("Range") == "bytes=0-1" {
					writer.Write([]byte("78"))
				} else {
					writer.Write([]byte("789"))
				}
			}
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/v1/a/c/o", nil)
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, resp.Header.Get("Content-Type"), "app/html")
	require.Equal(t, "345678456789", string(body))
	require.Equal(t, "/v1/a/c/o", paths[0])
	require.Equal(t, "/v1/a/hat/man", paths[1])
	require.Equal(t, "/v1/a/hat/a", paths[2])
	require.Equal(t, "/v1/a/hat/b", paths[3])
	require.Equal(t, "/v1/a/hat/c", paths[4])
	require.Equal(t, "/v1/a/hat/b", paths[5])
	require.Equal(t, "/v1/a/hat/c", paths[6])
}

func TestPutSlo(t *testing.T) {
	var heads []string
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "PUT" {
			require.Equal(t, "/v1/a/c/o", request.URL.Path)
			writer.WriteHeader(201)
		}
		if request.Method == "HEAD" {
			heads = append(heads, request.URL.Path)
			switch request.URL.Path {
			case "/v1/a/hat/a":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Content-Length", "3")
				writer.Header().Set("Etag", "\"202cb962ac59075b964b07152d234b70\"")
				writer.WriteHeader(200)
			case "/v1/a/hat/b":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Content-Length", "3")
				writer.Header().Set("Etag", "\"250cf8b51c773f3f8dc8b4be867a9a02\"")
				writer.WriteHeader(200)
			case "/v1/a/hat/c":
				writer.Header().Set("Content-Type", "octet")
				writer.Header().Set("Content-Length", "3")
				writer.Header().Set("Etag", "\"68053af2923e00204c3ca7c6a3150cf7\"")
				writer.WriteHeader(200)
			}
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("PUT", "/v1/a/c/o?multipart-manifest=put", bytes.NewBuffer([]byte(simplePutManifest)))
	req.Header.Set("Content-Type", "app/html")
	req.Header.Set("Content-Length", strconv.Itoa(len(simplePutManifest)))
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()
	//body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 201, resp.StatusCode)
	require.Equal(t, "/v1/a/hat/a", heads[0])
	require.Equal(t, "/v1/a/hat/b", heads[1])
	require.Equal(t, "/v1/a/hat/c", heads[2])
}

func TestDeleteSlo(t *testing.T) {
	var paths []string
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" {
			paths = append(paths, request.URL.Path)
			writer.Header().Set("X-Static-Large-Object", "True")
			writer.Header().Set("Content-Type", "app/html")
			writer.Header().Set("Content-Length", strconv.Itoa(len(simpleManifest)))
			writer.WriteHeader(200)
			writer.Write([]byte(simpleManifest))
			require.Equal(t, "/v1/a/c/o", request.URL.Path)
		}
		if request.Method == "DELETE" {
			switch request.URL.Path {
			case "/v1/a/hat/a":
				paths = append(paths, request.URL.Path)
				writer.Header().Set("Content-Type", "app/html")
				writer.Header().Set("Content-Length", "0")
				writer.WriteHeader(204)
			case "/v1/a/hat/b":
				paths = append(paths, request.URL.Path)
				writer.Header().Set("Content-Type", "app/html")
				writer.Header().Set("Content-Length", "0")
				writer.WriteHeader(204)
			case "/v1/a/hat/c":
				paths = append(paths, request.URL.Path)
				writer.Header().Set("Content-Type", "app/html")
				writer.Header().Set("Content-Length", "0")
				writer.WriteHeader(204)
			case "/v1/a/c/o":
				paths = append(paths, request.URL.Path)
				writer.Header().Set("Content-Type", "app/html")
				writer.Header().Set("Content-Length", "0")
				writer.WriteHeader(204)
			}
		}
	})
	sm := sloMiddleware{next: next}
	w := httptest.NewRecorder()
	req, err := http.NewRequest("DELETE", "/v1/a/c/o?multipart-manifest=delete", nil)
	req.Header.Set("Content-Length", "0")
	require.Nil(t, err)

	sm.ServeHTTP(w, req)
	resp := w.Result()

	require.Equal(t, 204, resp.StatusCode)
	require.Equal(t, "/v1/a/c/o", paths[0])
	require.Equal(t, "/v1/a/hat/a", paths[1])
	require.Equal(t, "/v1/a/hat/b", paths[2])
	require.Equal(t, "/v1/a/hat/c", paths[3])
	require.Equal(t, "/v1/a/c/o", paths[4])
}
