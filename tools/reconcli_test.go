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

package tools

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReconReportTimeFail(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/hummingbirdtime", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "{\"time\": \"2017-11-17T18:57:32.276688312Z\"}")
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	servers := []ipPort{{ip: host, port: port}}
	client := http.Client{Timeout: 10 * time.Second}
	w := bufio.NewWriter(os.Stdout)
	require.Equal(t, false, reconReportTime(client, servers, w))
}

func TestReconReportTimePass(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/hummingbirdtime", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := map[string]time.Time{"time": time.Now()}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	servers := []ipPort{{ip: host, port: port}}
	client := http.Client{Timeout: 10 * time.Second}
	w := bufio.NewWriter(os.Stdout)
	require.Equal(t, true, reconReportTime(client, servers, w))
}

func TestReconReportRingMd5Fail(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/ringmd5", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := map[string]string{"/a/object.ring.gz": "abcde"}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	servers := []ipPort{{ip: host, port: port}}
	client := http.Client{Timeout: 10 * time.Second}
	w := bufio.NewWriter(os.Stdout)
	require.Equal(t, false, reconReportRingMd5(
		client, servers, map[string]string{"/a/object.ring.gz": "abcdf"}, w))
}

func TestReconReportRingMd5Pass(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/ringmd5", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := map[string]string{"/a/object.ring.gz": "abcde"}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	servers := []ipPort{{ip: host, port: port}}
	client := http.Client{Timeout: 10 * time.Second}
	w := bufio.NewWriter(os.Stdout)
	require.Equal(t, true, reconReportRingMd5(
		client, servers, map[string]string{"/a/object.ring.gz": "abcde"}, w))
}

func TestReconReportQuarantine(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/quarantined", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := quarData{Accounts: 0, Containers: 5, Objects: 10}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts.Close()

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/quarantined", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := quarData{Accounts: 0, Containers: 5, Objects: 20}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts1.Close()
	u, _ := url.Parse(ts1.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	u, _ = url.Parse(ts.URL)
	host1, ports1, _ := net.SplitHostPort(u.Host)
	port1, _ := strconv.Atoi(ports1)

	servers := []ipPort{{ip: host, port: port}, {ip: host1, port: port1}}
	client := http.Client{Timeout: 10 * time.Second}
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	require.Equal(t, true, reconReportQuarantine(
		client, servers, w))
	w.Flush()
	out := buf.String()
	require.True(t, strings.Contains(out, "[quarantined_account] low: 0, high: 0, avg: 0.0"))
	require.True(t, strings.Contains(out, "[quarantined_container] low: 5, high: 5, avg: 5.0, total: 10"))
	require.True(t, strings.Contains(out, "[quarantined_objects] low: 10, high: 20, avg: 15.0, total: 30, Failed: 0.0"))
}

func TestReconReportAsync(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/async", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := map[string]int{"async_pending": 50}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts.Close()

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/async", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		content := map[string]int{"async_pending": 100}
		serialized, _ := json.MarshalIndent(content, "", "  ")
		w.Write(serialized)
	}))
	defer ts1.Close()
	u, _ := url.Parse(ts1.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	u, _ = url.Parse(ts.URL)
	host1, ports1, _ := net.SplitHostPort(u.Host)
	port1, _ := strconv.Atoi(ports1)

	servers := []ipPort{{ip: host, port: port}, {ip: host1, port: port1}}
	client := http.Client{Timeout: 10 * time.Second}
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	require.Equal(t, true, reconReportAsync(
		client, servers, w))
	w.Flush()
	out := buf.String()
	require.True(t, strings.Contains(out, "[async_pending] low: 50, high: 100, avg: 75.0, total: 150, Failed: 0.0%, no_result: 0, reported: 2"))
}
