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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReconReportTimeFail(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/hummingbirdtime", r.URL.Path)
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

	servers := []*ipPort{{ip: host, port: port}}
	client := &http.Client{Timeout: 10 * time.Second}
	require.Equal(t, false, getTimeReport(client, servers).Passed())
}

func TestReconReportTimePass(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/hummingbirdtime", r.URL.Path)
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

	servers := []*ipPort{{ip: host, port: port, scheme: "http"}}
	client := &http.Client{Timeout: 10 * time.Second}
	require.Equal(t, true, getTimeReport(client, servers).Passed())
}

func TestReconReportRingMd5Fail(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/ringmd5", r.URL.Path)
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

	client := &http.Client{Timeout: 10 * time.Second}
	require.Equal(t, false, getRingMD5Report(
		client,
		map[string]string{"/a/object.ring.gz": "abcdf"},
		map[string]map[string]*ipPort{fmt.Sprintf("%s:%d", host, port): {"object.ring.gz": {ip: host, port: port, scheme: "http"}}},
	).Passed())
}

func TestReconReportRingMd5Pass(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/ringmd5", r.URL.Path)
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

	client := &http.Client{Timeout: 10 * time.Second}
	require.Equal(t, true, getRingMD5Report(
		client,
		map[string]string{"/a/object.ring.gz": "abcde"},
		map[string]map[string]*ipPort{fmt.Sprintf("%s:%d", host, port): {"object.ring.gz": {ip: host, port: port, scheme: "http"}}},
	).Passed())
}

func TestReconReportQuarantine(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/quarantined", r.URL.Path)
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
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	u, _ = url.Parse(ts1.URL)
	host1, ports1, _ := net.SplitHostPort(u.Host)
	host1 = "0" + host1 // force it to seem like another server
	port1, _ := strconv.Atoi(ports1)

	servers := []*ipPort{{ip: host, port: port, scheme: "http"}, {ip: host1, port: port1, scheme: "http"}}
	client := &http.Client{Timeout: 10 * time.Second}
	report := getQuarantineReport(client, servers)
	require.Equal(t, true, report.Passed())
	out := report.String()
	require.True(t, strings.Contains(out, "[quarantined_account] low: 0, high: 0, avg: 0.0"))
	require.True(t, strings.Contains(out, "[quarantined_container] low: 5, high: 5, avg: 5.0, total: 10"))
	require.True(t, strings.Contains(out, "[quarantined_objects] low: 10, high: 20, avg: 15.0, total: 30, Failed: 0.0"))
}

func TestReconQuarantineDetailReport(t *testing.T) {
	t.Parallel()
	handlersRun := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/recon/quarantineddetail", r.URL.Path)
		handlersRun++
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`
{
  "accounts": {
    "sdb4": [
      {
        "NameOnDevice": "52f9146296db1c31308103a83a7667ed-8848222",
        "NameInURL": ""
      },
      {
        "NameOnDevice": "a2d288042a86975c5e000e0e4b8d5a2b-12343444",
        "NameInURL": "/.admin"
      }
    ]
  },
  "containers": {
    "sdb4": [
      {
        "NameOnDevice": "330db13d1978d2eaca43612c433bb1be-234234234",
        "NameInURL": "/.admin/disp-conts-204-270"
      },
      {
        "NameOnDevice": "ff2d04f90fe4099ce8ecc514bbf514b2-413332114",
        "NameInURL": ""
      }
    ]
  },
  "objects": {
    "sdb4": [
      {
        "NameOnDevice": "197ce7d697904ffaada1a16ee3f7a8c0-8585858",
        "NameInURL": ""
      },
      {
        "NameOnDevice": "a4f4d624d9a18c20addf439bcb7192e8-2399494",
        "NameInURL": "/AUTH_test/test-container/.git/objects/ea/0192ee16fc8ee99f594c42c6804012732d9153"
      }
    ]
  }
}
		`))
	}))
	defer ts.Close()

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/recon/quarantineddetail", r.URL.Path)
		handlersRun++
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`
{
  "objects": {
    "sdb4": [
      {
        "NameOnDevice": "893848384384aadaada1a16ee3f7a8c0-9293238",
        "NameInURL": "/AUTH_test/test-container/haha"
      }
    ]
  },
  "objects-1": {
    "sdb4": [
      {
        "NameOnDevice": "ef34e7d697904ffaada1a16ee3f7a8c0-9388223",
        "NameInURL": ""
      },
      {
        "NameOnDevice": "8383a624d9a18c20addf439bcb7192e8-2988930",
        "NameInURL": "/AUTH_something/test2/blah/blah/blah"
      }
    ]
  }
}
		`))
	}))
	defer ts1.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	u, _ = url.Parse(ts1.URL)
	host1, ports1, _ := net.SplitHostPort(u.Host)
	host1 = "0" + host1 // force it to seem like another server
	port1, _ := strconv.Atoi(ports1)

	servers := []*ipPort{{ip: host, port: port, scheme: "http"}, {ip: host1, port: port1, scheme: "http"}}
	client := &http.Client{Timeout: 10 * time.Second}
	report := getQuarantineDetailReport(client, servers)
	require.Equal(t, true, report.Passed(), report.String())
	out := report.String()
	require.True(t, strings.Contains(out, "2/2 hosts matched, 0 error[s] while checking hosts."), out)
	look := fmt.Sprintf(`  %s
    sdb4
      197ce7d697904ffaada1a16ee3f7a8c0-8585858 
      a4f4d624d9a18c20addf439bcb7192e8-2399494 /AUTH_test/test-container/.git/objects/ea/0192ee16fc8ee99f594c42c6804012732d9153
`, host)
	look1 := fmt.Sprintf(`  %s
      893848384384aadaada1a16ee3f7a8c0-9293238 /AUTH_test/test-container/haha
`, host1)
	if port < port1 {
		look = "\nobjects\n" + look + look1
	} else {
		look = "\nobjects\n" + look1 + look
	}
	look = fmt.Sprintf(`
objects-1
  %s
    sdb4
      8383a624d9a18c20addf439bcb7192e8-2988930 /AUTH_something/test2/blah/blah/blah
      ef34e7d697904ffaada1a16ee3f7a8c0-9388223 
`, host1)
	require.True(t, strings.Contains(out, look), fmt.Sprintf("\n%q\n%q", out, look))
	require.Equal(t, handlersRun, 2)
}

func TestReconReportAsync(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/async", r.URL.Path)
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
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	u, _ = url.Parse(ts1.URL)
	host1, ports1, _ := net.SplitHostPort(u.Host)
	port1, _ := strconv.Atoi(ports1)

	servers := map[string]*ipPort{"a": {ip: host, port: port, scheme: "http"}, "b": {ip: host1, port: port1, scheme: "http"}}
	client := &http.Client{Timeout: 10 * time.Second}
	report := &asyncReport{
		Name:  "Async Pending Report",
		Time:  time.Now().UTC(),
		Stats: map[int]map[string]int{},
	}
	getAsyncReportHelper(client, report, servers, 0)
	require.Equal(t, 0, len(report.Errors))
	out := report.String()
	require.True(t, strings.Contains(out, "[async_pending] low: 50, high: 100, avg: 75.0, total: 150, Failed: 0.0%, no_result: 0, reported: 2"))
}
