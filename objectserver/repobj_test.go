//  Copyright (c) 2018 Rackspace
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
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
)

func TestReplicateStabilizeDeletion(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			atomic.AddInt64(&calls, 1)
		}
		io.Copy(ioutil.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ot := newTestIndexDB(t, dir)

	ro := &repObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: true,
			Nursery:  true,
			Path:     fp.Name(),
		},
		client: http.DefaultClient,
		metadata: map[string]string{
			"Content-Length": "7",
			"name":           "/a/c/o",
		},
		idb: ot,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
			{Id: 4, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sda"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.Nil(t, ro.Stabilize(rng, node, 1))
	require.Equal(t, int64(2), calls)
}

func TestReplicateStabilizeDeletionFail(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			atomic.AddInt64(&calls, 1)
		}
		if strings.Contains(r.URL.Path, "sdd") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.Copy(ioutil.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ot := newTestIndexDB(t, dir)

	ro := &repObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: true,
			Nursery:  true,
			Path:     fp.Name(),
		},
		client: http.DefaultClient,
		metadata: map[string]string{
			"Content-Length": "7",
			"name":           "/a/c/o",
		},
		idb: ot,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
			{Id: 4, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sda"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.NotNil(t, ro.Stabilize(rng, node, 1))
	require.Equal(t, int64(2), calls)
}

func TestReplicateRestabilize(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			atomic.AddInt64(&calls, 1)
		}
		require.Equal(t, r.Header.Get("Meta-X-Object-Meta-Hey"), "there")
		require.Equal(t, r.Header.Get("X-Timestamp"), "1000.000")
		require.Equal(t, r.Header.Get("Meta-X-Timestamp"), "1000.000")
		io.Copy(ioutil.Discard, r.Body)
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ot := newTestIndexDB(t, dir)

	ro := &repObject{
		IndexDBItem: IndexDBItem{
			Hash:        "00000011111122222233333344444455",
			Restabilize: true,
			Path:        fp.Name(),
		},
		client: http.DefaultClient,
		metadata: map[string]string{
			"Content-Length":    "7",
			"name":              "/a/c/o",
			"X-Object-Meta-Hey": "there",
			"X-Timestamp":       "1000.000",
		},
		idb: ot,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
			{Id: 4, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sda"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.Nil(t, ro.Stabilize(rng, node, 1))
	require.Equal(t, int64(2), calls)
}

func TestReplicateCanStabilize(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			atomic.AddInt64(&calls, 1)
		}
		w.Header().Set("X-Timestamp", "1000.000")
		io.Copy(ioutil.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ot := newTestIndexDB(t, dir)

	hsh := "00000011111122222233333344444455"
	metad := map[string]string{
		"Content-Length":    "7",
		"name":              "/a/c/o",
		"X-Object-Meta-Hey": "there",
		"X-Timestamp":       "1000.000",
	}
	ro := &repObject{
		IndexDBItem: IndexDBItem{
			Hash:    hsh,
			Nursery: true,
			Path:    fp.Name(),
		},
		client:   http.DefaultClient,
		metadata: metad,
		idb:      ot,
	}
	node := &ring.Device{Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sda"}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sdd"},
			{Id: 4, Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sda"},
		},
		MockGetJobNodesHandoff: false,
	}
	afw, err := ot.TempFile(hsh, roShard, 10000, 10, true)
	require.Nil(t, err)
	md, err := json.Marshal(metad)
	require.Nil(t, err)

	err = ot.Commit(afw, hsh, roShard, 0, "PUT", "", md, true, "")
	require.Nil(t, err)
	nurseryPath, err := ot.WholeObjectPath(hsh, roShard, 0, true)
	require.Nil(t, err)
	_, err = os.Stat(nurseryPath)
	require.Nil(t, err)
	require.Nil(t, ro.Stabilize(rng, node, 1))
	_, err = os.Stat(nurseryPath)
	require.NotNil(t, err)
	stablePath, err := ot.WholeObjectPath(hsh, roShard, 0, false)
	require.Nil(t, err)
	_, err = os.Stat(stablePath)
	require.Nil(t, err)
	require.Equal(t, int64(2), calls)
}

func TestReplicateCanStabilizeFail(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			atomic.AddInt64(&calls, 1)
		}
		if strings.Contains(r.URL.Path, "sdd") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("X-Timestamp", "1000.000")
		io.Copy(ioutil.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ot := newTestIndexDB(t, dir)

	hsh := "00000011111122222233333344444455"
	metad := map[string]string{
		"Content-Length":    "7",
		"name":              "/a/c/o",
		"X-Object-Meta-Hey": "there",
		"X-Timestamp":       "1000.000",
	}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sdd"},
			{Id: 4, Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sda"},
		},
		MockGetJobNodesHandoff: false,
	}
	ro := &repObject{
		IndexDBItem: IndexDBItem{
			Hash:    hsh,
			Nursery: true,
			Path:    fp.Name(),
		},
		client:   http.DefaultClient,
		metadata: metad,
		idb:      ot,
		rng:      rng,
	}
	node := &ring.Device{Scheme: u.Scheme, Ip: u.Hostname(), Port: port, Device: "sda"}
	afw, err := ot.TempFile(hsh, roShard, 10000, 10, true)
	require.Nil(t, err)
	md, err := json.Marshal(metad)
	require.Nil(t, err)

	err = ot.Commit(afw, hsh, roShard, 0, "PUT", "", md, true, "")
	require.Nil(t, err)
	nurseryPath, err := ot.WholeObjectPath(hsh, roShard, 0, true)
	require.Nil(t, err)
	_, err = os.Stat(nurseryPath)
	require.Nil(t, err)
	require.NotNil(t, ro.Stabilize(rng, node, 1))
	_, err = os.Stat(nurseryPath)
	require.Nil(t, err)
	stablePath, err := ot.WholeObjectPath(hsh, roShard, 0, false)
	require.Nil(t, err)
	_, err = os.Stat(stablePath)
	require.NotNil(t, err)
	require.Equal(t, int64(2), calls)
}
