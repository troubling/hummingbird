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
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
)

func TestNurseryReplicate(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:       http.DefaultClient,
		dataShards:   3,
		parityShards: 2,
		chunkSize:    100,
		metadata: map[string]string{
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockGetJobNodes: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.Nil(t, to.nurseryReplicate(rng, 1, node))
	require.Equal(t, int64(2), calls)
}

func TestNurseryReplicateWithFailure(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	used := make(map[string]bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[9:12]
		if drive == "sdb" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		used[drive] = true
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:       http.DefaultClient,
		dataShards:   3,
		parityShards: 2,
		chunkSize:    100,
		metadata: map[string]string{
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockGetJobNodes: []*ring.Device{
			{Id: 1, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.Nil(t, to.nurseryReplicate(rng, 1, node))
	require.False(t, used["sdb"])
	require.True(t, used["sdc"])
	require.True(t, used["sdd"])
}

type CustomFakeRing struct {
	test.FakeRing
}

func (r *CustomFakeRing) GetNodes(partition uint64) (response []*ring.Device) {
	return r.MockDevices
}

func TestStabilize(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var mutex sync.Mutex
	methods := make(map[string]string)
	lengths := make(map[string]int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[10:13]
		mutex.Lock()
		methods[drive] = r.Method
		lengths[drive] = r.ContentLength
		mutex.Unlock()
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:       http.DefaultClient,
		dataShards:   3,
		parityShards: 2,
		chunkSize:    100,
		metadata: map[string]string{
			"name":           "/a/c/o",
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	rng := &CustomFakeRing{
		FakeRing: test.FakeRing{
			MockDevices: []*ring.Device{
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sda"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sde"},
			},
		},
	}
	require.Nil(t, to.Stabilize(rng, nil, 0))
	require.Equal(t, "PUT", methods["sda"])
	// 7 bytes / 3 shards rounds up to 3
	require.Equal(t, int64(3), lengths["sda"])
	require.Equal(t, "PUT", methods["sdb"])
	require.Equal(t, int64(3), lengths["sdb"])
	require.Equal(t, "PUT", methods["sdc"])
	require.Equal(t, int64(3), lengths["sdc"])
	require.Equal(t, "PUT", methods["sdd"])
	require.Equal(t, int64(3), lengths["sdd"])
	require.Equal(t, "PUT", methods["sde"])
	require.Equal(t, int64(3), lengths["sde"])
}

func TestStabilizeDelete(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	var mutex sync.Mutex
	methods := make(map[string]string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[10:13]
		mutex.Lock()
		methods[drive] = r.Method
		mutex.Unlock()
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: true,
			Path:     fp.Name(),
		},
		client:       http.DefaultClient,
		dataShards:   3,
		parityShards: 2,
		chunkSize:    100,
		metadata: map[string]string{
			"name":           "/a/c/o",
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	rng := &CustomFakeRing{
		FakeRing: test.FakeRing{
			MockDevices: []*ring.Device{
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sda"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sde"},
			},
		},
	}
	require.Nil(t, to.Stabilize(rng, nil, 0))
	require.Equal(t, "DELETE", methods["sda"])
	require.Equal(t, "DELETE", methods["sdb"])
	require.Equal(t, "DELETE", methods["sdc"])
	require.Equal(t, "DELETE", methods["sdd"])
	require.Equal(t, "DELETE", methods["sde"])
}

func TestDontStabilizeWithFailure(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[10:13]
		if drive == "sdb" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:       http.DefaultClient,
		dataShards:   2,
		parityShards: 1,
		chunkSize:    100,
		metadata: map[string]string{
			"name":           "/a/c/o",
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}

	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 4, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
		},
	}
	err = to.Stabilize(rng, node, 0)
	require.NotNil(t, err)
	require.Equal(t, "Failed to stabilize object", err.Error())
}

func TestParseECScheme(t *testing.T) {
	algo, dataShards, parityShards, chunkSize, err := parseECScheme("reedsolomon/1/2/16")
	require.Nil(t, err)
	require.Equal(t, "reedsolomon", algo)
	require.Equal(t, 1, dataShards)
	require.Equal(t, 2, parityShards)
	require.Equal(t, 16, chunkSize)

	algo, dataShards, parityShards, chunkSize, err = parseECScheme("1/2/16")
	require.NotNil(t, err)

	algo, dataShards, parityShards, chunkSize, err = parseECScheme("reedsolomon/1/2/X")
	require.NotNil(t, err)
}

func TestRangeBytesWriter(t *testing.T) {
	for i := 1; i < 20; i++ {
		b := &bytes.Buffer{}
		s := bytes.NewBuffer([]byte("THIS IS A TEST"))
		w := &rangeBytesWriter{startOffset: 2, length: 12, writer: b}
		io.CopyBuffer(w, s, make([]byte, i))
		require.Equal(t, "IS IS A TEST", b.String())

		b = &bytes.Buffer{}
		s = bytes.NewBuffer([]byte("THIS IS A TEST"))
		w = &rangeBytesWriter{startOffset: 1, length: 12, writer: b}
		io.CopyBuffer(w, s, make([]byte, i))
		require.Equal(t, "HIS IS A TES", b.String())

		b = &bytes.Buffer{}
		s = bytes.NewBuffer([]byte("THIS IS A TEST"))
		w = &rangeBytesWriter{startOffset: 0, length: 12, writer: b}
		io.CopyBuffer(w, s, make([]byte, i))
		require.Equal(t, "THIS IS A TE", b.String())

		b = &bytes.Buffer{}
		s = bytes.NewBuffer([]byte("THIS IS A TEST"))
		w = &rangeBytesWriter{startOffset: 0, length: 14, writer: b}
		io.CopyBuffer(w, s, make([]byte, i))
		require.Equal(t, "THIS IS A TEST", b.String())
	}
}

func TestRangeChunkAlign(t *testing.T) {
	testCases := []struct {
		start         int64
		end           int64
		chunkSize     int64
		dataShards    int
		expectedStart int64
		expectedEnd   int64
	}{
		{60, 81, 10, 2, 30, 50},
		{61, 80, 10, 2, 30, 40},
		{60, 81, 10, 3, 20, 30},
		{0, 81, 10, 3, 0, 30},
	}
	for _, tc := range testCases {
		shardStart, shardEnd := rangeChunkAlign(tc.start, tc.end, tc.chunkSize, tc.dataShards)
		require.Equal(t, tc.expectedStart, shardStart)
		require.Equal(t, tc.expectedEnd, shardEnd)
	}
}
