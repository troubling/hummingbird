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
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
)

func TestEcEngineConstructor(t *testing.T) {
	configString := "[app:object-server]\nmount_check=false\n"
	pol := conf.Policy{Index: 0, Type: "hec", Name: "gold",
		Aliases: []string{}, Default: true, Deprecated: false,
		Config: map[string]string{"policy_type": "hec",
			"default": "yes", "name": "gold", "data_frags": "2",
			"parity_frags": "1"}}
	config, _ := conf.StringConfig(configString)
	_, err := ecEngineConstructor(
		config, &pol, &flag.FlagSet{})
	require.Nil(t, err)
}

func getTestEce() (*ecEngine, error) {
	driveRoot, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	policy := conf.Policy{Index: 0, Type: "hec", Name: "gold",
		Aliases: []string{}, Default: true, Deprecated: false,
		Config: map[string]string{"policy_type": "hec",
			"default": "yes", "name": "gold", "data_frags": "2",
			"parity_frags": "1"}}
	testRing := &test.FakeRing{}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 256,
		MaxIdleConns:        0,
		IdleConnTimeout:     5 * time.Second,
		DisableCompression:  true,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
		ExpectContinueTimeout: 10 * time.Minute,
	}
	logger, _ := zap.NewProduction()
	engine := &ecEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: "a",
		hashPathSuffix: "b",
		reserve:        0,
		policy:         0,
		logger:         logger,
		ring:           testRing,
		idbs:           map[string]*IndexDB{},
		dbPartPower:    1,
		numSubDirs:     32,
		client: &http.Client{
			Timeout:   time.Second,
			Transport: transport,
		},
	}
	if engine.dataFrags, err = strconv.Atoi(policy.Config["data_frags"]); err != nil {
		return nil, err
	}
	if engine.parityFrags, err = strconv.Atoi(policy.Config["parity_frags"]); err != nil {
		return nil, err
	}
	if engine.chunkSize, err = strconv.Atoi(policy.Config["chunk_size"]); err != nil {
		engine.chunkSize = 1 << 20
	}
	return engine, nil
}

func TestGetObjectsToReplicate(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteItems := []*IndexDBItem{}
		d, err := json.Marshal(remoteItems)
		require.Nil(t, err)
		w.WriteHeader(200)
		w.Write(d)
	}))
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)

	ece, err := getTestEce()
	require.Nil(t, err)
	idb, err := ece.getDB("sdb1")
	require.Nil(t, err)

	timestamp := time.Now().UnixNano()
	body := "just testing"
	hsh0 := md5hash("object0")
	f, err := idb.TempFile(hsh0, 0, timestamp, int64(len(body)), true)
	require.Nil(t, err)
	f.Write([]byte(body))
	metadata, err := json.Marshal(map[string]string{"moo": "cow"})
	require.Nil(t, err)
	require.Nil(t, idb.Commit(f, hsh0, 0, timestamp, false, "", metadata, false, ""))
	osc := make(chan ObjectStabilizer)
	cancel := make(chan struct{})
	defer close(cancel)
	go ece.GetObjectsToReplicate(
		PriorityRepJob{FromDevice: &ring.Device{Device: "sdb1"},
			ToDevice: &ring.Device{Device: "sdb2", Scheme: "http", Port: port, Ip: host}}, osc, cancel)
	os := <-osc
	require.Equal(t, "cow", os.Metadata()["moo"])
	os = <-osc
	require.Nil(t, os)
}

func TestGetObjectsToReplicateRemoteHasAll(t *testing.T) {
	ece, err := getTestEce()
	require.Nil(t, err)
	idb, err := ece.getDB("sdb1")
	require.Nil(t, err)

	timestamp := time.Now().UnixNano()
	body := "just testing"
	hsh0 := "00000000000000000000000000000001"
	f, err := idb.TempFile(hsh0, 0, timestamp, int64(len(body)), true)
	require.Nil(t, err)
	f.Write([]byte(body))
	metadata, err := json.Marshal(map[string]string{"name": "o1"})
	require.Nil(t, err)
	require.Nil(t, idb.Commit(f, hsh0, 0, timestamp, false, "", metadata, false, ""))
	hsh1 := "00000000000000000000000000000002"
	metadata, err = json.Marshal(map[string]string{"name": "o2"})
	require.Nil(t, err)
	f, err = idb.TempFile(hsh1, 0, timestamp, int64(len(body)), true)
	require.Nil(t, err)
	f.Write([]byte(body))
	require.Nil(t, idb.Commit(f, hsh1, 0, timestamp, false, "", metadata, false, ""))
	remoteItems, err := idb.List("", "", "", 0)
	require.Nil(t, err)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d, err := json.Marshal(remoteItems)
		require.Nil(t, err)
		w.WriteHeader(200)
		w.Write(d)
	}))
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)

	osc := make(chan ObjectStabilizer)
	cancel := make(chan struct{})
	defer close(cancel)
	go ece.GetObjectsToReplicate(
		PriorityRepJob{FromDevice: &ring.Device{Device: "sdb1"},
			ToDevice: &ring.Device{Device: "sdb2", Scheme: "http", Port: port, Ip: host}}, osc, cancel)
	os := <-osc
	require.Nil(t, os)
}

func TestGetObjectsToReplicateRemoteHasSome(t *testing.T) {
	ece, err := getTestEce()
	require.Nil(t, err)
	idb, err := ece.getDB("sdb1")
	require.Nil(t, err)

	timestamp := time.Now().UnixNano()
	body := "just testing"
	hsh0 := "00000000000000000000000000000001"
	f, err := idb.TempFile(hsh0, 0, timestamp, int64(len(body)), true)
	require.Nil(t, err)
	f.Write([]byte(body))
	metadata, err := json.Marshal(map[string]string{"name": "o1"})
	require.Nil(t, err)
	require.Nil(t, idb.Commit(f, hsh0, 0, timestamp, false, "", metadata, false, ""))
	hsh1 := "00000000000000000000000000000002"
	metadata, err = json.Marshal(map[string]string{"name": "o2"})
	require.Nil(t, err)
	f, err = idb.TempFile(hsh1, 0, timestamp, int64(len(body)), true)
	require.Nil(t, err)
	f.Write([]byte(body))
	require.Nil(t, idb.Commit(f, hsh1, 0, timestamp, false, "", metadata, false, ""))
	remoteItems, err := idb.List("", "", "", 0)
	require.Nil(t, err)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newRemoteItems := []*IndexDBItem{remoteItems[0]}
		d, err := json.Marshal(newRemoteItems)
		require.Nil(t, err)
		w.WriteHeader(200)
		w.Write(d)
	}))
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)

	osc := make(chan ObjectStabilizer)
	cancel := make(chan struct{})
	defer close(cancel)
	go ece.GetObjectsToReplicate(
		PriorityRepJob{FromDevice: &ring.Device{Device: "sdb1"},
			ToDevice: &ring.Device{Device: "sdb2", Scheme: "http", Port: port, Ip: host}}, osc, cancel)
	os := <-osc
	require.Equal(t, "o2", os.Metadata()["name"])
	os = <-osc
	require.Nil(t, os)
}
