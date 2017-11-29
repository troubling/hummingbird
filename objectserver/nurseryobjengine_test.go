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

package objectserver

import (
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"
)

func TestGetStabilizer(t *testing.T) {
	testRing := &test.FakeRing{}
	dev := ring.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000}
	configString := "[app:object-server]\nmount_check=false\n"
	pol := conf.Policy{Index: 0, Type: "replication", Name: "gold",
		Aliases: []string{}, Default: true, Deprecated: false,
		Config: map[string]string{"policy_type": "replication",
			"default": "yes", "name": "gold"}}
	config, _ := conf.StringConfig(configString)
	neng, err := nurseryEngineConstructor(
		config, &pol, &flag.FlagSet{})
	require.Nil(t, err)
	_, err = newNurseryDevice(&dev, testRing, 0, nil, neng)
	require.Nil(t, err)

	seng, err := SwiftEngineConstructor(
		config, &pol, &flag.FlagSet{})
	_, err = newNurseryDevice(&dev, testRing, 0, nil, seng)
	require.NotNil(t, err)
}

func TestCanStabilize(t *testing.T) {
	testRing := &test.FakeRing{}
	dev := ring.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000}
	numCalls := 0
	called404 := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		numCalls++
		st := 200
		if strings.Index(r.URL.Path, "sdc") >= 0 {
			st = 404
			called404 = true
		}
		w.Header().Set("X-Backend-Data-Timestamp", "100")
		srv.SimpleErrorResponse(w, st, "")
	}))
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)

	testRing.MockDevices = []*ring.Device{
		{Ip: "127.0.0.1", Port: 5000, Device: "sda"},
		{Ip: host, Port: port, Device: "sdb"},
		{Ip: host, Port: port, Device: "sdc"}}
	obj := nurseryObject{metadata: map[string]string{"name": "/a/c/o", "X-Backend-Data-Timestamp": "100"}}
	assert.Nil(t, err)
	canStab, err := obj.canStabilize(testRing, &dev, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, numCalls)
	assert.True(t, canStab)
	assert.True(t, called404)
}

func TestCanNotStabilize(t *testing.T) {
	testRing := &test.FakeRing{}
	dev := ring.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000}
	numCalls := 0
	called404 := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		numCalls++
		st := 200
		if strings.Index(r.URL.Path, "sdc") >= 0 {
			st = 404
			called404 = true
		} else {
			w.Header().Set("X-Backend-Data-Timestamp", "100")
		}
		srv.SimpleErrorResponse(w, st, "")
	}))
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)

	testRing.MockDevices = []*ring.Device{
		{Ip: "127.0.0.1", Port: 5000, Device: "sda"},
		{Ip: host, Port: port, Device: "sdb"},
		{Ip: host, Port: port, Device: "sdc"}}
	obj := nurseryObject{metadata: map[string]string{"name": "/a/c/o", "X-Backend-Data-Timestamp": "100"}}
	assert.Nil(t, err)
	canStab, err := obj.canStabilize(testRing, &dev, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, numCalls)
	assert.False(t, canStab)
	assert.True(t, called404)
}

func TestNotifyPeers(t *testing.T) {
	testRing := &test.FakeRing{}
	dev := ring.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000}
	numCalls := 0
	called404 := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			numCalls++
		}
		st := 200
		if strings.Index(r.URL.Path, "sdc") >= 0 {
			st = 404
			called404 = true
		}
		srv.SimpleErrorResponse(w, st, "")
	}))
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)

	testRing.MockDevices = []*ring.Device{
		{Ip: "127.0.0.1", Port: 5000, Device: "sda"},
		{Ip: host, Port: port, ReplicationPort: port, Device: "sdb"},
		{Ip: host, Port: port, ReplicationPort: port, Device: "sdc"}}
	obj := nurseryObject{metadata: map[string]string{"name": "/a/c/o", "X-Backend-Data-Timestamp": "100"}}
	assert.Nil(t, err)
	err = obj.notifyPeers(testRing, &dev, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, numCalls)
	assert.True(t, called404)
}
