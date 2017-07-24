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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
)

func TestUpdaterListAsyncs(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	for policy := 0; policy < 3; policy++ {
		require.Nil(t, os.MkdirAll(filepath.Join(dir, "sda", AsyncDir(policy), "abc"), 0755))
		require.Nil(t, os.MkdirAll(filepath.Join(dir, "sda", AsyncDir(policy), "def"), 0755))
		f, err := os.Create(filepath.Join(dir, "sda", AsyncDir(policy), "abc", "d41d8cd98f00b204e9800998ecf8427e-1222222222.12345"))
		require.Nil(t, err)
		f.Close()
		f, err = os.Create(filepath.Join(dir, "sda", AsyncDir(policy), "def", "49f68a5c8493ec2c0bf489821c21fc3b-1333333333.12345"))
		require.Nil(t, err)
		f.Close()
	}

	r := &Replicator{deviceRoot: dir}
	dev := &ring.Device{Device: "sda"}
	for policy := 0; policy < 3; policy++ {
		u := newUpdateDevice(dev, 0, r)
		c := make(chan string)
		cancel := make(chan struct{})
		defer close(cancel)
		go u.listAsyncs(c, cancel)
		files := make(map[string]bool)
		for file := range c {
			files[filepath.Base(file)] = true
		}
		require.Equal(t, 2, len(files))
		require.True(t, files["d41d8cd98f00b204e9800998ecf8427e-1222222222.12345"])
		require.True(t, files["49f68a5c8493ec2c0bf489821c21fc3b-1333333333.12345"])
	}
}

func TestUpdaterProcessAsync(t *testing.T) {
	// make an async pending file
	ap := asyncPending{Headers: map[string]string{"Content-Type": "text/plain"}, Object: "o", Account: "a", Container: "c", Method: "PUT"}
	f, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer os.RemoveAll(f.Name())
	f.Write(pickle.PickleDumps(&ap))
	f.Close()

	// make a container server
	requestedPaths := make(map[string]bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPaths[r.URL.Path] = true
		require.Equal(t, "text/plain", r.Header.Get("Content-Type"))
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	fakering := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Ip: u.Hostname(), Port: port, Device: "sda"},
			{Ip: u.Hostname(), Port: port, Device: "sdb"},
			{Ip: u.Hostname(), Port: port, Device: "sdc"},
		},
	}
	r := &Replicator{updateStat: make(chan statUpdate, 100), client: http.DefaultClient, containerRing: fakering}
	dev := &ring.Device{Device: "sda"}
	updater := newUpdateDevice(dev, 0, r)

	updater.processAsync(f.Name())
	require.True(t, requestedPaths["/sda/0/a/c/o"])
	require.True(t, requestedPaths["/sdb/0/a/c/o"])
	require.True(t, requestedPaths["/sdc/0/a/c/o"])
}
