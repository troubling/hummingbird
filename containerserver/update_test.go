//  Copyright (c) 2016 Rackspace
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

package containerserver

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccountUpdate(t *testing.T) {
	info := &ContainerInfo{
		PutTimestamp:       "puttimestamp",
		DeleteTimestamp:    "deletetimestamp",
		ObjectCount:        1,
		BytesUsed:          1024,
		StoragePolicyIndex: 2,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/adevice/3/a/c", r.URL.Path)
		require.Equal(t, "puttimestamp", r.Header.Get("X-Put-Timestamp"))
		require.Equal(t, "deletetimestamp", r.Header.Get("X-Delete-Timestamp"))
		require.Equal(t, "1", r.Header.Get("X-Object-Count"))
		require.Equal(t, "1024", r.Header.Get("X-Bytes-Used"))
		require.Equal(t, "atxid", r.Header.Get("X-Trans-Id"))
		require.Equal(t, "2", r.Header.Get("X-Backend-Storage-Policy-Index"))
	}))
	defer ts.Close()
	url, err := url.Parse(ts.URL)
	require.Nil(t, err)
	server := &ContainerServer{updateClient: http.DefaultClient}
	request, err := http.NewRequest("PUT", "http://127.0.0.1/device/partition/account/container", nil)
	require.Nil(t, err)
	request.Header.Set("X-Account-Partition", "3")
	request.Header.Set("X-Account-Host", url.Host)
	request.Header.Set("X-Account-Device", "adevice")
	request.Header.Set("X-Trans-Id", "atxid")
	request.Header.Set("X-Account-Override-Deleted", "no")
	vars := map[string]string{"account": "a", "container": "c"}
	server.accountUpdate(request, vars, info, fakeLogger{})
}

func TestAccountUpdateBadHeaders(t *testing.T) {
	info := &ContainerInfo{
		PutTimestamp:       "puttimestamp",
		DeleteTimestamp:    "deletetimestamp",
		ObjectCount:        1,
		BytesUsed:          1024,
		StoragePolicyIndex: 2,
	}
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()
	url, err := url.Parse(ts.URL)
	require.Nil(t, err)
	server := &ContainerServer{updateClient: http.DefaultClient}
	request, err := http.NewRequest("PUT", "http://127.0.0.1/device/partition/account/container", nil)
	require.Nil(t, err)
	request.Header.Set("X-Account-Partition", "3")
	request.Header.Set("X-Account-Host", url.Host)
	request.Header.Set("X-Account-Device", "adevice,anotherdevice")
	request.Header.Set("X-Trans-Id", "atxid")
	vars := map[string]string{"account": "a", "container": "c"}
	server.accountUpdate(request, vars, info, fakeLogger{})
	require.False(t, called)
}

func TestAccountUpdateTimeout(t *testing.T) {
	info := &ContainerInfo{
		PutTimestamp:       "puttimestamp",
		DeleteTimestamp:    "deletetimestamp",
		ObjectCount:        1,
		BytesUsed:          1024,
		StoragePolicyIndex: 2,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second)
	}))
	defer ts.Close()
	url, err := url.Parse(ts.URL)
	require.Nil(t, err)
	server := &ContainerServer{updateClient: http.DefaultClient}
	request, err := http.NewRequest("PUT", "http://127.0.0.1/device/partition/account/container", nil)
	require.Nil(t, err)
	request.Header.Set("X-Account-Partition", "3")
	request.Header.Set("X-Account-Host", url.Host)
	request.Header.Set("X-Account-Device", "adevice")
	request.Header.Set("X-Trans-Id", "atxid")
	vars := map[string]string{"account": "a", "container": "c"}
	defer func(old time.Duration) {
		waitForAccountUpdate = old
	}(waitForAccountUpdate)
	waitForAccountUpdate = time.Millisecond
	start := time.Now()
	server.accountUpdate(request, vars, info, fakeLogger{})
	require.True(t, time.Since(start) < time.Second)
}
