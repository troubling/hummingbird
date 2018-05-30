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

package common

import (
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var xpectClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
		IdleConnTimeout:     5 * time.Second,
		DisableCompression:  true,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
		ExpectContinueTimeout: 10 * time.Minute, // TODO: this should probably be like infinity.
	},
	Timeout: 120 * time.Minute,
}

func TestExpectorSuccesses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer srv.Close()
	e := NewExpector(xpectClient)
	defer e.Close()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("PUT", srv.URL+"/"+strconv.Itoa(i), strings.NewReader("STUFF"))
		require.Nil(t, err)
		e.AddRequest(req)
	}
	require.Equal(t, 3, e.Successes(time.Second, []int{2}...))
}

func TestExpectorSuccessesWith404Fail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/1" {
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	e := NewExpector(xpectClient)
	defer e.Close()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("PUT", srv.URL+"/"+strconv.Itoa(i), strings.NewReader("STUFF"))
		require.Nil(t, err)
		e.AddRequest(req)
	}
	require.Equal(t, 2, e.Successes(time.Second, []int{2}...))
}

func TestExpectorSuccessesWith404Pass(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/1" {
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	e := NewExpector(xpectClient)
	defer e.Close()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("PUT", srv.URL+"/"+strconv.Itoa(i), strings.NewReader("STUFF"))
		require.Nil(t, err)
		e.AddRequest(req)
	}
	require.Equal(t, 3, e.Successes(time.Second, []int{2, 404}...))
}

func TestExpectorReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	e := NewExpector(xpectClient)
	defer e.Close()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("PUT", srv.URL+"/"+strconv.Itoa(i), strings.NewReader("STUFF"))
		require.Nil(t, err)
		e.AddRequest(req)
	}
	_, ready := e.Wait(time.Second)
	require.Equal(t, 3, len(ready))
	for _, r := range ready {
		require.Equal(t, true, r)
	}
}

func TestExpectorErrorRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/0" {
			w.WriteHeader(500)
			return
		}
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	e := NewExpector(xpectClient)
	defer e.Close()
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("PUT", srv.URL+"/"+strconv.Itoa(i), strings.NewReader("STUFF"))
		req.Header.Set("Content-Length", "5")
		req.Header.Set("Expect", "100-continue")
		require.Nil(t, err)
		e.AddRequest(req)
	}
	e.Wait(time.Second)

	req, err := http.NewRequest("PUT", srv.URL+"/NEXT", strings.NewReader("STUFF"))
	req.Header.Set("Content-Length", "5")
	req.Header.Set("Expect", "100-continue")
	require.Nil(t, err)
	e.AddRequest(req)

	require.Equal(t, 3, e.Successes(time.Second, []int{2}...))
	_, ready := e.Wait(time.Second)
	require.Equal(t, 4, len(ready))
	require.Equal(t, false, ready[0])
	require.Equal(t, true, ready[1])
	require.Equal(t, true, ready[2])
	require.Equal(t, true, ready[3])
}
