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
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/troubling/nectar/nectarutil"
)

type expectReader struct {
	io.ReadCloser
	index  int
	cancel chan struct{}
	ready  chan int
}

// Read() proxies to the underlying ReadCloser, but notifies the ready channel on the first read.
// Also checks with the cancel channel for each read.
func (p *expectReader) Read(b []byte) (int, error) {
	if p.ready != nil { // notify "ready" channel on first read
		select {
		case <-p.cancel:
			return 0, errors.New("Request was cancelled")
		case p.ready <- p.index:
			p.ready = nil
		}
	}
	select { // check for cancellation, otherwise just proxy to underlying ReadCloser.
	case <-p.cancel:
		return 0, errors.New("Request was cancelled")
	default:
		return p.ReadCloser.Read(b)
	}
}

type expectResponder struct {
	index int
	resp  *http.Response
}

// Expector helps organize a group of "Expect: 100-continue" requests.  It attempts to get all requests
// to a state where they've either given a non-100 response or they are ready to receive upload bodies.
type Expector struct {
	client        HTTPClient
	ready         chan int
	responded     chan expectResponder
	cancel        chan struct{}
	readyRequests []bool
	responses     []*http.Response
	closed        bool
}

// NewExpector returns an Expector object that uses the given http client.
func NewExpector(c HTTPClient) *Expector {
	e := &Expector{
		client:    c,
		ready:     make(chan int),
		responded: make(chan expectResponder),
		cancel:    make(chan struct{}),
	}
	return e
}

// AddRequest begins an http.Request and adds it to the set of requests being tracked by the Expector.
func (e *Expector) AddRequest(req *http.Request) {
	req.Header.Set("Expect", "100-Continue")
	e.readyRequests = append(e.readyRequests, false)
	e.responses = append(e.responses, nil)
	go func(i int, req *http.Request) {
		// replace the request's Body with one that we can notifies us on its first Read()
		req.Body = &expectReader{index: i, cancel: e.cancel, ReadCloser: req.Body, ready: e.ready}
		resp, err := e.client.Do(req)
		if err != nil {
			resp = nectarutil.ResponseStub(http.StatusServiceUnavailable, err.Error())
		}
		select {
		case <-e.cancel:
			resp.Body.Close()
		case e.responded <- expectResponder{i, resp}:
		}
	}(len(e.responses)-1, req)
}

// Wait waits up to timeout time for all of the Expector's requests to either be ready or have a response.
// It returns slices of responses (with nil responses for any requests that haven't received a response) and boolean "ready" states
// indicating if they are waiting to read from their Body.
func (e *Expector) Wait(timeout time.Duration) ([]*http.Response, []bool) {
	timer := time.After(timeout)
	for {
		// count requests that haven't reported ready or returned a response
		waitCount := 0
		for i := 0; i < len(e.responses); i++ {
			if e.readyRequests[i] == false && e.responses[i] == nil {
				waitCount++
			}
		}
		if waitCount <= 0 {
			break
		}
		// wait for any updates
		select {
		case index := <-e.ready:
			e.readyRequests[index] = true
		case resp := <-e.responded:
			e.responses[resp.index] = resp.resp
		case <-timer:
			return e.responses, e.readyRequests
		}
	}
	return e.responses, e.readyRequests
}

// Successes waits up to timeout time for all of the Expector's requests to return a response,
// then returns the number of responses with status code matching okStatuses
// okStatuses of {2,404} means (r.StatusCode/100 == 2 || r.StatusCode == 404)
func (e *Expector) Successes(timeout time.Duration, okStatuses ...int) (count int) {
	timer := time.After(timeout)
	for {
		// count number of requests from which we're still waiting for responses
		waitCount := 0
		for i := 0; i < len(e.responses); i++ {
			if e.responses[i] == nil {
				waitCount++
			}
		}
		if waitCount <= 0 {
			break
		}
		// wait for any updates
		select {
		case index := <-e.ready:
			e.readyRequests[index] = true
		case resp := <-e.responded:
			e.responses[resp.index] = resp.resp
		case <-timer:
			break
		}
	}
	// count the successes
	for _, r := range e.responses {
		if r != nil {
			ok := false
			for _, s := range okStatuses {
				if s < 100 {
					if r.StatusCode/100 == s {
						ok = true
					}
				} else if r.StatusCode == s {
					ok = true
				}
			}
			if ok {
				count++
			}
		}
	}
	return count
}

// Closes responses bodies and attempts to cancel any outstanding requests by returning an error on read from their Body.
func (e *Expector) Close() {
	if !e.closed {
		for _, resp := range e.responses {
			if resp != nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}
		}
		close(e.cancel)
		e.closed = true
	}
}
