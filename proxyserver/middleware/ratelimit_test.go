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

package middleware

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/troubling/hummingbird/common/test"
)

var now = 10 * nsPerSecond

type sleeper struct {
	SleepVals []time.Duration
}

func (s *sleeper) fakeSleep(t time.Duration) {
	s.SleepVals = append(s.SleepVals, t)
}

func fakeNowNano() int64 {
	return now
}

type FakeHandler struct {
}

func (h FakeHandler) ServeHTTP(http.ResponseWriter, *http.Request) {
}

func TestGetSleepTime(t *testing.T) {
	fakeMr := &test.FakeMemcacheRing{MockIncrResults: []int64{now}}

	rt := ratelimiter{accountLimit: 0, containerLimit: 0}

	oldSleep := sleep
	oldNowNano := nowNano
	defer func() {
		sleep = oldSleep
		nowNano = oldNowNano
	}()
	s := sleeper{}
	sleep = s.fakeSleep
	nowNano = fakeNowNano

	sleepTime, err := rt.getSleepTime(context.Background(), fakeMr, "hey", int64(1000))
	assert.Equal(t, sleepTime, int64(0))
	assert.Equal(t, err, nil)

	fakeMr = &test.FakeMemcacheRing{MockIncrResults: []int64{now + 2000}}
	sleepTime, err = rt.getSleepTime(context.Background(), fakeMr, "hey", int64(1000))
	assert.Equal(t, sleepTime, int64(2000))
	assert.Equal(t, err, nil)

	fakeMr = &test.FakeMemcacheRing{MockIncrResults: []int64{nsPerSecond}}
	sleepTime, err = rt.getSleepTime(context.Background(), fakeMr, "hey", int64(1000))
	assert.Equal(t, sleepTime, int64(0))
	assert.Equal(t, err, nil)
	assert.Equal(t, fakeMr.MockSetValues[0], now+nsPerSecond/1000)
}

/*
TODO:
need to figure out a decent way to put the FakeMemcacheRing
into the request's context

func TestServeHTTP(t *testing.T) {
	fakeMr := test.FakeMemcacheRing{MockIncrResults: []int64{now + 2000}}

	rt := ratelimiter{accountLimit: 10, containerLimit: 100, mc: &fakeMr}
	oldSleep := sleep
	oldNowNano := nowNano
	defer func() {
		sleep = oldSleep
		nowNano = oldNowNano
	}()
	s := sleeper{}
	sleep = s.fakeSleep
	nowNano = fakeNowNano
	fakeWriter := test.MockResponseWriter{}
	rt.next = FakeHandler{}

	req, _ := http.NewRequest("GET", "/v/a/co", nil)
	rt.ServeHTTP(fakeWriter, req)
	assert.Equal(t, len(s.SleepVals), 0)
	assert.Equal(t, len(fakeMr.MockIncrKeys), 0)

	req, _ = http.NewRequest("PUT", "/v/a/c", nil)
	rt.ServeHTTP(fakeWriter, req)
	assert.Equal(t, len(s.SleepVals), 1)

	rt.accountLimit = 0
	rt.ServeHTTP(fakeWriter, req)
	assert.Equal(t, len(s.SleepVals), 1)

	req, _ = http.NewRequest("PUT", "/v/a/c/o", nil)
	rt.ServeHTTP(fakeWriter, req)
	assert.Equal(t, len(s.SleepVals), 2)

	assert.Equal(t, fakeMr.MockIncrKeys[0], "ratelimit/a")
	assert.Equal(t, fakeMr.MockIncrKeys[1], "ratelimit/a/c")

	fakeMr = test.FakeMemcacheRing{MockIncrResults: []int64{now + 2000 + MaxSleep}}
	rt.mc = &fakeMr
	req, _ = http.NewRequest("PUT", "/v/a/c/o", nil)
	rt.ServeHTTP(fakeWriter, req)
	assert.Equal(t, len(s.SleepVals), 3)
	assert.Equal(t, s.SleepVals[len(s.SleepVals)-1], time.Second)
}
*/
