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
	"fmt"
	"net/http"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

const rateBuffer = int64(5 * time.Second)
const maxSleep = int64(60 * time.Second)
const nsPerSecond = int64(1000000000)

var writeMethods = map[string]bool{"PUT": true, "DELETE": true, "POST": true}

// will sleep on write requests if the client starts to exceed the
// specified rate. The maximum rate allowed per sec is only as
// accurate as the clocks in the proxy layer- meaning if your clocks
// are accurate to 1/100 of a second then the max reliable rate/sec
// you can set is 100/sec.

type ratelimiter struct {
	accountLimit   int64
	containerLimit int64
	next           http.Handler
}

var sleep = func(s time.Duration) {
	time.Sleep(s)
}

var nowNano = func() int64 {
	return time.Now().UnixNano()
}

// returns int64 of ns to sleep before serving request
func (r *ratelimiter) getSleepTime(mc ring.MemcacheRing, key string, ratePs int64) (int64, error) {
	nsPerRequest := nsPerSecond / ratePs
	runningTime, err := mc.Incr(key, nsPerRequest, 3600)
	if err != nil {
		return 0, err
	}
	sleepTime := int64(0)
	now := nowNano()
	if int64(now-runningTime) > rateBuffer {
		// nothing has happened in a while, set new clocktime
		mc.Set(key, now+nsPerRequest, 3600)
	} else {
		sleepTime = runningTime - now - nsPerRequest
		if sleepTime < 0 {
			sleepTime = 0
		}
	}
	return sleepTime, nil
}

func (r *ratelimiter) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	isWrite := writeMethods[request.Method]
	pathParts, err := common.ParseProxyPath(request.URL.Path)
	if !isWrite || err != nil || pathParts["container"] == "" {
		r.next.ServeHTTP(writer, request)
		return
	}
	ctx := GetProxyContext(request)
	if ctx == nil {
		ctx.Logger.Debug("Error ratelimiter getting ctx")
		return
	}
	limit := int64(0)
	var ratekey string
	if pathParts["object"] == "" {
		ratekey = fmt.Sprintf(
			"ratelimit/%s", pathParts["account"])
		limit = r.accountLimit
	} else {
		ratekey = fmt.Sprintf(
			"ratelimit/%s/%s", pathParts["account"], pathParts["container"])
		limit = r.containerLimit
	}
	if limit > 0 {
		sleepTime, err := r.getSleepTime(ctx.Cache, ratekey, limit)
		if err == nil {
			if sleepTime > maxSleep {
				sleep(time.Second)
				srv.StandardResponse(writer, 498)
				return
			}
			sleep(time.Duration(sleepTime))
		} else {
			if ctx := GetProxyContext(request); ctx != nil {
				ctx.Logger.Debug("Ratelimiter errored while getting sleep time", zap.Error(err))
			}
		}
	}
	r.next.ServeHTTP(writer, request)
}

func NewRatelimiter(config conf.Section) (func(http.Handler) http.Handler, error) {

	accLimit := int64(config.GetInt("account_db_max_writes_per_sec", 0))
	contLimit := int64(config.GetInt("container_db_max_writes_per_sec", 0))
	//TODO: add account metadata global-write-ratelimit ratelimiter
	RegisterInfo("ratelimit", map[string]interface{}{"account_ratelimit": accLimit, "container_ratelimits": [][]int64{[]int64{contLimit}}, "max_sleep_time_seconds": float64(60.0)})
	return func(next http.Handler) http.Handler {
		return &ratelimiter{
			accountLimit:   accLimit,
			containerLimit: contLimit,
			next:           next,
		}
	}, nil
}
