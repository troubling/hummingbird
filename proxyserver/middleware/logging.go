//  Copyright (c) 2015-2017 Rackspace
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
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func NewRequestLogger(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	requestsMetric := metricsScope.Counter("requests")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				start := time.Now()
				next.ServeHTTP(writer, request)
				ctx := GetProxyContext(request)
				_, status := ctx.Response()
				ctx.Logger.Info("Request log",
					zap.String("remoteAddr", request.RemoteAddr),
					zap.String("eventTime", time.Now().Format("02/Jan/2006:15:04:05 -0700")),
					zap.String("method", request.Method),
					zap.String("urlPath", common.Urlencode(request.URL.Path)),
					zap.Int("status", status),
					zap.String("contentLength", common.GetDefault(writer.Header(), "Content-Length", "-")),
					zap.String("referer", common.GetDefault(request.Header, "Referer", "-")),
					zap.String("userAgent", common.GetDefault(request.Header, "User-Agent", "-")),
					zap.Float64("requestTimeSeconds", time.Since(start).Seconds()))
				if ctx.Source == "" {
					requestsMetric.Inc(1)
					metricsScope.Counter(request.Method + "_requests").Inc(1)
					metricsScope.Counter(fmt.Sprintf("%d_responses", status)).Inc(1)
				}
			},
		)
	}, nil
}
