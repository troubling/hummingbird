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

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

func NewRequestLogger(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	requestsMetric := metricsScope.Counter("requests")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			start := time.Now()
			newWriter := &srv.WebWriter{ResponseWriter: writer, Status: 500}
			newReader := &srv.CountingReadCloser{ReadCloser: request.Body}
			request.Body = newReader
			next.ServeHTTP(newWriter, request)
			ctx := GetProxyContext(request)
			srv.LogRequestLine(ctx.Logger, request, start, newWriter, newReader)
			if ctx.Source == "" {
				requestsMetric.Inc(1)
				metricsScope.Counter(request.Method + "_requests").Inc(1)
				metricsScope.Counter(fmt.Sprintf("%d_responses", newWriter.Status)).Inc(1)
			}
		})
	}, nil
}
