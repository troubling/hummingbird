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
	"io"
	"net/http"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type countingReadCloser struct {
	io.ReadCloser
	byteCount int
}

func (crc *countingReadCloser) Read(b []byte) (n int, err error) {
	n, err = crc.ReadCloser.Read(b)
	crc.byteCount += n
	return n, err
}

type countingResponseWriter struct {
	http.ResponseWriter
	byteCount int
}

func (crw *countingResponseWriter) Write(b []byte) (n int, err error) {
	n, err = crw.ResponseWriter.Write(b)
	crw.byteCount += n
	return n, err
}

func NewRequestLogger(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	requestsMetric := metricsScope.Counter("requests")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				start := time.Now()
				crw := &countingResponseWriter{ResponseWriter: writer}
				writer = crw
				crc := &countingReadCloser{ReadCloser: request.Body}
				request.Body = crc
				next.ServeHTTP(writer, request)
				ctx := GetProxyContext(request)
				_, status := ctx.Response()
				ctx.Logger.Info("Request log",
					zap.String("remoteAddr", request.RemoteAddr),
					zap.String("eventTime", time.Now().Format("02/Jan/2006:15:04:05 -0700")),
					zap.String("method", request.Method),
					zap.String("urlPath", common.Urlencode(request.URL.Path)),
					zap.Int("status", status),
					zap.Int("contentBytesIn", crc.byteCount),
					zap.Int("contentBytesOut", crw.byteCount),
					zap.String("contentLengthIn", common.GetDefault(request.Header, "Content-Length", "-")),
					zap.String("contentLengthOut", common.GetDefault(writer.Header(), "Content-Length", "-")),
					zap.String("referer", common.GetDefault(request.Header, "Referer", "-")),
					zap.String("userAgent", common.GetDefault(request.Header, "User-Agent", "-")),
					zap.Float64("requestTimeSeconds", time.Since(start).Seconds()),
					zap.Float64("requestTimeHeaderSeconds", ctx.responseSent.Sub(start).Seconds()),
				)
				if ctx.Source == "" {
					requestsMetric.Inc(1)
					metricsScope.Counter(request.Method + "_requests").Inc(1)
					metricsScope.Counter(fmt.Sprintf("%d_responses", status)).Inc(1)
				}
			},
		)
	}, nil
}
