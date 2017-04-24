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
	"net/http"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
)

func NewRequestLogger(config conf.Section) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				start := time.Now()
				next.ServeHTTP(writer, request)
				ctx := GetProxyContext(request)
				_, status := ctx.Response()
				ctx.Logger.LogInfo("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
					request.RemoteAddr,
					time.Now().Format("02/Jan/2006:15:04:05 -0700"),
					request.Method,
					common.Urlencode(request.URL.Path),
					status,
					common.GetDefault(writer.Header(), "Content-Length", "-"),
					common.GetDefault(request.Header, "Referer", "-"),
					common.GetDefault(request.Header, "X-Trans-Id", "-"),
					common.GetDefault(request.Header, "User-Agent", "-"),
					time.Since(start).Seconds(),
					"-") // TODO: "additional info"?
			},
		)
	}, nil
}
