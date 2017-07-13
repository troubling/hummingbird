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

	"github.com/troubling/hummingbird/common/conf"
	"github.com/uber-go/tally"
)

func NewHealthcheck(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/healthcheck" && request.Method == "GET" {
					writer.Header().Set("Content-Length", "2")
					writer.WriteHeader(http.StatusOK)
					writer.Write([]byte("OK"))
					return
				}
				next.ServeHTTP(writer, request)
			},
		)
	}, nil
}
