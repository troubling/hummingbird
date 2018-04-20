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
	"strconv"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func containerQuota(metric tally.Counter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			ctx := GetProxyContext(request)
			_, account, container, obj := getPathParts(request)
			if container == "" {
				next.ServeHTTP(writer, request)
				return
			}

			if obj == "" && (request.Method == "PUT" || request.Method == "POST") {
				qBytes := request.Header.Get("X-Container-Meta-Quota-Bytes")
				if qBytes != "" {
					if _, err := strconv.ParseInt(qBytes, 10, 64); err != nil {
						srv.SimpleErrorResponse(writer, http.StatusBadRequest, "Invalid bytes quota.")
						return
					}
				}
				qCount := request.Header.Get("X-Container-Meta-Quota-Count")
				if qCount != "" {
					if _, err := strconv.ParseInt(qCount, 10, 64); err != nil {
						srv.SimpleErrorResponse(writer, http.StatusBadRequest, "Invalid count quota.")
						return
					}
				}
			} else if obj != "" && request.Method == "PUT" {
				ci, err := ctx.C.GetContainerInfo(account, container)
				if err != nil {
					next.ServeHTTP(writer, request)
					return
				}
				qBytes := ci.Metadata["Quota-Bytes"]
				if qBytes != "" {
					if quota, err := strconv.ParseInt(qBytes, 10, 64); err == nil {
						newSize := ci.ObjectBytes + request.ContentLength
						if quota < newSize {
							srv.SimpleErrorResponse(writer, http.StatusRequestEntityTooLarge, "Upload exceeds quota.")
							return
						}
					}
				}
				qCount := ci.Metadata["Quota-Count"]
				if qCount != "" {
					if quota, err := strconv.ParseInt(qCount, 10, 64); err == nil {
						newCount := ci.ObjectCount + 1
						if quota < newCount {
							srv.SimpleErrorResponse(writer, http.StatusRequestEntityTooLarge, "Upload exceeds quota.")
							return
						}
					}
				}
			}
			next.ServeHTTP(writer, request)
		})
	}
}

func NewContainerQuota(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	RegisterInfo("container_quotas", map[string]interface{}{})
	return containerQuota(metricsScope.Counter("container_quotas")), nil
}
