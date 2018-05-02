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
	"net/http"
	"strconv"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"

	"github.com/uber-go/tally"
)

func accountQuota(metric tally.Counter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if !(request.Method == "PUT" || request.Method == "POST") {
				next.ServeHTTP(writer, request)
				return
			}

			ctx := GetProxyContext(request)
			_, account, container, obj := getPathParts(request)

			if container == "" {
				var quotaSet bool
				if _, ok := request.Header["X-Account-Meta-Quota-Bytes"]; ok {
					quotaSet = true
				}
				removeQuota := request.Header.Get("X-Remove-Account-Meta-Quota-Bytes")
				if quotaSet || removeQuota != "" {
					if ctx.Authorize != nil {
						if ok, st := ctx.Authorize(request); !ok {
							srv.StandardResponse(writer, st)
							return
						}
					}
					if !ctx.ResellerRequest {
						srv.StandardResponse(writer, http.StatusForbidden)
						return
					}
				}
				quota := request.Header.Get("X-Account-Meta-Quota-Bytes")
				if quota != "" {
					if _, err := strconv.ParseInt(quota, 10, 64); err != nil {
						srv.SimpleErrorResponse(writer, http.StatusBadRequest, "Invalid bytes quota.")
						return
					}
				}
				next.ServeHTTP(writer, request)
				return
			}

			if request.Method == "POST" || obj == "" {
				next.ServeHTTP(writer, request)
				return
			}

			ai, err := ctx.GetAccountInfo(request.Context(), account)
			if err != nil {
				next.ServeHTTP(writer, request)
				return
			}

			qBytes := ai.Metadata["Quota-Bytes"]
			if qBytes != "" {
				if quota, err := strconv.ParseInt(qBytes, 10, 64); err == nil {
					newSize := ai.ObjectBytes + request.ContentLength
					if quota < newSize {
						srv.SimpleErrorResponse(writer, http.StatusRequestEntityTooLarge, "Upload exceeds quota.")
						return
					}
				}
			}
			next.ServeHTTP(writer, request)
		})
	}
}

func NewAccountQuota(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	RegisterInfo("account_quotas", map[string]interface{}{})
	return accountQuota(metricsScope.Counter("account_quotas")), nil
}
