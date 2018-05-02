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
	"strings"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

type corsMiddleware struct {
	next http.Handler
}

type cors struct {
	origin string
	ci     *client.ContainerInfo
}

func (c *cors) HandleCors(writer http.ResponseWriter, status int) int {
	if c.origin == "" || !common.IsOriginAllowed(c.ci.Metadata["Access-Control-Allow-Origin"], c.origin) {
		return status
	}
	if writer.Header().Get("Access-Control-Expose-Headers") == "" {
		corsExposeHeaders := []string{"Cache-Control", "Content-Language",
			"Content-Type", "Expires", "Last-Modified", "Pragma", "Etag",
			"X-Timestamp", "X-Trans-Id", "X-Openstack-Request-Id"}
		for k := range writer.Header() {
			if strings.HasPrefix(
				k, "X-Container-Meta") || strings.HasPrefix(
				k, "X-Object-Meta") {
				corsExposeHeaders = append(corsExposeHeaders, k)
			}
		}
		if c.ci.Metadata["Access-Control-Expose-Headers"] != "" {
			for _, h := range strings.Split(
				c.ci.Metadata["Access-Control-Expose-Headers"], " ") {
				corsExposeHeaders = append(corsExposeHeaders, h)
			}
		}
		writer.Header().Set(
			"Access-Control-Expose-Headers", strings.ToLower(strings.Join(corsExposeHeaders, ", ")))
	}
	if writer.Header().Get("Access-Control-Allow-Origin") == "" {
		if c.ci.Metadata["Access-Control-Allow-Origin"] == "*" {
			writer.Header().Set("Access-Control-Allow-Origin", "*")
		} else {
			writer.Header().Set("Access-Control-Allow-Origin", c.origin)
		}
	}
	return status
}

func (cm *corsMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	origin := request.Header.Get("Origin")
	ctx := GetProxyContext(request)
	if ctx == nil || origin == "" {
		cm.next.ServeHTTP(writer, request)
		return
	}
	pathParts, err := common.ParseProxyPath(request.URL.Path)
	if err != nil || pathParts["container"] == "" {
		cm.next.ServeHTTP(writer, request)
		return
	}
	if ci, err := ctx.C.GetContainerInfo(request.Context(), pathParts["account"], pathParts["container"]); err == nil {
		cHandler := &cors{origin: origin, ci: ci}
		w := srv.NewCustomWriter(writer, cHandler.HandleCors)
		cm.next.ServeHTTP(w, request)
		return
	}
	cm.next.ServeHTTP(writer, request)
}

func NewCors(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler {
		return &corsMiddleware{
			next: next,
		}
	}, nil
}
