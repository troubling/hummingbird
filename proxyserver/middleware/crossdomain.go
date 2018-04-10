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

	"github.com/troubling/hummingbird/common/conf"
	"github.com/uber-go/tally"
)

func crossdomain(policy string, requestsMetric tally.Counter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/crossdomain.xml" && request.Method == "GET" {
				writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
				writer.WriteHeader(http.StatusOK)
				writer.Write([]byte(policy))
				return
			}
			next.ServeHTTP(writer, request)
		})
	}
}

func NewCrossDomain(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	RegisterInfo("crossdomain", map[string]interface{}{})
	defaultPolicy := `<allow-access-from domain="*" secure="false" />`
	crossDomainPolicy := config.GetDefault("cross_domain_policy", defaultPolicy)
	policy := fmt.Sprintf("<?xml version=\"1.0\"?>\n"+
		"<!DOCTYPE cross-domain-policy SYSTEM "+
		"\"http://www.adobe.com/xml/dtds/cross-domain-policy.dtd\" >\n"+
		"<cross-domain-policy>\n"+
		"%s\n"+
		"</cross-domain-policy>", crossDomainPolicy)

	return crossdomain(policy, metricsScope.Counter("crossdomain_requests")), nil
}
