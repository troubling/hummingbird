//  Copyright (c) 2018 Rackspace
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

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

// ServerTracer is a Middleware  that tries to join with an
// OpenTracing trace found in the HTTP request headers and starts a new Span.
// If no trace could be found in the HTTP request headers, the Span will be a trace root.
func ServerTracer(tracer opentracing.Tracer,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if tracer != nil {
				// Try to join to a trace propagated in `req`.
				wireContext, err := tracer.Extract(
					opentracing.TextMap,
					opentracing.HTTPHeadersCarrier(req.Header),
				)
				if err != nil && err != opentracing.ErrSpanContextNotFound {
					srv.GetLogger(req).Error("error encountered while trying to extract span", zap.Any("err", err))
				}

				// create span
				span := tracer.StartSpan(fmt.Sprintf("HTTP %s", req.Method), ext.RPCServerOption(wireContext))
				defer span.Finish()
				ext.HTTPMethod.Set(span, req.Method)
				ext.HTTPUrl.Set(span, req.URL.String())
				// store span in context
				ctx := opentracing.ContextWithSpan(req.Context(), span)

				// update request context to include our new span
				req = req.WithContext(ctx)
			}
			// next middleware or actual request handler
			next.ServeHTTP(w, req)
		})
	}
}
