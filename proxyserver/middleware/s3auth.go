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
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/uber-go/tally"
)

type S3AuthInfo struct {
	Key          string
	Signature    string
	StringToSign string
	Account      string
}

func (s *S3AuthInfo) validateSignature(secret []byte) bool {
	// S3 Auth signature V2 Validation
	mac := hmac.New(sha1.New, secret)
	mac.Write([]byte(s.StringToSign))
	sig1 := mac.Sum(nil)
	sig2, err := base64.StdEncoding.DecodeString(s.Signature)
	if err != nil {
		return false
	}
	// TODO: Add support for constat time compare
	return hmac.Equal(sig1, sig2)
}

type s3AuthHandler struct {
	next           http.Handler
	ctx            *ProxyContext
	requestsMetric tally.Counter
}

func (s *s3AuthHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	// Check if this is an S3 request
	authStr := request.Header.Get("Authorization")
	if authStr == "" {
		authStr = request.Form.Get("AWSAccessKeyId")
	}
	if authStr == "" || ctx.S3Auth != nil {
		// Not an S3 request or already processed
		s.next.ServeHTTP(writer, request)
		return
	}
	// TODO: Handle parameter style auth
	// TODO: Handle V2 signature validation
	// Setup the string to be signed
	var buf bytes.Buffer
	buf.WriteString(request.Method)
	buf.WriteString("\n")
	buf.WriteString(request.Header.Get("Content-MD5"))
	buf.WriteString("\n")
	buf.WriteString(request.Header.Get("Content-Type"))
	buf.WriteString("\n")
	if request.Header.Get("x-amz-date") != "" {
		buf.WriteString("\n")
	} else {
		buf.WriteString(request.Header.Get("Date"))
		buf.WriteString("\n")
	}
	akeys := make([]string, 0)
	for k := range request.Header {
		if strings.HasPrefix(k, "x-amz-") {
			akeys = append(akeys, k)
		}
	}
	// the headers need to be in sorted order before signing
	sort.Strings(akeys)
	for _, k := range akeys {
		for _, v := range request.Header[k] {
			buf.WriteString(fmt.Sprintf("%s:%s", strings.ToLower(k), v))
			buf.WriteString("\n")
		}
	}
	// NOTE: The following is for V2 Auth
	parts := strings.SplitN(strings.Split(authStr, " ")[1], ":", 2)
	key := parts[0]
	signature := parts[1]

	buf.WriteString(request.URL.RequestURI())
	ctx.S3Auth = &S3AuthInfo{
		StringToSign: buf.String(),
		Key:          key,
		Signature:    signature,
	}

	// TODO: Handle V4 signature validation

	s.next.ServeHTTP(writer, request)
}

func NewS3Auth(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	enabled, ok := config.Section["enabled"]
	if !ok || strings.Compare(strings.ToLower(enabled), "false") == 0 {
		// s3api is disabled, so pass the request on
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				next.ServeHTTP(writer, request)
			})
		}, nil
	}
	RegisterInfo("s3Auth", map[string]interface{}{})
	return s3Auth(metricsScope.Counter("s3Auth_requests")), nil
}

func s3Auth(requestsMetric tally.Counter) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			(&s3AuthHandler{next: next, requestsMetric: requestsMetric}).ServeHTTP(writer, request)
		})
	}
}
