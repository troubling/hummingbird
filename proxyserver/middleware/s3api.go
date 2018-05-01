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

// To enable, add the following to `/etc/hummingbird/proxy-server.conf`
//
//   [filter:s3api]
//   enabled = true
//
// Example using boto2 and haio:
//
//  from boto.s3.connection import S3Connection
//  connection = S3Connection(
//    aws_access_key_id='AUTH_test',
//    aws_secret_access_key='AUTH_070cb6ce-7bce-bedd-6cc6-1e3053a34a93',
//    port=8080,
//    host='127.0.0.1',
//    is_secure=False,
//    calling_format=boto.s3.connection.OrdinaryCallingFormat()
//  )
//  connection.get_all_buckets()

package middleware

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

const (
	s3Xmlns = "http://s3.amazonaws.com/doc/2006-03-01"
)

type s3Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type s3BucketInfo struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type s3BucketList struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Owner   s3Owner        `xml:"Owner"`
	Buckets []s3BucketInfo `xml:"Buckets>Bucket"`
}

func NewS3BucketList() *s3BucketList {
	return &s3BucketList{Xmlns: s3Xmlns}
}

type s3Handler struct {
	next           http.Handler
	ctx            *ProxyContext
	account        string
	container      string
	object         string
	path           string
	requestsMetric tally.Counter
}

func (s *s3Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	// Check if this is an S3 request
	authStr := request.Header.Get("Authorization")
	if authStr == "" {
		authStr = request.Form.Get("AWSAccessKeyId")
	}
	if authStr == "" {
		// Not an S3 request
		s.next.ServeHTTP(writer, request)
		return
	}
	// TODO: Handle V2 signature validation
	// TODO: Handle V4 signature validation
	// Skip auth for now for S3 req
	ctx.Authorize = func(r *http.Request) (bool, int) { return true, http.StatusOK }
	s.container, s.object, _, _ = getPathSegments(request.URL.Path)
	// NOTE: The following is for V2 Auth
	// TODO: Validate the account string
	parts := strings.SplitN(strings.Split(authStr, " ")[1], ":", 2)
	s.account = parts[0]

	// TODO: Validate the container
	// Generate the hbird api path
	if s.object != "" {
		s.path = fmt.Sprintf("/v1/%s/%s/%s", s.account, s.container, s.object)
	} else if s.container != "" {
		s.path = fmt.Sprintf("/v1/%s/%s", s.account, s.container)
	} else if s.account != "" {
		s.path = fmt.Sprintf("/v1/%s", s.account)
	}
	// TODO: Handle metadata?

	if s.object != "" {
		s.handleObjectRequest(writer, request)
	} else if s.container != "" {
		s.handleContainerRequest(writer, request)
	} else {
		s.handleAccountRequest(writer, request)
	}
}

func (s *s3Handler) handleObjectRequest(writer http.ResponseWriter, request *http.Request) {
	// If we didn't get to anything, then return no implemented
	srv.SimpleErrorResponse(writer, http.StatusNotImplemented, "Not Implemented")
}

func (s *s3Handler) handleContainerRequest(writer http.ResponseWriter, request *http.Request) {
	// If we didn't get to anything, then return no implemented
	srv.SimpleErrorResponse(writer, http.StatusNotImplemented, "Not Implemented")
}

func (s *s3Handler) handleAccountRequest(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	if request.Method == "GET" {
		newReq, err := ctx.newSubrequest("GET", s.path, http.NoBody, request, "s3api")
		if err != nil {
			// TODO: Need S3 error responses?
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		newReq.Header.Set("Accept", "application/json")
		cap := NewCaptureWriter()
		ctx.serveHTTPSubrequest(cap, newReq)
		containerListing := []accountserver.ContainerListingRecord{}
		err = json.Unmarshal(cap.body, &containerListing)
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		bucketList := NewS3BucketList()
		bucketList.Owner.ID = s.account
		// TODO: Figure out what to put for display name
		bucketList.Owner.DisplayName = "Some Name Goes Here"
		for _, c := range containerListing {
			// TODO: Get s3 style date from timestamp
			bucketList.Buckets = append(bucketList.Buckets, s3BucketInfo{
				Name:         c.Name,
				CreationDate: "2006-02-03T16:45:09.000Z",
			})
		}
		output, err := xml.MarshalIndent(bucketList, "", "  ")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		output = []byte(xml.Header + string(output))
		headers := writer.Header()
		headers.Set("Content-Type", "application/xml; charset=utf-8")
		headers.Set("Content-Length", strconv.Itoa(len(output)))
		writer.WriteHeader(200)
		writer.Write(output)
		return

	}

	// If we didn't get to anything, then return not implemented
	srv.SimpleErrorResponse(writer, http.StatusNotImplemented, "Not Implemented")
}

func NewS3Api(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	enabled, ok := config.Section["enabled"]
	if !ok || strings.Compare(strings.ToLower(enabled), "false") == 0 {
		// s3api is disabled, so pass the request on
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				next.ServeHTTP(writer, request)
			})
		}, nil
	}
	RegisterInfo("s3Api", map[string]interface{}{})
	return s3Api(metricsScope.Counter("s3Api_requests")), nil
}

func s3Api(requestsMetric tally.Counter) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			(&s3Handler{next: next, requestsMetric: requestsMetric}).ServeHTTP(writer, request)
		})
	}
}
