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

// To use with keystone, create s3 style credentials
//   `openstack ec2 credentials create`
//
// To enable, add the following to `/etc/hummingbird/proxy-server.conf`
//
//   [filter:s3api]
//   enabled = true
//
// Example using boto2 and haio with tempauth:
//
//  from boto.s3.connection import S3Connection
//  connection = S3Connection(
//    aws_access_key_id="test:tester",
//    aws_secret_access_key="testing",
//    port=8080,
//    host='127.0.0.1',
//    is_secure=False,
//    calling_format=boto.s3.connection.OrdinaryCallingFormat()
//  )
//  connection.get_all_buckets()
//
//  If you are using keystone auth, substitue the key and access key returned from keystone

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

type s3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestId string   `xml:"RequestId"`
}

func NewS3Error() *s3Error {
	return &s3Error{}
}

// TODO: Figure out how to plumb S3 Responses
func S3ErrorResponse(w http.ResponseWriter, statusCode int, resource, requestId string) {
	s3err := NewS3Error()
	// TODO: Add all error codes
	if statusCode == 403 {
		statusCode = 403
		s3err.Code = "AccessDenied"
		s3err.Message = "Access Denied"
	}
	s3err.Resource = resource
	s3err.RequestId = requestId
	output, err := xml.MarshalIndent(s3err, "", "  ")
	if err != nil {
		srv.SimpleErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	output = []byte(xml.Header + string(output))
	headers := w.Header()
	headers.Set("Content-Type", "application/xml; charset=utf-8")
	headers.Set("Content-Length", strconv.Itoa(len(output)))
	w.WriteHeader(statusCode)
	w.Write(output)

}

type s3ApiHandler struct {
	next           http.Handler
	ctx            *ProxyContext
	account        string
	container      string
	object         string
	path           string
	signature      string
	requestsMetric tally.Counter
}

func (s *s3ApiHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	// Check if this is an S3 request
	if ctx.S3Auth == nil || strings.HasPrefix(strings.ToLower(request.URL.Path), "/v1/") {
		// Not an S3 request
		s.next.ServeHTTP(writer, request)
		return
	}

	s.container, s.object, _, _ = getPathSegments(request.URL.Path)
	s.account = ctx.S3Auth.Account

	// TODO: Validate the container
	// Generate the hbird api path
	if s.object != "" {
		s.path = fmt.Sprintf("/v1/AUTH_%s/%s/%s", s.account, s.container, s.object)
	} else if s.container != "" {
		s.path = fmt.Sprintf("/v1/AUTH_%s/%s", s.account, s.container)
	} else {
		s.path = fmt.Sprintf("/v1/AUTH_%s", s.account)
	}
	// TODO: Handle metadata?

	if s.object != "" {
		s.handleObjectRequest(writer, request)
		return
	} else if s.container != "" {
		s.handleContainerRequest(writer, request)
		return
	} else {
		s.handleAccountRequest(writer, request)
		return
	}
}

func (s *s3ApiHandler) handleObjectRequest(writer http.ResponseWriter, request *http.Request) {
	// If we didn't get to anything, then return no implemented
	srv.SimpleErrorResponse(writer, http.StatusNotImplemented, "Not Implemented")
}

func (s *s3ApiHandler) handleContainerRequest(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)

	if request.Method == "DELETE" {
		newReq, err := ctx.newSubrequest("DELETE", s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
		}
		newReq.Header.Set("Accept", "application/json")
		cap := NewCaptureWriter()
		ctx.serveHTTPSubrequest(cap, newReq)
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		} else {
			writer.WriteHeader(204)
			return
		}
	}

	// If we didn't get to anything, then return no implemented
	srv.SimpleErrorResponse(writer, http.StatusNotImplemented, "Not Implemented")
}

func (s *s3ApiHandler) handleAccountRequest(writer http.ResponseWriter, request *http.Request) {
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
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		}
		containerListing := []accountserver.ContainerListingRecord{}
		err = json.Unmarshal(cap.body, &containerListing)
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		bucketList := NewS3BucketList()
		// TODO: figure out how to get the account
		bucketList.Owner.ID = "ACCOUNT"
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
			(&s3ApiHandler{next: next, requestsMetric: requestsMetric}).ServeHTTP(writer, request)
		})
	}
}
