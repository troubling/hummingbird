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
//  import boto
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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/containerserver"
	"github.com/uber-go/tally"
)

const (
	s3Xmlns                      = "http://s3.amazonaws.com/doc/2006-03-01"
	s3MultipartCompleteBodyLimit = 65536
	s3MultipartMaxParts          = 1000
)

type s3Response struct {
	Code    string
	Message string
}

var s3Responses = map[int]s3Response{
	403:   {"AccessDenied", "Access Denied"},
	404:   {"NotFound", "Not Found"}, // TODO: S3 responds with differetn 404 messages
	405:   {"MethodNotAllowed", "The specified method is not allowed against this resource."},
	411:   {"MissingContentLength", "You must provide the Content-Length HTTP header."},
	500:   {"InternalError", "We encountered an internal error. Please try again."},
	501:   {"NotImplemented", "A header you provided implies functionality that is not implemented."},
	503:   {"ServiceUnavailable", "Reduce your request rate."},
	40000: {"InvalidBucketName", "The specified bucket is not valid."},
	40001: {"BucketAlreadyExists", "The specified bucket is not valid."},
	40300: {"SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided."},
	40400: {"NoSuchBucket", "The specified bucket does not exist."},
}

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

type s3InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string
	Key      string
	UploadId string
}

type s3CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string
	Bucket   string
	Key      string
	ETag     string
}

type s3CompleteMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		ETag       string
		PartNumber int
	} `xml:"Part"`
}

type s3ListPartsResultPart struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int64
}

type s3ListPartsResult struct {
	XMLName   xml.Name `xml:"ListPartsResult"`
	Xmlns     string   `xml:"xmlns,attr"`
	Bucket    string
	Key       string
	UploadId  string
	Initiator struct {
		ID          string
		DisplayName string
	}
	Owner struct {
		ID          string
		DisplayName string
	}
	StorageClass string
	MaxParts     int
	IsTruncated  bool
	Parts        []s3ListPartsResultPart `xml:"Part"`
}

type s3ListMultipartUploadsUpload struct {
	Key       string
	UploadId  string
	Initiator struct {
		ID          string
		DisplayName string
	}
	Owner struct {
		ID          string
		DisplayName string
	}
	StorageClass string
	Initiated    string
}

type s3ListMultipartUploadsResult struct {
	XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
	Xmlns              string   `xml:"xmlns,attr"`
	Bucket             string
	UploadIdMarker     string
	NextKeyMarker      string
	NextUploadIdMarker string
	MaxUploads         int
	IsTruncated        bool
	Uploads            []s3ListMultipartUploadsUpload `xml:"Upload"`
}

func NewS3BucketList() *s3BucketList {
	return &s3BucketList{Xmlns: s3Xmlns}
}

type s3ObjectInfo struct {
	Name         string   `xml:"Key"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int64    `xml:"Size"`
	Owner        *s3Owner `xml:"Owner,omitempty"`
	StorageClass string   `xml:"StorageClass"`
}

type s3Prefix struct {
	Prefix string `xml:"Prefix,omitempty"`
}

type s3ObjectListV1 struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Xmlns       string         `xml:"xmlns,attr"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	Marker      string         `xml:"Marker"`
	NextMarker  string         `xml:"NextMarker,omitempty"`
	MaxKeys     int            `xml:"MaxKeys"`
	Delimiter   string         `xml:"Delimiter,omitempty"`
	IsTruncated bool           `xml:"IsTruncated"`
	Objects     []s3ObjectInfo `xml:"Contents"`
	Prefixes    []s3Prefix     `xml:"CommonPrefixes,omitempty"`
}

type s3ObjectListV2 struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	NextContinuationToken string         `xml:"NextContinationToken,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	KeyCount              string         `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Objects               []s3ObjectInfo `xml:"Contents"`
	Prefixes              []s3Prefix     `xml:"CommonPrefixes,omitempty"`
}

type s3ObjectList interface {
	SetName(val string)
	SetPrefix(val string)
	SetMarker(val string)
	SetNextMarker(val string)
	SetMaxKeys(val int)
	SetDelimiter(val string)
	SetIsTruncated(val bool)
	SetNextContinuationToken(val string)
	SetContinuationToken(val string)
	SetStartAfter(val string)
	SetKeyCount(val string)
	GetObjects() []s3ObjectInfo
	SetObjects(val []s3ObjectInfo)
	GetPrefixes() []s3Prefix
	SetPrefixes(val []s3Prefix)
}

func (l *s3ObjectListV1) SetName(val string)                  { l.Name = val }
func (l *s3ObjectListV1) SetPrefix(val string)                { l.Prefix = val }
func (l *s3ObjectListV1) SetMarker(val string)                { l.Marker = val }
func (l *s3ObjectListV1) SetNextMarker(val string)            { l.NextMarker = val }
func (l *s3ObjectListV1) SetMaxKeys(val int)                  { l.MaxKeys = val }
func (l *s3ObjectListV1) SetDelimiter(val string)             { l.Delimiter = val }
func (l *s3ObjectListV1) SetIsTruncated(val bool)             { l.IsTruncated = val }
func (l *s3ObjectListV1) SetNextContinuationToken(val string) {}
func (l *s3ObjectListV1) SetContinuationToken(val string)     {}
func (l *s3ObjectListV1) SetStartAfter(val string)            {}
func (l *s3ObjectListV1) SetKeyCount(val string)              {}
func (l *s3ObjectListV1) GetObjects() []s3ObjectInfo          { return l.Objects }
func (l *s3ObjectListV1) SetObjects(val []s3ObjectInfo)       { l.Objects = val }
func (l *s3ObjectListV1) GetPrefixes() []s3Prefix             { return l.Prefixes }
func (l *s3ObjectListV1) SetPrefixes(val []s3Prefix)          { l.Prefixes = val }

func (l *s3ObjectListV2) SetName(val string)                  { l.Name = val }
func (l *s3ObjectListV2) SetPrefix(val string)                { l.Prefix = val }
func (l *s3ObjectListV2) SetMarker(val string)                {}
func (l *s3ObjectListV2) SetNextMarker(val string)            {}
func (l *s3ObjectListV2) SetMaxKeys(val int)                  { l.MaxKeys = val }
func (l *s3ObjectListV2) SetDelimiter(val string)             { l.Delimiter = val }
func (l *s3ObjectListV2) SetIsTruncated(val bool)             { l.IsTruncated = val }
func (l *s3ObjectListV2) SetNextContinuationToken(val string) { l.NextContinuationToken = val }
func (l *s3ObjectListV2) SetContinuationToken(val string)     { l.ContinuationToken = val }
func (l *s3ObjectListV2) SetStartAfter(val string)            { l.StartAfter = val }
func (l *s3ObjectListV2) SetKeyCount(val string)              { l.KeyCount = val }
func (l *s3ObjectListV2) GetObjects() []s3ObjectInfo          { return l.Objects }
func (l *s3ObjectListV2) SetObjects(val []s3ObjectInfo)       { l.Objects = val }
func (l *s3ObjectListV2) GetPrefixes() []s3Prefix             { return l.Prefixes }
func (l *s3ObjectListV2) SetPrefixes(val []s3Prefix)          { l.Prefixes = val }

func NewS3ObjectList(ver string) s3ObjectList {
	if ver == "2" {
		return &s3ObjectListV2{Xmlns: s3Xmlns}
	}
	return &s3ObjectListV1{Xmlns: s3Xmlns}
}

type ObjectListingRecord struct {
	Name         string `json:"name"`
	LastModified string `json:"last_modified"`
	Size         int64  `json:"bytes"`
	ContentType  string `json:"content_type"`
	ETag         string `json:"hash"`
	Subdir       string `json:"subdir"`
}

type s3CopyObject struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
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

// This will wrap http.ResponseWriter to support s3 style xml responses on errors
// TODO: This may still need some work for more specific error responses
type s3ResponseWriterWrapper struct {
	writer    http.ResponseWriter
	hijack    bool
	resource  string
	requestId string
	msg       []byte
}

func newS3ResponseWriterWrapper(w http.ResponseWriter, r *http.Request) *s3ResponseWriterWrapper {
	ctx := GetProxyContext(r)
	return &s3ResponseWriterWrapper{
		writer:    w,
		hijack:    false,
		resource:  r.URL.Path,
		requestId: ctx.TxId,
	}
}

func (w *s3ResponseWriterWrapper) Header() http.Header {
	return w.writer.Header()
}

func (w *s3ResponseWriterWrapper) WriteHeader(statusCode int) {
	if statusCode/100 != 2 {
		// We are going to hijack to return an S3 style result
		w.hijack = true
		if statusCode == 401 {
			statusCode = 403 // S3 returns 403 instead of 401
		}
		msg := NewS3Error()
		msg.Code = s3Responses[statusCode].Code
		msg.Message = s3Responses[statusCode].Message
		msg.Resource = w.resource
		msg.RequestId = w.requestId
		output, err := xml.MarshalIndent(msg, "", "  ")
		if err != nil {
			w.hijack = false
			w.WriteHeader(500)
		}
		output = []byte(xml.Header + string(output))
		headers := w.writer.Header()
		headers.Set("Content-Type", "application/xml; charset=utf-8")
		headers.Set("Content-Length", strconv.Itoa(len(output)))
		w.msg = output
	}
	w.writer.WriteHeader(statusCode)
}

func (w *s3ResponseWriterWrapper) Write(buf []byte) (int, error) {
	if !w.hijack {
		return w.writer.Write(buf)
	} else {
		n, err := w.writer.Write(w.msg)
		return n, err
	}
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

func s3PathSplit(path string) (string, string) {
	if len(path) > 0 && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	parts := strings.SplitN(path, "/", 3)
	switch len(parts) {
	case 3:
		return parts[1], parts[2]
	case 2:
		return parts[1], ""
	default:
		return "", ""
	}
}

var bucketIsIP = regexp.MustCompile(`(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}`)
var bucketValidChars = regexp.MustCompile(`^[-.a-z0-9]*$`)

func validBucketName(s string) bool {
	if len(s) < 3 || len(s) > 63 {
		return false
	}
	if !(unicode.IsLetter(rune(s[0])) || unicode.IsDigit(rune(s[0]))) {
		return false
	}
	if strings.Index(s, ".-") != -1 {
		return false
	}
	if strings.Index(s, "-.") != -1 {
		return false
	}
	if strings.Index(s, "..") != -1 {
		return false
	}
	if s[len(s)-1] == '.' {
		return false
	}
	if bucketIsIP.MatchString(s) {
		return false
	}
	if !bucketValidChars.MatchString(s) {
		return false
	}
	return true
}

func NoSuchBucketResponse(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(40400)
	writer.Write(nil)
}

func SignatureDoesNotMatchResponse(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(40300)
	writer.Write(nil)
}

func InvalidBucketNameResponse(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(40000)
	writer.Write(nil)
}

func BucketAlreadyExistsResponse(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(40001)
	writer.Write(nil)
}

func (s *s3ApiHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	// Check if this is an S3 request
	if ctx.S3Auth == nil || strings.HasPrefix(strings.ToLower(request.URL.Path), "/v1/") {
		// Not an S3 request
		s.next.ServeHTTP(writer, request)
		return
	}

	s.container, s.object = s3PathSplit(request.URL.Path)
	s.account = ctx.S3Auth.Account

	if s.container != "" {
		if !validBucketName(s.container) {
			InvalidBucketNameResponse(writer, request)
			return
		}
	}

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
	ctx := GetProxyContext(request)
	request.ParseForm()

	if request.Method == "GET" || request.Method == "HEAD" {
		if uploadId := request.Form.Get("uploadId"); uploadId != "" {
			newReq, err := ctx.newSubrequest("GET", fmt.Sprintf("/v1/AUTH_%s/%s+segments?prefix=%s-%s/", common.Urlencode(s.account),
				common.Urlencode(s.container), common.Urlencode(uploadId), common.Urlencode(s.object)), http.NoBody, request, "s3api")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			newReq.Header.Set("Accept", "application/json")
			w := NewCaptureWriter()
			ctx.serveHTTPSubrequest(w, newReq)
			if w.status/100 != 2 {
				srv.StandardResponse(writer, w.status)
				return
			}
			objectListing := []containerserver.ObjectListingRecord{}
			if err = json.Unmarshal(w.body, &objectListing); err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			x := s3ListPartsResult{
				Xmlns:        s3Xmlns,
				Bucket:       s.container,
				Key:          s.object,
				UploadId:     uploadId,
				StorageClass: "STANDARD",
				MaxParts:     s3MultipartMaxParts,
				IsTruncated:  false,
			}
			for _, obj := range objectListing {
				i := 0
				if i, err = strconv.Atoi(obj.Name[len(uploadId)+2+len(s.object):]); err != nil || i < 1 {
					continue
				}
				x.Parts = append(x.Parts, s3ListPartsResultPart{
					PartNumber:   i,
					LastModified: obj.LastModified,
					ETag:         obj.ETag,
					Size:         obj.Size,
				})
			}
			output, err := xml.MarshalIndent(x, "", "  ")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
			writer.WriteHeader(200)
			writer.Write([]byte(xml.Header))
			writer.Write(output)
			return
		}
		newReq, err := ctx.newSubrequest(request.Method, s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
		}
		newReq.Header.Set("Range", request.Header.Get("Range"))
		newReq.Header.Set("If-Match", request.Header.Get("If-Match"))
		newReq.Header.Set("If-None-Match", request.Header.Get("If-None-Match"))
		newReq.Header.Set("If-Modified-Since", request.Header.Get("If-Modified-Since"))
		newReq.Header.Set("If-UnModified-Since", request.Header.Get("If-UnModified-Since"))
		ctx.serveHTTPSubrequest(writer, newReq)
		return
	}

	if request.Method == "DELETE" {
		if uploadId := request.Form.Get("uploadId"); uploadId != "" {
			newReq, err := ctx.newSubrequest("GET", fmt.Sprintf("/v1/AUTH_%s/%s+segments?prefix=%s-%s", common.Urlencode(s.account),
				common.Urlencode(s.container), common.Urlencode(uploadId), common.Urlencode(s.object)), http.NoBody, request, "s3api")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
			}
			newReq.Header.Set("Accept", "application/json")
			w := NewCaptureWriter()
			ctx.serveHTTPSubrequest(w, newReq)
			if w.status/100 != 2 {
				srv.StandardResponse(writer, w.status)
				return
			}
			objectListing := []containerserver.ObjectListingRecord{}
			err = json.Unmarshal(w.body, &objectListing)
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			for _, obj := range objectListing {
				newReq, err := ctx.newSubrequest("DELETE", fmt.Sprintf("/v1/AUTH_%s/%s+segments/%s", common.Urlencode(s.account),
					common.Urlencode(s.container), common.Urlencode(obj.Name)), http.NoBody, request, "s3api")
				if err != nil {
					srv.StandardResponse(writer, http.StatusInternalServerError)
				}
				w := NewCaptureWriter()
				ctx.serveHTTPSubrequest(w, newReq)
				if w.status/100 != 2 {
					srv.StandardResponse(writer, w.status)
					return
				}
			}
			writer.WriteHeader(204)
			return
		}
		newReq, err := ctx.newSubrequest("DELETE", s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
		}
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

	if request.Method == "PUT" {
		if uploadId := request.Form.Get("uploadId"); uploadId != "" {
			if partNumber, err := strconv.Atoi(request.Form.Get("partNumber")); err != nil || partNumber < 1 || partNumber > s3MultipartMaxParts {
				srv.StandardResponse(writer, http.StatusBadRequest)
				return
			} else {
				s.path = fmt.Sprintf("/v1/AUTH_%s/%s+segments/%s-%s/%08d", common.Urlencode(s.account),
					common.Urlencode(s.container), common.Urlencode(uploadId), common.Urlencode(s.object), partNumber)
			}
		}
		method := "PUT"
		dest := ""
		// Check to see if this is a copy request
		copySource := request.Header.Get("X-Amz-Copy-Source")
		if copySource != "" {
			method = "COPY"
			dest = s.path
			c, o := s3PathSplit(copySource)
			s.path = fmt.Sprintf("/v1/AUTH_%s/%s/%s", s.account, c, o)
		}
		newReq, err := ctx.newSubrequest(method, s.path, request.Body, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		if copySource != "" {
			pathMap, err := common.ParseProxyPath(dest)
			if err != nil {
				srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
				return
			}
			newReq.Header.Set("Destination", fmt.Sprintf("/%s/%s", pathMap["container"], pathMap["object"]))
		}
		newReq.Header.Set("Content-Length", request.Header.Get("Content-Length"))
		newReq.Header.Set("Content-Type", request.Header.Get("Content-Type"))
		cap := NewCaptureWriter()
		ctx.serveHTTPSubrequest(cap, newReq)
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		} else {
			if copySource != "" {
				copyResult := &s3CopyObject{}
				copyResult.ETag = "\"" + cap.Header().Get("ETag") + "\""
				copyResult.LastModified = cap.Header().Get("Last-Modified")
				output, err := xml.MarshalIndent(copyResult, "", "  ")
				if err != nil {
					srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
					return
				}
				output = []byte(xml.Header + string(output))
				writer.WriteHeader(200)
				writer.Write(output)
			} else {
				writer.Header().Set("ETag", "\""+cap.Header().Get("ETag")+"\"")
				writer.Header().Set("Content-Length", cap.Header().Get("Content-Length"))
				writer.WriteHeader(200)
			}
			return
		}
	}

	if request.Method == "POST" {
		if _, upload := request.Form["uploads"]; upload && request.Form.Get("uploads") == "" {
			uploadId := fmt.Sprintf("%x", rand.Int63())

			newReq, err := ctx.newSubrequest("PUT", fmt.Sprintf("/v1/AUTH_%s/%s+segments", common.Urlencode(s.account),
				common.Urlencode(s.container)), http.NoBody, request, "s3api")
			newReq.Header.Set("Content-Length", "0")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			c := NewCaptureWriter()
			ctx.serveHTTPSubrequest(c, newReq)
			if c.status/100 != 2 {
				srv.StandardResponse(writer, c.status)
				return
			}

			newReq, err = ctx.newSubrequest("PUT", fmt.Sprintf("/v1/AUTH_%s/%s+segments/%s-%s", common.Urlencode(s.account),
				common.Urlencode(s.container), common.Urlencode(uploadId), common.Urlencode(s.object)), http.NoBody, request, "s3api")
			newReq.Header.Set("Content-Length", "0")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			c = NewCaptureWriter()
			ctx.serveHTTPSubrequest(c, newReq)
			if c.status/100 != 2 {
				srv.StandardResponse(writer, c.status)
				return
			}
			output, err := xml.MarshalIndent(s3InitiateMultipartUploadResult{
				Xmlns:    s3Xmlns,
				Bucket:   s.container,
				Key:      s.object,
				UploadId: uploadId,
			}, "", "  ")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
			writer.WriteHeader(200)
			writer.Write([]byte(xml.Header))
			writer.Write(output)
			return
		} else if uploadId := request.Form.Get("uploadId"); uploadId != "" {
			body, err := ioutil.ReadAll(io.LimitReader(request.Body, s3MultipartCompleteBodyLimit))
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			completeMU := s3CompleteMultipartUpload{}
			if err := xml.Unmarshal(body, &completeMU); err != nil {
				srv.StandardResponse(writer, http.StatusBadRequest)
				return
			}

			type bodyPart struct {
				Path string `json:"path"`
			}
			slobj := []bodyPart{}
			for _, part := range completeMU.Parts {
				slobj = append(slobj, bodyPart{Path: fmt.Sprintf("/%s+segments/%s-%s/%08d", common.Urlencode(s.container),
					common.Urlencode(uploadId), common.Urlencode(s.object), part.PartNumber)})
			}
			slobjBody, err := json.Marshal(slobj)
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			newReq, err := ctx.newSubrequest("PUT", fmt.Sprintf("/v1/AUTH_%s/%s/%s?multipart-manifest=put",
				common.Urlencode(s.account), common.Urlencode(s.container), common.Urlencode(s.object)),
				bytes.NewBuffer(slobjBody), request, "s3api")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			newReq.Header.Set("Content-Length", "0")
			c := NewCaptureWriter()
			ctx.serveHTTPSubrequest(c, newReq)
			if c.status/100 != 2 {
				srv.StandardResponse(writer, c.status)
				return
			}

			output, err := xml.MarshalIndent(s3CompleteMultipartUploadResult{
				Xmlns:    s3Xmlns,
				Location: "", // TODO
				Bucket:   s.container,
				Key:      s.object,
				ETag:     "", // TODO
			}, "", "  ")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
			writer.WriteHeader(200)
			writer.Write([]byte(xml.Header))
			writer.Write(output)
			return
		} else {
			srv.StandardResponse(writer, http.StatusNotImplemented)
			return
		}
	}

	// If we didn't get to anything, then return method not allowed
	srv.StandardResponse(writer, http.StatusMethodNotAllowed)
}

func (s *s3ApiHandler) handleContainerRequest(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	request.ParseForm()

	if request.Method == "HEAD" {
		newReq, err := ctx.newSubrequest("HEAD", s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		cap := NewCaptureWriter()
		ctx.serveHTTPSubrequest(cap, newReq)
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		} else {
			writer.WriteHeader(200)
			return
		}
	}

	if request.Method == "DELETE" {
		newReq, err := ctx.newSubrequest("DELETE", s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
		}
		cap := NewCaptureWriter()
		ctx.serveHTTPSubrequest(cap, newReq)
		if cap.status == 404 {
			NoSuchBucketResponse(writer, request)
			return
		}
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		} else {
			writer.WriteHeader(204)
			return
		}
	}

	if request.Method == "PUT" {
		newReq, err := ctx.newSubrequest("PUT", s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
		}
		cap := NewCaptureWriter()
		ctx.serveHTTPSubrequest(cap, newReq)
		/* Can't overwrite a bucket in s3, so we'll lie about it here. */
		if cap.status == http.StatusAccepted {
			BucketAlreadyExistsResponse(writer, request)
			return
		}
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		} else {
			writer.WriteHeader(200)
			return
		}
	}

	if request.Method == "GET" {
		if _, upload := request.Form["uploads"]; upload && request.Form.Get("uploads") == "" {
			newReq, err := ctx.newSubrequest("GET", fmt.Sprintf("/v1/AUTH_%s/%s+segments?prefix=&delimiter=/", s.account, s.container),
				http.NoBody, request, "s3api")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			newReq.Header.Set("Accept", "application/json")
			w := NewCaptureWriter()
			ctx.serveHTTPSubrequest(w, newReq)
			if w.status/100 != 2 {
				srv.StandardResponse(writer, w.status)
				return
			}
			objectListing := []containerserver.ObjectListingRecord{}
			err = json.Unmarshal(w.body, &objectListing)
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}

			uploadList := s3ListMultipartUploadsResult{
				Bucket: s.container,
			}
			for _, obj := range objectListing {
				ops := strings.SplitN(obj.Name, "-", 1)
				if len(ops) != 2 {
					srv.StandardResponse(writer, http.StatusInternalServerError)
					return
				}
				uploadList.Uploads = append(uploadList.Uploads, s3ListMultipartUploadsUpload{
					Key:          ops[1],
					UploadId:     ops[0],
					StorageClass: "STANDARD",
					Initiated:    obj.LastModified,
				})
			}
			output, err := xml.MarshalIndent(uploadList, "", "  ")
			if err != nil {
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
			writer.WriteHeader(200)
			writer.Write([]byte(xml.Header))
			writer.Write(output)
			return
		}
		q := request.URL.Query()
		maxKeys, err := strconv.Atoi(q.Get("max-keys"))
		if err != nil {
			maxKeys = 1000
		}
		newReq, err := ctx.newSubrequest("GET", s.path, http.NoBody, request, "s3api")
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		newReq.Header.Set("Accept", "application/json")
		nq := newReq.URL.Query()
		nq.Set("limit", strconv.Itoa(maxKeys+1))
		ver := q.Get("list-type")
		marker := q.Get("marker")
		prefix := q.Get("prefix")
		delimiter := q.Get("delimiter")
		fetchOwner := false
		if ver == "2" {
			marker = q.Get("start-after")
			cont := q.Get("continuation-token")
			if cont != "" {
				if b, err := base64.StdEncoding.DecodeString(cont); err == nil {
					marker = string(b)
				}
			}
			fetchOwner, err = strconv.ParseBool(q.Get("fetch-owner"))
			if err != nil {
				fetchOwner = false
			}
		}
		if marker != "" {
			nq.Set("marker", marker)
		}
		if prefix != "" {
			nq.Set("prefix", prefix)
		}
		if delimiter != "" {
			nq.Set("delimiter", delimiter)
		}
		cap := NewCaptureWriter()
		newReq.URL.RawQuery = nq.Encode()
		ctx.serveHTTPSubrequest(cap, newReq)
		if cap.status == 404 {
			NoSuchBucketResponse(writer, request)
			return
		}
		if cap.status/100 != 2 {
			srv.StandardResponse(writer, cap.status)
			return
		}
		objectListing := []ObjectListingRecord{}
		err = json.Unmarshal(cap.body, &objectListing)
		if err != nil {
			srv.SimpleErrorResponse(writer, http.StatusInternalServerError, err.Error())
			return
		}
		truncated := maxKeys > 0 && len(objectListing) > maxKeys
		next := ""
		if len(objectListing) > maxKeys {
			next = objectListing[maxKeys-1].Name
			objectListing = objectListing[:maxKeys]
		}
		objectList := NewS3ObjectList(ver)
		objectList.SetName(s.container)
		objectList.SetMaxKeys(maxKeys)
		objectList.SetDelimiter(delimiter)
		objectList.SetIsTruncated(truncated)
		objectList.SetPrefix(prefix)
		if truncated {
			objectList.SetNextContinuationToken(base64.StdEncoding.EncodeToString([]byte(next)))
			if delimiter != "" {
				objectList.SetNextMarker(next)
			}
		}
		objectList.SetContinuationToken(q.Get("continuation-token"))
		objectList.SetStartAfter(q.Get("start-after"))
		objectList.SetKeyCount(strconv.Itoa(len(objectListing)))
		objectList.SetMarker(marker)
		var prefixes []string
		for _, o := range objectListing {
			if o.Subdir != "" {
				prefixes = append(prefixes, o.Subdir)
				continue
			}
			obj := s3ObjectInfo{
				Name:         o.Name,
				LastModified: o.LastModified + "Z",
				ETag:         "\"" + o.ETag + "\"",
				Size:         o.Size,
				StorageClass: "STANDARD",
			}
			if fetchOwner || ver != "2" {
				obj.Owner = &s3Owner{
					ID:          ctx.S3Auth.Account,
					DisplayName: ctx.S3Auth.Account,
				}
			}
			objectList.SetObjects(append(objectList.GetObjects(), obj))
		}
		if len(prefixes) > 0 {
			sort.Strings(prefixes)
			for _, prefix := range prefixes {
				objectList.SetPrefixes(append(objectList.GetPrefixes(), s3Prefix{Prefix: prefix}))
			}
		}
		output, err := xml.MarshalIndent(objectList, "", "  ")
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
	// If we didn't get to anything, then return method not allowed
	srv.StandardResponse(writer, http.StatusMethodNotAllowed)
}

func (s *s3ApiHandler) handleAccountRequest(writer http.ResponseWriter, request *http.Request) {
	ctx := GetProxyContext(request)
	if request.Method == "GET" {
		newReq, err := ctx.newSubrequest("GET", s.path, http.NoBody, request, "s3api")
		if err != nil {
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
		bucketList.Owner.ID = ctx.S3Auth.Account
		bucketList.Owner.DisplayName = ctx.S3Auth.Account
		// NOTE: The container list api doesn't have a creation date for the container, so we use an "arbitrary" date.
		for _, c := range containerListing {
			bucketList.Buckets = append(bucketList.Buckets, s3BucketInfo{
				Name:         c.Name,
				CreationDate: "2009-02-03T16:45:09.000Z",
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

	// If we didn't get to anything, then return method not allowed
	srv.StandardResponse(writer, http.StatusMethodNotAllowed)
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
