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
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

type mrw struct {
	io.Writer
	header http.Header
	err    error
}

func (w *mrw) WriteHeader(int)     {}
func (w *mrw) Header() http.Header { return w.header }

func firstRange(rangeHeader string) string {
	rangeHeader = strings.Replace(strings.ToLower(rangeHeader), " ", "", -1)
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return ""
	}
	rangeHeader = rangeHeader[6:]
	return "bytes=" + strings.Split(rangeHeader, ",")[0]
}

func multirange(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		rangeHeader := request.Header.Get("Range")
		if request.Method != "GET" || rangeHeader == "" || !strings.Contains(rangeHeader, ",") {
			next.ServeHTTP(writer, request)
			return
		}
		if apiReq, _, _, object := getPathParts(request); !apiReq || object == "" {
			next.ServeHTTP(writer, request)
			return
		}
		var ranges []common.HttpRange

		ctx := GetProxyContext(request)
		var contentLength int64
		var contentType string
		var mw *common.MultiWriter

		subreq, err := http.NewRequest("GET", request.URL.Path, nil)
		if err != nil {
			srv.StandardResponse(writer, 500)
			return
		}
		subreq.Header.Set("Range", firstRange(rangeHeader))

		uw := &mrw{Writer: ioutil.Discard, header: make(http.Header)}
		subw := srv.NewCustomWriter(uw, func(w http.ResponseWriter, status int) int {
			if status != http.StatusPartialContent {
				uw.err = fmt.Errorf("Bad status code %d", status)
				uw.Writer = writer
				for k := range uw.header {
					writer.Header().Set(k, uw.header.Get(k))
				}
				writer.WriteHeader(status)
				return status
			}
			contentType = uw.header.Get("Content-Type")
			rspRange := uw.header.Get("Content-Range")
			rrp := strings.Split(rspRange, "/")
			if contentLength, err = strconv.ParseInt(rrp[len(rrp)-1], 10, 64); err != nil {
				uw.err = fmt.Errorf("Error parsing content-length from response")
				writer.Header().Set("Content-Range", uw.header.Get("Content-Range"))
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return http.StatusInternalServerError
			}
			if ranges, err = common.ParseRange(rangeHeader, contentLength); err != nil {
				uw.err = fmt.Errorf("Error parsing multiple ranges from request")
				writer.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", contentLength))
				srv.StandardResponse(writer, http.StatusRequestedRangeNotSatisfiable)
				return http.StatusRequestedRangeNotSatisfiable
			}
			mw = common.NewMultiWriter(writer, contentType, contentLength)
			for _, rng := range ranges {
				mw.Expect(rng.Start, rng.End)
			}
			writer.Header().Set("Content-Length", strconv.FormatInt(mw.ContentLength(), 10))
			writer.Header().Set("Content-Type", "multipart/byteranges;boundary="+mw.Boundary())
			writer.WriteHeader(http.StatusPartialContent)

			part, err := mw.CreatePart(ranges[0].Start, ranges[0].End)
			if err != nil {
				uw.err = err
			} else {
				uw.Writer = part
			}
			return status
		})
		if ctx.Subrequest(subw, subreq, "multirange", false); uw.err != nil {
			return
		}

		for _, rng := range ranges[1:] {
			if subreq, err = http.NewRequest("GET", request.URL.Path, nil); err != nil {
				srv.StandardResponse(writer, 500)
				return
			}
			subreq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rng.Start, rng.End-1))
			if err != nil {
				return // we just can't complete this request
			}
			uw := &mrw{Writer: ioutil.Discard, header: make(http.Header)}
			subw := srv.NewCustomWriter(uw, func(w http.ResponseWriter, status int) int {
				part, err := mw.CreatePart(rng.Start, rng.End)
				if err != nil {
					uw.err = err
				} else if status != http.StatusPartialContent {
					uw.err = fmt.Errorf("Bad status code %d", status)
				} else {
					uw.Writer = part
				}
				return status
			})
			if ctx.Subrequest(subw, subreq, "multirange", false); uw.err != nil {
				return
			}
		}
		mw.Close()
	})
}

// NewMultirange returns an instance of the multirange middleware with the given config.
//
// This middleware intercepts object GET requests with multiple ranges in the Range header and
// turns them into separate single-range requests on the backend, combining them into a multipart
// response.  This should simplify the implementation of things like xLO and the object server.
func NewMultirange(config conf.Section) (func(http.Handler) http.Handler, error) {
	return multirange, nil
}
