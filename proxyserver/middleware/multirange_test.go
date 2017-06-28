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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

func TestMultiRange(t *testing.T) {
	data := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/octet-stream")
		contentLength := int64(len(data))
		rng := request.Header.Get("Range")
		if rng == "" {
			writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			io.WriteString(writer, data)
			return
		}
		ranges, err := common.ParseRange(rng, contentLength)
		if err != nil {
			writer.Header().Set("Content-Length", "0")
			writer.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", contentLength))
			writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		} else if ranges != nil && len(ranges) == 1 {
			writer.Header().Set("Content-Length", strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))
			writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, contentLength))
			writer.WriteHeader(http.StatusPartialContent)
			io.WriteString(writer, data[ranges[0].Start:ranges[0].End])
		} else if ranges != nil && len(ranges) > 1 {
			srv.StandardResponse(writer, 500)
		}
	})
	section := conf.Section{}
	mrh, err := NewMultirange(section)
	require.Nil(t, err)
	h := mrh(handler)

	w := httptest.NewRecorder()

	r, err := http.NewRequest("GET", "/v1/a/c/o", nil)
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext",
		&ProxyContext{
			Authorize:              func(r *http.Request) (bool, int) { return true, http.StatusOK },
			Logger:                 zap.NewNop(),
			ProxyContextMiddleware: &ProxyContextMiddleware{next: h},
		}))
	r.Header.Set("Range", "bytes=20-,-6,0-3")
	require.Nil(t, err)
	h.ServeHTTP(w, r)
	require.Nil(t, err)

	assert.Equal(t, http.StatusPartialContent, w.Code)
	contentType, params, err := mime.ParseMediaType(w.Header().Get("Content-Type"))
	require.Nil(t, err)
	require.Equal(t, "multipart/byteranges", contentType)
	require.NotEqual(t, "", params["boundary"])
	assert.Equal(t, "511", w.Header().Get("Content-Length"))
	mr := multipart.NewReader(w.Body, params["boundary"])

	part, err := mr.NextPart()
	require.Nil(t, err)
	require.Equal(t, "application/octet-stream", part.Header.Get("Content-Type"))
	dat, err := ioutil.ReadAll(part)
	require.Nil(t, err)
	require.Equal(t, "UVWXYZ", string(dat))

	part, err = mr.NextPart()
	require.Nil(t, err)
	require.Equal(t, "application/octet-stream", part.Header.Get("Content-Type"))
	dat, err = ioutil.ReadAll(part)
	require.Nil(t, err)
	require.Equal(t, "UVWXYZ", string(dat))

	part, err = mr.NextPart()
	require.Nil(t, err)
	require.Equal(t, "application/octet-stream", part.Header.Get("Content-Type"))
	dat, err = ioutil.ReadAll(part)
	require.Nil(t, err)
	require.Equal(t, "ABCD", string(dat))

	_, err = mr.NextPart()
	require.NotNil(t, err)
}

func TestMultiRangeNotFound(t *testing.T) {
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		srv.StandardResponse(writer, http.StatusNotFound)
	})
	section := conf.Section{}
	mrh, err := NewMultirange(section)
	require.Nil(t, err)
	h := mrh(handler)

	w := httptest.NewRecorder()

	r, err := http.NewRequest("GET", "/v1/a/c/o", nil)
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext",
		&ProxyContext{
			Authorize:              func(r *http.Request) (bool, int) { return true, http.StatusOK },
			Logger:                 zap.NewNop(),
			ProxyContextMiddleware: &ProxyContextMiddleware{next: h},
		}))
	r.Header.Set("Range", "bytes=1-2,2-3")
	require.Nil(t, err)
	h.ServeHTTP(w, r)
	require.Nil(t, err)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.Equal(t, "text/html; charset=UTF-8", w.Header().Get("Content-Type"))
	body, err := ioutil.ReadAll(w.Body)
	require.Nil(t, err)
	require.Equal(t, "<html><h1>Not Found</h1><p>The resource could not be found.</p></html>", string(body))
}
