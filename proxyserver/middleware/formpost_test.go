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
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func makeFormpostRequest(t *testing.T, body, boundary string, next http.Handler) *httptest.ResponseRecorder {
	br := bytes.NewBufferString(body)
	newr, err := http.NewRequest("POST", "/v1/AUTH_test/container", br)
	require.Nil(t, err)
	newr.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	neww := httptest.NewRecorder()
	ctx := &ProxyContext{
		containerInfoCache: map[string]*ContainerInfo{
			"container/AUTH_test/container": {Metadata: map[string]string{"Temp-Url-Key": "mykey"}},
		},
		accountInfoCache: map[string]*AccountInfo{
			"account/AUTH_test": {Metadata: map[string]string{"Temp-Url-Key": "mykey"}}},
		capWriter: &proxyWriter{ResponseWriter: neww, Status: 501},
	}
	newr = newr.WithContext(context.WithValue(newr.Context(), "proxycontext", ctx))
	formpost(next).ServeHTTP(neww, newr)
	return neww
}

func formPostBody(key, boundary, redirect string, maxFileSize, maxFileCount int, expires time.Time) string {
	exp := strconv.FormatInt(expires.Unix(), 10)
	mac := hmac.New(sha1.New, []byte(key))
	fmt.Fprintf(mac, "%s\n%s\n%d\n%d\n%s", "/v1/AUTH_test/container", redirect,
		maxFileSize, maxFileCount, exp)
	sig := mac.Sum(nil)

	br := &bytes.Buffer{}
	w := multipart.NewWriter(br)
	w.SetBoundary(boundary)

	if redirect != "" {
		w.WriteField("redirect", redirect)
	}
	w.WriteField("max_file_size", strconv.Itoa(maxFileSize))
	w.WriteField("max_file_count", strconv.Itoa(maxFileCount))
	w.WriteField("expires", exp)
	w.WriteField("signature", hex.EncodeToString(sig))

	ff, _ := w.CreateFormFile("file1", "testfile1.txt")
	io.WriteString(ff, "Test File\nOne\n")

	ff, _ = w.CreateFormFile("file2", "testfile2.txt")
	io.WriteString(ff, "Test\nFile\nTwo\n")

	ff, _ = w.CreateFormFile("file3", "")
	io.WriteString(ff, "some stuff")

	w.Close()

	return br.String()
}

func TestFormPost(t *testing.T) {
	puts := []string{}
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "PUT" {
			puts = append(puts, request.URL.Path)
			writer.WriteHeader(201)
		}
	})
	boundary := "168072824752491622650073"
	body := formPostBody("mykey", boundary, "http://brim.net", 1024, 10, time.Now().Add(time.Minute))
	neww := makeFormpostRequest(t, body, boundary, next)
	require.Equal(t, 303, neww.Code)
	require.Equal(t, 2, len(puts))
}

func TestExpiredFormPost(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {})
	boundary := "168072824752491622650073"
	body := formPostBody("mykey", boundary, "", 1024, 10, time.Now().Add(-time.Hour))
	neww := makeFormpostRequest(t, body, boundary, next)
	require.Equal(t, 401, neww.Code)
}

func TestFormPostAuthorizer(t *testing.T) {
	auth := formpostAuthorizer(FP_SCOPE_ACCOUNT, "a", "c")
	req, err := http.NewRequest("GET", "/v1/a", nil)
	require.Nil(t, err)
	require.True(t, auth(req))
	req, err = http.NewRequest("GET", "/v1/a2", nil)
	require.Nil(t, err)
	require.False(t, auth(req))

	auth = formpostAuthorizer(FP_SCOPE_CONTAINER, "a", "c")
	req, err = http.NewRequest("GET", "/v1/a/c", nil)
	require.Nil(t, err)
	require.True(t, auth(req))
	req, err = http.NewRequest("GET", "/v1/a/c2", nil)
	require.Nil(t, err)
	require.False(t, auth(req))
	req, err = http.NewRequest("GET", "/v1/a2/c", nil)
	require.Nil(t, err)
	require.False(t, auth(req))

	auth = formpostAuthorizer(FP_INVALID, "a", "c")
	req, err = http.NewRequest("GET", "/v1/a2/c", nil)
	require.Nil(t, err)
	require.False(t, auth(req))
}

func TestFpLimitReader(t *testing.T) {
	ur := strings.NewReader("hello")
	r := fpLimitReader{Reader: ur, l: 2}
	scratch := make([]byte, 1)
	r.Read(scratch)
	require.False(t, r.overRead())
	r.Read(scratch)
	require.False(t, r.overRead())
	r.Read(scratch)
	require.True(t, r.overRead())
}

// func authorizeFormpost(ctx *ProxyContext, account, container, path string, attrs map[string]string) int {
func TestAuthenticateFormpost(t *testing.T) {
	ctx := &ProxyContext{
		containerInfoCache: map[string]*ContainerInfo{
			"container/a/c": {Metadata: map[string]string{
				"Temp-Url-Key":   "containerkey",
				"Temp-Url-Key-2": "containerkey2",
			}},
		},
		accountInfoCache: map[string]*AccountInfo{
			"account/a": {Metadata: map[string]string{
				"Temp-Url-Key":   "accountkey",
				"Temp-Url-Key-2": "accountkey2",
			}}},
	}

	require.Equal(t, FP_ERROR,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "X"}))
	require.Equal(t, FP_EXPIRED,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "12345"}))
	require.Equal(t, FP_ERROR,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "9999999999", "signature": "X"}))

	// account key 1
	require.Equal(t, FP_SCOPE_ACCOUNT,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "9999999999",
			"signature": "1d4cb17a0d70b32f7987fe49b1990020bab52ae6"}))
	// account key 2
	require.Equal(t, FP_SCOPE_ACCOUNT,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "9999999999",
			"signature": "9749158451be0383af1ec6d8e10f09a2d0d5f2b1"}))
	// container key 1
	require.Equal(t, FP_SCOPE_CONTAINER,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "9999999999",
			"signature": "3320ff06b119d287a1c8d18d4356cd91e8518fe7"}))
	// container key 2
	require.Equal(t, FP_SCOPE_CONTAINER,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "9999999999",
			"signature": "a3c3dd56ad65f5b87eeb384f9e0406c79511b556"}))
	// invalid key
	require.Equal(t, FP_INVALID,
		authenticateFormpost(ctx, "a", "c", "/v1/a/c", map[string]string{"expires": "9999999999",
			"signature": "1111111111111111111111111111111111111111"}))
}
