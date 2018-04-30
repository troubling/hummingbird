//  Copyright (c) 2015 Rackspace
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

package common

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPutTooBig(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = MAX_FILE_SIZE + 1
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusRequestEntityTooLarge, status)
}

func TestBadTransferEncoding(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = -1
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusLengthRequired, status)

	req.TransferEncoding = []string{"notchunked"}
	status, _ = CheckObjPut(req, "o")
	require.Equal(t, status, http.StatusLengthRequired)
}

func TestLengthOnCopyFrom(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = 1
	req.Header.Set("X-Copy-From", "/v1/a/c/otherobject")
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestNameTooLong(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = 1
	req.Header.Set("Content-Length", "1")
	status, _ := CheckObjPut(req, strings.Repeat("o", MAX_OBJECT_NAME_LENGTH+1))
	require.Equal(t, http.StatusBadRequest, status)
}

func TestNoContentType(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = 1
	req.Header.Set("Content-Length", "1")
	req.Header.Set("Content-Type", "")
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestNoContentLength(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = -1
	req.Header.Set("Content-Length", "")
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusLengthRequired, status)
}

func TestBadXDeleteAt(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = 1
	req.Header.Set("Content-Length", "1")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Delete-At", "1")
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusBadRequest, status)

	req.Header.Set("X-Delete-At", "!")
	status, _ = CheckObjPut(req, "o")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestBadXDeleteAfter(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.ContentLength = 1
	req.Header.Set("Content-Length", "1")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Delete-After", "-1")
	status, _ := CheckObjPut(req, "o")
	require.Equal(t, http.StatusBadRequest, status)
	status, _ = CheckObjPost(req, "o")
	require.Equal(t, http.StatusBadRequest, status)

	req.Header.Set("X-Delete-After", "!")
	status, _ = CheckObjPut(req, "o")
	require.Equal(t, http.StatusBadRequest, status)
	status, _ = CheckObjPost(req, "o")
	require.Equal(t, http.StatusBadRequest, status)

	req.Header.Set("X-Delete-After", "5")
	status, _ = CheckObjPut(req, "o")
	xda := req.Header.Get("X-Delete-At")
	require.True(t, xda == fmt.Sprintf("%d", time.Now().Unix()+5) || xda == fmt.Sprintf("%d", time.Now().Unix()+4))

	req.Header.Set("X-Delete-After", "5")
	status, _ = CheckObjPost(req, "o")
	xda = req.Header.Get("X-Delete-At")
	require.True(t, xda == fmt.Sprintf("%d", time.Now().Unix()+5) || xda == fmt.Sprintf("%d", time.Now().Unix()+4))
}

func TestTooBigHeader(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X", strings.Repeat("X", MAX_HEADER_SIZE+1))
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestUnnamedMeta(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Object-Meta-", "X")
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestUnderscoreMeta(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Object-Meta-My_Underscore_Key", "X")
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusOK, status)
	fmt.Printf("header: %+v\n", req.Header)
	require.Equal(t, []string{"X"}, req.Header["X-Object-Meta-My-Underscore-Key"])
	_, ok := req.Header["X-Object-Meta-My_Underscore_Key"]
	require.Equal(t, false, ok)
}

func TestLongMetaName(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set(fmt.Sprintf("X-Object-Meta-%s", strings.Repeat("X", MAX_META_NAME_LENGTH+1)), "X")
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestLongMetaValue(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Object-Meta-Key", strings.Repeat("X", MAX_META_VALUE_LENGTH+1))
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestTooManyMetas(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	for i := 0; i < MAX_META_COUNT+1; i++ {
		req.Header.Set(fmt.Sprintf("X-Object-Meta-%d", i), "X")
	}
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestTooMuchMeta(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	for i := 0; i < MAX_META_COUNT; i++ {
		req.Header.Set(fmt.Sprintf("X-Object-Meta-%d", i), strings.Repeat("X", MAX_META_VALUE_LENGTH))
	}
	status, _ := CheckMetadata(req, "Object")
	require.Equal(t, http.StatusBadRequest, status)
}

func TestContainerNameTooLong(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c", nil)
	require.Nil(t, err)
	req.ContentLength = 1
	status, _ := CheckContainerPut(req, strings.Repeat("o", MAX_CONTAINER_NAME_LENGTH+1))
	require.Equal(t, http.StatusBadRequest, status)
}
