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
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common/test"
)

func TestHandleCorsStar(t *testing.T) {
	theHeader := make(http.Header, 1)
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader}

	c := &cors{
		ci: &client.ContainerInfo{Metadata: map[string]string{
			"Access-Control-Allow-Origin": "*"}}}

	status := c.HandleCors(fakeWriter, 200)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "")
	require.Equal(t, status, 200)
	c.origin = "hey.com"
	c.HandleCors(fakeWriter, 200)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "*")
	require.True(t, strings.Index(theHeader.Get("Access-Control-Expose-Headers"), "etag") >= 0)
	require.Equal(t, status, 200)
}

func TestHandleCorsSpec(t *testing.T) {
	theHeader := make(http.Header, 1)
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader}

	c := &cors{
		origin: "hey.com",
		ci: &client.ContainerInfo{Metadata: map[string]string{
			"Access-Control-Allow-Origin":   "there.com",
			"Access-Control-Expose-Headers": "a b"}}}

	c.HandleCors(fakeWriter, 200)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "")
	require.Equal(t, theHeader.Get("Access-Control-Expose-Headers"), "")

	c.origin = "there.com"
	status := c.HandleCors(fakeWriter, 200)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "there.com")
	require.True(t, strings.Index(theHeader.Get("Access-Control-Expose-Headers"), "a, b") >= 0)
	require.Equal(t, status, 200)
}
