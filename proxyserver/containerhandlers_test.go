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

package proxyserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"
	"github.com/troubling/hummingbird/proxyserver/middleware"
)

func TestOptionsHandler(t *testing.T) {
	p := ProxyServer{}
	theHeader := make(http.Header, 1)
	theStatus := map[string]int{"S": 1}
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader, StatusMap: theStatus}

	r := httptest.NewRequest("OPTIONS", "/v1/a/c/o", nil)
	ctx := &middleware.ProxyContext{
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{
			"container/a/c": {Metadata: map[string]string{"Access-Control-Allow-Origin": "there.com"}},
		}, zap.NewNop()),
	}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c"})
	p.OptionsHandler(fakeWriter, r)
	require.True(t, strings.HasPrefix(theHeader.Get("Allow"), "HEAD, GET"))
	r.Header.Set("Origin", "hey.com")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, fakeWriter.StatusMap["S"], 401)

	r.Header.Set("Origin", "there.com")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, fakeWriter.StatusMap["S"], 401)
	r.Header.Set("Access-Control-Request-Method", "GET")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, fakeWriter.StatusMap["S"], 200)
	r.Header.Set("Access-Control-Request-Method", "MOO")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, fakeWriter.StatusMap["S"], 401)

	r.Header.Set("Access-Control-Request-Method", "GET")
	r.Header.Set("Origin", "there.com")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "there.com")
}

func TestOptionsHandlerStar(t *testing.T) {
	p := ProxyServer{}
	theHeader := make(http.Header, 1)
	theStatus := map[string]int{"S": 1}
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader, StatusMap: theStatus}

	r := httptest.NewRequest("OPTIONS", "/v1/a/c/o", nil)
	ctx := &middleware.ProxyContext{
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{
			"container/a/c": {Metadata: map[string]string{"Access-Control-Allow-Origin": "*"}},
		}, zap.NewNop()),
	}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c"})
	p.OptionsHandler(fakeWriter, r)
	require.True(t, strings.HasPrefix(theHeader.Get("Allow"), "HEAD, GET"))
	r.Header.Set("Origin", "hey.com")
	r.Header.Set("Access-Control-Request-Method", "GET")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, fakeWriter.StatusMap["S"], 200)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "*")
}

func TestOptionsHandlerNotSetup(t *testing.T) {
	p := ProxyServer{}
	theHeader := make(http.Header, 1)
	theStatus := map[string]int{"S": 1}
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader, StatusMap: theStatus}

	r := httptest.NewRequest("OPTIONS", "/v1/a/c/o", nil)
	ctx := &middleware.ProxyContext{
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{
			"container/a/c": {Metadata: map[string]string{}},
		}, zap.NewNop()),
	}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c"})
	p.OptionsHandler(fakeWriter, r)
	require.True(t, strings.HasPrefix(theHeader.Get("Allow"), "HEAD, GET"))
	r.Header.Set("Origin", "hey.com")
	r.Header.Set("Access-Control-Request-Method", "GET")
	p.OptionsHandler(fakeWriter, r)
	require.Equal(t, fakeWriter.StatusMap["S"], 401)
	require.Equal(t, theHeader.Get("Access-Control-Allow-Origin"), "")
}
