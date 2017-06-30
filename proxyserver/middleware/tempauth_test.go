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
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/test"
)

func TestGetUserGroups(t *testing.T) {
	ta := &tempAuth{
		reseller:  "AUTH_",
		resellers: []string{"AUTH_", "SERVICE_"},
	}

	tu := testUser{
		Account:  "test",
		Username: "tester",
		Password: "testing",
		Roles:    []string{".admin"}}

	groups := ta.getUserGroups(&tu)
	require.Equal(t, "test", groups[0])
	require.Equal(t, "test:tester", groups[1])
	require.Equal(t, "AUTH_test", groups[2])
	require.Equal(t, "SERVICE_test", groups[3])

	require.Equal(t, 4, len(groups))

	tu = testUser{
		Account:   "test",
		Username:  "tester",
		Password:  "testing",
		AccountID: "SERVICE_test",
		Roles:     []string{".admin"}}
	groups = ta.getUserGroups(&tu)
	require.Equal(t, 4, len(groups))

	tu = testUser{
		Account:   "test",
		Username:  "tester",
		Password:  "testing",
		AccountID: "MOO_test",
		Roles:     []string{".admin"}}
	groups = ta.getUserGroups(&tu)
	require.Equal(t, 5, len(groups))

	tu = testUser{
		Account:  "test",
		Username: "tester",
		Password: "testing",
	}

	groups = ta.getUserGroups(&tu)
	require.Equal(t, "test", groups[0])
	require.Equal(t, "test:tester", groups[1])
	require.Equal(t, 2, len(groups))
}

func TestHandleGetToken(t *testing.T) {
	theHeader := make(http.Header, 1)
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader, StatusMap: map[string]int{"a": 12}}

	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("stuff"))
	})
	fakeContext := NewFakeProxyContext(passthrough)
	fakeMr := &test.FakeMemcacheRing{}
	fakeContext.Cache = fakeMr

	authReq, err := http.NewRequest("GET", "/auth/v1.0", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	tu := testUser{
		Account:  "test",
		Username: "tester",
		Password: "testing",
		Roles:    []string{".admin"},
	}
	ta := &tempAuth{
		reseller:  "AUTH_",
		resellers: []string{"AUTH_", "SERVICE_"},
		next:      passthrough,
		testUsers: []testUser{tu},
	}
	authReq.Header.Set("X-Auth-User", "test:tester")
	authReq.Header.Set("X-Auth-Key", "testing")
	authReq.Host = "127.0.0.1"
	ta.handleGetToken(fakeWriter, authReq)
	require.True(t, strings.HasPrefix(theHeader.Get("X-Auth-Token"), "AUTH_"))
	require.Equal(t, "http://127.0.0.1/v1/AUTH_test", theHeader.Get("X-Storage-Url"))
	require.Equal(t, 200, fakeWriter.StatusMap["S"])

	cData := fakeMr.MockSetValues[0].(*cachedAuth)
	require.Equal(t, 4, len(cData.Groups))
	require.Equal(t, "test:tester", cData.Groups[1])
}

func TestHandleGetTokenFail(t *testing.T) {
	theHeader := make(http.Header, 1)
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader, StatusMap: map[string]int{"a": 12}}

	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("stuff"))
	})
	fakeContext := NewFakeProxyContext(passthrough)
	fakeMr := &test.FakeMemcacheRing{}
	fakeContext.Cache = fakeMr

	authReq, err := http.NewRequest("GET", "/auth/v1.0", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	tu := testUser{
		Account:  "test",
		Username: "tester",
		Password: "testing",
	}
	ta := &tempAuth{
		reseller:  "AUTH_",
		resellers: []string{"AUTH_", "SERVICE_"},
		next:      passthrough,
		testUsers: []testUser{tu},
	}
	authReq.Header.Set("X-Auth-User", "test:tester")
	authReq.Header.Set("X-Auth-Key", "testingggg")
	authReq.Host = "127.0.0.1"
	ta.handleGetToken(fakeWriter, authReq)
	require.Equal(t, "", theHeader.Get("X-Storage-Url"))
	require.Equal(t, 401, fakeWriter.StatusMap["S"])
}

func TestAuthorize(t *testing.T) {
	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("stuff"))
	})
	tu := testUser{
		Account:  "test",
		Username: "tester",
		Password: "testing",
	}
	ta := &tempAuth{
		reseller:  "AUTH_",
		resellers: []string{"AUTH_", "SERVICE_"},
		next:      passthrough,
		testUsers: []testUser{tu},
	}
	authReq, _ := http.NewRequest("GET", "//v1/AUTH_moo", nil)
	ok, st := ta.authorize(authReq)
	require.False(t, ok)
	require.Equal(t, 401, st)

	authReq, _ = http.NewRequest("OPTIONS", "/v1/AUTH_moo", nil)
	ok, st = ta.authorize(authReq)
	require.True(t, ok)
	require.Equal(t, 200, st)

	authReq, _ = http.NewRequest("GET", "/v1/MOO_moo", nil)
	ok, st = ta.authorize(authReq)
	require.False(t, ok)
	require.Equal(t, 401, st)

	fakeContext := NewFakeProxyContext(passthrough)
	authReq, _ = http.NewRequest("GET", "/v1/AUTH_test", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	fakeContext.RemoteUsers = []string{"AUTH_test"}
	ok, st = ta.authorize(authReq)
	require.True(t, ok)
	require.Equal(t, 200, st)

	fakeContext.RemoteUsers = []string{"SERVICE_test"}
	ok, st = ta.authorize(authReq)
	require.Equal(t, 403, st)

	fakeContext.RemoteUsers = []string{"AUTH_test"}
	authReq, _ = http.NewRequest("DELETE", "/v1/AUTH_test", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.Equal(t, 403, st)

	fakeContext.RemoteUsers = []string{".reseller_admin"}
	authReq, _ = http.NewRequest("GET", "/v1/SERVICE_test", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.Equal(t, 200, st)

	fakeContext = NewFakeProxyContext(passthrough)
	fakeContext.RemoteUsers = []string{"AUTH_test"}
	authReq, _ = http.NewRequest("GET", "/v1/AUTH_test/c/o", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.True(t, ok)
	require.Equal(t, 200, st)
	ta.accountRules = map[string]map[string][]string{"AUTH_": {"require_group": {"ops"}}}
	ok, st = ta.authorize(authReq)
	require.Equal(t, 403, st)

	fakeContext.RemoteUsers = []string{"AUTH_test", "ops"}
	authReq, _ = http.NewRequest("GET", "/v1/AUTH_test/c/o", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.Equal(t, 200, st)

}

func TestAuthorizeAcl(t *testing.T) {
	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("stuff"))
	})
	tu := testUser{
		Account:  "test",
		Username: "tester3",
		Password: "testing3",
	}
	ta := &tempAuth{
		reseller:  "AUTH_",
		resellers: []string{"AUTH_", "SERVICE_"},
		next:      passthrough,
		testUsers: []testUser{tu},
	}
	fakeContext := NewFakeProxyContext(passthrough)
	fakeContext.RemoteUsers = []string{"test", "test:tester3"}
	authReq, _ := http.NewRequest("GET", "/v1/AUTH_test/c/o", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st := ta.authorize(authReq)
	require.False(t, ok)
	require.Equal(t, 403, st)

	fakeContext.ACL = "test:tester3"
	authReq, _ = http.NewRequest("GET", "/v1/AUTH_test/c/o", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.Equal(t, 200, st)

	authReq, _ = http.NewRequest("GET", "/v1/AUTH_test/c/o", nil)
	fakeContext.ACL = ".r:*"
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.Equal(t, 200, st)

	authReq, _ = http.NewRequest("GET", "/v1/AUTH_test/c/", nil)
	fakeContext.ACL = ".r:*"
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	ok, st = ta.authorize(authReq)
	require.Equal(t, 403, st)
}

func TestServeHTTP(t *testing.T) {
	theHeader := make(http.Header, 1)
	fakeWriter := test.MockResponseWriter{SaveHeader: &theHeader, StatusMap: map[string]int{"a": 12}}

	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("stuff"))
	})
	fakeContext := NewFakeProxyContext(passthrough)
	//require.Equal(t, nil, fakeContext.Authorize)
	fakeMr := &test.FakeMemcacheRing{}
	fakeContext.Cache = fakeMr

	authReq, err := http.NewRequest("GET", "/v1/MOO_test", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	authReq.Header.Set("X-Auth-Token", "abcde")

	tu := testUser{
		Account:  "test",
		Username: "tester3",
		Password: "testing3",
	}
	ta := &tempAuth{
		reseller:  "AUTH_",
		resellers: []string{"AUTH_", "SERVICE_"},
		next:      passthrough,
		testUsers: []testUser{tu},
	}

	require.True(t, fakeContext.Authorize == nil)
	ta.ServeHTTP(fakeWriter, authReq)
	require.True(t, fakeContext.Authorize == nil)
	require.Equal(t, 0, len(fakeContext.RemoteUsers))

	authReq, err = http.NewRequest("GET", "/v1/AUTH_test", nil)
	authReq = authReq.WithContext(context.WithValue(authReq.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	authReq.Header.Set("X-Auth-Token", "abcde")
	ta.ServeHTTP(fakeWriter, authReq)
	require.False(t, fakeContext.Authorize == nil)
	require.Equal(t, 0, len(fakeContext.RemoteUsers))

	fakeContext.Authorize = nil
	authReq.Header.Set("X-Auth-Token", "AUTH_abcde")
	ta.ServeHTTP(fakeWriter, authReq)
	require.False(t, fakeContext.Authorize == nil)
	require.Equal(t, 0, len(fakeContext.RemoteUsers))

	fakeContext.Authorize = nil
	authReq.Header.Set("X-Auth-Token", "AUTH_abcde")
	ca := cachedAuth{Groups: []string{"hat"}, Expires: time.Now().Unix() + 100}
	caM, _ := json.Marshal(ca)
	fakeMr.MockGetStructured = map[string][]byte{"auth:AUTH_abcde": caM}
	ta.ServeHTTP(fakeWriter, authReq)
	ctx := GetProxyContext(authReq)
	require.False(t, ctx.Authorize == nil)
	require.Equal(t, "hat", fakeContext.RemoteUsers[0])
}
