//  Copyright (c) 2015-2017 Rackspace
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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

type testUser struct {
	Account  string
	Username string
	Password string
	Roles    []string
	Url      string
}

type tempAuth struct {
	testUsers []testUser
	next      http.Handler
}

func (ta *tempAuth) login(account, user, key string) (string, string, error) {
	for _, tu := range ta.testUsers {
		if tu.Account == account && tu.Username == user && tu.Password == key {
			token := common.UUID()
			return token, tu.Url, nil
		}
	}
	return "", "", errors.New("User not found.")
}

type cachedAuth struct {
	Authenticated bool   `json:"authed"`
	User          string `json:"user"`
}

func (ta *tempAuth) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/auth/v1.0" {
		user := request.Header.Get("X-Auth-User")
		parts := strings.Split(user, ":")
		if len(parts) != 2 {
			srv.StandardResponse(writer, 400)
			return
		}
		account := parts[0]
		user = parts[1]
		password := request.Header.Get("X-Auth-Key")
		token, url, err := ta.login(account, user, password)
		if err != nil {
			srv.StandardResponse(writer, 401)
			return
		}
		if ctx := GetProxyContext(request); ctx != nil {
			ctx.Cache.Set("auth:"+token, &cachedAuth{Authenticated: true, User: user}, 3600)
			ctx.RemoteUser = user
		}
		writer.Header().Set("X-Storage-Token", token)
		writer.Header().Set("X-Auth-Token", token)
		if url != "" {
			writer.Header().Set("X-Storage-URL", url)
		} else {
			writer.Header().Set("X-Storage-URL", fmt.Sprintf("http://%s/v1/AUTH_%s", request.Host, account))
		}
		srv.StandardResponse(writer, 200)
		return
	} else if strings.HasPrefix(request.URL.Path, "/v1") || strings.HasPrefix(request.URL.Path, "/V1") {
		token := request.Header.Get("X-Auth-Token")
		ctx := GetProxyContext(request)
		if ctx.Authorize == nil {
			var authed cachedAuth
			if err := ctx.Cache.GetStructured("auth:"+token, &authed); err != nil {
				srv.StandardResponse(writer, 401)
				return
			}
			ctx.RemoteUser = authed.User
			ctx.Authorize = func(r *http.Request) bool {
				return authed.Authenticated
			}
		}
	}
	ta.next.ServeHTTP(writer, request)
}

func NewTempAuth(config conf.Section) (func(http.Handler) http.Handler, error) {
	users := []testUser{}
	for key, val := range config.Section {
		keyparts := strings.Split(key, "_")
		valparts := strings.Fields(val)
		if len(keyparts) != 3 || keyparts[0] != "user" {
			continue
		}
		vallen := len(valparts)
		if vallen < 1 {
			continue
		}
		url := ""
		groups := []string{}
		if vallen > 1 {
			urlSpot := 0
			s := valparts[vallen-1]
			if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
				urlSpot = 1
				url = s
			}
			for _, group := range valparts[1 : vallen-urlSpot] {
				groups = append(groups, group)
			}
		}
		user := testUser{keyparts[1], keyparts[2], valparts[0], groups, url}
		users = append(users, user)
	}
	RegisterInfo("tempauth", map[string]interface{}{"account_acls": true})
	return func(next http.Handler) http.Handler {
		return &tempAuth{
			next:      next,
			testUsers: users,
		}
	}, nil
}
