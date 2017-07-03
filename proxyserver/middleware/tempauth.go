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
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

type testUser struct {
	Account   string
	Username  string
	Password  string
	Roles     []string
	Url       string
	AccountID string
}

type tempAuth struct {
	testUsers    []testUser
	resellers    []string
	reseller     string
	accountRules map[string]map[string][]string
	next         http.Handler
}

func (ta *tempAuth) getUser(account, user, key string) *testUser {
	for _, tu := range ta.testUsers {
		if tu.Account == account && tu.Username == user && tu.Password == key {
			return &tu
		}
	}
	return nil
}

type cachedAuth struct {
	Groups  []string
	Expires int64
}

func (ta *tempAuth) getUserGroups(tu *testUser) []string {
	groups := []string{tu.Account, fmt.Sprintf("%s:%s", tu.Account, tu.Username)}
	isAdmin := false
	for _, r := range tu.Roles {
		if r == ".admin" {
			isAdmin = true
		} else {
			groups = append(groups, r)
		}
	}
	if isAdmin {
		for _, r := range ta.resellers {
			groups = append(groups, r+tu.Account)
		}
		if tu.AccountID != "" && !common.StringInSlice(tu.AccountID, groups) {
			groups = append(groups, tu.AccountID)
		}
	}
	return groups
}

func (ta *tempAuth) handleGetToken(writer http.ResponseWriter, request *http.Request) {

	if request.Method != "GET" {
		srv.StandardResponse(writer, 400)
		return
	}
	user := request.Header.Get("X-Auth-User")
	parts := strings.Split(user, ":")
	if len(parts) != 2 {
		srv.StandardResponse(writer, 401)
		return
	}
	account := parts[0]
	user = parts[1]
	password := request.Header.Get("X-Auth-Key")
	tUser := ta.getUser(account, user, password)
	if tUser == nil {
		srv.StandardResponse(writer, 401)
		return
	}
	var token string
	ctx := GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	var prevToken string
	userGroups := ta.getUserGroups(tUser)
	if err := ctx.Cache.GetStructured("authuser:"+user, &prevToken); err == nil {
		var ca cachedAuth
		if err = ctx.Cache.GetStructured("auth:"+prevToken, &ca); err == nil {
			if ca.Expires > time.Now().Unix() && len(userGroups) == len(ca.Groups) {
				eq := true
				for i, r := range userGroups {
					if r != ca.Groups[i] {
						eq = false
					}
				}
				if eq {
					token = prevToken
				}
			}
		}
	}
	if token == "" {
		token = ta.reseller + common.UUID()
		now := time.Now().Unix()
		ctx.Cache.Set("auth:"+token, &cachedAuth{Expires: now + 86400, Groups: userGroups}, 86400)
		ctx.Cache.Set("authuser:"+user, &token, 86400)
	}

	ctx.RemoteUsers = []string{user}
	writer.Header().Set("X-Storage-Token", token)
	writer.Header().Set("X-Auth-Token", token)
	if tUser.Url != "" {
		writer.Header().Set("X-Storage-URL", tUser.Url)
	} else {
		writer.Header().Set("X-Storage-URL", fmt.Sprintf("http://%s/v1/%s%s", request.Host, ta.reseller, account))
	}
	srv.StandardResponse(writer, 200)

}

func (ta *tempAuth) getReseller(account string) (string, bool) {
	// dosn't handle empty resellers yet
	if strings.HasPrefix(account, ta.reseller) {
		return ta.reseller, true
	}
	for _, r := range ta.resellers {
		if strings.HasPrefix(account, r) {
			return r, true
		}
	}
	return "", false
}

func (ta *tempAuth) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/auth/v1.0" {
		ta.handleGetToken(writer, request)
		return
	} else if strings.HasPrefix(request.URL.Path, "/v1") || strings.HasPrefix(request.URL.Path, "/V1") {
		token := request.Header.Get("X-Auth-Token")
		if token == "" {
			token = request.Header.Get("X-Storage-Token")
		}
		ctx := GetProxyContext(request)
		if ctx == nil {
			ta.next.ServeHTTP(writer, request)
			return
		}
		if ctx.Authorize == nil {
			pathParts, err := common.ParseProxyPath(request.URL.Path)
			if err != nil {
				ta.next.ServeHTTP(writer, request)
				return
			}
			if token != "" && strings.HasPrefix(token, ta.reseller) {
				if curReseller, ok := ta.getReseller(pathParts["account"]); ok && curReseller == ta.reseller {
					var ca cachedAuth
					if err := ctx.Cache.GetStructured("auth:"+token, &ca); err != nil {
						ctx.Authorize = func(r *http.Request) (bool, int) {
							return false, http.StatusUnauthorized
						}
					} else {
						if st := request.Header.Get("X-Service-Token"); st != "" {
							var caSt cachedAuth
							if err := ctx.Cache.GetStructured("auth:"+st, &caSt); err == nil {
								for _, g := range caSt.Groups {
									ca.Groups = append(ca.Groups, g)
								}
							}
						}
						ctx.RemoteUsers = ca.Groups
						ctx.Authorize = ta.authorize
					}
				} else if ok {
					ctx.Authorize = ta.authorize
				}
			} else {
				if _, ok := ta.getReseller(pathParts["account"]); ok {
					// i do handle the req's reseller auth. allow anonymous authorize
					ctx.Authorize = ta.authorize
				}
			}
		}
	}
	ta.next.ServeHTTP(writer, request)
}

func (ta *tempAuth) authorize(r *http.Request) (bool, int) {
	pathParts, err := common.ParseProxyPath(r.URL.Path)
	if err != nil {
		return false, http.StatusNotFound
	}
	if r.Method == "OPTIONS" {
		return true, http.StatusOK
	}
	reseller, ok := ta.getReseller(pathParts["account"])
	if !ok {
		return false, http.StatusUnauthorized
	}
	ctx := GetProxyContext(r)
	if ctx == nil {
		return false, http.StatusUnauthorized
	}
	s := http.StatusUnauthorized
	if len(ctx.RemoteUsers) != 0 {
		s = http.StatusForbidden
	}
	if common.StringInSlice(".reseller_admin", ctx.RemoteUsers) &&
		!common.StringInSlice(pathParts["account"], ta.resellers) &&
		!strings.HasPrefix(pathParts["account"], ".") {
		ctx.StorageOwner = true
		return true, http.StatusOK
	}
	if common.StringInSlice(pathParts["account"], ctx.RemoteUsers) &&
		(pathParts["container"] != "" || !common.StringInSlice(r.Method, []string{"PUT", "DELETE"})) {
		// The user is admin for the account and is not trying to do an account DELETE or PUT
		var requireGroups []string
		if rule, ok := ta.accountRules[reseller]; ok {
			requireGroups = rule["require_group"]
		}
		if len(requireGroups) == 0 {
			ctx.StorageOwner = true
			return true, http.StatusOK
		} else {
			for _, rg := range requireGroups {
				if common.StringInSlice(rg, ctx.RemoteUsers) {
					ctx.StorageOwner = true
					return true, http.StatusOK
				}
			}
		}
	}
	referrers, roles := ParseACL(ctx.ACL)
	if auth, _ := AuthorizeUnconfirmedIdentity(r, pathParts["object"], referrers, roles); auth {
		return true, http.StatusOK
	}
	for _, ru := range ctx.RemoteUsers {
		if common.StringInSlice(ru, roles) {
			return true, http.StatusOK
		}
	}
	return false, s
}

func NewTempAuth(config conf.Section) (func(http.Handler) http.Handler, error) {
	users := []testUser{}
	defaultRules := map[string][]string{"require_group": {}}
	resellerPrefixes, accountRules := conf.ReadResellerOptions(config, defaultRules)
	reseller := resellerPrefixes[0]
	for key, val := range config.Section {
		keyparts := strings.Split(key, "_")
		valparts := strings.Fields(val)
		if len(keyparts) != 3 || keyparts[0] != "user" {
			continue
		}
		account, user := keyparts[1], keyparts[2]
		vallen := len(valparts)
		if vallen < 1 {
			continue
		}
		url := ""
		accountID := reseller + account
		groups := []string{}
		if vallen > 1 {
			urlSpot := 0
			s := valparts[vallen-1]
			if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
				urlSpot = 1
				url = s
				urlParts := strings.Split(url, "/")
				accountID = urlParts[len(urlParts)-1]
			}
			for _, group := range valparts[1 : vallen-urlSpot] {
				groups = append(groups, group)
			}
		}

		users = append(users, testUser{account, user, valparts[0], groups, url, accountID})
	}
	RegisterInfo("tempauth", map[string]interface{}{"account_acls": false})
	return func(next http.Handler) http.Handler {
		return &tempAuth{
			next:         next,
			testUsers:    users,
			resellers:    resellerPrefixes,
			reseller:     reseller,
			accountRules: accountRules,
		}
	}, nil
}
