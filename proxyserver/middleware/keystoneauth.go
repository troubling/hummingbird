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
	"net/http"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"go.uber.org/zap"
)

type keystoneAuth struct {
	resellerPrefixes  []string
	accountRules      map[string]map[string][]string
	resellerAdminRole string
	defaultDomainID   string
	next              http.Handler
}

func (ka *keystoneAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer ka.next.ServeHTTP(w, r)
	ctx := GetProxyContext(r)
	if ctx.Authorize != nil {
		return
	}
	pathParts, err := common.ParseProxyPath(r.URL.Path)
	if err != nil {
		return
	}
	if _, ok := ka.getAccountPrefix(pathParts["account"]); !ok {
		return
	}
	identityMap := extractIdentity(r)
	if len(identityMap) == 0 {
		ctx.Authorize = ka.authorizeAnonymous
		return
	}
	ctx.RemoteUsers = []string{identityMap["tenantName"]}
	ctx.Authorize = ka.authorize
	ctx.addSubrequestCopy(keystoneSubrequestCopy)
	userRoles := common.SliceFromCSV(identityMap["roles"])
	for _, r := range userRoles {
		if ka.resellerAdminRole == strings.ToLower(r) {
			ctx.ResellerRequest = true
			break
		}
	}
}

func (ka *keystoneAuth) accountMatchesTenant(account string, tenantID string) bool {
	for _, prefix := range ka.resellerPrefixes {
		if fmt.Sprintf("%s%s", prefix, tenantID) == account {
			return true
		}
	}
	return false
}

func (ka *keystoneAuth) getProjectDomainID(r *http.Request, account string) string {
	ctx := GetProxyContext(r)
	ai, err := ctx.GetAccountInfo(account)
	if err != nil {
		return "" // TODO: I assume this is what we want here
	}
	return ai.SysMetadata["Project-Domain-Id"]
}

func (ka *keystoneAuth) setProjectDomainID(r *http.Request, pathParts map[string]string, identityMap map[string]string) {
	for k := range r.Header {
		if k == "X-Account-Sysmeta-Project-Domain-Id" {
			return
		}
	}
	if pathParts["object"] != "" || (pathParts["container"] != "" && r.Method != "PUT") ||
		!common.StringInSlice(r.Method, []string{"PUT", "POST"}) {
		return
	}
	tenantID := identityMap["tenantID"]
	sysmetaID := ka.getProjectDomainID(r, pathParts["account"])
	reqID, newID := "", ""
	if ka.accountMatchesTenant(pathParts["account"], tenantID) {
		reqID = identityMap["projectDomainID"]
		newID = reqID
	}
	if sysmetaID == "" && reqID == ka.defaultDomainID {
		newID = reqID
	}
	if newID != "" {
		r.Header.Set("X-Account-Sysmeta-Project-Domain-Id", newID)
	}
}

func (ka *keystoneAuth) isNameAllowedinACL(r *http.Request, account string, identityMap map[string]string) bool {
	userDomainID := identityMap["userDomainID"]
	if userDomainID != "" && userDomainID != ka.defaultDomainID {
		return false
	}
	projectDomainID := identityMap["projectDomainID"]
	if projectDomainID != "" && projectDomainID != ka.defaultDomainID {
		return false
	}
	tenantID := identityMap["tenantID"]
	allow := false
	if ka.accountMatchesTenant(account, tenantID) {
		allow = true
	} else if common.StringInSlice(ka.getProjectDomainID(r, account), []string{ka.defaultDomainID, ""}) {
		allow = true
	}
	return allow
}

func (ka *keystoneAuth) authorizeCrossTenant(userID string, userName string,
	tenantID string, tenantName string, roles []string, allowNames bool) string {
	tenantMatch := []string{tenantID, "*"}
	userMatch := []string{userID, "*"}
	if allowNames {
		tenantMatch = append(tenantMatch, tenantName)
		userMatch = append(userMatch, userName)
	}
	for _, tenant := range tenantMatch {
		for _, user := range userMatch {
			s := fmt.Sprintf("%s:%s", tenant, user)
			if common.StringInSlice(s, roles) {
				return s
			}
		}
	}
	return ""
}

func (ka *keystoneAuth) authorize(r *http.Request) (bool, int) {
	identityMap := extractIdentity(r)
	ctx := GetProxyContext(r)
	if ctx == nil {
		return false, http.StatusUnauthorized
	}
	s := http.StatusUnauthorized
	if len(ctx.RemoteUsers) != 0 {
		s = http.StatusForbidden
	}
	tenantID := identityMap["tenantID"]
	tenantName := identityMap["tenantName"]
	userID := identityMap["userID"]
	userName := identityMap["userName"]

	referrers, roles := ParseACL(ctx.ACL)

	// allow OPTIONS requests to proceed as normal
	if r.Method == "OPTIONS" {
		return true, http.StatusOK
	}
	pathParts, err := common.ParseProxyPath(r.URL.Path)
	if err != nil {
		ctx.Logger.Error("Unable to parse URL", zap.Error(err))
		return false, s
	}

	ka.setProjectDomainID(r, pathParts, identityMap)
	userRoles := []string{}
	for _, userRole := range common.SliceFromCSV(identityMap["roles"]) {
		userRoles = append(userRoles, strings.ToLower(userRole))
	}
	userServiceRoles := []string{}
	for _, userServiceRole := range common.SliceFromCSV(identityMap["serviceRoles"]) {
		userServiceRoles = append(userServiceRoles, strings.ToLower(userServiceRole))
	}
	if common.StringInSlice(ka.resellerAdminRole, userRoles) {
		ctx.Logger.Debug("User has reseller admin authorization", zap.String("userid", tenantID))
		return true, http.StatusOK
	}

	if pathParts["container"] == "" && pathParts["object"] == "" &&
		r.Method == "DELETE" {
		ctx.Logger.Debug("User is not allowed to delete its own account",
			zap.String("tenantName", tenantName),
			zap.String("userName", userName))
		return false, s
	}
	matchedACL := ""
	if len(roles) > 0 {
		allowNames := ka.isNameAllowedinACL(r, pathParts["account"], identityMap)
		matchedACL = ka.authorizeCrossTenant(userID, userName, tenantID, tenantName, roles, allowNames)
	}
	if matchedACL != "" {
		ctx.Logger.Debug("user allowed in ACL authorizing", zap.String("user", matchedACL))
		return true, http.StatusOK
	}

	isAuthorized, authErr := AuthorizeUnconfirmedIdentity(r, pathParts["object"], referrers, roles)

	if isAuthorized {
		return true, http.StatusOK
	}

	if !ka.accountMatchesTenant(pathParts["account"], tenantID) {
		return false, s
	}
	accountPrefix, _ := ka.getAccountPrefix(pathParts["account"])
	operatorRoles := ka.accountRules[accountPrefix]["operator_roles"]
	haveOperatorRole := false
	for _, or := range operatorRoles {
		if common.StringInSlice(or, userRoles) {
			haveOperatorRole = true
			break
		}
	}
	serviceRoles := ka.accountRules[accountPrefix]["service_roles"]
	haveServiceRole := false
	for _, or := range serviceRoles {
		if common.StringInSlice(or, userServiceRoles) {
			haveServiceRole = true
			break
		}
	}
	allowed := false
	if haveOperatorRole && (len(serviceRoles) > 0 && haveServiceRole) {
		allowed = true
	} else if haveOperatorRole && len(serviceRoles) == 0 {
		allowed = true
	}
	if allowed {
		return true, http.StatusOK
	}
	if !isAuthorized && authErr == nil {
		return false, s
	}

	for _, role := range roles {
		if common.StringInSlice(role, userRoles) {
			return true, http.StatusOK
		}

	}
	return false, s
}

func (ka *keystoneAuth) getAccountPrefix(account string) (string, bool) {
	// Empty prefix matches everything, so try to match others first
	for _, prefix := range ka.resellerPrefixes {
		if prefix != "" && strings.HasPrefix(account, prefix) {
			return prefix, true
		}
	}
	for _, prefix := range ka.resellerPrefixes {
		if prefix == "" {
			return "", true
		}
	}
	return "", false
}

func (ka *keystoneAuth) authorizeAnonymous(r *http.Request) (bool, int) {
	ctx := GetProxyContext(r)
	pathParts, err := common.ParseProxyPath(r.URL.Path)
	s := http.StatusUnauthorized
	if len(ctx.RemoteUsers) != 0 {
		s = http.StatusForbidden
	}
	if err != nil {
		ctx.Logger.Error("Unable to parse URL", zap.Error(err))
		return false, s
	}
	// allow OPTIONS requests to proceed as normal
	if r.Method == "OPTIONS" {
		return true, http.StatusOK
	}
	isAuthorized := false
	if pathParts["account"] != "" {
		if prefix, ok := ka.getAccountPrefix(pathParts["account"]); ok {
			if common.StringInSlice(prefix, ka.resellerPrefixes) {
				isAuthorized = true
			}
		}
	}
	if !isAuthorized {
		return false, s
	}

	referrers, roles := ParseACL(ctx.ACL)
	isAuthorized, _ = AuthorizeUnconfirmedIdentity(r, pathParts["object"], referrers, roles)

	if !isAuthorized {
		return false, s
	}
	return true, http.StatusOK
}

func keystoneSubrequestCopy(dst, src *http.Request) {
	for _, h := range []string{"X-Identity-Status", "X-Service-Identity-Status", "X-User-Id", "X-User-Name", "X-Project-Id", "X-Project-Name", "X-Roles", "X-Service-Roles", "X-User-Domain-Id", "X-User-Domain-Name", "X-Project-Domain-Id", "X-Project-Domain-Name"} {
		if v := src.Header.Get(h); v != "" {
			dst.Header.Set(h, v)
		} else {
			delete(dst.Header, h)
		}
	}
}

func extractIdentity(r *http.Request) map[string]string {
	identity := make(map[string]string)
	if r.Header.Get("X-Identity-Status") != "Confirmed" ||
		!common.StringInSlice(r.Header.Get("X-Service-Identity-Status"), []string{"Confirmed", ""}) {
		return identity
	}

	identity["userID"] = r.Header.Get("X-User-Id")
	identity["userName"] = r.Header.Get("X-User-Name")
	identity["tenantID"] = r.Header.Get("X-Project-Id")
	identity["tenantName"] = r.Header.Get("X-Project-Name")
	identity["roles"] = r.Header.Get("X-Roles")
	identity["serviceRoles"] = r.Header.Get("X-Service-Roles")
	identity["userDomainID"] = r.Header.Get("X-User-Domain-Id")
	identity["userDomainName"] = r.Header.Get("X-User-Domain-Name")
	identity["projectDomainID"] = r.Header.Get("X-Project-Domain-Id")
	identity["projectDomainName"] = r.Header.Get("X-Project-Domain-Name")

	return identity
}

func NewKeystoneAuth(config conf.Section) (func(http.Handler) http.Handler, error) {
	defaultRules := map[string][]string{"operator_roles": {"admin", "swiftoperator"},
		"service_roles": {}}
	resellerPrefixes, accountRules := conf.ReadResellerOptions(config, defaultRules)
	return func(next http.Handler) http.Handler {
		return &keystoneAuth{
			next:              next,
			resellerPrefixes:  resellerPrefixes,
			accountRules:      accountRules,
			resellerAdminRole: strings.ToLower(config.GetDefault("reseller_admin_role", "ResellerAdmin")),
			defaultDomainID:   config.GetDefault("default_domain_id", "default"),
		}
	}, nil
}
