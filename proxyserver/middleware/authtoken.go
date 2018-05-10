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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/tracing"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type identity struct {
	client          common.HTTPClient
	authURL         string
	authPlugin      string
	projectDomainID string
	userDomainID    string
	projectName     string
	userName        string
	password        string
	userAgent       string
}

type authToken struct {
	*identity
	next           http.Handler
	cacheDur       time.Duration
	preValidateDur time.Duration
	preValidations map[string]bool
	lock           sync.Mutex
}

var authHeaders = []string{"X-Identity-Status",
	"X-Service-Identity-Status",
	"X-Domain-Id",
	"X-Domain-Name",
	"X-Project-Id",
	"X-Project-Name",
	"X-Project-Domain-Id",
	"X-Project-Domain-Name",
	"X-User-Id",
	"X-User-Name",
	"X-User-Domain-Id",
	"X-User-Domain-Name",
	"X-Roles",
	"X-Service-Domain-Id",
	"X-Service-Domain-Name",
	"X-Service-Project-Id",
	"X-Service-Project-Name",
	"X-Service-Project-Domain-Id",
	"X-Service-Project-Domain-Name",
	"X-Service-User-Id",
	"X-Service-User-Name",
	"X-Service-User-Domain-Id",
	"X-Service-User-Domain-Name",
	"X-Service-Roles",
	"X-Service-Catalog",
	"X-Is-Admin-Project",
	//Deprecated Headers
	"X-Role",
	"X-User",
	"X-Tenant-Id",
	"X-Tenant-Name",
	"X-Tenant",
}

type domain struct {
	ID      string `json:"id"`
	Name    string `json:"name,omitempty"`
	Enabled bool   `json:"enabled,omitempty"`
}

type project struct {
	ID      string  `json:"id,omitempty"`
	Name    string  `json:"name,omitempty"`
	Enabled bool    `json:"enabled,omitempty"`
	Domain  *domain `json:"domain"`
}

type token struct {
	ExpiresAt     time.Time `json:"expires_at"`
	MemcacheTtlAt time.Time
	IssuedAt      time.Time `json:"issued_at"`
	Methods       []string
	User          struct {
		ID      string
		Name    string
		Email   string
		Enabled bool
		Domain  struct {
			ID   string
			Name string
		}
	}
	Project *project
	Domain  *domain
	Roles   *[]struct {
		ID   string
		Name string
	}
}

func (t token) Valid() bool {
	now := time.Now().Unix()
	return now < t.ExpiresAt.Unix()
}

func (t token) populateReqHeader(r *http.Request, headerPrefix string) {
	r.Header.Set(fmt.Sprintf("X%s-User-Id", headerPrefix), t.User.ID)
	r.Header.Set(fmt.Sprintf("X%s-User-Name", headerPrefix), t.User.Name)
	r.Header.Set(fmt.Sprintf("X%s-User-Domain-Id", headerPrefix), t.User.Domain.ID)
	r.Header.Set(fmt.Sprintf("X%s-User-Domain-Name", headerPrefix), t.User.Domain.Name)

	if project := t.Project; project != nil {
		r.Header.Set(fmt.Sprintf("X%s-Project-Name", headerPrefix), project.Name)
		r.Header.Set(fmt.Sprintf("X%s-Project-Id", headerPrefix), project.ID)
		r.Header.Set(fmt.Sprintf("X%s-Project-Domain-Name", headerPrefix), project.Domain.Name)
		r.Header.Set(fmt.Sprintf("X%s-Project-Domain-Id", headerPrefix), project.Domain.ID)
	}

	if domain := t.Domain; domain != nil {
		r.Header.Set(fmt.Sprintf("X%s-Domain-Id", headerPrefix), domain.ID)
		r.Header.Set(fmt.Sprintf("X%s-Domain-Name", headerPrefix), domain.Name)
	}

	if roles := t.Roles; roles != nil {
		roleNames := []string{}
		for _, role := range *t.Roles {
			roleNames = append(roleNames, role.Name)
		}
		r.Header.Set(fmt.Sprintf("X%s-Roles", headerPrefix), strings.Join(roleNames, ","))
	}
}

type identityReq struct {
	Auth struct {
		Identity struct {
			Methods  []string `json:"methods"`
			Password struct {
				User struct {
					Domain struct {
						ID string `json:"id"`
					} `json:"domain"`
					Name     string `json:"name"`
					Password string `json:"password"`
				} `json:"user"`
			} `json:"password"`
		} `json:"identity"`

		Scope struct {
			Project *project `json:"project"`
		} `json:"scope"`
	} `json:"auth"`
}

type identityResponse struct {
	Error *struct {
		Code    int
		Message string
		Title   string
	}
	Token *token
}

func (at *authToken) preValidate(ctx context.Context, proxyCtx *ProxyContext, authToken string) {
	at.lock.Lock()
	defer at.lock.Unlock()
	_, ok := at.preValidations[authToken]
	if ok {
		return
	} else {
		at.preValidations[authToken] = true
	}
	go func() {
		at.validate(ctx, proxyCtx, authToken)
		at.lock.Lock()
		defer at.lock.Unlock()
		delete(at.preValidations, authToken)
	}()
}

func (at *authToken) fetchAndValidateToken(ctx context.Context, proxyCtx *ProxyContext, authToken string) (*token, bool) {
	if proxyCtx == nil {
		return nil, false
	}
	var cachedToken token
	if err := proxyCtx.Cache.GetStructured(authToken, &cachedToken); err == nil {
		if at.preValidateDur > 0 && !cachedToken.MemcacheTtlAt.IsZero() {
			invalidateEarlyTime := time.Now().Add(at.preValidateDur)
			if cachedToken.MemcacheTtlAt.Before(invalidateEarlyTime) {
				at.preValidate(ctx, proxyCtx, authToken)
			}
		}
		proxyCtx.Logger.Debug("Found cache token",
			zap.String("token", authToken))
		return &cachedToken, true
	}
	return at.validate(ctx, proxyCtx, authToken)
}

func (at *authToken) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proxyCtx := GetProxyContext(r)
	if proxyCtx.Authorize != nil {
		at.next.ServeHTTP(w, r)
		return
	}
	removeAuthHeaders(r)
	r.Header.Set("X-Identity-Status", "Invalid")
	serviceAuthToken := r.Header.Get("X-Service-Token")
	if serviceAuthToken != "" {
		serviceToken, serviceTokenValid := at.fetchAndValidateToken(r.Context(), proxyCtx, serviceAuthToken)
		if serviceToken != nil && serviceTokenValid {
			r.Header.Set("X-Service-Identity-Status", "Confirmed")
			serviceToken.populateReqHeader(r, "-Service")
		} else {
			r.Header.Set("X-Service-Identity-Status", "Invalid")
		}
	}

	userAuthToken := r.Header.Get("X-Auth-Token")
	if userAuthToken == "" {
		userAuthToken = r.Header.Get("X-Storage-Token")
	}
	if userAuthToken != "" {
		userToken, userTokenValid := at.fetchAndValidateToken(r.Context(), proxyCtx, userAuthToken)
		if userToken != nil && userTokenValid {
			r.Header.Set("X-Identity-Status", "Confirmed")
			userToken.populateReqHeader(r, "")
		}
	}
	at.next.ServeHTTP(w, r)
}

func (at *authToken) validate(ctx context.Context, proxyCtx *ProxyContext, authToken string) (*token, bool) {
	tok, err := at.doValidate(ctx, authToken)
	if err != nil {
		proxyCtx.Logger.Debug("Failed to validate token", zap.Error(err))
		return nil, false
	}

	if tok != nil {
		ttl := at.cacheDur
		if expiresIn := tok.ExpiresAt.Sub(time.Now()); expiresIn < ttl && expiresIn > 0 {
			ttl = expiresIn
		}
		tok.MemcacheTtlAt = time.Now().Add(ttl)
		proxyCtx.Cache.Set(authToken, *tok, int(ttl/time.Second))
		return tok, true
	}
	return nil, false
}

func (at *authToken) doValidate(ctx context.Context, token string) (*token, error) {
	if !strings.HasSuffix(at.authURL, "/") {
		at.authURL += "/"
	}
	req, err := http.NewRequest("GET", at.authURL+"v3/auth/tokens?nocatalog", nil)
	if err != nil {
		return nil, err
	}
	serverAuthToken, err := at.serverAuth(ctx)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Auth-Token", serverAuthToken)
	req.Header.Set("X-Subject-Token", token)
	req.Header.Set("User-Agent", at.userAgent)
	req = req.WithContext(ctx)
	r, err := at.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	if r.StatusCode >= 400 {
		return nil, errors.New(r.Status)
	}

	var resp identityResponse
	if err = json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nil, err
	}

	if e := resp.Error; e != nil {
		return nil, fmt.Errorf("%s : %s", r.Status, e.Message)
	}
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", r.Status)
	}
	if resp.Token == nil {
		return nil, errors.New("Response didn't contain token context")
	}
	if !resp.Token.Valid() {
		return nil, errors.New("Returned token is not valid")

	}
	return resp.Token, nil
}

// serverAuth return the X-Auth-Token to use or an error.
func (at *authToken) serverAuth(ctx context.Context) (string, error) {
	authReq := &identityReq{}
	authReq.Auth.Identity.Methods = []string{at.authPlugin}
	authReq.Auth.Identity.Password.User.Domain.ID = at.userDomainID
	authReq.Auth.Identity.Password.User.Name = at.userName
	authReq.Auth.Identity.Password.User.Password = at.password
	authReq.Auth.Scope.Project = &project{Domain: &domain{ID: at.projectDomainID}, Name: at.projectName}
	authReqBody, err := json.Marshal(authReq)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", at.authURL+"v3/auth/tokens", bytes.NewBuffer(authReqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)
	resp, err := at.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return "", fmt.Errorf("server auth token request gave status %d", resp.StatusCode)
	}
	rv := resp.Header.Get("X-Subject-Token")
	return rv, nil
}

func removeAuthHeaders(r *http.Request) {
	for _, header := range authHeaders {
		r.Header.Del(header)
	}
}

func NewAuthToken(section conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler {
		tokenCacheDur := time.Duration(int(section.GetInt("token_cache_time", 300))) * time.Second
		c := &http.Client{
			Timeout: 5 * time.Second,
		}
		authTokenMiddleware := &authToken{
			next:           next,
			cacheDur:       tokenCacheDur,
			preValidateDur: (tokenCacheDur / 10),
			preValidations: make(map[string]bool),
			identity: &identity{authURL: section.GetDefault("auth_uri", "http://127.0.0.1:5000/"),
				authPlugin:      section.GetDefault("auth_plugin", "password"),
				projectDomainID: section.GetDefault("project_domain_id", "default"),
				userDomainID:    section.GetDefault("user_domain_id", "default"),
				projectName:     section.GetDefault("project_name", "service"),
				userName:        section.GetDefault("username", "swift"),
				password:        section.GetDefault("password", "password"),
				userAgent:       section.GetDefault("user_agent", "hummingbird-keystone-middleware/1.0"),
				client:          c},
		}
		if section.GetConfig().HasSection("tracing") {
			clientTracer, _, err := tracing.Init("proxy-keystone-client", zap.NewNop(), section.GetConfig().GetSection("tracing"))
			if err == nil {
				enableHTTPTrace := section.GetConfig().GetBool("tracing", "enable_httptrace", true)
				authTokenMiddleware.client, err = client.NewTracingClient(clientTracer, c, enableHTTPTrace)
				if err != nil { // In case of error revert to normal http client
					authTokenMiddleware.client = c
				}
			}
		}
		return authTokenMiddleware
	}, nil
}
