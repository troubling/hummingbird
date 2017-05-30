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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

var (
	autoCreateAccounts = true // TODO; figure out how to plumb this in as a config
	serverInfo         = make(map[string]interface{})
	sil                sync.Mutex
	excludeHeaders     = []string{
		"X-Account-Sysmeta-",
		"X-Container-Sysmeta-",
		"X-Object-Sysmeta-",
		"X-Object-Transient-Sysmeta-",
		"X-Backend-",
	}
)

func RegisterInfo(name string, data interface{}) {
	sil.Lock()
	defer sil.Unlock()
	serverInfo[name] = data
}

func serverInfoDump() ([]byte, error) {
	sil.Lock()
	defer sil.Unlock()
	data, err := json.Marshal(serverInfo)
	return data, err
}

type AccountInfo struct {
	ContainerCount int64
	ObjectCount    int64
	ObjectBytes    int64
	Metadata       map[string]string
	SysMetadata    map[string]string
}

type AuthorizeFunc func(r *http.Request) bool

type ProxyContextMiddleware struct {
	next       http.Handler
	log        srv.LowLevelLogger
	Cache      ring.MemcacheRing
	policyList conf.PolicyList
	httpClient *http.Client
}

type proxyWriter struct {
	http.ResponseWriter
	Status          int
	ResponseStarted bool
}

func (w *proxyWriter) WriteHeader(status int) {
	// strip out any bad headers before calling real WriteHeader
	for k := range w.Header() {
		for _, ex := range excludeHeaders {
			if strings.HasPrefix(k, ex) {
				delete(w.Header(), k)
			}
		}
	}
	w.Status = status
	w.ResponseStarted = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *proxyWriter) Response() (bool, int) {
	return w.ResponseStarted, w.Status
}

type ProxyContext struct {
	*ProxyContextMiddleware
	C                  client.ProxyClient
	Authorize          AuthorizeFunc
	Logger             srv.LowLevelLogger
	containerInfoCache map[string]*client.ContainerInfo
	accountInfoCache   map[string]*AccountInfo
	capWriter          *proxyWriter
	httpClient         *http.Client
}

func GetProxyContext(r *http.Request) *ProxyContext {
	if rv := r.Context().Value("proxycontext"); rv != nil {
		return rv.(*ProxyContext)
	}
	return nil
}

func (ctx *ProxyContext) Response() (bool, int) {
	return ctx.capWriter.Response()
}

func (ctx *ProxyContext) DefaultPolicyIndex() int {
	return ctx.policyList.Default()
}

func (ctx *ProxyContext) PolicyByName(name string) *conf.Policy {
	for _, v := range ctx.policyList {
		if v.Name == name {
			return v
		}
	}
	return nil
}

func (ctx *ProxyContext) GetContainerInfo(account, container string) *client.ContainerInfo {
	var err error
	key := fmt.Sprintf("container/%s/%s", account, container)
	ci := ctx.containerInfoCache[key]
	if ci == nil {
		if err := ctx.Cache.GetStructured(key, &ci); err != nil {
			ci = nil
		}
	}
	if ci == nil {
		headers, code := ctx.C.HeadContainer(account, container, nil)
		if code/100 != 2 {
			return nil
		}
		ci = &client.ContainerInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		if ci.ObjectCount, err = strconv.ParseInt(headers.Get("X-Container-Object-Count"), 10, 64); err != nil {
			return nil
		}
		if ci.ObjectBytes, err = strconv.ParseInt(headers.Get("X-Container-Bytes-Used"), 10, 64); err != nil {
			return nil
		}
		if ci.StoragePolicyIndex, err = strconv.Atoi(headers.Get("X-Backend-Storage-Policy-Index")); err != nil {
			return nil
		}
		for k := range headers {
			if strings.HasPrefix(k, "X-Container-Meta-") {
				ci.Metadata[k[17:]] = headers.Get(k)
			} else if strings.HasPrefix(k, "X-Container-Sysmeta-") {
				ci.SysMetadata[k[20:]] = headers.Get(k)
			}
		}
		ctx.Cache.Set(key, ci, 30)
	}
	return ci
}

func (ctx *ProxyContext) HTTPClient() *http.Client {
	return ctx.httpClient
}

func (ctx *ProxyContext) InvalidateContainerInfo(account, container string) {
	key := fmt.Sprintf("container/%s/%s", account, container)
	delete(ctx.containerInfoCache, key)
	ctx.Cache.Delete(key)
}

func getPathParts(request *http.Request) (bool, string, string, string) {
	parts := strings.SplitN(request.URL.Path, "/", 5)
	apiRequest := len(parts) > 0 && parts[1] == "v1"
	if len(parts) == 5 {
		return apiRequest, parts[2], parts[3], parts[4]
	} else if len(parts) == 4 {
		return apiRequest, parts[2], parts[3], ""
	} else if len(parts) == 3 {
		return apiRequest, parts[2], "", ""
	}
	return apiRequest, "", "", ""
}

func (ctx *ProxyContext) GetAccountInfo(account string) *AccountInfo {
	var err error
	key := fmt.Sprintf("account/%s", account)
	ai := ctx.accountInfoCache[key]
	if ai == nil {
		if err := ctx.Cache.GetStructured(key, &ai); err != nil {
			ai = nil
		}
	}
	if ai == nil {
		headers, code := ctx.C.HeadAccount(account, nil)
		if code == 404 && autoCreateAccounts {
			ctx.C.PutAccount(account, http.Header{"X-Timestamp": []string{common.GetTimestamp()}})
			headers, code = ctx.C.HeadAccount(account, nil)
		}
		if code/100 != 2 {
			return nil
		}
		ai = &AccountInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		if ai.ContainerCount, err = strconv.ParseInt(headers.Get("X-Account-Container-Count"), 10, 64); err != nil {
			return nil
		}
		if ai.ObjectCount, err = strconv.ParseInt(headers.Get("X-Account-Object-Count"), 10, 64); err != nil {
			return nil
		}
		if ai.ObjectBytes, err = strconv.ParseInt(headers.Get("X-Account-Bytes-Used"), 10, 64); err != nil {
			return nil
		}
		for k := range headers {
			if strings.HasPrefix(k, "X-Account-Meta-") {
				ai.Metadata[k[15:]] = headers.Get(k)
			} else if strings.HasPrefix(k, "X-Account-Sysmeta-") {
				ai.SysMetadata[k[18:]] = headers.Get(k)
			}
		}
		ctx.Cache.Set(key, ai, 30)
	}
	return ai
}

func (ctx *ProxyContext) InvalidateAccountInfo(account string) {
	key := fmt.Sprintf("account/%s", account)
	delete(ctx.accountInfoCache, key)
	ctx.Cache.Delete(key)
}

func (ctx *ProxyContext) NewSubRequest(request *http.Request, method string, url string, body io.Reader) (*http.Request, error) {
	subRequest, err := http.NewRequest(method, url, body)
	if err != nil {
		ctx = GetProxyContext(request)
		ctx.Logger.Error("Couldn't create http.Request", zap.Error(err))
		return nil, err
	}
	copyKeys := []string{"X-Auth-Token", "X-Trans-Id"}
	for _, key := range copyKeys {
		subRequest.Header.Set(key, request.Header.Get(key))
	}
	subRequest = subRequest.WithContext(context.WithValue(subRequest.Context(), "proxycontext", ctx))
	return subRequest, nil
}

func (ctx *ProxyContext) Subrequest(method, path string, body io.Reader, writer http.ResponseWriter) (*http.Request, error) {
	req, err := http.NewRequest(method, path, body)
	if err != nil {
		return nil, err
	}
	newctx := &ProxyContext{
		ProxyContextMiddleware: ctx.ProxyContextMiddleware,
		Authorize:              ctx.Authorize,
		Logger:                 ctx.Logger,
		containerInfoCache:     ctx.containerInfoCache,
		accountInfoCache:       ctx.accountInfoCache,
		capWriter:              &proxyWriter{ResponseWriter: writer, Status: 501},
	}
	newctx.capWriter.Header().Set("X-Trans-Id", ctx.capWriter.Header().Get("X-Trans-Id"))
	return req.WithContext(context.WithValue(req.Context(), "proxycontext", newctx)), nil
}

func (m *ProxyContextMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !srv.ValidateRequest(request) {
		srv.StandardResponse(writer, 400)
		return
	}

	if request.URL.Path == "/info" && request.Method == "GET" {
		if data, err := serverInfoDump(); err != nil {
			srv.StandardResponse(writer, 500)
		} else {
			writer.WriteHeader(200)
			writer.Header().Set("Content-Type", "application/json")
			writer.Write(data)
		}
		return
	}

	for k := range request.Header {
		for _, ex := range excludeHeaders {
			if strings.HasPrefix(k, ex) || k == "X-Timestamp" {
				delete(request.Header, k)
			}
		}
	}

	newWriter := &proxyWriter{ResponseWriter: writer, Status: 501, ResponseStarted: false}
	transId := common.GetTransactionId()
	request.Header.Set("X-Trans-Id", transId)
	newWriter.Header().Set("X-Trans-Id", transId)
	request.Header.Set("X-Timestamp", common.GetTimestamp())
	logr := m.log.With(zap.String("txn", transId))
	ctx := &ProxyContext{
		ProxyContextMiddleware: m,
		Authorize:              nil,
		Logger:                 logr,
		containerInfoCache:     make(map[string]*client.ContainerInfo),
		accountInfoCache:       make(map[string]*AccountInfo),
		capWriter:              newWriter,
		httpClient:             m.httpClient,
	}
	var err error
	ctx.C, err = client.NewProxyDirectClient(ctx)
	if err != nil {
		srv.StandardResponse(writer, 500)
		return
	}
	// we'll almost certainly need the AccountInfo and ContainerInfo for the current path, so pre-fetch them in parallel.
	apiRequest, account, container, _ := getPathParts(request)
	if apiRequest && account != "" {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			if ctx.GetAccountInfo(account) == nil {
				hdr := http.Header{
					"X-Timestamp": []string{common.GetTimestamp()},
				}
				ctx.C.PutAccount(account, hdr) // Auto-create the account if we can't get its info
			}
			wg.Done()
		}()
		if container != "" {
			wg.Add(1)
			go func() {
				ctx.GetContainerInfo(account, container)
				wg.Done()
			}()
		}
		wg.Wait()
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", ctx))
	m.next.ServeHTTP(newWriter, request)
}

func NewContext(mc ring.MemcacheRing, log srv.LowLevelLogger, policyList conf.PolicyList, httpClient *http.Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return &ProxyContextMiddleware{
			Cache:      mc,
			log:        log,
			next:       next,
			policyList: policyList,
			httpClient: httpClient,
		}
	}
}
