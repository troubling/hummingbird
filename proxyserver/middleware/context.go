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
type subrequestCopy func(dst, src *http.Request)

type ProxyContextMiddleware struct {
	next              http.Handler
	log               srv.LowLevelLogger
	Cache             ring.MemcacheRing
	proxyDirectClient *client.ProxyDirectClient
}

type ProxyContext struct {
	*ProxyContextMiddleware
	C                client.ProxyClient
	Authorize        AuthorizeFunc
	RemoteUser       string
	ResellerRequest  bool
	ACL              string
	subrequestCopy   subrequestCopy
	Logger           srv.LowLevelLogger
	TxId             string
	responseSent     bool
	status           int
	accountInfoCache map[string]*AccountInfo
	depth            int
	Source           string
}

func GetProxyContext(r *http.Request) *ProxyContext {
	if rv := r.Context().Value("proxycontext"); rv != nil {
		return rv.(*ProxyContext)
	}
	return nil
}

func (ctx *ProxyContext) Response() (bool, int) {
	return ctx.responseSent, ctx.status
}

func (ctx *ProxyContext) addSubrequestCopy(f subrequestCopy) {
	if ctx.subrequestCopy == nil {
		ctx.subrequestCopy = f
		return
	}
	ca := ctx.subrequestCopy
	ctx.subrequestCopy = func(dst, src *http.Request) {
		ca(dst, src)
		f(dst, src)
	}
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
	key := fmt.Sprintf("account/%s", account)
	ai := ctx.accountInfoCache[key]
	if ai == nil {
		if err := ctx.Cache.GetStructured(key, &ai); err != nil {
			ai = nil
		}
	}
	if ai == nil {
		resp := ctx.C.HeadAccount(account, nil)
		if resp.StatusCode == http.StatusNotFound && autoCreateAccounts {
			resp.Body.Close()
			ctx.C.PutAccount(account, http.Header{"X-Timestamp": []string{common.GetTimestamp()}})
			resp = ctx.C.HeadAccount(account, nil)
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil
		}
		ai = &AccountInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		var err error
		if ai.ContainerCount, err = strconv.ParseInt(resp.Header.Get("X-Account-Container-Count"), 10, 64); err != nil {
			return nil
		}
		if ai.ObjectCount, err = strconv.ParseInt(resp.Header.Get("X-Account-Object-Count"), 10, 64); err != nil {
			return nil
		}
		if ai.ObjectBytes, err = strconv.ParseInt(resp.Header.Get("X-Account-Bytes-Used"), 10, 64); err != nil {
			return nil
		}
		for k := range resp.Header {
			if strings.HasPrefix(k, "X-Account-Meta-") {
				ai.Metadata[k[15:]] = resp.Header.Get(k)
			} else if strings.HasPrefix(k, "X-Account-Sysmeta-") {
				ai.SysMetadata[k[18:]] = resp.Header.Get(k)
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

func (ctx *ProxyContext) newSubrequest(method, urlStr string, body io.Reader, req *http.Request, source string) (*http.Request, error) {
	subreq, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	subctx := &ProxyContext{
		ProxyContextMiddleware: ctx.ProxyContextMiddleware,
		Authorize:              ctx.Authorize,
		RemoteUser:             ctx.RemoteUser,
		subrequestCopy:         ctx.subrequestCopy,
		Logger:                 ctx.Logger.With(zap.String("src", source)),
		C:                      ctx.C,
		TxId:                   ctx.TxId,
		accountInfoCache:       ctx.accountInfoCache,
		responseSent:           false,
		status:                 500,
		depth:                  ctx.depth + 1,
		Source:                 source,
	}
	subreq = subreq.WithContext(context.WithValue(req.Context(), "proxycontext", subctx))
	if subctx.subrequestCopy != nil {
		subctx.subrequestCopy(subreq, req)
	}
	if v := req.Header.Get("Referer"); v != "" {
		subreq.Header.Set("Referer", v)
	}
	subreq.Header.Set("X-Trans-Id", subctx.TxId)
	subreq.Header.Set("X-Timestamp", common.GetTimestamp())
	return subreq, nil
}

func (ctx *ProxyContext) serveHTTPSubrequest(writer http.ResponseWriter, subreq *http.Request) {
	subctx := GetProxyContext(subreq)
	// TODO: check subctx.depth
	subwriter := srv.NewCustomWriter(writer, func(w http.ResponseWriter, status int) int {
		subctx.responseSent = true
		subctx.status = status
		return status
	})
	subwriter.Header().Set("X-Trans-Id", subctx.TxId)
	ctx.next.ServeHTTP(subwriter, subreq)
}

func (m *ProxyContextMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !srv.ValidateRequest(writer, request) {
		return
	}

	if request.URL.Path == "/info" {
		if request.Method == "GET" {
			if data, err := serverInfoDump(); err != nil {
				srv.StandardResponse(writer, 500)
			} else {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(200)
				writer.Write(data)
			}
			return
		} else if request.Method == "OPTIONS" {
			writer.Header().Set("Allow", "HEAD, GET, OPTIONS")
			writer.WriteHeader(200)
			return
		} else if request.Method == "HEAD" {
			if _, err := serverInfoDump(); err != nil {
				srv.StandardResponse(writer, 500)
			} else {
				writer.WriteHeader(200)
			}
			return
		}
	}

	for k := range request.Header {
		for _, ex := range excludeHeaders {
			if strings.HasPrefix(k, ex) || k == "X-Timestamp" {
				delete(request.Header, k)
			}
		}
	}

	transId := common.GetTransactionId()
	request.Header.Set("X-Trans-Id", transId)
	writer.Header().Set("X-Trans-Id", transId)
	writer.Header().Set("X-Openstack-Request-Id", transId)
	request.Header.Set("X-Timestamp", common.GetTimestamp())
	logr := m.log.With(zap.String("txn", transId))
	ctx := &ProxyContext{
		ProxyContextMiddleware: m,
		Authorize:              nil,
		Logger:                 logr,
		TxId:                   transId,
		responseSent:           false,
		status:                 500,
		accountInfoCache:       make(map[string]*AccountInfo),
		C:                      client.NewProxyClient(m.proxyDirectClient, m.Cache, make(map[string]*client.ContainerInfo)),
	}
	// we'll almost certainly need the AccountInfo and ContainerInfo for the current path, so pre-fetch them in parallel.
	apiRequest, account, container, _ := getPathParts(request)
	if apiRequest && account != "" {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.GetAccountInfo(account) == nil {
				hdr := http.Header{
					"X-Timestamp": []string{common.GetTimestamp()},
				}
				ctx.C.PutAccount(account, hdr) // Auto-create the account if we can't get its info
			}
		}()
		if container != "" {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx.C.GetContainerInfo(account, container)
			}()
		}
		wg.Wait()
	}
	newWriter := srv.NewCustomWriter(writer, func(w http.ResponseWriter, status int) int {
		// strip out any bad headers before calling real WriteHeader
		for k := range w.Header() {
			if k == "X-Account-Sysmeta-Project-Domain-Id" {
				w.Header().Set("X-Account-Project-Domain-Id", w.Header().Get(k))
			}
			for _, ex := range excludeHeaders {
				if strings.HasPrefix(k, ex) {
					delete(w.Header(), k)
				}
			}
		}
		ctx.responseSent = true
		ctx.status = status
		return status
	})
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", ctx))
	m.next.ServeHTTP(newWriter, request)
}

func NewContext(mc ring.MemcacheRing, log srv.LowLevelLogger, proxyDirectClient *client.ProxyDirectClient) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return &ProxyContextMiddleware{
			Cache:             mc,
			log:               log,
			next:              next,
			proxyDirectClient: proxyDirectClient,
		}
	}
}
