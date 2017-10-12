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
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

var (
	serverInfo     = make(map[string]interface{})
	sil            sync.Mutex
	excludeHeaders = []string{
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
	StatusCode     int `json:"status"`
}

type AuthorizeFunc func(r *http.Request) (bool, int)
type subrequestCopy func(dst, src *http.Request)

type ProxyContextMiddleware struct {
	next              http.Handler
	log               srv.LowLevelLogger
	Cache             ring.MemcacheRing
	proxyDirectClient *client.ProxyDirectClient
	debugResponses    bool
}

type ProxyContext struct {
	*ProxyContextMiddleware
	C                client.ProxyClient
	Authorize        AuthorizeFunc
	RemoteUsers      []string
	StorageOwner     bool
	ACL              string
	subrequestCopy   subrequestCopy
	Logger           srv.LowLevelLogger
	TxId             string
	responseSent     time.Time
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

func (ctx *ProxyContext) Response() (time.Time, int) {
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
	apiRequest, account, container, object := getPathSegments(request.URL.Path)
	return apiRequest == "v1", account, container, object
}

func getPathSegments(requestPath string) (string, string, string, string) {
	parts := strings.SplitN(requestPath, "/", 5)
	switch len(parts) {
	case 5:
		return parts[1], parts[2], parts[3], parts[4]
	case 4:
		return parts[1], parts[2], parts[3], ""
	case 3:
		return parts[1], parts[2], "", ""
	case 2:
		return parts[1], "", "", ""
	default:
		return "", "", "", ""
	}
}

func (ctx *ProxyContext) GetAccountInfo(account string) (*AccountInfo, error) {
	key := fmt.Sprintf("account/%s", account)
	ai := ctx.accountInfoCache[key]
	if ai == nil {
		if err := ctx.Cache.GetStructured(key, &ai); err != nil {
			ai = nil
		}
	}
	if ai != nil && ai.StatusCode != 0 && ai.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%d error retrieving info for account %s", ai.StatusCode, account)
	}
	if ai == nil {
		resp := ctx.C.HeadAccount(account, nil)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			ctx.Cache.Set(key, &AccountInfo{StatusCode: resp.StatusCode}, 30)
			return nil, fmt.Errorf("%d error retrieving info for account %s", resp.StatusCode, account)
		}
		ai = &AccountInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
			StatusCode:  resp.StatusCode,
		}
		var err error
		if ai.ContainerCount, err = strconv.ParseInt(resp.Header.Get("X-Account-Container-Count"), 10, 64); err != nil {
			return nil, fmt.Errorf("Error retrieving info for account %s : %s", account, err)
		}
		if ai.ObjectCount, err = strconv.ParseInt(resp.Header.Get("X-Account-Object-Count"), 10, 64); err != nil {
			return nil, fmt.Errorf("Error retrieving info for account %s : %s", account, err)
		}
		if ai.ObjectBytes, err = strconv.ParseInt(resp.Header.Get("X-Account-Bytes-Used"), 10, 64); err != nil {
			return nil, fmt.Errorf("Error retrieving info for account %s : %s", account, err)
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
	return ai, nil
}

func (ctx *ProxyContext) InvalidateAccountInfo(account string) {
	key := fmt.Sprintf("account/%s", account)
	delete(ctx.accountInfoCache, key)
	ctx.Cache.Delete(key)
}

func (ctx *ProxyContext) AutoCreateAccount(account string, headers http.Header) {
	h := http.Header{"X-Timestamp": []string{common.GetTimestamp()},
		"X-Trans-Id": []string{ctx.TxId}}
	for key := range headers {
		if strings.HasPrefix(key, "X-Account-Sysmeta-") {
			h[key] = []string{headers.Get(key)}
		}
	}
	resp := ctx.C.PutAccount(account, h)
	if resp.StatusCode/100 == 2 {
		ctx.InvalidateAccountInfo(account)
	}
}

func (ctx *ProxyContext) newSubrequest(method, urlStr string, body io.Reader, req *http.Request, source string) (*http.Request, error) {
	if source == "" {
		panic("Programmer error: You must supply the source with newSubrequest. If you want the subrequest to be treated a user request (billing, quotas, etc.) you can set the source to \"-\"")
	}
	if source == "-" {
		source = ""
	}
	subreq, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	subctx := &ProxyContext{
		ProxyContextMiddleware: ctx.ProxyContextMiddleware,
		Authorize:              ctx.Authorize,
		RemoteUsers:            ctx.RemoteUsers,
		subrequestCopy:         ctx.subrequestCopy,
		Logger:                 ctx.Logger.With(zap.String("src", source)),
		C:                      ctx.C,
		TxId:                   ctx.TxId,
		accountInfoCache:       ctx.accountInfoCache,
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
		subctx.responseSent = time.Now()
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
		if request.URL.Query().Get("swiftinfo_sig") != "" || request.URL.Query().Get("swiftinfo_expires") != "" {
			writer.WriteHeader(403)
			return
		}
		if request.Method == "GET" {
			if data, err := serverInfoDump(); err != nil {
				srv.StandardResponse(writer, 500)
			} else {
				writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
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
			ctx.GetAccountInfo(account)
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
		if status == http.StatusUnauthorized && w.Header().Get("Www-Authenticate") == "" {
			if account != "" {
				w.Header().Set("Www-Authenticate", fmt.Sprintf("Swift realm=\"%s\"", common.Urlencode(account)))
			} else {
				w.Header().Set("Www-Authenticate", "Swift realm=\"unknown\"")
			}
		}

		if m.debugResponses && status/100 != 2 {
			buf := debug.Stack()
			w.Header().Set("X-Source-Code", string(buf))
		}

		ctx.responseSent = time.Now()
		ctx.status = status
		return status
	})
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", ctx))
	m.next.ServeHTTP(newWriter, request)
}

func NewContext(debugResponses bool, mc ring.MemcacheRing, log srv.LowLevelLogger, proxyDirectClient *client.ProxyDirectClient) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return &ProxyContextMiddleware{
			Cache:             mc,
			log:               log,
			next:              next,
			proxyDirectClient: proxyDirectClient,
			debugResponses:    debugResponses,
		}
	}
}
