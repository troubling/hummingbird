//  Copyright (c) 2015 Rackspace
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
	"net/http"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/proxyserver/middleware"
)

func (server *ProxyServer) AccountGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil {
		if ok, s := ctx.Authorize(request); !ok {
			srv.StandardResponse(writer, s)
			return
		}
	}
	options := make(map[string]string)
	if request.ParseForm() == nil {
		for k, v := range request.Form {
			if listingQueryParms[k] && len(v) > 0 {
				options[k] = v[0]
			}
		}
	}
	resp := ctx.C.GetAccount(vars["account"], options, request.Header)
	if resp.StatusCode == http.StatusNotFound && server.accountAutoCreate {
		resp.Body.Close()
		ctx.AutoCreateAccount(vars["account"], request.Header)
		resp = ctx.C.GetAccount(vars["account"], options, request.Header)
	}
	for k := range resp.Header {
		writer.Header().Set(k, resp.Header.Get(k))
	}
	writer.WriteHeader(resp.StatusCode)
	defer resp.Body.Close()
	common.Copy(resp.Body, writer)
}

func (server *ProxyServer) AccountHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil {
		if ok, s := ctx.Authorize(request); !ok {
			srv.StandardResponse(writer, s)
			return
		}
	}
	resp := ctx.C.HeadAccount(vars["account"], request.Header)
	if resp.StatusCode == http.StatusNotFound && server.accountAutoCreate {
		resp.Body.Close()
		ctx.AutoCreateAccount(vars["account"], request.Header)
		resp = ctx.C.HeadAccount(vars["account"], request.Header)
	}
	for k := range resp.Header {
		writer.Header().Set(k, resp.Header.Get(k))
	}
	resp.Body.Close()
	writer.WriteHeader(resp.StatusCode)
}

func (server *ProxyServer) AccountPostHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil {
		if ok, s := ctx.Authorize(request); !ok {
			srv.StandardResponse(writer, s)
			return
		}
	}
	if status, str := CheckMetadata(request, "Account"); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	defer ctx.InvalidateAccountInfo(vars["account"])
	resp := ctx.C.PostAccount(vars["account"], request.Header)
	if resp.StatusCode == http.StatusNotFound && server.accountAutoCreate {
		resp.Body.Close()
		ctx.AutoCreateAccount(vars["account"], request.Header)
		resp = ctx.C.PostAccount(vars["account"], request.Header)
	}
	resp.Body.Close()
	srv.StandardResponse(writer, resp.StatusCode)
}

func (server *ProxyServer) AccountPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil {
		if ok, s := ctx.Authorize(request); !ok {
			srv.StandardResponse(writer, s)
			return
		}
	}
	if status, str := CheckMetadata(request, "Account"); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	defer ctx.InvalidateAccountInfo(vars["account"])
	resp := ctx.C.PutAccount(vars["account"], request.Header)
	resp.Body.Close()
	srv.StandardResponse(writer, resp.StatusCode)
}

func (server *ProxyServer) AccountDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil {
		if ok, s := ctx.Authorize(request); !ok {
			srv.StandardResponse(writer, s)
			return
		}
	}
	defer ctx.InvalidateAccountInfo(vars["account"])
	resp := ctx.C.DeleteAccount(vars["account"], request.Header)
	resp.Body.Close()
	srv.StandardResponse(writer, resp.StatusCode)
}
