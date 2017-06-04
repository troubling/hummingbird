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
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	options := make(map[string]string)
	if request.ParseForm() == nil {
		for k, v := range request.Form {
			if listingQueryParms[k] && len(v) > 0 {
				options[k] = v[0]
			}
		}
	}
	r, headers, code := ctx.C.GetAccount(vars["account"], options, request.Header)
	for k := range headers {
		writer.Header().Set(k, headers.Get(k))
	}
	writer.WriteHeader(code)
	if r != nil {
		defer r.Close()
		common.Copy(r, writer)
	}
}

func (server *ProxyServer) AccountHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	headers, code := ctx.C.HeadAccount(vars["account"], request.Header)
	for k := range headers {
		writer.Header().Set(k, headers.Get(k))
	}
	writer.WriteHeader(code)
}

func (server *ProxyServer) AccountPostHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	if status, str := CheckMetadata(request, "Account"); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	defer ctx.InvalidateAccountInfo(vars["account"])
	srv.StandardResponse(writer, ctx.C.PostAccount(vars["account"], request.Header))
}

func (server *ProxyServer) AccountPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	if status, str := CheckMetadata(request, "Account"); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	defer ctx.InvalidateAccountInfo(vars["account"])
	srv.StandardResponse(writer, ctx.C.PutAccount(vars["account"], request.Header))
}

func (server *ProxyServer) AccountDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	defer ctx.InvalidateAccountInfo(vars["account"])
	srv.StandardResponse(writer, ctx.C.DeleteAccount(vars["account"], request.Header))
}
