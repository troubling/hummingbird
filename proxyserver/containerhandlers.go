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

func (server *ProxyServer) ContainerGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetAccountInfo(vars["account"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	options := map[string]string{
		"format":     request.FormValue("format"),
		"limit":      request.FormValue("limit"),
		"marker":     request.FormValue("marker"),
		"end_marker": request.FormValue("end_marker"),
		"prefix":     request.FormValue("prefix"),
		"delimiter":  request.FormValue("delimiter"),
		"reverse":    request.FormValue("reverse"),
	}
	r, headers, code := ctx.C.GetContainer(vars["account"], vars["container"], options, request.Header)
	for k := range headers {
		writer.Header().Set(k, headers.Get(k))
	}
	writer.WriteHeader(code)
	if r != nil {
		defer r.Close()
		common.Copy(r, writer)
	}
}

func (server *ProxyServer) ContainerHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetAccountInfo(vars["account"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	headers, code := ctx.C.HeadContainer(vars["account"], vars["container"], request.Header)
	for k := range headers {
		writer.Header().Set(k, headers.Get(k))
	}
	writer.WriteHeader(code)
}

func (server *ProxyServer) ContainerPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetAccountInfo(vars["account"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	if status, str := CheckContainerPut(request, vars["container"]); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	defer ctx.InvalidateContainerInfo(vars["account"], vars["container"])
	request.Header.Set("X-Timestamp", common.GetTimestamp())
	srv.StandardResponse(writer, ctx.C.PutContainer(vars["account"], vars["container"], request.Header))
}

func (server *ProxyServer) ContainerDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetAccountInfo(vars["account"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	defer ctx.InvalidateContainerInfo(vars["account"], vars["container"])
	request.Header.Set("X-Timestamp", common.GetTimestamp())
	srv.StandardResponse(writer, ctx.C.DeleteContainer(vars["account"], vars["container"], request.Header))
}
