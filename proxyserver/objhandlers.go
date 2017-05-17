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
	"mime"
	"net/http"
	"path/filepath"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/proxyserver/middleware"
)

func (server *ProxyServer) ObjectGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetContainerInfo(vars["account"], vars["container"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	r, headers, code := ctx.C.GetObject(vars["account"], vars["container"], vars["obj"], request.Header)
	for k := range headers {
		writer.Header().Set(k, headers.Get(k))
	}
	writer.WriteHeader(code)
	if r != nil {
		defer r.Close()
		common.Copy(r, writer)
	}
}

func (server *ProxyServer) ObjectHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetContainerInfo(vars["account"], vars["container"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	headers, code := ctx.C.HeadObject(vars["account"], vars["container"], vars["obj"], request.Header)
	for k := range headers {
		writer.Header().Set(k, headers.Get(k))
	}
	writer.WriteHeader(code)
}

func (server *ProxyServer) ObjectDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetContainerInfo(vars["account"], vars["container"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	request.Header.Set("X-Timestamp", common.GetTimestamp())
	srv.StandardResponse(writer, ctx.C.DeleteObject(vars["account"], vars["container"], vars["obj"], request.Header))
}

func (server *ProxyServer) ObjectPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}
	if ctx.GetContainerInfo(vars["account"], vars["container"]) == nil {
		srv.StandardResponse(writer, 404)
		return
	}
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		srv.StandardResponse(writer, 401)
		return
	}
	if request.Header.Get("Content-Type") == "" {
		contentType := mime.TypeByExtension(filepath.Ext(vars["obj"]))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		request.Header.Set("Content-Type", contentType)
	}
	if status, str := CheckObjPut(request, vars["obj"]); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	request.Header.Set("X-Timestamp", common.GetTimestamp())
	srv.StandardResponse(writer, ctx.C.PutObject(vars["account"], vars["container"], vars["obj"], request.Header, request.Body))
}
