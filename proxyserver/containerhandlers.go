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
	"fmt"
	"net/http"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/proxyserver/middleware"
)

var listingQueryParms = map[string]bool{
	"format":     true,
	"limit":      true,
	"marker":     true,
	"end_marker": true,
	"prefix":     true,
	"delimiter":  true,
	"reverse":    true,
	"path":       true,
}

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
	options := make(map[string]string)
	if request.ParseForm() == nil {
		for k, v := range request.Form {
			if listingQueryParms[k] && len(v) > 0 {
				options[k] = v[0]
			}
		}
	}
	resp := ctx.C.GetContainer(vars["account"], vars["container"], options, request.Header)
	defer resp.Body.Close()
	ctx.ACL = resp.Header.Get("X-Container-Read")
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	for k := range resp.Header {
		writer.Header().Set(k, resp.Header.Get(k))
	}
	handleCors(writer, request)
	writer.WriteHeader(resp.StatusCode)
	common.Copy(resp.Body, writer)
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
	resp := ctx.C.HeadContainer(vars["account"], vars["container"], request.Header)
	resp.Body.Close()
	ctx.ACL = resp.Header.Get("X-Container-Read")
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	for k := range resp.Header {
		writer.Header().Set(k, resp.Header.Get(k))
	}
	handleCors(writer, request)
	writer.WriteHeader(resp.StatusCode)
}

func (server *ProxyServer) ContainerPostHandler(writer http.ResponseWriter, request *http.Request) {
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
	if err := cleanACLs(request); err != nil {
		srv.SimpleErrorResponse(writer, 400, err.Error())
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
	if status, str := CheckContainerPut(request, vars["container"]); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	resp := ctx.C.PostContainer(vars["account"], vars["container"], request.Header)
	resp.Body.Close()
	handleCors(writer, request)
	srv.StandardResponse(writer, resp.StatusCode)
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
	if err := cleanACLs(request); err != nil {
		srv.SimpleErrorResponse(writer, 400, err.Error())
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
	if status, str := CheckContainerPut(request, vars["container"]); status != http.StatusOK {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(status)
		writer.Write([]byte(str))
		return
	}
	resp := ctx.C.PutContainer(vars["account"], vars["container"], request.Header)
	resp.Body.Close()
	handleCors(writer, request)
	srv.StandardResponse(writer, resp.StatusCode)
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
		if ctx.RemoteUser != "" {
			srv.StandardResponse(writer, 403)
			return
		}
		srv.StandardResponse(writer, 401)
		return
	}
	resp := ctx.C.DeleteContainer(vars["account"], vars["container"], request.Header)
	resp.Body.Close()
	handleCors(writer, request)
	srv.StandardResponse(writer, resp.StatusCode)
}

func cleanACLs(r *http.Request) error {
	for _, header := range []string{"X-Container-Read", "X-Container-Write"} {
		if r.Header.Get(header) != "" {
			cleanedValue, err := middleware.CleanACL(header, r.Header.Get(header))
			if err != nil {
				return err
			}
			r.Header.Set(header, cleanedValue)
		}
	}
	return nil
}

func setVary(writer http.ResponseWriter, h string) {
	if v := writer.Header().Get("Vary"); v != "" {
		writer.Header().Set("Vary", fmt.Sprintf("%s, %s", v, h))
	} else {
		writer.Header().Set("Vary", h)
	}
}

var publicMethods = []string{"HEAD", "GET", "PUT", "POST", "OPTIONS", "DELETE", "COPY"}

func handleCors(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	origin := request.Header.Get("Origin")
	if ctx == nil || origin == "" {
		return
	}
	if ci := ctx.C.GetContainerInfo(vars["account"], vars["container"]); ci != nil {
		if !common.IsOriginAllowed(ci.Metadata["Access-Control-Allow-Origin"], origin) {
			return
		}
		if writer.Header().Get("Access-Control-Expose-Headers") == "" {
			corsExposeHeaders := []string{"Cache-Control", "Content-Language",
				"Content-Type", "Expires", "Last-Modified", "Pragma", "Etag",
				"X-Timestamp", "X-Trans-Id", "X-Openstack-Request-Id"}
			for k := range writer.Header() {
				if strings.HasPrefix(
					k, "X-Container-Meta") || strings.HasPrefix(
					k, "X-Object-Meta") {
					corsExposeHeaders = append(corsExposeHeaders, k)
				}
			}
			if ci.Metadata["Access-Control-Expose-Headers"] != "" {
				for _, h := range strings.Split(
					ci.Metadata["Access-Control-Expose-Headers"], " ") {
					corsExposeHeaders = append(corsExposeHeaders, h)
				}
			}
			writer.Header().Set(
				"Access-Control-Expose-Headers", strings.Join(corsExposeHeaders, ","))
		}
		if writer.Header().Get("Access-Control-Allow-Origin") == "" {
			if ci.Metadata["Access-Control-Allow-Origin"] == "*" {
				writer.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				writer.Header().Set("Access-Control-Allow-Origin", origin)
			}
		}
	}
}

func (server *ProxyServer) OptionsHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	ctx := middleware.GetProxyContext(request)
	origin := request.Header.Get("Origin")
	methodString := strings.Join(publicMethods, ", ")
	if origin == "" || vars["container"] == "" {
		writer.Header().Set("Allow", methodString)
		srv.StandardResponse(writer, 200)
		return
	}
	if rqm := request.Header.Get("Access-Control-Request-Method"); rqm == "" {
		srv.SimpleErrorResponse(writer, 401, "")
		return
	} else {
		found := false
		for _, method := range publicMethods {
			if rqm == method {
				found = true
				break
			}
		}
		if !found {
			srv.SimpleErrorResponse(writer, 401, "")
			return
		}
	}
	if ci := ctx.C.GetContainerInfo(vars["account"], vars["container"]); ci != nil {
		if common.IsOriginAllowed(ci.Metadata["Access-Control-Allow-Origin"], origin) {
			writer.Header().Set("Allow", methodString)
			if ci.Metadata["Access-Control-Allow-Origin"] == "*" {
				writer.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				writer.Header().Set("Access-Control-Allow-Origin", origin)
				setVary(writer, "Origin")
			}
			if ma := ci.Metadata["Access-Control-Max-Age"]; ma != "" {
				writer.Header().Set("Access-Control-Max-Age", ma)
			}
			if rh := request.Header.Get("Access-Control-Request-Headers"); rh != "" {
				writer.Header().Set("Access-Control-Allow-Headers", rh)
				setVary(writer, "Access-Control-Request-Headers")
			}
			srv.StandardResponse(writer, 200)
			return
		}
	}
	srv.SimpleErrorResponse(writer, 401, "")
	return
}
