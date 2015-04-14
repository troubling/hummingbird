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

package common

import (
	"net/http"

	"github.com/dimfeld/httptreemux"
)

type router struct {
	*httptreemux.TreeMux
}

func (r *router) Get(path string, handler http.Handler) {
	r.GET(path, wrapHandler(handler))
}

func (r *router) Put(path string, handler http.Handler) {
	r.PUT(path, wrapHandler(handler))
}
func (r *router) Head(path string, handler http.Handler) {
	r.HEAD(path, wrapHandler(handler))
}

func (r *router) Delete(path string, handler http.Handler) {
	r.DELETE(path, wrapHandler(handler))
}
func (r *router) Replicate(path string, handler http.Handler) {
	r.Handle("REPLICATE", path, wrapHandler(handler))
}

func NewRouter() *router {
	r := httptreemux.New()
	r.RedirectTrailingSlash = false
	r.RedirectCleanPath = false
	return &router{TreeMux: r}
}

func wrapHandler(h http.Handler) httptreemux.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, ps map[string]string) {
		SetVars(r, ps)
		h.ServeHTTP(w, r)
	}
}
