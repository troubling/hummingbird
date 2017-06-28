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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

const (
	SCOPE_INVALID = iota
	SCOPE_ACCOUNT
	SCOPE_CONTAINER
)

type tuWriter struct {
	http.ResponseWriter
	method   string
	filename string
	obj      string
	expires  string
	inline   bool
}

func dispositionFormat(dtype string, filename string) string {
	return fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s",
		dtype, common.Urlencode(filename), common.Urlencode(filename))
}

func (w *tuWriter) WriteHeader(status int) {
	if (w.method == "GET" || w.method == "HEAD") && status/100 == 2 {
		for k := range w.Header() {
			if strings.HasPrefix(k, "X-Object-Meta") && !strings.HasPrefix(k, "X-Object-Meta-Public-") {
				w.Header().Del(k)
			}
		}
		if w.inline {
			if w.filename == "" {
				w.Header().Set("Content-Disposition", "inline")
			} else {
				w.Header().Set("Content-Disposition", dispositionFormat("inline", w.filename))
			}
		} else if w.filename != "" {
			w.Header().Set("Content-Disposition", dispositionFormat("attachment", w.filename))
		} else if w.Header().Get("Content-Disposition") == "" {
			w.Header().Set("Content-Disposition", dispositionFormat("attachment", filepath.Base(w.obj)))
		}
		w.Header().Set("Expires", w.expires)
	}
	w.ResponseWriter.WriteHeader(status)
}

func checkhmac(key, sig []byte, method, path string, expires time.Time) bool {
	if method == "HEAD" {
		for _, meth := range []string{"HEAD", "GET", "POST", "PUT"} {
			mac := hmac.New(sha1.New, key)
			fmt.Fprintf(mac, "%s\n%d\n%s", meth, expires.Unix(), path)
			if hmac.Equal(sig, mac.Sum(nil)) {
				return true
			}
		}
		return false
	} else {
		mac := hmac.New(sha1.New, key)
		fmt.Fprintf(mac, "%s\n%d\n%s", method, expires.Unix(), path)
		return hmac.Equal(sig, mac.Sum(nil))
	}
}

func tempurl(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "OPTIONS" {
			next.ServeHTTP(writer, request)
			return
		}
		ctx := GetProxyContext(request)
		if ctx.Authorize != nil {
			next.ServeHTTP(writer, request)
			return
		}
		q := request.URL.Query()
		sig := q.Get("temp_url_sig")
		exps := q.Get("temp_url_expires")
		_, inline := q["inline"]

		if sig == "" && exps == "" {
			next.ServeHTTP(writer, request)
			return
		} else if sig == "" || exps == "" {
			srv.StandardResponse(writer, 401)
			return
		}

		expires, err := common.ParseDate(exps)
		if err != nil || time.Now().After(expires) {
			srv.StandardResponse(writer, 401)
			return
		}

		sigb, err := hex.DecodeString(sig)
		if err != nil {
			srv.StandardResponse(writer, 401)
			return
		}

		apiReq, account, container, obj := getPathParts(request)
		if !apiReq || account == "" || container == "" {
			srv.StandardResponse(writer, 401)
			return
		}

		if bh := request.Header.Get("X-Object-Manifest"); bh != "" && (request.Method == "PUT" || request.Method == "POST") {
			srv.StandardResponse(writer, 400)
			return
		}

		path := ""
		if _, hasPrefix := q["temp_url_prefix"]; hasPrefix {
			prefix := q.Get("temp_url_prefix")
			if !strings.HasPrefix(obj, prefix) {
				srv.StandardResponse(writer, 401)
				return
			}
			path = fmt.Sprintf("prefix:/v1/%s/%s/%s", account, container, prefix)
		} else {
			path = fmt.Sprintf("/v1/%s/%s/%s", account, container, obj)
		}

		scope := SCOPE_INVALID
		if ai := ctx.GetAccountInfo(account); ai != nil {
			if key, ok := ai.Metadata["Temp-Url-Key"]; ok && checkhmac([]byte(key), sigb, request.Method, path, expires) {
				scope = SCOPE_ACCOUNT
			} else if key, ok := ai.Metadata["Temp-Url-Key-2"]; ok && checkhmac([]byte(key), sigb, request.Method, path, expires) {
				scope = SCOPE_ACCOUNT
			} else if ci := ctx.C.GetContainerInfo(account, container); ci != nil {
				if key, ok := ci.Metadata["Temp-Url-Key"]; ok && checkhmac([]byte(key), sigb, request.Method, path, expires) {
					scope = SCOPE_CONTAINER
				} else if key, ok := ci.Metadata["Temp-Url-Key-2"]; ok && checkhmac([]byte(key), sigb, request.Method, path, expires) {
					scope = SCOPE_CONTAINER
				}
			}
		}
		if scope == SCOPE_INVALID {
			srv.StandardResponse(writer, 401)
			return
		}
		ctx.RemoteUsers = []string{".tempurl"}
		ctx.Authorize = func(r *http.Request) (bool, int) {
			ar, a, c, _ := getPathParts(r)
			if ar && ((scope == SCOPE_ACCOUNT && a == account) || (scope == SCOPE_CONTAINER && c == container)) {
				return true, http.StatusOK
			}
			return false, http.StatusForbidden
		}

		next.ServeHTTP(
			&tuWriter{
				ResponseWriter: writer,
				method:         request.Method,
				obj:            obj,
				filename:       q.Get("filename"),
				expires:        expires.Format(time.RFC1123),
				inline:         inline,
			},
			request,
		)
	})
}

func NewTempURL(config conf.Section) (func(http.Handler) http.Handler, error) {
	RegisterInfo("tempurl", map[string]interface{}{
		"methods":                 []string{"GET", "HEAD", "PUT", "POST", "DELETE"},
		"incoming_remove_headers": []string{"x-timestamp"},
		"incoming_allow_headers":  []string{},
		"outgoing_remove_headers": []string{"x-object-meta-*"}, "outgoing_allow_headers": []string{"x-object-meta-public-*"},
	})
	return tempurl, nil
}
