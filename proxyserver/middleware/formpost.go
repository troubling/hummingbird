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
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

const (
	FP_INVALID = iota
	FP_ERROR
	FP_EXPIRED
	FP_SCOPE_ACCOUNT
	FP_SCOPE_CONTAINER
)

type fpLimitReader struct {
	io.Reader
	l int64
	r int64
}

func (o *fpLimitReader) overRead() bool {
	return o.r > o.l
}

func (o *fpLimitReader) Read(p []byte) (int, error) {
	i, err := o.Reader.Read(p)
	o.r += int64(i)
	if o.r > o.l {
		return 0, errors.New("Read over limit")
	}
	return i, err
}

func authenticateFormpost(ctx context.Context, proxyCtx *ProxyContext, account, container, path string, attrs map[string]string) int {
	if expires, err := common.ParseDate(attrs["expires"]); err != nil {
		return FP_ERROR
	} else if time.Now().After(expires) {
		return FP_EXPIRED
	}

	sigb, err := hex.DecodeString(attrs["signature"])
	if err != nil || len(sigb) == 0 {
		return FP_ERROR
	}

	checkhmac := func(key []byte) bool {
		mac := hmac.New(sha1.New, key)
		fmt.Fprintf(mac, "%s\n%s\n%s\n%s\n%s", path, attrs["redirect"],
			attrs["max_file_size"], attrs["max_file_count"], attrs["expires"])
		return hmac.Equal(sigb, mac.Sum(nil))
	}

	if ai, err := proxyCtx.GetAccountInfo(ctx, account); err == nil {
		if key, ok := ai.Metadata["Temp-Url-Key"]; ok && checkhmac([]byte(key)) {
			return FP_SCOPE_ACCOUNT
		} else if key, ok := ai.Metadata["Temp-Url-Key-2"]; ok && checkhmac([]byte(key)) {
			return FP_SCOPE_ACCOUNT
		} else if ci, err := proxyCtx.C.GetContainerInfo(ctx, account, container); err == nil {
			if key, ok := ci.Metadata["Temp-Url-Key"]; ok && checkhmac([]byte(key)) {
				return FP_SCOPE_CONTAINER
			} else if key, ok := ci.Metadata["Temp-Url-Key-2"]; ok && checkhmac([]byte(key)) {
				return FP_SCOPE_CONTAINER
			}
		}
	}
	return FP_INVALID
}

func formpostRespond(writer http.ResponseWriter, status int, message, redirect string) {
	if redirect == "" {
		body := fmt.Sprintf("<h1>%d %s</h1>FormPost: %s", status, http.StatusText(status), message)
		writer.Header().Set("Content-Type", "text/html")
		writer.Header().Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
		writer.WriteHeader(status)
		writer.Write([]byte(body))
	} else {
		glue := "?"
		if strings.Contains(redirect, "?") {
			glue = "&"
		}
		redir := fmt.Sprintf("%s%sstatus=%d&message=%s", redirect, glue, status, common.Urlencode(message))
		body := fmt.Sprintf("<html><body><p><a href=\"%s\">Click to continue...</a></p></body></html>",
			html.EscapeString(redir))
		writer.Header().Set("Location", redir)
		writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
		writer.WriteHeader(303)
		io.WriteString(writer, body)
	}
}

func formpostAuthorizer(scope int, account, container string) func(r *http.Request) (bool, int) {
	return func(r *http.Request) (bool, int) {
		ar, a, c, _ := getPathParts(r)
		if scope == FP_SCOPE_ACCOUNT {
			if ar && a == account {
				return true, http.StatusOK
			}
		} else if scope == FP_SCOPE_CONTAINER {
			if ar && a == account && c == container {
				return true, http.StatusOK
			}
		}
		return false, http.StatusForbidden
	}
}

func formpost(formpostRequestsMetric tally.Counter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != "POST" {
				next.ServeHTTP(writer, request)
				return
			}

			contentType, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
			if err != nil || contentType != "multipart/form-data" || params["boundary"] == "" {
				next.ServeHTTP(writer, request)
				return
			}

			apiReq, account, container, _ := getPathParts(request)
			if !apiReq || account == "" || container == "" {
				srv.StandardResponse(writer, 401)
				return
			}

			ctx := GetProxyContext(request)
			if ctx.Authorize != nil {
				next.ServeHTTP(writer, request)
				return
			}

			validated := false
			attrs := map[string]string{
				"redirect":       "",
				"max_file_size":  "0",
				"max_file_count": "0",
				"expires":        "0",
			}
			mr := multipart.NewReader(request.Body, params["boundary"])
			var maxFileCount, fileCount, maxFileSize int64
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				} else if err != nil {
					formpostRespond(writer, 400, "invalid request", attrs["redirect"])
					return
				}
				if fn := p.FileName(); fn == "" {
					data, err := ioutil.ReadAll(&io.LimitedReader{R: p, N: 8192})
					if err != nil {
						formpostRespond(writer, 400, "error reading form value", attrs["redirect"])
						return
					}
					if len(attrs) > 64 {
						formpostRespond(writer, 400, "too many form post values", attrs["redirect"])
						return
					}
					attrs[p.FormName()] = string(data)
				} else {
					if !validated {
						if maxFileCount, err = strconv.ParseInt(attrs["max_file_count"], 10, 64); err != nil || maxFileCount <= 0 {
							formpostRespond(writer, 400, "max_file_count not valid", attrs["redirect"])
							return
						}
						if maxFileSize, err = strconv.ParseInt(attrs["max_file_size"], 10, 64); err != nil || maxFileSize < 0 {
							formpostRespond(writer, 400, "max_file_size not valid", attrs["redirect"])
							return
						}
						scope := authenticateFormpost(request.Context(), ctx, account, container, request.URL.Path, attrs)
						switch scope {
						case FP_EXPIRED:
							formpostRespond(writer, 401, "Form Expired", attrs["redirect"])
							return
						case FP_INVALID:
							formpostRespond(writer, 401, "Invalid Signature", attrs["redirect"])
							return
						case FP_ERROR:
							formpostRespond(writer, 400, "invalid request", attrs["redirect"])
							return
						default:
							ctx.RemoteUsers = []string{".formpost"}
							ctx.Authorize = formpostAuthorizer(scope, account, container)
							validated = true
						}
					}

					fileCount++
					if fileCount > maxFileCount {
						formpostRespond(writer, 400, "max file count exceeded", attrs["redirect"])
						return
					}

					path := request.URL.Path
					if !strings.HasSuffix(path, "/") && strings.Count(path, "/") < 4 {
						path += "/"
					}
					path += fn
					neww := httptest.NewRecorder()
					flr := &fpLimitReader{Reader: p, l: maxFileSize}
					newreq, err := ctx.newSubrequest("PUT", path, flr, request, "formpost")
					if err != nil {
						formpostRespond(writer, 500, "internal server error", attrs["redirect"])
						return
					}
					newreq.Header.Set("X-Delete-At", attrs["x_delete_at"])
					newreq.Header.Set("X-Delete-After", attrs["x_delete_after"])
					newreq.TransferEncoding = []string{"chunked"}
					if attrs["content-type"] != "" {
						newreq.Header.Set("Content-Type", attrs["content-type"])
					} else {
						newreq.Header.Set("Content-Type", "application/octet-stream")
					}
					ctx.serveHTTPSubrequest(neww, newreq)
					if flr.overRead() {
						formpostRespond(writer, 400, "max_file_size exceeded", attrs["redirect"])
						return
					}
					if neww.Code/100 != 2 {
						formpostRespond(writer, neww.Code, "upload error", attrs["redirect"])
						return
					}
				}
				p.Close()
			}
			formpostRespond(writer, 201, "Success.", attrs["redirect"])
		})
	}
}

func NewFormPost(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	RegisterInfo("formpost", map[string]interface{}{})
	return formpost(metricsScope.Counter("formpost_requests")), nil
}
