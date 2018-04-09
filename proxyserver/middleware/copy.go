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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

type CopyWriter struct {
	http.ResponseWriter
	Logger        srv.LowLevelLogger
	accountName   string
	containerName string
	objectName    string
	origReqMethod string
	postAsCopy    bool
}

func (cw *CopyWriter) WriteHeader(status int) {
	if cw.postAsCopy && status == http.StatusCreated {
		cw.ResponseWriter.WriteHeader(http.StatusAccepted)
	} else {
		cw.ResponseWriter.WriteHeader(status)
	}
}

type copyMiddleware struct {
	next http.Handler
}

func (cw *CopyWriter) getSrcAccountName(request *http.Request) string {
	copyFromAccount := request.Header.Get("X-Copy-From-Account")
	name, err := common.CheckNameFormat(request, copyFromAccount, "Account")
	if err == nil {
		return name
	}
	return cw.accountName
}

func (cw *CopyWriter) getDestAccountName(request *http.Request) string {
	destAccount := request.Header.Get("Destination-Account")
	name, err := common.CheckNameFormat(request, destAccount, "Account")
	if err == nil {
		request.Header.Set("X-Copy-From-Account", cw.accountName)
		cw.accountName = name
		request.Header.Del("Destination-Account")
	}
	return cw.accountName
}

func getHeaderContainerObjectName(request *http.Request, header string) (string, string, error) {
	path, err := url.QueryUnescape(request.Header.Get(header))
	if err != nil {
		return "", "", fmt.Errorf("Invalid %s: %s", header, err)
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	parts := strings.SplitN(path, "/", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("Invalid %s", header)
	}
	name, name_err := common.CheckNameFormat(request, parts[1], "Account")
	if name_err != nil {
		return "", "", fmt.Errorf("Invalid %s: %s", header, name_err)
	}
	return name, parts[2], nil
}

func (c *copyMiddleware) handlePostAsCopy(writer *CopyWriter, request *http.Request) {
	request.Method = "PUT"
	writer.postAsCopy = true

	request.URL.Path = fmt.Sprintf("/v1/%s/%s/%s", writer.accountName, writer.containerName, writer.objectName)
	request.ContentLength = 0
	request.Header.Del("Range")
	objectPath := fmt.Sprintf("/%s/%s", writer.containerName, writer.objectName)
	request.Header.Set("X-Copy-From", common.Urlencode(objectPath))

	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		srv.StandardResponse(writer, 400)
		return
	}
	values.Set("multipart-manifest", "get")
	values.Set("format", "raw")
	request.URL.RawQuery = values.Encode()

	c.handlePut(writer, request)
}

func (c *copyMiddleware) handleCopy(writer *CopyWriter, request *http.Request) {
	if request.Header.Get("Destination") == "" {
		// FIXME.
		// swift has: body='Destination header required'
		srv.StandardResponse(writer, 412)
		return
	}
	destAccount := writer.getDestAccountName(request)
	destContainer, destObject, err := getHeaderContainerObjectName(request, "Destination")
	if err != nil {
		srv.StandardResponse(writer, 412)
		return
	}
	source := fmt.Sprintf("%s/%s", writer.containerName, writer.objectName)
	writer.containerName = destContainer
	writer.objectName = destObject

	request.Method = "PUT"
	request.URL.Path = fmt.Sprintf("/v1/%s/%s/%s", destAccount, destContainer, destObject)
	request.ContentLength = 0
	request.Header.Set("X-Copy-From", common.Urlencode(source))
	request.Header.Del("Destination")
	request.TransferEncoding = []string{"chunked"}

	c.handlePut(writer, request)
}

func RemoveItemsWithPrefix(header http.Header, prefix string) {
	for k := range header {
		if strings.HasPrefix(k, prefix) {
			header.Del(k)
		}
	}
}

func copyItemsWithPrefix(dest, src http.Header, prefix string) {
	for k, v := range src {
		if strings.HasPrefix(k, prefix) {
			dest.Del(k)
			for _, v1 := range v {
				dest.Add(k, v1)
			}
		}
	}
}

// TODO: This seems like it might copy headers it shouldn't.
// Also, shouldn't it be called copyHeaders instead?
func CopyItems(dest, src http.Header) {
	for k, v := range src {
		dest.Del(k)
		for _, v1 := range v {
			dest.Add(k, v1)
		}
	}
}

func copyMetaItems(dest, src http.Header) {
	copyItemsWithPrefix(dest, src, "X-Object-Meta-")
	copyItemsWithPrefix(dest, src, "X-Object-Sysmeta-")
	copyItemsWithPrefix(dest, src, "X-Object-Transient-Sysmeta-")
	dest.Set("X-Delete-At", src.Get("X-Delete-At"))
}

func excludeContains(exclude []string, k string) bool {
	for _, ex := range exclude {
		if k == ex {
			return true
		}
	}
	return false
}

func CopyItemsExclude(dest, src http.Header, exclude []string) {
	for k, v := range src {
		if !excludeContains(exclude, k) {
			dest.Del(k)
			for _, v1 := range v {
				dest.Add(k, v1)
			}
		}
	}
}

func (c *copyMiddleware) handlePut(writer *CopyWriter, request *http.Request) {
	if request.ContentLength != 0 {
		srv.StandardResponse(writer, 400)
		return
	}

	srcAccountName := writer.getSrcAccountName(request)
	srcContainer, srcObject, err := getHeaderContainerObjectName(request, "X-Copy-From")
	if err != nil {
		srv.StandardResponse(writer, 412)
		return
	}

	srcPath := fmt.Sprintf("/v1/%s/%s/%s", srcAccountName, srcContainer, srcObject)

	post := writer.origReqMethod == "POST"
	var auth AuthorizeFunc
	if !post {
		writer.Logger.Info(fmt.Sprintf("Copying object from %s to %s", srcPath, request.URL.Path))
	} else {
		// POST doesn't need to auth the internal GET; if they issued a POST
		// and it was authorized and we happen to need to do a GET+PUT for our
		// own reasons, that's fine.
		auth = func(r *http.Request) (bool, int) { return true, http.StatusOK }
	}

	srcBody, srcHeader, srcStatus := PipedGet(common.Urlencode(srcPath), request, "copy", auth)
	if srcBody != nil {
		defer srcBody.Close()
	}
	if srcStatus >= 300 {
		// Return the result of the GET
		for k := range srcHeader {
			writer.Header().Set(k, srcHeader.Get(k))
		}
		writer.WriteHeader(srcStatus)
		if srcBody != nil {
			common.Copy(srcBody, writer)
		}
		return
	}

	origHeader := make(map[string][]string)
	CopyItems(origHeader, request.Header)
	if post {
		// Post-as-copy: ignore new sysmeta, copy existing sysmeta
		RemoveItemsWithPrefix(request.Header, "X-Object-Sysmeta-")
		copyItemsWithPrefix(request.Header, srcHeader, "X-Object-Sysmeta-")
	} else if common.LooksTrue(request.Header.Get("X-Fresh-Metadata")) {
		// # x-fresh-metadata only applies to copy, not post-as-copy: ignore
		// existing user metadata, update existing sysmeta with new
		copyItemsWithPrefix(request.Header, srcHeader, "X-Object-Sysmeta-")
		copyItemsWithPrefix(request.Header, origHeader, "X-Object-Sysmeta-")
	} else {
		// First copy existing sysmeta, user meta and other headers from the
		// source to the request, apart from headers that are conditionally
		// copied below and timestamps.
		exclude := []string{"X-Static-Large-Object", "X-Object-Manifest",
			"Etag", "Content-Type", "X-Timestamp", "X-Backend-Timestamp"}
		CopyItemsExclude(request.Header, srcHeader, exclude)
		// now update with original req headers
		CopyItems(request.Header, origHeader)
	}

	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		srv.StandardResponse(writer, 400)
		return
	}
	if values.Get("multipart-manifest") == "get" {
		if srcHeader.Get("X-Static-Large-Object") != "" {
			values.Set("multipart-manifest", "put")
		}
		if srcHeader.Get("X-Object-Manifest") != "" {
			values.Del("multipart-manifest")
			request.Header.Set("X-Object-Manifest", srcHeader.Get("X-Object-Manifest"))
		}
	}

	request.URL.RawQuery = values.Encode()
	request.ContentLength = 0
	request.Body = srcBody

	if srcStatus == http.StatusOK &&
		srcHeader.Get("X-Static-Large-Object") == "" &&
		(srcHeader.Get("X-Object-Manifest") == "" || request.URL.Query().Get("multipart-manifest") == "get") {
		// copy source etag so that copied content is verified, unless:
		//  - not a 200 OK response: source etag may not match the actual
		//    content, for example with a 206 Partial Content response to a
		//    ranged request
		//  - SLO manifest: etag cannot be specified in manifest PUT; SLO
		//    generates its own etag value which may differ from source
		//  - SLO: etag in SLO response is not hash of actual content
		//  - DLO: etag in DLO response is not hash of actual content
		request.Header.Set("Etag", srcHeader.Get("Etag"))
	} else {
		// since we're not copying the source etag, make sure that any
		// container update override values are not copied.
		RemoveItemsWithPrefix(request.Header, "X-Object-Sysmeta-Container-Update-Override-")
	}

	request.Header.Del("X-Copy-From")
	request.Header.Del("X-Copy-From-Account")

	// If the copy request does not explicitly override content-type,
	// use the one present in the source object.
	if request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", srcHeader.Get("Content-Type"))
	}

	var respHeader http.Header = make(map[string][]string)

	respHeader.Set("X-Copied-From-Account", common.Urlencode(srcAccountName))
	copiedFrom := fmt.Sprintf("%s/%s", srcContainer, srcObject)
	respHeader.Set("X-Copied-From", common.Urlencode(copiedFrom))
	if srcHeader.Get("Last-Modified") != "" {
		respHeader.Set("X-Copied-From-Last-Modified", srcHeader.Get("Last-Modified"))
	}

	copyMetaItems(respHeader, request.Header)

	for k, v := range respHeader {
		for _, v1 := range v {
			writer.Header().Add(k, v1)
		}
	}

	c.next.ServeHTTP(writer, request)
}

func (c *copyMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	apiReq, account, container, object := getPathParts(request)
	if !apiReq || account == "" || container == "" || object == "" {
		c.next.ServeHTTP(writer, request)
		return
	}
	ctx := GetProxyContext(request)
	if ctx == nil {
		srv.StandardResponse(writer, 500)
		return
	}

	cw := &CopyWriter{
		ResponseWriter: writer,
		Logger:         ctx.Logger,
		accountName:    account,
		containerName:  container,
		objectName:     object,
	}

	if request.Method == "PUT" && request.Header.Get("X-Copy-From") != "" {
		c.handlePut(cw, request)
		return
	} else if request.Method == "COPY" {
		cw.origReqMethod = "COPY"
		c.handleCopy(cw, request)
		return
	} else if request.Method == "POST" {
		// TODO: Replace with PipeResponse stuff from #154
		subrec := httptest.NewRecorder()
		c.next.ServeHTTP(subrec, request)
		subresp := subrec.Result()
		defer subresp.Body.Close()
		if subresp.StatusCode == http.StatusConflict {
			cw.origReqMethod = "POST"
			c.handlePostAsCopy(cw, request)
		} else {
			// Copy headers that weren't previously set.
			for k, vs := range subresp.Header {
				if _, ok := writer.Header()[k]; !ok {
					for _, v := range vs {
						writer.Header().Add(k, v)
					}
				}
			}
			writer.WriteHeader(subresp.StatusCode)
			io.Copy(writer, subresp.Body)
		}
		return
	}
	c.next.ServeHTTP(writer, request)
}

func NewCopyMiddleware(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler { return &copyMiddleware{next: next} }, nil
}
