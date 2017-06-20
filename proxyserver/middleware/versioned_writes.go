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
	"bytes"
	"encoding/json"
	//"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

const (
	DELETE_MARKER_CONTENT_TYPE = "application/x-deleted;swift_versions_deleted=1"
	CLIENT_VERSIONS_LOC        = "X-Versions-Location"
	CLIENT_HISTORY_LOC         = "X-History-Location"
	SYSMETA_VERSIONS_LOC       = "X-Container-Sysmeta-Versions-Location"
	SYSMETA_VERSIONS_MODE      = "X-Container-Sysmeta-Versions-Mode"
)

// Blatantly stolen from xlo.
type mySegItem struct {
	Hash         string `json:"hash"`
	LastModified string `json:"last_modified"`
	Bytes        int64  `json:"bytes"`
	Name         string `json:"name"`
	ContentType  string `json:"content_type"`
	Range        string `json:"range,omitempty"`
	SubSlo       bool   `json:"sub_slo,omitempty"`
}

type versionedWrites struct {
	next    http.Handler
	enabled bool
}

type listingWriter struct {
	http.ResponseWriter
	buffer  *bytes.Buffer
	success bool
}

func (l *listingWriter) WriteHeader(status int) {
	if status/100 == 2 {
		l.success = true
		l.buffer = bytes.NewBuffer(make([]byte, 0))
	}
}

func (l *listingWriter) Write(stuff []byte) (int, error) {
	if l.success {
		l.buffer.Write(stuff)
	}
	return len(stuff), nil
}

type VersionedObjectWriter struct {
	header http.Header
	status int
}

func (vcw *VersionedObjectWriter) Write(stuff []byte) (int, error) {
	return len(stuff), nil
}

func (vcw *VersionedObjectWriter) Header() http.Header {
	return vcw.header
}

func (vcw *VersionedObjectWriter) WriteHeader(status int) {
	vcw.status = status
}

func NewVersionedObjectWriter() *VersionedObjectWriter {
	header := make(map[string][]string)
	return &VersionedObjectWriter{
		status: 500,
		header: header,
	}
}

type VersionedContainerWriter struct {
	http.ResponseWriter
}

func (vcw *VersionedContainerWriter) WriteHeader(status int) {
	mode := ""
	location := ""
	if vcw.ResponseWriter.Header().Get(SYSMETA_VERSIONS_LOC) != "" {
		location = vcw.ResponseWriter.Header().Get(SYSMETA_VERSIONS_LOC)
	}
	if vcw.ResponseWriter.Header().Get(SYSMETA_VERSIONS_MODE) != "" {
		mode = vcw.ResponseWriter.Header().Get(SYSMETA_VERSIONS_MODE)
	}

	if location != "" {
		if mode == "history" {
			vcw.ResponseWriter.Header().Set(CLIENT_HISTORY_LOC, location)
		} else {
			vcw.ResponseWriter.Header().Set(CLIENT_VERSIONS_LOC, location)
		}
	}

	vcw.ResponseWriter.WriteHeader(status)
}

func (v *versionedWrites) handleContainer(writer http.ResponseWriter, request *http.Request) {
	versionsLocs, v_ok := request.Header[CLIENT_VERSIONS_LOC]
	historyLocs, h_ok := request.Header[CLIENT_HISTORY_LOC]

	if v_ok && h_ok {
		srv.SimpleErrorResponse(writer, 400, fmt.Sprintf("Only one of %s or %s may be specified", CLIENT_VERSIONS_LOC, CLIENT_HISTORY_LOC))
		return
	}

	if v_ok || h_ok {
		val := ""
		var mode string
		if v_ok {
			val = versionsLocs[0]
			mode = "stack"
		} else {
			val = historyLocs[0]
			mode = "history"
		}
		if val != "" {
			request.Header.Set(SYSMETA_VERSIONS_LOC, val)
			request.Header.Set(SYSMETA_VERSIONS_MODE, mode)

			request.Header.Del(CLIENT_VERSIONS_LOC)
			request.Header.Del(CLIENT_HISTORY_LOC)
			// Adding a location trumps removing them.
			request.Header.Del("X-Remove-Versions-Location")
			request.Header.Del("X-Remove-History-Location")
		} else {
			request.Header.Set("X-Remove-Versions-Location", "x")
		}
	}

	if request.Header.Get("X-Remove-Versions-Location") != "" || request.Header.Get("X-Remove-History-Location") != "" {
		request.Header.Set(CLIENT_VERSIONS_LOC, "")
		request.Header.Set(SYSMETA_VERSIONS_LOC, "")
		request.Header.Set(SYSMETA_VERSIONS_MODE, "")
		request.Header.Del("X-Remove-Versions-Location")
		request.Header.Del("X-Remove-History-Location")
	}

	vcw := &VersionedContainerWriter{
		ResponseWriter: writer,
	}

	v.next.ServeHTTP(vcw, request)
}

func (v *versionedWrites) versionedObjectPrefix(object string) string {
	return fmt.Sprintf("%03x/%s", len(object), object)
}

func (v *versionedWrites) versionedObjectName(object string, ts string) string {
	return v.versionedObjectPrefix(object) + ts
}

func (v *versionedWrites) containerListing(writer http.ResponseWriter, ctx *ProxyContext, path string) (listing []mySegItem, err error) {
	request, err := http.NewRequest("GET", path, http.NoBody)
	if err != nil {
		return listing, err
	}
	lw := &listingWriter{ResponseWriter: writer}
	ctx.Subrequest(lw, request, "VW", false)
	if err = json.Unmarshal(lw.buffer.Bytes(), &listing); err != nil {
		return listing, err
	}
	return listing, nil
}

func (v *versionedWrites) putDeletedMarker(writer http.ResponseWriter, ctx *ProxyContext, path string) (http.Header, int) {
	request, err := http.NewRequest("PUT", path, http.NoBody)
	if err != nil {
		ctx.Logger.Error("putDeletedMarker PUT error", zap.Error(err))
		return nil, 500
	}
	request.Header.Set("Content-Type", DELETE_MARKER_CONTENT_TYPE)
	request.ContentLength = 0

	vow := NewVersionedObjectWriter()
	ctx.Subrequest(vow, request, "VW", false)
	return vow.Header(), vow.status
}

func (v *versionedWrites) putVersionedObj(writer http.ResponseWriter, ctx *ProxyContext, path string, body io.ReadCloser, header http.Header) (http.Header, int) {
	request, err := http.NewRequest("PUT", path, body)
	if err != nil {
		ctx.Logger.Error("putVersionedObj PUT error", zap.Error(err))
		return nil, 400
	}

	CopyItemsExclude(request.Header, header, []string{"X-Timestamp"})
	vow := NewVersionedObjectWriter()
	ctx.Subrequest(vow, request, "VW", false)
	return vow.Header(), vow.status
}

func (v *versionedWrites) copyObject(writer http.ResponseWriter, request *http.Request, dest string, src string) bool {
	ctx := GetProxyContext(request)
	pipe := &PipeResponse{}
	srcBody, srcHeader, srcStatus := pipe.Get(src, request, "VW")
	if srcBody != nil {
		defer srcBody.Close()
	}
	if srcStatus == http.StatusNotFound {
		ctx.Logger.Info("copyObject not found", zap.String("path", src))
		return false
	}
	if srcStatus/100 != 2 {
		ctx.Logger.Info("copyObject srcStatus", zap.Int("status", srcStatus))
		return false
	}
	_, destStatus := v.putVersionedObj(writer, ctx, dest, srcBody, srcHeader)
	if destStatus/100 != 2 {
		ctx.Logger.Info("copyObject destStatus", zap.Int("status", destStatus))
		return false
	}
	return true
}

func (v *versionedWrites) copyCurrent(writer http.ResponseWriter, request *http.Request, account string, versionContainer string, object string) (bool, int) {
	ctx := GetProxyContext(request)
	ci := ctx.C.GetContainerInfo(account, versionContainer)
	ctx.ACL = ci.WriteACL
	if ctx.Authorize != nil && !ctx.Authorize(request) {
		return false, 401
	}

	pipe := &PipeResponse{}

	srcBody, srcHeader, srcStatus := pipe.Get(request.URL.Path, request, "VW")
	if srcBody != nil {
		defer srcBody.Close()
	}
	// Silently ignore manifest and 404
	if srcHeader.Get("X-Object-Manifest") != "" {
		return true, 200
	}
	if srcStatus == http.StatusNotFound {
		return true, 200
	}
	if srcStatus/100 != 2 {
		ctx.Logger.Info(fmt.Sprintf("Bad status in copyCurrent GET: %v", srcStatus))
		return false, srcStatus
	}

	ts, err := common.StandardizeTimestamp(srcHeader.Get("X-Timestamp"))
	if err != nil {
		ctx.Logger.Info(fmt.Sprintf("Bad X-Timestamp"))
		return false, 500
	}
	if ts == "" {
		if lm, err := common.ParseDate(srcHeader.Get("Last-Modified")); err == nil {
			ts = common.CanonicalTimestampFromTime(lm)
		} else {
			ctx.Logger.Info(fmt.Sprintf("Bad Last-Modified"))
			return false, 500
		}
	}
	versObjName := v.versionedObjectName(object, ts)
	path := fmt.Sprintf("/v1/%s/%s/%s", account, versionContainer, versObjName)
	_, destStatus := v.putVersionedObj(writer, ctx, path, srcBody, srcHeader)
	if destStatus/100 != 2 {
		ctx.Logger.Info(fmt.Sprintf("Bad status in copyCurrent PUT: %v", srcStatus))
		return false, destStatus
	}
	return true, destStatus
}

func (v *versionedWrites) handleObjectDeleteHistory(writer http.ResponseWriter, request *http.Request, account string, versionsContainer string, object string) {
	ctx := GetProxyContext(request)
	ok, status := v.copyCurrent(writer, request, account, versionsContainer, object)
	if !ok || returnIfStatusError(writer, status) {
		return
	}
	versObjectName := v.versionedObjectName(object, common.GetTimestamp())
	markerPath := fmt.Sprintf("/v1/%s/%s/%s", account, versionsContainer, versObjectName)
	_, status = v.putDeletedMarker(writer, ctx, markerPath)
	if returnIfStatusError(writer, status) {
		return
	}
	v.next.ServeHTTP(writer, request)
}

func (v *versionedWrites) deleteObject(writer http.ResponseWriter, ctx *ProxyContext, path string) (http.Header, int) {
	request, err := http.NewRequest("DELETE", path, http.NoBody)
	if err != nil {
		ctx.Logger.Error("deleteObject error", zap.Error(err))
		return nil, 500
	}
	vow := NewVersionedObjectWriter()
	ctx.Subrequest(vow, request, "VW", false)
	return vow.Header(), vow.status
}

func (v *versionedWrites) headObject(writer http.ResponseWriter, ctx *ProxyContext, path string) (http.Header, int) {
	request, err := http.NewRequest("HEAD", path, http.NoBody)
	if err != nil {
		ctx.Logger.Error("headObject error", zap.Error(err))
		return nil, 500
	}
	vow := NewVersionedObjectWriter()
	ctx.Subrequest(vow, request, "VW", false)
	return vow.Header(), vow.status
}

func (v *versionedWrites) handleObjectDeleteStack(writer http.ResponseWriter, request *http.Request, account string, container string, versionsContainer string, object string) {
	ctx := GetProxyContext(request)
	listingPath := fmt.Sprintf("/v1/%s/%s?format=json&prefix=%s&reverse=on", account, versionsContainer, v.versionedObjectPrefix(object))
	listing, err := v.containerListing(writer, ctx, listingPath)
	if err != nil {
		srv.SimpleErrorResponse(writer, 500, "Failed to get versions container listing")
		return
	}
	authed := false
	headStatus := -1
	/* This is where things get weird.
	   In stack mode when a delete happens, we restore the most recent version, and then delete that
	   backed up version. If that fails for any reason, we loop through the versions until we can
	   restore one.
	   If the most recent version is a delete marker from the history type of versioning, a bunch of
	   things happen. If they object getting deleted actually exists, we go ahead and delete it.
	   Otherwise, we go ahead and try to restore the next possible version.
	*/
	for versionIndex := 0; versionIndex < len(listing); versionIndex++ {
		previousVersion := listing[versionIndex]
		if !authed {
			ctx := GetProxyContext(request)
			ci := ctx.C.GetContainerInfo(account, container)
			ctx.ACL = ci.WriteACL
			if ctx.Authorize != nil && !ctx.Authorize(request) {
				srv.StandardResponse(writer, 401)
				return
			}
			authed = true
		}
		if previousVersion.ContentType != DELETE_MARKER_CONTENT_TYPE {
			previousVersionPath := fmt.Sprintf("/v1/%s/%s/%s", account, versionsContainer, previousVersion.Name)
			if v.copyObject(writer, request, request.URL.Path, previousVersionPath) {
				// Restored, now delete the backup.
				request.URL.Path = previousVersionPath
				v.next.ServeHTTP(writer, request)
				return
			} else {
				// Couldn't restore the previous version, so try the next one.
				continue
			}
		} else {
			if headStatus == -1 {
				_, headStatus = v.headObject(writer, ctx, request.URL.Path)
			}
			if headStatus != http.StatusNotFound {
				// Object exists, just let it get deleted.
				break
			}
			for versionIndex++; versionIndex < len(listing); versionIndex++ {
				versionToRestore := listing[versionIndex]
				if versionToRestore.ContentType == DELETE_MARKER_CONTENT_TYPE {
					// The restore version was a delete marker, so all that happens is the
					// previousVersion empty marker gets deleted, and we're done restoring.
					break
				}
				restorePath := fmt.Sprintf("/v1/%s/%s/%s", account, versionsContainer, versionToRestore.Name)
				if v.copyObject(writer, request, request.URL.Path, restorePath) {
					// Successfully restored, so delete the history version
					_, status := v.deleteObject(writer, ctx, restorePath)
					if status != http.StatusNotFound && returnIfStatusError(writer, status) {
						ctx.Logger.Info("VW deleteObject failure", zap.Int("status", status))
						return
					}
					break
				} else {
					// Couldn't restore the previous version, so try the next one.
					continue
				}
			}
			// Restored a version, delete the top delete marker.
			request.URL.Path = fmt.Sprintf("/v1/%s/%s/%s", account, versionsContainer, previousVersion.Name)
		}
		if request.Header.Get("X-If-Delete-At") != "" {
			request.Header.Del("X-If-Delete-At")
		}
		break
	}
	v.next.ServeHTTP(writer, request)
	return
}

func (v *versionedWrites) handleObjectPut(writer http.ResponseWriter, request *http.Request, account string, versionsContainer string, object string) {
	if request.Header.Get("X-Object-Manifest") != "" {
		v.next.ServeHTTP(writer, request)
		return
	}
	ok, status := v.copyCurrent(writer, request, account, versionsContainer, object)
	if !ok || returnIfStatusError(writer, status) {
		return
	}
	v.next.ServeHTTP(writer, request)
}

func (v *versionedWrites) handleObject(writer http.ResponseWriter, request *http.Request) {
	_, account, container, object := getPathParts(request)
	ctx := GetProxyContext(request)
	ci := ctx.C.GetContainerInfo(account, container)
	if ci == nil {
		v.next.ServeHTTP(writer, request)
		return
	}

	versionsContainer := ci.SysMetadata["Versions-Location"]
	versionsMode := ci.SysMetadata["Versions-Mode"]
	if versionsMode == "" {
		versionsMode = "stack"
	}
	if versionsContainer == "" {
		v.next.ServeHTTP(writer, request)
		return
	}
	if unescapedVersionsContainer, err := url.QueryUnescape(versionsContainer); err == nil {
		versionsContainer = strings.Split(unescapedVersionsContainer, "/")[0]
	} else {
		ctx.Logger.Info(fmt.Sprintf("Bad versions container: %s", versionsContainer))
		v.next.ServeHTTP(writer, request)
		return
	}

	if request.Method == "PUT" {
		v.handleObjectPut(writer, request, account, versionsContainer, object)
	} else if versionsMode == "history" {
		v.handleObjectDeleteHistory(writer, request, account, versionsContainer, object)
	} else {
		v.handleObjectDeleteStack(writer, request, account, container, versionsContainer, object)
	}
}

func returnIfStatusError(writer http.ResponseWriter, status int) bool {
	if status/100 == 2 {
		return false
	}
	if status/100 == 4 {
		srv.StandardResponse(writer, 412)
	} else {
		srv.StandardResponse(writer, 500)
	}
	return true
}

func (v *versionedWrites) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	apiReq, account, container, object := getPathParts(request)
	if container == "" && object == "" {
		v.next.ServeHTTP(writer, request)
	}
	if !apiReq || account == "" {
		srv.StandardResponse(writer, 401)
		return
	}
	if GetProxyContext(request).Source == "VW" {
		v.next.ServeHTTP(writer, request)
		return
	}

	if !v.enabled {
		// Can't set these headers if disabled
		if (request.Method == "POST" || request.Method == "PUT") &&
			(request.Header.Get(CLIENT_VERSIONS_LOC) != "" ||
				request.Header.Get(CLIENT_HISTORY_LOC) != "") {

			srv.SimpleErrorResponse(writer, 412, "Versioned Writes is disabled")
			return
		}
		v.next.ServeHTTP(writer, request)
		return
	}

	if object == "" {
		v.handleContainer(writer, request)
	} else if request.Method == "PUT" || request.Method == "DELETE" {
		v.handleObject(writer, request)
	} else {
		v.next.ServeHTTP(writer, request)
	}
}

func NewVersionedWrites(config conf.Section) (func(http.Handler) http.Handler, error) {
	return func(next http.Handler) http.Handler {
		return &versionedWrites{
			next:    next,
			enabled: config.GetBool("allowed_versioned_writes", true),
		}
	}, nil
}
