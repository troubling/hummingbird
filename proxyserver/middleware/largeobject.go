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
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

var maxManifestSize = 1024 * 1024 * 2 // TODO add a check for this
var maxManifestLen = 1000

type segItem struct {
	Hash         string `json:"hash"`
	LastModified string `json:"last_modified"`
	Bytes        int64  `json:"bytes"`
	Name         string `json:"name"`
	ContentType  string `json:"content_type"`
	Range        string `json:"range,omitempty"`
	SubSlo       bool   `json:"sub_slo,omitempty"`
}

func (si segItem) segLenHash() (int64, string) {
	if si.Range != "" {
		segRange := si.makeRange()
		return segRange.End - segRange.Start, fmt.Sprintf(
			"%s:%s;", si.Hash, si.Range)
	}
	return int64(si.Bytes), si.Hash
}

// will return the segment range specified, or a range for the whole body
func (si segItem) makeRange() common.HttpRange {
	if si.Range != "" {
		ranges, err := common.ParseRange(fmt.Sprintf("bytes=%s", si.Range), int64(si.Bytes))
		if err == nil && len(ranges) == 1 {
			return ranges[0]
		}
	}
	return common.HttpRange{Start: 0, End: int64(si.Bytes)}
}

type sloPutManifest struct {
	Path      string `json:"path"`
	Etag      string `json:"etag"`
	SizeBytes int64  `json:"size_bytes"`
	Range     string `json:"range,omitempty"`
}

func splitSegPath(thePath string) (string, string, error) {
	segPathParts := strings.SplitN(strings.TrimLeft(thePath, "/"), "/", 2)
	if len(segPathParts) != 2 || segPathParts[0] == "" || segPathParts[1] == "" {
		return "", "", errors.New(fmt.Sprintf("invalid segment path: %s", thePath))
	}
	return segPathParts[0], segPathParts[1], nil
}

type xloIdentifyWriter struct {
	http.ResponseWriter
	funcName string
	status   int
	isSlo    bool
	isDlo    bool
	body     *bytes.Buffer
}

func (sw *xloIdentifyWriter) WriteHeader(status int) {
	sw.status = status
	if theDlo := sw.Header().Get("X-Object-Manifest"); theDlo != "" && sw.funcName != "get" {
		sw.isDlo = true
	}
	if isSlo := sw.Header().Get("X-Static-Large-Object"); isSlo == "True" {
		sw.isSlo = true
	}
	if !sw.isSlo && !sw.isDlo {
		sw.ResponseWriter.WriteHeader(status)
		return
	}
	sw.body = bytes.NewBuffer(make([]byte, 0))
}

func (sw *xloIdentifyWriter) Write(b []byte) (int, error) {
	if sw.isDlo {
		return len(b), nil
	} else if sw.isSlo {
		return sw.body.Write(b)
	} else {
		return sw.ResponseWriter.Write(b)
	}
}

type xloForwardBodyWriter struct {
	http.ResponseWriter
	// If constructed with status != 0 xloForwardBodyWriter will call x.ResponseWriter.WriteHeader.
	status int
	header http.Header
}

func (x *xloForwardBodyWriter) Header() http.Header {
	return x.header
}

func (x *xloForwardBodyWriter) WriteHeader(status int) {
	if x.status != 0 {
		if status/100 != 2 {
			x.ResponseWriter.WriteHeader(http.StatusConflict)
		} else {
			x.ResponseWriter.WriteHeader(x.status)
		}
	}
	x.status = status
}

func (x *xloForwardBodyWriter) Write(b []byte) (int, error) {
	return x.ResponseWriter.Write(b)
}

type xloCaptureWriter struct {
	status int
	body   []byte
	header http.Header
}

func (x *xloCaptureWriter) Header() http.Header    { return x.header }
func (x *xloCaptureWriter) WriteHeader(status int) { x.status = status }
func (x *xloCaptureWriter) Write(b []byte) (int, error) {
	x.body = append(x.body, b...)
	return len(b), nil
}

func needToRefetchManifest(sw *xloIdentifyWriter, request *http.Request) bool {
	if request.Method == "HEAD" {
		return true
	}
	if request.Header.Get("Range") != "" && (sw.status == 416) {
		return true
	}
	if request.Header.Get("Range") != "" && (sw.status == 200 || sw.status == 206) {
		re := regexp.MustCompile(`bytes (\d+)-(\d+)/(\d+)$`)
		res := re.FindStringSubmatch(sw.Header().Get("Content-Range"))
		if res == nil || len(res) != 4 {
			return true
		}
		end, _ := strconv.ParseInt(res[2], 10, 64)
		length, _ := strconv.ParseInt(res[3], 10, 64)
		got_everything := (res[1] == "0" && end == length-1)
		return !got_everything
	}
	return false
}

type xloMiddleware struct {
	next                    http.Handler
	dloGetRequestsMetric    tally.Counter
	sloGetRequestsMetric    tally.Counter
	sloPutRequestsMetric    tally.Counter
	sloDeleteRequestsMetric tally.Counter
}

func (xlo *xloMiddleware) feedOutSegments(sw *xloIdentifyWriter, request *http.Request, manifest []segItem, reqRange common.HttpRange, status int) {
	ctx := GetProxyContext(request)
	pathMap, err := common.ParseProxyPath(request.URL.Path)
	if err != nil || pathMap["account"] == "" {
		sw.ResponseWriter.WriteHeader(http.StatusBadRequest)
		ctx.Logger.Error("invalid origReq path", zap.String("path", request.URL.Path), zap.Error(err))
		return
	}
	writeHeader := true
	for _, si := range manifest {
		segLen, _ := si.segLenHash()
		if reqRange.Start >= segLen {
			reqRange.Start -= segLen
			reqRange.End -= segLen
			if reqRange.End < 0 {
				if writeHeader {
					sw.ResponseWriter.WriteHeader(status)
				}
				return
			}
			continue
		}
		if reqRange.End < 0 {
			if writeHeader {
				sw.ResponseWriter.WriteHeader(status)
			}
			return
		}
		segmentRange := si.makeRange()
		subReqStart := segmentRange.Start
		if reqRange.Start > 0 {
			subReqStart += reqRange.Start
		}
		subReqEnd := segmentRange.End
		if subReqEnd > segmentRange.Start+reqRange.End {
			subReqEnd = segmentRange.Start + reqRange.End
		}
		if subReqEnd <= 0 {
			continue
		}
		container, object, err := splitSegPath(si.Name)
		if err != nil {
			if writeHeader {
				sw.ResponseWriter.WriteHeader(http.StatusConflict)
			}
			return
		}
		newPath := fmt.Sprintf("/v1/%s/%s/%s", pathMap["account"], container, object)
		newReq, err := ctx.newSubrequest("GET", newPath, http.NoBody, request, "slo")
		if err != nil {
			ctx.Logger.Error("error building subrequest", zap.Error(err))
			if writeHeader {
				sw.ResponseWriter.WriteHeader(http.StatusConflict)
			}
			return
		}
		newReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", subReqStart, subReqEnd-1))
		sw2 := &xloForwardBodyWriter{ResponseWriter: sw.ResponseWriter, header: make(http.Header)}
		if writeHeader {
			sw2.status = status
			writeHeader = false
		}
		ctx.serveHTTPSubrequest(sw2, newReq)
		if sw2.status/100 != 2 {
			ctx.Logger.Debug("segment not found", zap.String("path", newPath),
				zap.String("Segment404", "404"))
			break
		}
		reqRange.Start -= segLen
		reqRange.End -= segLen
	}
	if writeHeader {
		sw.ResponseWriter.WriteHeader(status)
	}
}

func (xlo *xloMiddleware) buildSloManifest(request *http.Request, manPath string) (manifest []segItem, status int, err error) {
	ctx := GetProxyContext(request)
	newReq, err := ctx.newSubrequest("GET", fmt.Sprintf("%s?multipart-manifest=get", manPath), http.NoBody, request, "slo")
	if err != nil {
		return manifest, http.StatusInternalServerError, err
	}
	swRefetch := &xloCaptureWriter{header: make(http.Header)}
	ctx.serveHTTPSubrequest(swRefetch, newReq)
	if swRefetch.status != 200 && swRefetch.body == nil {
		return nil, swRefetch.status, errors.New("Error fetching manifest")
	}
	err = json.Unmarshal(swRefetch.body, &manifest)
	return manifest, http.StatusOK, err
}

func (xlo *xloMiddleware) buildDloManifest(sw *xloIdentifyWriter, request *http.Request, account string, container string, prefix string) (manifest []segItem, status int, err error) {
	ctx := GetProxyContext(request)
	newReq, err := ctx.newSubrequest("GET", fmt.Sprintf("/v1/%s/%s?format=json&prefix=%s", account, container, prefix), http.NoBody, request, "slo")
	if err != nil {
		return manifest, 500, err
	}
	swRefetch := &xloCaptureWriter{header: make(http.Header)}
	ctx.serveHTTPSubrequest(swRefetch, newReq)
	if swRefetch.status != 200 || swRefetch.body == nil {
		return nil, swRefetch.status, fmt.Errorf("Error %d fetching manifest", swRefetch.status)
	}
	if err = json.Unmarshal(swRefetch.body, &manifest); err != nil {
		return manifest, 500, err
	}
	for i := range manifest {
		manifest[i].Name = fmt.Sprintf("%s/%s", container, manifest[i].Name)
	}
	return manifest, 200, nil
}

func convertManifest(manifestBytes []byte) ([]byte, error) {
	var savedManifest []segItem
	var putManifest []sloPutManifest
	err := json.Unmarshal(manifestBytes, &savedManifest)
	if err != nil {
		return []byte{}, err
	}
	for _, si := range savedManifest {
		putManifest = append(putManifest, sloPutManifest{
			Path: si.Name, Etag: si.Hash, SizeBytes: si.Bytes, Range: si.Range})
	}
	newBody, err := json.Marshal(putManifest)
	if err != nil {
		return []byte{}, err
	}
	return []byte(newBody), nil
}

func (xlo *xloMiddleware) byteFeeder(sw *xloIdentifyWriter, request *http.Request, xloEtag string, xloContentLengthStr string, manifest []segItem) {
	xloContentLength := int64(0)
	if xloContentLengthStr != "" {
		if cl, err := strconv.ParseInt(xloContentLengthStr, 10, 64); err == nil {
			xloContentLength = cl
		} else {
			xloContentLengthStr = ""
		}
	}
	if xloEtag == "" || xloContentLengthStr == "" {
		xloEtagGen := md5.New()
		xloContentLengthGen := int64(0)
		for _, si := range manifest {
			segLen, segHash := si.segLenHash()
			xloContentLengthGen += segLen
			io.WriteString(xloEtagGen, segHash)
		}
		xloEtag = fmt.Sprintf("%x", xloEtagGen.Sum(nil))
		xloContentLength = xloContentLengthGen
	}
	reqRangeStr := request.Header.Get("Range")
	reqRange := common.HttpRange{Start: 0, End: xloContentLength}
	if reqRangeStr != "" {
		if ranges, err := common.ParseRange(reqRangeStr, xloContentLength); err == nil {
			xloContentLength = 0
			if len(ranges) != 1 {
				sw.ResponseWriter.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", xloContentLength))
				srv.SimpleErrorResponse(sw.ResponseWriter, http.StatusRequestedRangeNotSatisfiable, "invalid multi range")
				return
			}
			reqRange = ranges[0]
			xloContentLength += reqRange.End - reqRange.Start
		} else {
			sw.ResponseWriter.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", xloContentLength))
			srv.SimpleErrorResponse(sw.ResponseWriter, http.StatusRequestedRangeNotSatisfiable, "invalid range")
			return
		}
	}
	if request.Header.Get("If-Match") != "" {
		ifMatches := common.ParseIfMatch(request.Header.Get("If-Match"))
		if !ifMatches[strings.Trim(xloEtag, "\"")] {
			srv.SimpleErrorResponse(sw.ResponseWriter, 412, "")
			return
		}
	}
	if request.Header.Get("If-None-Match") != "" {
		ifNoneMatches := common.ParseIfMatch(request.Header.Get("If-None-Match"))
		if ifNoneMatches[strings.Trim(xloEtag, "\"")] {
			srv.SimpleErrorResponse(sw.ResponseWriter, 304, "")
			return
		}
	}
	sw.Header().Set("Content-Length", strconv.FormatInt(xloContentLength, 10))
	sw.Header().Set("Content-Type", sw.Header().Get("Content-Type"))
	sw.Header().Set("Etag", fmt.Sprintf("\"%s\"", xloEtag))
	status := http.StatusOK
	if reqRangeStr != "" {
		sw.Header().Set("Content-Range", fmt.Sprintf("%d-%d/%s", reqRange.Start, reqRange.End, xloContentLengthStr))
		status = http.StatusPartialContent
	}
	xlo.feedOutSegments(sw, request, manifest, reqRange, status)
}

func (xlo *xloMiddleware) handleDloGet(sw *xloIdentifyWriter, request *http.Request) {
	xlo.dloGetRequestsMetric.Inc(1)
	pathMap, err := common.ParseProxyPath(request.URL.Path)
	if err != nil || pathMap["object"] == "" {
		srv.SimpleErrorResponse(sw.ResponseWriter, 400, fmt.Sprintf(
			"invalid must multipath PUT to an object path: %s", request.URL.Path))
		return
	}
	container, prefix, err := splitSegPath(sw.Header().Get("X-Object-Manifest"))
	if err != nil {
		srv.SimpleErrorResponse(sw.ResponseWriter, 400, "invalid dlo manifest path")
		return
	}
	manifest, status, err := xlo.buildDloManifest(sw, request, pathMap["account"], container, prefix)
	if err != nil {
		srv.SimpleErrorResponse(sw.ResponseWriter, status,
			fmt.Sprintf("can not build dlo manifest at: %s?%s", container, prefix))
		return
	}
	xlo.byteFeeder(sw, request, "", "", manifest)
}

func (xlo *xloMiddleware) handleSloGet(sw *xloIdentifyWriter, request *http.Request) {
	xlo.sloGetRequestsMetric.Inc(1)
	// next has already been called and this is an SLO
	//TODO: what does comment at slo.py#624 mean?
	contentType, _, _ := common.ParseContentTypeForSlo(sw.Header().Get("Content-Type"), 0)
	sw.Header().Set("Content-Type", contentType)

	if sw.funcName == "get" {
		manifestBytes := sw.body.Bytes()
		var err error
		if request.URL.Query().Get("format") == "raw" {
			manifestBytes, err = convertManifest(manifestBytes)
			if err != nil {
				srv.SimpleErrorResponse(sw.ResponseWriter, 400, "invalid slo manifest")
				return
			}
		} else {
			sw.Header().Set("Content-Type", "application/json; charset=utf-8")
		}
		sw.Header().Set("Content-Length", strconv.Itoa(len(manifestBytes)))
		sw.Header().Set("Etag", strings.Trim(sw.Header().Get("Etag"), "\""))
		sw.ResponseWriter.WriteHeader(http.StatusOK)
		sw.ResponseWriter.Write(manifestBytes)
		return
	}
	sloEtag := sw.Header().Get("X-Object-Sysmeta-Slo-Etag")
	savedContentLength := sw.Header().Get("X-Object-Sysmeta-Slo-Size")
	isConditional := ((request.Header.Get("If-Match") != "" ||
		request.Header.Get("If-None-Match") != "") &&
		(sw.status == 304 || sw.status == 412))

	if (request.Method == "HEAD" || isConditional) && (sloEtag != "" || savedContentLength != "") {
		sw.Header().Set("Content-Length", savedContentLength)
		sw.Header().Set("Etag", fmt.Sprintf("\"%s\"", sloEtag))
		sw.ResponseWriter.WriteHeader(sw.status)
		return
	}
	var manifest []segItem
	var status int
	var err error
	manifestBytes := sw.body.Bytes()
	if needToRefetchManifest(sw, request) {
		manifest, status, err = xlo.buildSloManifest(request, request.URL.Path)
	} else {
		err = json.Unmarshal(manifestBytes, &manifest)
		status = http.StatusInternalServerError
	}
	if err != nil {
		srv.SimpleErrorResponse(sw.ResponseWriter, status, "invalid slo manifest")
		return
	}
	xlo.byteFeeder(sw, request, sloEtag, savedContentLength, manifest)
}

func parsePutSloManifest(body io.ReadCloser) (manifest []sloPutManifest, errs []string) {
	dec := json.NewDecoder(body)
	if d, err := dec.Token(); err != nil {
		return manifest, []string{"Invalid manifest json- not a list."}
	} else if r, ok := d.(json.Delim); !ok || r != '[' {
		return manifest, []string{"Invalid manifest json- not a list."}
	}
	for i := 0; dec.More(); i++ {
		if i > maxManifestLen {
			errs = append(errs, "Invalid manifest json- too many segments")
			break
		}
		var manItem sloPutManifest
		if err := dec.Decode(&manItem); err == io.EOF {
			break
		} else if err != nil {
			errs = append(errs, "Invalid manifest json- invalid format.")
			break
		}
		if strings.Index(strings.TrimLeft(manItem.Path, "/"), "/") == -1 {
			errs = append(errs,
				fmt.Sprintf("Index %d: path does not refer to an object. Path must be of the form /container/object.", i))
			continue
		}
		// cant really check this here because you can send size_bytes as None now
		if manItem.SizeBytes < 0 {
			errs = append(errs, fmt.Sprintf("Index %d: too small; each segment must be at least 1 byte.", i))
			continue
		}
		if manItem.Range != "" {
			if strings.Count(manItem.Range, "-") != 1 {
				errs = append(errs, fmt.Sprintf("Index %d: invalid or multiple ranges (only one allowed)", i))
				continue
			}
		}
		manifest = append(manifest, manItem)
	}
	return manifest, errs
}

func (xlo *xloMiddleware) handleSloPut(writer http.ResponseWriter, request *http.Request) {
	xlo.sloPutRequestsMetric.Inc(1)
	pathMap, err := common.ParseProxyPath(request.URL.Path)
	if err != nil || pathMap["object"] == "" {
		srv.SimpleErrorResponse(writer, 400, fmt.Sprintf(
			"invalid must multipath PUT to an object path: %s", request.URL.Path))
		return
	}
	contentLength := request.Header.Get("Content-Length")
	if contentLength == "" && request.Header.Get("Transfer-Encoding") != "chunked" {
		srv.StandardResponse(writer, 411)
		return
	}
	if request.Header.Get("X-Copy-From") != "" {
		srv.SimpleErrorResponse(writer, 405, "Multipart Manifest PUTs cannot be COPY requests")
		return
	}
	manifest, errs := parsePutSloManifest(request.Body)
	if len(errs) > 0 {
		srv.SimpleErrorResponse(writer, 400, strings.Join(errs, "\n"))
		return
	}
	var toPutManifest []segItem
	i := 0
	totalSize := int64(0)
	sloEtag := md5.New()
	ctx := GetProxyContext(request)
	for _, spm := range manifest {
		spmContainer, spmObject, err := splitSegPath(spm.Path)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid manifest path: %s", spm.Path))
			break
		}
		if spmContainer == pathMap["container"] && spmObject == pathMap["object"] {
			errs = append(errs, fmt.Sprintf("manifest cannot reference itself: %s", spm.Path))
			break
		}

		newPath := fmt.Sprintf("/v1/%s/%s/%s", pathMap["account"], spmContainer, spmObject)
		newReq, err := ctx.newSubrequest("HEAD", newPath, http.NoBody, request, "slo")
		if err != nil {
			ctx.Logger.Error("Couldn't create http.Request", zap.Error(err))
			return
		}
		pw := &xloCaptureWriter{header: make(http.Header)}
		ctx.serveHTTPSubrequest(pw, newReq)
		if pw.status != 200 {
			errs = append(errs, fmt.Sprintf("%d %s response on segment: %s", pw.status, http.StatusText(pw.status), newPath))
			continue
		}
		contentLength, err := strconv.ParseInt(pw.Header().Get("Content-Length"), 10, 64)
		if err != nil {
			errs = append(errs, fmt.Sprintf("bad content-length on segment: %s", newPath))
			continue
		}
		segEtag := strings.Trim(pw.Header().Get("Etag"), "\"")
		isSlo := false
		if xslo := pw.Header().Get("X-Static-Large-Object"); xslo == "True" {
			isSlo = true
		}
		if spm.SizeBytes > 0 && contentLength != spm.SizeBytes {
			errs = append(errs,
				fmt.Sprintf("Unmatching ContentLength (manifest %d) != (segment actual %d) response on segment: %s", spm.SizeBytes, contentLength, newPath))
			continue
		}
		segmentSize := contentLength
		parsedRange := spm.Range
		if spm.Range != "" {
			ranges, err := common.ParseRange(fmt.Sprintf("bytes=%s", spm.Range), contentLength)
			if err != nil {
				errs = append(errs,
					fmt.Sprintf("Index %d: invalid range", i))
				continue
			}
			if len(ranges) != 1 {
				errs = append(errs,
					fmt.Sprintf("Index %d:  multiple ranges (only one allowed)", i))
				continue
			}
			segmentSize = int64(ranges[0].End - ranges[0].Start)
			parsedRange = fmt.Sprintf("%d-%d", ranges[0].Start, ranges[0].End-1) // why -1? because...
		}
		totalSize += segmentSize
		if spm.Etag != "" && spm.Etag != segEtag {
			errs = append(errs,
				fmt.Sprintf("Etag Mismatch on %s: %s != %s", spm.Path, spm.Etag, segEtag))
			continue
		}
		lastModDate, _ := common.ParseDate(pw.Header().Get("Last-Modified"))

		contentType, _, _ := common.ParseContentTypeForSlo(pw.Header().Get("Content-Type"), 0)
		newSi := segItem{Name: spm.Path, Bytes: contentLength,
			Hash: segEtag, Range: parsedRange, SubSlo: isSlo,
			ContentType:  contentType,
			LastModified: lastModDate.Format("2006-01-02T15:04:05.00000")}
		_, newSiHash := newSi.segLenHash()
		io.WriteString(sloEtag, newSiHash)
		toPutManifest = append(toPutManifest, newSi)
	}
	if len(errs) > 0 {
		srv.SimpleErrorResponse(writer, 400, strings.Join(errs, "\n"))
		return
	}
	xloEtagGen := fmt.Sprintf("%x", sloEtag.Sum(nil))
	if reqEtag := request.Header.Get("Etag"); reqEtag != "" {
		if strings.Trim(reqEtag, "\"") != xloEtagGen {
			srv.SimpleErrorResponse(writer, 422, "Invalid Etag")
			return
		}
	}
	contentType := request.Header.Get("Content-Type")
	if contentType == "" {
		pathMap, _ := common.ParseProxyPath(request.URL.Path)
		contentType = mime.TypeByExtension(filepath.Ext(pathMap["object"]))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	newBody, err := json.Marshal(toPutManifest)
	putReq, err := ctx.newSubrequest("PUT", request.URL.Path, bytes.NewReader(newBody), request, "slo")
	if err != nil {
		ctx.Logger.Error("Creating new request", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	for k := range request.Header {
		if strings.HasPrefix(k, "X-Object-Meta-") {
			putReq.Header.Set(k, request.Header.Get(k))
		}
	}
	putReq.Header.Set("Content-Type", fmt.Sprintf("%s;swift_bytes=%d", contentType, totalSize))
	putReq.Header.Set("X-Static-Large-Object", "True")
	putReq.Header.Set("X-Object-Sysmeta-Slo-Etag", xloEtagGen)
	putReq.Header.Set("X-Object-Sysmeta-Slo-Size", fmt.Sprintf("%d", totalSize))
	if err != nil {
		srv.SimpleErrorResponse(writer, 400, "could not build slo manifest")
		return
	}
	putReq.Header.Set("Etag", fmt.Sprintf("%x", md5.Sum(newBody)))
	putReq.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
	if request.Header.Get("If-None-Match") != "" {
		putReq.Header.Set("If-None-Match", request.Header.Get("If-None-Match"))
	}
	ctx.serveHTTPSubrequest(writer, putReq)
	return
}

func (xlo *xloMiddleware) deleteAllSegments(w http.ResponseWriter, request *http.Request, manifest []segItem) error {
	pathMap, err := common.ParseProxyPath(request.URL.Path)
	if err != nil || pathMap["account"] == "" {
		return errors.New(fmt.Sprintf("invalid path to slo delete: %s", request.URL.Path))
	}
	ctx := GetProxyContext(request)
	for _, si := range manifest {
		container, object, err := splitSegPath(si.Name)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid slo item: %s", si.Name))
		}
		newPath := fmt.Sprintf("/v1/%s/%s/%s?multipart-manifest=delete", pathMap["account"], container, object)
		newReq, err := ctx.newSubrequest("DELETE", newPath, http.NoBody, request, "slo")
		if err != nil {
			return errors.New(fmt.Sprintf("error building subrequest: %s", err))
		}
		sw := &xloCaptureWriter{header: make(http.Header)}
		ctx.serveHTTPSubrequest(sw, newReq)
	}
	return nil
}

func (xlo *xloMiddleware) handleSloDelete(writer http.ResponseWriter, request *http.Request) {
	xlo.sloDeleteRequestsMetric.Inc(1)
	pathMap, err := common.ParseProxyPath(request.URL.Path)
	if err != nil || pathMap["object"] == "" {
		srv.SimpleErrorResponse(writer, 400, fmt.Sprintf(
			"invalid must multipath DELETE to an object path: %s", request.URL.Path))
		return
	}
	manifest, status, err := xlo.buildSloManifest(request, request.URL.Path)
	if err != nil {
		srv.SimpleErrorResponse(writer, status, fmt.Sprintf("invalid manifest json: %d %s", status, err))
		return
	}
	if err = xlo.deleteAllSegments(writer, request, manifest); err != nil {
		srv.SimpleErrorResponse(writer, 400, fmt.Sprintf("error deleting slo: %s", err))
		return
	}
	xlo.next.ServeHTTP(writer, request)
	return
}

func updateEtagIsAt(request *http.Request, etagLoc string) {
	curHeader := request.Header.Get("X-Backend-Etag-Is-At")
	if curHeader == "" {
		curHeader = etagLoc
	} else {
		curHeader = fmt.Sprintf("%s,%s", curHeader, etagLoc)
	}
	request.Header.Set("X-Backend-Etag-Is-At", curHeader)
}

func isValidDloHeader(manifest string) bool {
	if !strings.HasPrefix(manifest, "/") &&
		strings.Index(manifest, "?") == -1 &&
		strings.Index(manifest, "&") == -1 {
		m := strings.SplitN(manifest, "/", 2)
		if len(m) == 2 && m[0] != "" && m[1] != "" {
			return true
		}
	}
	return false
}

func (xlo *xloMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	xloFuncName := request.URL.Query().Get("multipart-manifest")
	if request.Method == "PUT" && request.Header.Get("X-Object-Manifest") != "" {
		if !isValidDloHeader(request.Header.Get("X-Object-Manifest")) {
			srv.SimpleErrorResponse(writer, 400, fmt.Sprintf(
				"X-Object-Manifest must be in the format container/prefix"))
			return
		}
		if xloFuncName == "put" {
			srv.SimpleErrorResponse(writer, 400, fmt.Sprintf("Cannot be both SLO and DLO"))
			return
		}
	}
	if request.Method == "PUT" && xloFuncName == "put" {
		xlo.handleSloPut(writer, request)
		return
	}
	if request.Method == "DELETE" && xloFuncName == "delete" {
		xlo.handleSloDelete(writer, request)
		return
	}
	if request.Method == "GET" || request.Method == "HEAD" {
		if xloFuncName != "get" {
			updateEtagIsAt(request, "X-Object-Sysmeta-Slo-Etag")
		}
		sw := &xloIdentifyWriter{ResponseWriter: writer, funcName: xloFuncName}
		xlo.next.ServeHTTP(sw, request)
		if sw.isSlo {
			xlo.handleSloGet(sw, request)
		} else if sw.isDlo {
			xlo.handleDloGet(sw, request)
		}
		return
	}
	xlo.next.ServeHTTP(writer, request)
}

func NewXlo(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	RegisterInfo("slo", map[string]interface{}{"max_manifest_segments": 1000, "max_manifest_size": 2097152, "min_segment_size": 1048576})
	RegisterInfo("dlo", map[string]interface{}{"max_segments": 10000})
	dloGetRequestsMetric := metricsScope.Counter("dlo_GET_requests")
	sloGetRequestsMetric := metricsScope.Counter("slo_GET_requests")
	sloPutRequestsMetric := metricsScope.Counter("slo_PUT_requests")
	sloDeleteRequestsMetric := metricsScope.Counter("slo_DELETE_requests")
	return func(next http.Handler) http.Handler {
		return &xloMiddleware{
			next:                    next,
			dloGetRequestsMetric:    dloGetRequestsMetric,
			sloGetRequestsMetric:    sloGetRequestsMetric,
			sloPutRequestsMetric:    sloPutRequestsMetric,
			sloDeleteRequestsMetric: sloDeleteRequestsMetric,
		}
	}, nil
}
