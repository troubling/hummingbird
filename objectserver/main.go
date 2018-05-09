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

package objectserver

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/textproto"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/justinas/alice"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/tracing"
	"github.com/troubling/hummingbird/middleware"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type ObjectServer struct {
	driveRoot          string
	hashPathPrefix     string
	hashPathSuffix     string
	reconCachePath     string
	checkEtags         bool
	checkMounts        bool
	allowedHeaders     map[string]bool
	logger             srv.LowLevelLogger
	logLevel           zap.AtomicLevel
	diskInUse          *common.KeyedLimit
	accountDiskInUse   *common.KeyedLimit
	expiringDivisor    int64
	updateClient       common.HTTPClient
	objEngines         map[int]ObjectEngine
	updateTimeout      time.Duration
	asyncWG            sync.WaitGroup // Used to wait on async goroutines
	metricsCloser      io.Closer
	traceCloser        io.Closer
	tracer             opentracing.Tracer
	updateClientCloser io.Closer
}

func (server *ObjectServer) Type() string {
	return "object"
}

func (server *ObjectServer) Background(flags *flag.FlagSet) chan struct{} {
	return nil
}

func (server *ObjectServer) Finalize() {
	server.asyncWG.Wait()
	if server.metricsCloser != nil {
		server.metricsCloser.Close()
	}
	if server.traceCloser != nil {
		server.traceCloser.Close()
	}
	if server.updateClientCloser != nil {
		server.updateClientCloser.Close()
	}
}

func (server *ObjectServer) newObject(req *http.Request, vars map[string]string, needData bool) (Object, error) {
	policy, err := strconv.Atoi(req.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}
	engine, ok := server.objEngines[policy]
	if !ok {
		return nil, fmt.Errorf("Engine for policy index %d not found.", policy)
	}
	return engine.New(vars, needData, &server.asyncWG)
}

func resolveEtag(req *http.Request, metadata map[string]string) string {
	etag := metadata["ETag"]
	for _, ph := range strings.Split(req.Header.Get("X-Backend-Etag-Is-At"), ",") {
		ph = strings.Trim(ph, " ")
		if altEtag, exists := metadata[http.CanonicalHeaderKey(ph)]; exists && ph != "" {
			etag = altEtag
		}
	}
	return etag
}

func (server *ObjectServer) ObjGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	headers := writer.Header()
	obj, err := server.newObject(request, vars, request.Method == "GET")
	if err != nil {
		srv.GetLogger(request).Error("Unable to open object.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	ifMatches := common.ParseIfMatch(request.Header.Get("If-Match"))
	ifNoneMatches := common.ParseIfMatch(request.Header.Get("If-None-Match"))

	metadata := obj.Metadata()
	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	headers.Set("X-Backend-Data-Timestamp", metadata["X-Backend-Data-Timestamp"])
	headers.Set("X-Backend-Meta-Timestamp", metadata["X-Backend-Meta-Timestamp"])
	if !obj.Exists() {
		if ifMatches["*"] {
			srv.StandardResponse(writer, http.StatusPreconditionFailed)
		} else {
			srv.StandardResponse(writer, http.StatusNotFound)
		}
		return
	}
	etag := resolveEtag(request, metadata)

	if deleteAt, ok := metadata["X-Delete-At"]; ok {
		if deleteTime, err := common.ParseDate(deleteAt); err == nil && deleteTime.Before(time.Now()) {
			srv.StandardResponse(writer, http.StatusNotFound)
			return
		}
	}

	lastModified, err := common.ParseDate(metadata["X-Timestamp"])
	if err != nil {
		srv.GetLogger(request).Error("Error getting timestamp",
			zap.String("timestamp", metadata["X-Timestamp"]), zap.String("obj", obj.Repr()), zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	headers.Set("Last-Modified", common.FormatLastModified(lastModified))
	headers.Set("ETag", "\""+etag+"\"")
	xTimestamp, err := common.GetEpochFromTimestamp(metadata["X-Timestamp"])
	if err != nil {
		srv.GetLogger(request).Error("Error getting the epoch time from x-timestamp", zap.Error(err))
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	headers.Set("X-Timestamp", xTimestamp)
	for key, value := range metadata {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			headers.Set(key, value)
		}
	}

	if len(ifMatches) > 0 && !ifMatches[etag] && !ifMatches["*"] {
		srv.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	}

	if len(ifNoneMatches) > 0 && (ifNoneMatches[etag] || ifNoneMatches["*"]) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	if ius, err := common.ParseDate(request.Header.Get("If-Unmodified-Since")); err == nil && lastModified.After(ius) {
		srv.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	}

	if ims, err := common.ParseDate(request.Header.Get("If-Modified-Since")); err == nil && lastModified.Before(ims) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	headers.Set("Accept-Ranges", "bytes")
	headers.Set("Content-Type", metadata["Content-Type"])
	headers.Set("Content-Length", metadata["Content-Length"])

	if rangeHeader := request.Header.Get("Range"); rangeHeader != "" {
		ranges, err := common.ParseRange(rangeHeader, obj.ContentLength())
		if err != nil {
			headers.Set("Content-Length", "0")
			headers.Set("Content-Range", fmt.Sprintf("bytes */%d", obj.ContentLength()))
			writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		} else if ranges != nil && len(ranges) == 1 {
			headers.Set("Content-Length", strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))
			headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, obj.ContentLength()))
			writer.WriteHeader(http.StatusPartialContent)
			obj.CopyRange(writer, ranges[0].Start, ranges[0].End)
			return
		} else if ranges != nil && len(ranges) > 1 {
			w := common.NewMultiWriter(writer, metadata["Content-Type"], obj.ContentLength())
			for _, rng := range ranges {
				w.Expect(rng.Start, rng.End)
			}
			headers.Set("Content-Length", strconv.FormatInt(w.ContentLength(), 10))
			headers.Set("Content-Type", "multipart/byteranges;boundary="+w.Boundary())
			writer.WriteHeader(http.StatusPartialContent)
			for _, rng := range ranges {
				part, err := w.CreatePart(rng.Start, rng.End)
				if err != nil {
					return
				}
				obj.CopyRange(part, rng.Start, rng.End)
			}
			w.Close()
			return
		}
	}
	writer.WriteHeader(http.StatusOK)
	if request.Method == "GET" {
		if server.checkEtags {
			hash := md5.New()
			_, err := obj.Copy(writer, hash)
			if err != nil {
				srv.GetLogger(request).Error("Error copying body", zap.Error(err))
			} else if hex.EncodeToString(hash.Sum(nil)) != metadata["ETag"] {
				obj.Quarantine()
			}
		} else {
			_, err := obj.Copy(writer)
			if err != nil {
				srv.GetLogger(request).Error("Error copying body", zap.Error(err))
			}
		}
	} else {
		writer.Write([]byte{})
	}
}

func (server *ObjectServer) ObjPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	outHeaders := writer.Header()

	requestTimestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.GetLogger(request).Error("Error standardizing request X-Timestamp", zap.Error(err))
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	if vars["obj"] == "" {
		http.Error(writer, fmt.Sprintf("Invalid path: %s", request.URL.Path), http.StatusBadRequest)
		return
	}
	if request.Header.Get("Content-Type") == "" {
		http.Error(writer, "No content type", http.StatusBadRequest)
		return
	}
	if deleteAt := request.Header.Get("X-Delete-At"); deleteAt != "" {
		if deleteTime, err := common.ParseDate(deleteAt); err != nil || deleteTime.Before(time.Now()) {
			http.Error(writer, "X-Delete-At in past", 400)
			return
		}
	}

	obj, err := server.newObject(request, vars, false)
	if err != nil {
		srv.GetLogger(request).Error("Error getting obj", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if obj.Exists() {
		if inm := request.Header.Get("If-None-Match"); inm == "*" {
			srv.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
		metadata := obj.Metadata()
		if requestTime, err := common.ParseDate(requestTimestamp); err == nil {
			if lastModified, err := common.ParseDate(metadata["X-Timestamp"]); err == nil && !requestTime.After(lastModified) {
				outHeaders.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
				srv.StandardResponse(writer, http.StatusConflict)
				return
			}
		}
		if inm := request.Header.Get("If-None-Match"); inm != "*" && strings.Contains(inm, metadata["ETag"]) {
			srv.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
	}

	tempFile, err := obj.SetData(request.ContentLength)
	if err == DriveFullError {
		srv.GetLogger(request).Debug("Not enough space available")
		srv.CustomErrorResponse(writer, 507, vars)
		return
	} else if err != nil {
		srv.GetLogger(request).Error("Error making new file", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}

	hash := md5.New()
	totalSize, err := common.Copy(request.Body, tempFile, hash)
	if err == io.ErrUnexpectedEOF || (request.ContentLength >= 0 && totalSize != request.ContentLength) {
		srv.StandardResponse(writer, 499)
		return
	} else if err != nil {
		srv.GetLogger(request).Error("Error writing to file", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	metadata := map[string]string{
		"name":           "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
		"X-Timestamp":    requestTimestamp,
		"Content-Type":   request.Header.Get("Content-Type"),
		"Content-Length": strconv.FormatInt(totalSize, 10),
		"ETag":           hex.EncodeToString(hash.Sum(nil)),
	}
	for key := range request.Header {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			metadata[key] = request.Header.Get(key)
		}
	}
	requestEtag := strings.Trim(strings.ToLower(request.Header.Get("ETag")), "\"")
	if requestEtag != "" && requestEtag != metadata["ETag"] {
		http.Error(writer, "Unprocessable Entity", 422)
		return
	}
	outHeaders.Set("ETag", metadata["ETag"])

	if err := obj.Commit(metadata); err != nil {
		srv.GetLogger(request).Error("Error saving object", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	server.containerUpdates(writer, request, metadata, vars, srv.GetLogger(request))
	srv.StandardResponse(writer, http.StatusCreated)
}

func (server *ObjectServer) ObjPostHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)

	requestTimestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.GetLogger(request).Error("Error standardizing request X-Timestamp", zap.Error(err))
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	if vars["obj"] == "" {
		http.Error(writer, fmt.Sprintf("Invalid path: %s", request.URL.Path), http.StatusBadRequest)
		return
	}
	if deleteAt := request.Header.Get("X-Delete-At"); deleteAt != "" {
		if deleteAtTime, err := common.ParseDate(deleteAt); err != nil || deleteAtTime.Before(time.Now()) {
			http.Error(writer, "X-Delete-At in past", 400)
			return
		}
	}

	obj, err := server.newObject(request, vars, false)
	if err != nil {
		srv.GetLogger(request).Error("Error getting obj", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()
	if !obj.Exists() {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}

	origMetadata := obj.Metadata()
	if requestTime, err := common.ParseDate(requestTimestamp); err == nil {
		if origLastModified, err := common.ParseDate(origMetadata["X-Timestamp"]); err == nil && !requestTime.After(origLastModified) {
			writer.Header().Set("X-Backend-Timestamp", origMetadata["X-Timestamp"])
			srv.StandardResponse(writer, http.StatusConflict)
			return
		}
	}
	if t := request.Header.Get("Content-Type"); t != "" && t != origMetadata["Content-Type"] {
		http.Error(writer, fmt.Sprintf("Content-Type may not be sent with object POST: %q", t), http.StatusConflict)
		return
	}

	if xda := request.Header.Get("X-Delete-At"); xda != "" && xda != origMetadata["X-Delete-AT"] {
		http.Error(writer, fmt.Sprintf("X-Delete-At may not be sent with object POST: %q", xda), http.StatusConflict)
		return
	}

	metadata := make(map[string]string)
	if v, ok := origMetadata["X-Static-Large-Object"]; ok {
		metadata["X-Static-Large-Object"] = v
	}
	if v, ok := origMetadata["Ec-Scheme"]; ok {
		metadata["Ec-Scheme"] = v
	}
	copyHdrs := map[string]bool{"Content-Disposition": true, "Content-Encoding": true, "X-Delete-At": true, "X-Object-Manifest": true, "X-Static-Large-Object": true}
	for _, v := range strings.Fields(request.Header.Get("X-Backend-Replication-Headers")) {
		copyHdrs[v] = true
	}
	for key := range request.Header {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			copyHdrs[key] ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Transient-Sysmeta-") {
			metadata[key] = request.Header.Get(key)
		}
	}
	metadata["name"] = "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"]
	metadata["X-Timestamp"] = requestTimestamp

	if err := obj.CommitMetadata(metadata); err != nil {
		srv.GetLogger(request).Error("Error saving object meta file", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	srv.StandardResponse(writer, http.StatusAccepted)
}

func (server *ObjectServer) ObjDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	headers := writer.Header()
	requestTimestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.GetLogger(request).Error("Error standardizing request X-Timestamp", zap.Error(err))
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	responseStatus := http.StatusNotFound

	obj, err := server.newObject(request, vars, false)
	if err != nil {
		srv.GetLogger(request).Error("Error getting obj", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if ida := request.Header.Get("X-If-Delete-At"); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			srv.StandardResponse(writer, http.StatusBadRequest)
			return
		}
		if !obj.Exists() {
			srv.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
		metadata := obj.Metadata()
		if _, ok := metadata["X-Delete-At"]; ok {
			if ida != metadata["X-Delete-At"] {
				srv.StandardResponse(writer, http.StatusPreconditionFailed)
				return
			}
		} else {
			srv.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
	}

	if obj.Exists() {
		responseStatus = http.StatusNoContent
		metadata := obj.Metadata()
		if origTimestamp, ok := metadata["X-Timestamp"]; ok && origTimestamp >= requestTimestamp {
			headers.Set("X-Backend-Timestamp", origTimestamp)
			srv.StandardResponse(writer, http.StatusConflict)
			return
		}
	} else {
		responseStatus = http.StatusNotFound
	}

	metadata := map[string]string{
		"X-Timestamp": requestTimestamp,
		"name":        "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
	}
	if err := obj.Delete(metadata); err == DriveFullError {
		srv.GetLogger(request).Debug("Not enough space available")
		srv.CustomErrorResponse(writer, 507, vars)
		return
	} else if err != nil {
		srv.GetLogger(request).Error("Error deleting object", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	server.containerUpdates(writer, request, metadata, vars, srv.GetLogger(request))
	srv.StandardResponse(writer, responseStatus)
}

func (server *ObjectServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
	return
}

func (server *ObjectServer) ReconHandler(writer http.ResponseWriter, request *http.Request) {
	middleware.ReconHandler(server.driveRoot, server.reconCachePath, server.checkMounts, writer, request)
	return
}

func (server *ObjectServer) OptionsHandler(writer http.ResponseWriter, request *http.Request) {
	middleware.OptionsHandler("object-server", writer, request)
	return
}

func (server *ObjectServer) DiskUsageHandler(writer http.ResponseWriter, request *http.Request) {
	data, err := server.diskInUse.MarshalJSON()
	if err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
	} else {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
	}
	return
}

func (server *ObjectServer) LogRequest(next http.Handler) http.Handler {
	return srv.LogRequest(server.logger, next)
}

func (server *ObjectServer) AcquireDevice(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		vars := srv.GetVars(request)
		if device, ok := vars["device"]; ok && device != "" {
			devicePath := filepath.Join(server.driveRoot, device)
			if server.checkMounts {
				if mounted, err := fs.IsMount(devicePath); err != nil || mounted != true {
					vars["Method"] = request.Method
					srv.CustomErrorResponse(writer, 507, vars)
					return
				}
			}

			forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
			if concRequests := server.diskInUse.Acquire(device, forceAcquire); concRequests != 0 {
				writer.Header().Set("X-Disk-Usage", strconv.FormatInt(concRequests, 10))
				srv.StandardResponse(writer, 503)
				return
			}
			defer server.diskInUse.Release(device)

			if account, ok := vars["account"]; ok && account != "" {
				limitKey := fmt.Sprintf("%s/%s", device, account)
				if concRequests := server.accountDiskInUse.Acquire(limitKey, false); concRequests != 0 {
					srv.StandardResponse(writer, 498)
					return
				}
				defer server.accountDiskInUse.Release(limitKey)
			}
		}
		next.ServeHTTP(writer, request)
	}
	return http.HandlerFunc(fn)
}

func (server *ObjectServer) updateDeviceLocks(seconds int64) {
	reloadTime := time.Duration(seconds) * time.Second
	for {
		time.Sleep(reloadTime)
		for _, key := range server.diskInUse.Keys() {
			lockPath := filepath.Join(server.driveRoot, key, "lock_device")
			if fs.Exists(lockPath) {
				server.diskInUse.Lock(key)
			} else {
				server.diskInUse.Unlock(key)
			}
		}
	}
}

func (server *ObjectServer) GetHandler(config conf.Config, metricsPrefix string) http.Handler {
	var metricsScope tally.Scope
	metricsScope, server.metricsCloser = tally.NewRootScope(tally.ScopeOptions{
		Prefix:         metricsPrefix,
		Tags:           map[string]string{},
		CachedReporter: promreporter.NewReporter(promreporter.Options{}),
		Separator:      promreporter.DefaultSeparator,
	}, time.Second)
	commonHandlers := alice.New(
		middleware.NewDebugResponses(config.GetBool("debug", "debug_x_source_code", false)),
		server.LogRequest,
		middleware.RecoverHandler,
		middleware.ValidateRequest,
		server.AcquireDevice,
	)
	router := srv.NewRouter()
	router.Get("/metrics", prometheus.Handler())
	router.Get("/loglevel", server.logLevel)
	router.Put("/loglevel", server.logLevel)
	router.Get("/healthcheck", commonHandlers.ThenFunc(server.HealthcheckHandler))
	router.Get("/diskusage", commonHandlers.ThenFunc(server.DiskUsageHandler))
	router.Put("/ring/*ring_path", commonHandlers.ThenFunc(middleware.RingHandler))
	router.Get("/recon/:method/:recon_type", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/recon/:method", commonHandlers.ThenFunc(server.ReconHandler))
	router.Delete("/recon/:device/:method/:recon_type/*item_path", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjGetHandler))
	router.Head("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjGetHandler))
	router.Put("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjPutHandler))
	router.Post("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjPostHandler))
	router.Delete("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjDeleteHandler))
	router.Options("/", commonHandlers.ThenFunc(server.OptionsHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("Invalid path: %s", r.URL.Path), http.StatusBadRequest)
	})
	for policy, objEngine := range server.objEngines {
		if rhoe, ok := objEngine.(PolicyHandlerRegistrator); ok {
			rhoe.RegisterHandlers(func(method, path string, handler http.HandlerFunc) {
				router.HandlePolicy(method, path, policy, commonHandlers.ThenFunc(handler))
			})
		}
	}
	return alice.New(middleware.Metrics(metricsScope), middleware.GrepObject, middleware.ServerTracer(server.tracer)).Then(router)
}

func NewServer(serverconf conf.Config, flags *flag.FlagSet, cnf srv.ConfigLoader) (*srv.IpPort, srv.Server, srv.LowLevelLogger, error) {
	var ipPort *srv.IpPort
	var err error
	server := &ObjectServer{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		allowedHeaders: map[string]bool{
			"Content-Disposition":   true,
			"Content-Encoding":      true,
			"X-Delete-At":           true,
			"X-Object-Manifest":     true,
			"X-Static-Large-Object": true,
		},
	}
	server.hashPathPrefix, server.hashPathSuffix, err = cnf.GetHashPrefixAndSuffix()
	if err != nil {
		return ipPort, nil, nil, err
	}
	if server.objEngines, err = buildEngines(serverconf, flags, cnf); err != nil {
		return ipPort, nil, nil, err
	}

	server.driveRoot = serverconf.GetDefault("app:object-server", "devices", "/srv/node")
	server.reconCachePath = serverconf.GetDefault("app:object-server", "recon_cache_path", "/var/cache/swift")
	server.checkMounts = serverconf.GetBool("app:object-server", "mount_check", true)
	server.checkEtags = serverconf.GetBool("app:object-server", "check_etags", false)
	server.diskInUse = common.NewKeyedLimit(serverconf.GetLimit("app:object-server", "disk_limit", 25, 0))
	server.accountDiskInUse = common.NewKeyedLimit(serverconf.GetLimit("app:object-server", "account_rate_limit", 0, 0))
	server.expiringDivisor = serverconf.GetInt("app:object-server", "expiring_objects_container_divisor", 86400)
	bindIP := serverconf.GetDefault("app:object-server", "bind_ip", "0.0.0.0")
	bindPort := int(serverconf.GetInt("app:object-server", "bind_port", common.DefaultObjectServerPort))
	certFile := serverconf.GetDefault("app:object-server", "cert_file", "")
	keyFile := serverconf.GetDefault("app:object-server", "key_file", "")
	if allowedHeaders, ok := serverconf.Get("app:object-server", "allowed_headers"); ok {
		headers := strings.Split(allowedHeaders, ",")
		for i := range headers {
			server.allowedHeaders[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(headers[i]))] = true
		}
	}
	logLevelString := serverconf.GetDefault("app:object-server", "log_level", "INFO")
	server.logLevel = zap.NewAtomicLevel()
	server.logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	if server.logger, err = srv.SetupLogger("object-server", &server.logLevel, flags); err != nil {
		return ipPort, nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	server.updateTimeout = time.Duration(serverconf.GetFloat("app:object-server", "container_update_timeout", 0.25) * float64(time.Second))
	connTimeout := time.Duration(serverconf.GetFloat("app:object-server", "conn_timeout", 1.0) * float64(time.Second))
	nodeTimeout := time.Duration(serverconf.GetFloat("app:object-server", "node_timeout", 10.0) * float64(time.Second))
	transport := &http.Transport{
		Dial:                (&net.Dialer{Timeout: connTimeout}).Dial,
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
		IdleConnTimeout:     5 * time.Second,
		DisableCompression:  true,
	}
	if certFile != "" && keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error getting TLS config: %v", err)
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up http2: %v", err)
		}
	}
	httpClient := &http.Client{
		Timeout:   nodeTimeout,
		Transport: transport,
	}
	server.updateClient = httpClient
	if serverconf.HasSection("tracing") {
		server.tracer, server.traceCloser, err = tracing.Init("objectserver", server.logger, serverconf.GetSection("tracing"))
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up tracer: %v", err)
		}
		updateClientTracer, updateClientCloser, err := tracing.Init("objectserver-client", server.logger, serverconf.GetSection("tracing"))
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up tracer: %v", err)
		}
		if updateClientCloser != nil {
			server.updateClientCloser = updateClientCloser
		}
		enableHTTPTrace := serverconf.GetBool("tracing", "enable_httptrace", true)
		server.updateClient, err = client.NewTracingClient(updateClientTracer, httpClient, enableHTTPTrace)
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up tracing client: %v", err)
		}
	}
	deviceLockUpdateSeconds := serverconf.GetInt("app:object-server", "device_lock_update_seconds", 0)
	if deviceLockUpdateSeconds > 0 {
		go server.updateDeviceLocks(deviceLockUpdateSeconds)
	}
	ipPort = &srv.IpPort{Ip: bindIP, Port: bindPort, CertFile: certFile, KeyFile: keyFile}
	return ipPort, server, server.logger, nil
}
