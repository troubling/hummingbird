//  Copyright (c) 2016 Rackspace
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

package containerserver

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // install pprof http handlers
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/justinas/alice"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
)

// GetHashPrefixAndSuffix is a pointer to hummingbird's function of the same name, for overriding in tests.
var GetHashPrefixAndSuffix = conf.GetHashPrefixAndSuffix

// GetSyncRealms is a pointer to hummingbird's function of the same name, for overriding in tests.
var GetSyncRealms = conf.GetSyncRealms

// LoadPolicies is a pointer to hummingbird's function of the same name, for overriding in tests.
var LoadPolicies = conf.LoadPolicies

// ContainerServer contains all of the information for a running container server.
type ContainerServer struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	logger           srv.LowLevelLogger
	logLevel         string
	diskInUse        *common.KeyedLimit
	checkMounts      bool
	containerEngine  ContainerEngine
	updateClient     *http.Client
	autoCreatePrefix string
	syncRealms       conf.SyncRealmList
	defaultPolicy    int
}

var saveHeaders = map[string]bool{
	"X-Container-Read":     true,
	"X-Container-Write":    true,
	"X-Container-Sync-Key": true,
	"X-Container-Sync-To":  true,
	"X-Versions-Location":  true,
}

func formatTimestamp(ts string) string {
	if len(ts) == 16 && ts[10] == '.' {
		return ts
	}
	t, err := strconv.ParseFloat(ts, 64)
	if err != nil {
		return "0000000000.00000"
	}
	ret := strconv.FormatFloat(t, 'f', 5, 64)
	if len(ret) < 16 {
		return strings.Repeat("0", 16-len(ret)) + ret
	}
	return ret
}

// ContainerGetHandler handles GET and HEAD requests for a container.
func (server *ContainerServer) ContainerGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	if err := request.ParseForm(); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	db, err := server.containerEngine.Get(vars)
	if err == ErrorNoSuchContainer {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else if err != nil {
		srv.GetLogger(request).LogError("Unable to get container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.containerEngine.Return(db)
	info, err := db.GetInfo()
	if err != nil {
		srv.GetLogger(request).LogError("Unable to get container info: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	headers := writer.Header()
	if ts, err := common.GetEpochFromTimestamp(info.CreatedAt); err == nil {
		headers.Set("X-Backend-Timestamp", ts)
	}
	if ts, err := common.GetEpochFromTimestamp(info.PutTimestamp); err == nil {
		headers.Set("X-Backend-Put-Timestamp", ts)
	}
	if ts, err := common.GetEpochFromTimestamp(info.DeleteTimestamp); err == nil {
		headers.Set("X-Backend-Delete-Timestamp", ts)
	}
	if ts, err := common.GetEpochFromTimestamp(info.StatusChangedAt); err == nil {
		headers.Set("X-Backend-Status-Changed-At", ts)
	}
	headers.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(info.StoragePolicyIndex))
	metadata, err := db.GetMetadata()
	if err != nil {
		srv.GetLogger(request).LogError("Unable to get metadata: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	for key, value := range metadata {
		headers.Set(key, value)
	}
	if deleted, err := db.IsDeleted(); err != nil {
		srv.GetLogger(request).LogError("Error calling IsDeleted: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	} else if deleted {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else {
		headers.Set("X-Container-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
		headers.Set("X-Container-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
		if ts, err := common.GetEpochFromTimestamp(info.CreatedAt); err == nil {
			headers.Set("X-Timestamp", ts)
		}
		if ts, err := common.GetEpochFromTimestamp(info.PutTimestamp); err == nil {
			headers.Set("X-Put-Timestamp", ts)
		}
	}
	if request.Method == "HEAD" {
		writer.WriteHeader(http.StatusNoContent)
		writer.Write([]byte(""))
		return
	}
	limit, _ := strconv.ParseInt(request.FormValue("limit"), 10, 64)
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	marker := request.Form.Get("marker")
	delimiter := request.Form.Get("delimiter")
	endMarker := request.Form.Get("end_marker")
	prefix := request.Form.Get("prefix")
	var path *string
	if v, ok := request.Form["path"]; ok && len(v) > 0 {
		path = &v[0]
	}
	policyIndex, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policyIndex = info.StoragePolicyIndex
	}
	reverse := common.LooksTrue(request.Form.Get("reverse"))
	objects, err := db.ListObjects(int(limit), marker, endMarker, prefix, delimiter, path, reverse, policyIndex)
	if err != nil {
		srv.GetLogger(request).LogError("Unable to list objects: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	format := request.Form.Get("format")
	if format == "" { /* TODO: real accept parsing */
		accept := request.Header.Get("Accept")
		if strings.Contains(accept, "application/json") {
			format = "json"
		} else if strings.Contains(accept, "application/xml") || strings.Contains(accept, "text/xml") {
			format = "xml"
		} else {
			format = "text"
		}
	}
	if format == "text" {
		response := ""
		for _, obj := range objects {
			if or, ok := obj.(*ObjectListingRecord); ok {
				response += or.Name + "\n"
			} else if sr, ok := obj.(*SubdirListingRecord); ok {
				response += sr.Name + "\n"
			}
		}
		if len(response) > 0 {
			headers.Set("Content-Length", strconv.Itoa(len(response)))
			writer.WriteHeader(200)
			writer.Write([]byte(response))
		} else {
			headers.Set("Content-Length", "0")
			writer.WriteHeader(204)
			writer.Write([]byte(""))
		}
	} else if format == "json" {
		output, err := json.Marshal(objects)
		if err != nil {
			srv.StandardResponse(writer, http.StatusInternalServerError)
			return
		}
		headers.Set("Content-Type", "application/json; charset=utf-8")
		headers.Set("Content-Length", strconv.Itoa(len(output)))
		writer.WriteHeader(200)
		writer.Write(output)
	} else if format == "xml" {
		type Container struct {
			XMLName xml.Name `xml:"container"`
			Name    string   `xml:"name,attr"`
			Objects []interface{}
		}
		container := &Container{Name: vars["container"], Objects: objects}
		writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
		output, _ := xml.MarshalIndent(container, "", "  ")
		headers.Set("Content-Length", strconv.Itoa(len(output)+39))
		writer.WriteHeader(200)
		writer.Write([]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"))
		writer.Write(output)
	}
}

// ContainerPutHandler handles PUT requests for a container.
func (server *ContainerServer) ContainerPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	if syncTo := request.Header.Get("X-Container-Sync-To"); syncTo != "" {
		if !server.syncRealms.ValidateSyncTo(syncTo) {
			srv.StandardResponse(writer, http.StatusBadRequest)
			return
		}
	}
	policyIndex, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policyIndex = -1
	}
	defaultPolicyIndex, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Default"))
	if err != nil {
		defaultPolicyIndex = server.defaultPolicy
	}
	metadata := make(map[string][]string)
	for key, values := range request.Header {
		_, inSaveHeaders := saveHeaders[key]
		if !(strings.HasPrefix(key, "X-Container-Meta-") || strings.HasPrefix(key, "X-Container-Sysmeta-") || inSaveHeaders) {
			continue
		} else if len(values) == 0 || values[0] == "" {
			continue
		}
		metadata[key] = []string{request.Header.Get(key), timestamp}
	}
	created, db, err := server.containerEngine.Create(vars, timestamp, metadata, policyIndex, defaultPolicyIndex)
	if err == ErrorPolicyConflict {
		srv.StandardResponse(writer, http.StatusConflict)
		return
	} else if err != nil {
		srv.GetLogger(request).LogError("Unable to create database: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.containerEngine.Return(db)
	if info, err := db.GetInfo(); err == nil {
		server.accountUpdate(request, vars, info, srv.GetLogger(request))
	}
	if created {
		srv.StandardResponse(writer, http.StatusCreated)
	} else {
		srv.StandardResponse(writer, http.StatusAccepted)
	}
}

// ContainerDeleteHandler handles DELETE requests for the container.
func (server *ContainerServer) ContainerDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	db, err := server.containerEngine.Get(vars)
	if err == ErrorNoSuchContainer {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else if err != nil {
		srv.GetLogger(request).LogError("Unable to get container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.containerEngine.Return(db)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	info, err := db.GetInfo()
	if err != nil {
		srv.GetLogger(request).LogError("Unable to get container info: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	if info.ObjectCount > 0 {
		srv.StandardResponse(writer, http.StatusConflict)
		return
	}
	if err = db.Delete(timestamp); err != nil {
		srv.GetLogger(request).LogError("Unable to delete database: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	info, err = db.GetInfo()
	if err == nil {
		server.accountUpdate(request, vars, info, srv.GetLogger(request))
	}
	writer.WriteHeader(http.StatusNoContent)
	writer.Write([]byte(""))
}

// ContainerPostHandler handles POST requests for a container.
func (server *ContainerServer) ContainerPostHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	if syncTo := request.Header.Get("X-Container-Sync-To"); syncTo != "" {
		if !server.syncRealms.ValidateSyncTo(syncTo) {
			srv.StandardResponse(writer, http.StatusBadRequest)
			return
		}
	}
	updates := make(map[string][]string)
	for key := range request.Header {
		_, inSaveHeaders := saveHeaders[key]
		if !(strings.HasPrefix(key, "X-Container-Meta-") || strings.HasPrefix(key, "X-Container-Sysmeta") || inSaveHeaders) {
			continue
		}
		updates[key] = []string{request.Header.Get(key), timestamp}
	}
	db, err := server.containerEngine.Get(vars)
	if err == ErrorNoSuchContainer {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else if err != nil {
		srv.GetLogger(request).LogError("Unable to get container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.containerEngine.Return(db)
	if deleted, err := db.IsDeleted(); err != nil {
		srv.GetLogger(request).LogError("Error calling IsDeleted: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	} else if deleted {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}
	if err := db.UpdateMetadata(updates); err == ErrorInvalidMetadata {
		srv.StandardResponse(writer, http.StatusBadRequest)
	} else if err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		writer.WriteHeader(http.StatusNoContent)
		writer.Write([]byte(""))
	}
}

// ObjPutHandler handles the PUT of object records to a container.
func (server *ContainerServer) ObjPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	contentType := request.Header.Get("X-Content-Type")
	etag := request.Header.Get("X-Etag")
	size, err := strconv.ParseInt(request.Header.Get("X-Size"), 10, 64)
	if err != nil || contentType == "" || etag == "" {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	policyIndex, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policyIndex = 0
	}
	db, err := server.containerEngine.Get(vars)
	if err == ErrorNoSuchContainer {
		if strings.HasPrefix(vars["account"], server.autoCreatePrefix) {
			if _, db, err = server.containerEngine.Create(vars, timestamp, map[string][]string{}, policyIndex, 0); err != nil {
				srv.GetLogger(request).LogError("Unable to auto-create container: %v", err)
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
		} else {
			srv.StandardResponse(writer, http.StatusNotFound)
			return
		}
	} else if err != nil {
		srv.GetLogger(request).LogError("Unable to get container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.containerEngine.Return(db)
	if err := db.PutObject(vars["obj"], timestamp, size, contentType, etag, policyIndex); err != nil {
		srv.GetLogger(request).LogError("Error adding object to container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	srv.StandardResponse(writer, http.StatusCreated)
}

// ObjDeleteHandler handles the DELETE of object records in a container.
func (server *ContainerServer) ObjDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	policyIndex, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policyIndex = 0
	}
	db, err := server.containerEngine.Get(vars)
	if err == ErrorNoSuchContainer {
		if strings.HasPrefix(vars["account"], server.autoCreatePrefix) {
			if _, db, err = server.containerEngine.Create(vars, timestamp, map[string][]string{}, policyIndex, 0); err != nil {
				srv.GetLogger(request).LogError("Unable to auto-create container: %v", err)
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
		} else {
			srv.StandardResponse(writer, http.StatusNotFound)
			return
		}
	} else if err != nil {
		srv.GetLogger(request).LogError("Unable to get container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.containerEngine.Return(db)
	if err := db.DeleteObject(vars["obj"], timestamp, policyIndex); err != nil {
		srv.GetLogger(request).LogError("Error adding object to container: %v", err)
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
	writer.Write([]byte(""))
}

// HealthcheckHandler implements a basic health check, that just returns "OK".
func (server *ContainerServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
}

// ReconHandler delegates incoming /recon calls to the common recon handler.
func (server *ContainerServer) ReconHandler(writer http.ResponseWriter, request *http.Request) {
	middleware.ReconHandler(server.driveRoot, writer, request)
}

// DiskUsageHandler returns information on the current outstanding HTTP requests per-disk.
func (server *ContainerServer) DiskUsageHandler(writer http.ResponseWriter, request *http.Request) {
	if data, err := server.diskInUse.MarshalJSON(); err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
	} else {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
	}
}

// LogRequest is a middleware that logs requests and also sets up a logger in the request context.
func (server *ContainerServer) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		newWriter := &srv.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		requestLogger := &srv.RequestLogger{Request: request, Logger: server.logger, W: newWriter}
		defer requestLogger.LogPanics("LOGGING REQUEST")
		start := time.Now()
		request = srv.SetLogger(request, requestLogger)
		next.ServeHTTP(newWriter, request)
		forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
		if (request.Method != "REPLICATE" && request.Method != "REPCONN") || server.logLevel == "DEBUG" {
			extraInfo := "-"
			if forceAcquire {
				extraInfo = "FA"
			}
			server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
				request.RemoteAddr,
				time.Now().Format("02/Jan/2006:15:04:05 -0700"),
				request.Method,
				common.Urlencode(request.URL.Path),
				newWriter.Status,
				common.GetDefault(newWriter.Header(), "Content-Length", "-"),
				common.GetDefault(request.Header, "Referer", "-"),
				common.GetDefault(request.Header, "X-Trans-Id", "-"),
				common.GetDefault(request.Header, "User-Agent", "-"),
				time.Since(start).Seconds(),
				extraInfo))
		}
	}
	return http.HandlerFunc(fn)
}

// AcquireDevice is a middleware that makes sure the device is available - mounted and not beyond its max concurrency.
func (server *ContainerServer) AcquireDevice(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		vars := srv.GetVars(request)
		if device, ok := vars["device"]; ok && device != "" {
			devicePath := filepath.Join(server.driveRoot, device)
			if server.checkMounts {
				if mounted, err := common.IsMount(devicePath); err != nil || !mounted {
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
		}
		next.ServeHTTP(writer, request)
	}
	return http.HandlerFunc(fn)
}

func (server *ContainerServer) updateDeviceLocks(seconds int64) {
	reloadTime := time.Duration(seconds) * time.Second
	for {
		time.Sleep(reloadTime)
		for _, key := range server.diskInUse.Keys() {
			lockPath := filepath.Join(server.driveRoot, key, "lock_device")
			if common.Exists(lockPath) {
				server.diskInUse.Lock(key)
			} else {
				server.diskInUse.Unlock(key)
			}
		}
	}
}

// GetHandler returns the server's http handler - it sets up routes and instantiates middleware.
func (server *ContainerServer) GetHandler(config conf.Config) http.Handler {
	commonHandlers := alice.New(server.LogRequest, middleware.ValidateRequest, server.AcquireDevice)
	router := srv.NewRouter()
	router.Get("/healthcheck", commonHandlers.ThenFunc(server.HealthcheckHandler))
	router.Get("/diskusage", commonHandlers.ThenFunc(server.DiskUsageHandler))
	router.Get("/recon/:method/:recon_type", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/recon/:method", commonHandlers.ThenFunc(server.ReconHandler))
	router.Put("/:device/tmp/:filename", commonHandlers.ThenFunc(server.ContainerTmpUploadHandler))
	router.Put("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjPutHandler))
	router.Delete("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjDeleteHandler))
	router.Put("/:device/:partition/:account/:container", commonHandlers.ThenFunc(server.ContainerPutHandler))
	router.Get("/:device/:partition/:account/:container", commonHandlers.ThenFunc(server.ContainerGetHandler))
	router.Head("/:device/:partition/:account/:container", commonHandlers.ThenFunc(server.ContainerGetHandler))
	router.Delete("/:device/:partition/:account/:container", commonHandlers.ThenFunc(server.ContainerDeleteHandler))
	router.Post("/:device/:partition/:account/:container", commonHandlers.ThenFunc(server.ContainerPostHandler))
	router.Replicate("/:device/:partition/:hash", commonHandlers.ThenFunc(server.ContainerReplicateHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("Invalid path: %s", r.URL.Path), http.StatusBadRequest)
	})
	return alice.New(middleware.GrepObject).Then(router)
}

// GetServer parses configs and command-line flags, returning a configured server object and the ip and port it should bind on.
func GetServer(serverconf conf.Config, flags *flag.FlagSet) (bindIP string, bindPort int, serv srv.Server, logger srv.LowLevelLogger, err error) {
	server := &ContainerServer{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: ""}
	server.syncRealms = GetSyncRealms()
	server.hashPathPrefix, server.hashPathSuffix, err = GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}
	policies := LoadPolicies()
	server.defaultPolicy = policies.Default()
	server.autoCreatePrefix = serverconf.GetDefault("app:container-server", "auto_create_account_prefix", ".")
	server.driveRoot = serverconf.GetDefault("app:container-server", "devices", "/srv/node")
	server.checkMounts = serverconf.GetBool("app:container-server", "mount_check", true)
	server.logLevel = serverconf.GetDefault("app:container-server", "log_level", "INFO")
	server.diskInUse = common.NewKeyedLimit(serverconf.GetLimit("app:container-server", "disk_limit", 25, 10000))
	bindIP = serverconf.GetDefault("app:container-server", "bind_ip", "0.0.0.0")
	bindPort = int(serverconf.GetInt("app:container-server", "bind_port", 6000))
	if server.logger, err = srv.SetupLogger(serverconf, flags, "app:container-server", "container-server"); err != nil {
		return "", 0, nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	server.containerEngine = newLRUEngine(server.driveRoot, server.hashPathPrefix, server.hashPathSuffix, 32)
	connTimeout := time.Duration(serverconf.GetFloat("app:container-server", "conn_timeout", 1.0) * float64(time.Second))
	nodeTimeout := time.Duration(serverconf.GetFloat("app:container-server", "node_timeout", 10.0) * float64(time.Second))
	server.updateClient = &http.Client{
		Timeout:   nodeTimeout,
		Transport: &http.Transport{Dial: (&net.Dialer{Timeout: connTimeout}).Dial},
	}
	return bindIP, bindPort, server, server.logger, nil
}
