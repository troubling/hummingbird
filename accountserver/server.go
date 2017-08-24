//  Copyright (c) 2016-2017 Rackspace
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

package accountserver

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" // install pprof http handlers
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/justinas/alice"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
)

// GetHashPrefixAndSuffix is a pointer to hummingbird's function of the same name, for overriding in tests.
var GetHashPrefixAndSuffix = conf.GetHashPrefixAndSuffix

// AccountServer contains all of the information for a running account server.
type AccountServer struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	logger           srv.LowLevelLogger
	logLevel         zap.AtomicLevel
	diskInUse        *common.KeyedLimit
	checkMounts      bool
	accountEngine    AccountEngine
	updateClient     *http.Client
	autoCreatePrefix string
	policyList       conf.PolicyList
	metricsCloser    io.Closer
}

func formatTimestamp(ts string) (string, error) {
	if len(ts) == 16 && ts[10] == '.' {
		return ts, nil
	}
	t, err := strconv.ParseFloat(ts, 64)
	if err != nil {
		return "", err
	}
	ret := strconv.FormatFloat(t, 'f', 5, 64)
	if len(ret) < 16 {
		return strings.Repeat("0", 16-len(ret)) + ret, nil
	}
	return ret, nil
}

func (server *AccountServer) Type() string {
	return "account"
}

func (server *AccountServer) Finalize() {
	if server.metricsCloser != nil {
		server.metricsCloser.Close()
	}
}

// AccountGetHandler handles GET and HEAD requests for an account.
func (server *AccountServer) AccountGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	if err := request.ParseForm(); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	db, err := server.accountEngine.Get(vars)
	if err == ErrorNoSuchAccount {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else if err != nil {
		srv.GetLogger(request).Error("Unable to get account.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.accountEngine.Return(db)
	info, err := db.GetInfo()
	if err != nil {
		srv.GetLogger(request).Error("Unable to get account info. ", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	headers := writer.Header()
	if ts, err := formatTimestamp(info.CreatedAt); err == nil {
		headers.Set("X-Backend-Timestamp", ts)
	}
	if ts, err := formatTimestamp(info.PutTimestamp); err == nil {
		headers.Set("X-Backend-Put-Timestamp", ts)
	}
	if ts, err := formatTimestamp(info.DeleteTimestamp); err == nil {
		headers.Set("X-Backend-Delete-Timestamp", ts)
	}
	if ts, err := formatTimestamp(info.StatusChangedAt); err == nil {
		headers.Set("X-Backend-Status-Changed-At", ts)
	}
	metadata, err := db.GetMetadata()
	if err != nil {
		srv.GetLogger(request).Error("Unable to get metadata.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	for key, value := range metadata {
		headers.Set(key, value)
	}
	if deleted, err := db.IsDeleted(); err != nil {
		srv.GetLogger(request).Error("Error calling IsDeleted.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	} else if deleted {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else {
		headers.Set("X-Account-Container-Count", strconv.FormatInt(info.ContainerCount, 10))
		headers.Set("X-Account-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
		headers.Set("X-Account-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
		if ts, err := formatTimestamp(info.CreatedAt); err == nil {
			headers.Set("X-Timestamp", ts)
		}
		if ts, err := formatTimestamp(info.PutTimestamp); err == nil {
			headers.Set("X-Put-Timestamp", ts)
		}
	}
	if policyStats, err := db.PolicyStats(); err != nil {
		srv.GetLogger(request).Error("Error calling PolicyStats.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	} else {
		for _, policyStat := range policyStats {
			var prefix string
			if policy := server.policyList[policyStat.StoragePolicyIndex]; policy != nil {
				prefix = fmt.Sprintf("X-Account-Storage-Policy-%s-", policy.Name)
			} else {
				prefix = fmt.Sprintf("X-Account-Storage-Policy-%d-", policyStat.StoragePolicyIndex)
			}
			headers.Set(prefix+"Container-Count", fmt.Sprintf("%d", policyStat.ContainerCount))
			headers.Set(prefix+"Object-Count", fmt.Sprintf("%d", policyStat.ContainerCount))
			headers.Set(prefix+"Bytes-Used", fmt.Sprintf("%d", policyStat.BytesUsed))
		}
	}
	if request.Method == "HEAD" {
		headers.Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusNoContent)
		writer.Write([]byte(""))
		return
	}
	limit, _ := strconv.ParseInt(request.FormValue("limit"), 10, 64)
	if limit > 10000 {
		srv.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	} else if limit <= 0 {
		limit = 10000
	}
	marker := request.Form.Get("marker")
	delimiter := request.Form.Get("delimiter")
	endMarker := request.Form.Get("end_marker")
	prefix := request.Form.Get("prefix")
	reverse := common.LooksTrue(request.Form.Get("reverse"))
	containers, err := db.ListContainers(int(limit), marker, endMarker, prefix, delimiter, reverse)
	if err != nil {
		srv.GetLogger(request).Error("Unable to list containers.", zap.Error(err))
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
		for _, obj := range containers {
			if or, ok := obj.(*ContainerListingRecord); ok {
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
		output, err := json.Marshal(containers)
		if err != nil {
			srv.StandardResponse(writer, http.StatusInternalServerError)
			return
		}
		headers.Set("Content-Type", "application/json; charset=utf-8")
		headers.Set("Content-Length", strconv.Itoa(len(output)))
		writer.WriteHeader(200)
		writer.Write(output)
	} else if format == "xml" {
		type Account struct {
			XMLName    xml.Name `xml:"account"`
			Name       string   `xml:"name,attr"`
			Containers []interface{}
		}
		container := &Account{Name: vars["account"], Containers: containers}
		writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
		output, _ := xml.Marshal(container)
		headers.Set("Content-Length", strconv.Itoa(len(output)+39))
		writer.WriteHeader(200)
		writer.Write([]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"))
		writer.Write(output)
	}
}

// AccountPutHandler handles PUT requests for an account.
func (server *AccountServer) AccountPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	metadata := make(map[string][]string)
	for key := range request.Header {
		if strings.HasPrefix(key, "X-Account-Meta-") || strings.HasPrefix(key, "X-Account-Sysmeta-") {
			metadata[key] = []string{request.Header.Get(key), timestamp}
		}
	}
	created, db, err := server.accountEngine.Create(vars, timestamp, metadata)
	if err != nil {
		srv.GetLogger(request).Error("Unable to create database.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.accountEngine.Return(db)
	if created {
		srv.StandardResponse(writer, http.StatusCreated)
	} else {
		srv.StandardResponse(writer, http.StatusAccepted)
	}
}

// AccountDeleteHandler handles DELETE requests for an account.
func (server *AccountServer) AccountDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	db, err := server.accountEngine.Get(vars)
	if err == ErrorNoSuchAccount {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else if err != nil {
		srv.GetLogger(request).Error("Unable to get account.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.accountEngine.Return(db)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	info, err := db.GetInfo()
	if err != nil {
		srv.GetLogger(request).Error("Unable to get account info.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	if info.ContainerCount > 0 {
		srv.StandardResponse(writer, http.StatusConflict)
		return
	}
	if err = db.Delete(timestamp); err != nil {
		srv.GetLogger(request).Error("Unable to delete database", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	info, err = db.GetInfo()
	writer.WriteHeader(http.StatusNoContent)
	writer.Write([]byte(""))
}

// AccountPostHandler handles POST requests for an account.
func (server *AccountServer) AccountPostHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	timestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	updates := make(map[string][]string)
	for key := range request.Header {
		if strings.HasPrefix(key, "X-Account-Meta-") || strings.HasPrefix(key, "X-Account-Sysmeta-") {
			updates[key] = []string{request.Header.Get(key), timestamp}
		}
	}
	db, err := server.accountEngine.Get(vars)
	if err == ErrorNoSuchAccount {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	} else if err != nil {
		srv.GetLogger(request).Error("Unable to get account.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.accountEngine.Return(db)
	if deleted, err := db.IsDeleted(); err != nil {
		srv.GetLogger(request).Error("Error calling IsDeleted", zap.Error(err))
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

// ContainerPutHandler handles the PUT of container records to an account.
func (server *AccountServer) ContainerPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	putTimestamp, err := common.StandardizeTimestamp(request.Header.Get("X-Put-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	var objectCount, bytesUsed, storagePolicyIndex int64
	if objectCount, err = strconv.ParseInt(request.Header.Get("X-Object-Count"), 10, 64); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	if bytesUsed, err = strconv.ParseInt(request.Header.Get("X-Bytes-Used"), 10, 64); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	if storagePolicyIndex, err = strconv.ParseInt(request.Header.Get("X-Backend-Storage-Policy-Index"), 10, 64); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	deleteTimestamp := request.Header.Get("X-Delete-Timestamp")
	db, err := server.accountEngine.Get(vars)
	if err == ErrorNoSuchAccount {
		if strings.HasPrefix(vars["account"], server.autoCreatePrefix) {
			if _, db, err = server.accountEngine.Create(vars, putTimestamp, map[string][]string{}); err != nil {
				srv.GetLogger(request).Error("Unable to auto-create account.", zap.Error(err))
				srv.StandardResponse(writer, http.StatusInternalServerError)
				return
			}
		} else {
			srv.StandardResponse(writer, http.StatusNotFound)
			return
		}
	} else if err != nil {
		srv.GetLogger(request).Error("Unable to get account.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer server.accountEngine.Return(db)
	if err := db.PutContainer(vars["container"], putTimestamp, deleteTimestamp, objectCount, bytesUsed, int(storagePolicyIndex)); err != nil {
		srv.GetLogger(request).Error("Error adding object to container.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	srv.StandardResponse(writer, http.StatusCreated)
}

// HealthcheckHandler implements a basic health check, that just returns "OK".
func (server *AccountServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
}

// ReconHandler delegates incoming /recon calls to the common recon handler.
func (server *AccountServer) ReconHandler(writer http.ResponseWriter, request *http.Request) {
	middleware.ReconHandler(server.driveRoot, server.checkMounts, writer, request)
}

// DiskUsageHandler returns information on the current outstanding HTTP requests per-disk.
func (server *AccountServer) DiskUsageHandler(writer http.ResponseWriter, request *http.Request) {
	if data, err := server.diskInUse.MarshalJSON(); err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
	} else {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
	}
}

// LogRequest is a middleware that logs requests and also sets up a logger in the request context.
func (server *AccountServer) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		newWriter := &srv.WebWriter{ResponseWriter: writer, Status: 500}
		start := time.Now()
		logr := server.logger.With(zap.String("txn", request.Header.Get("X-Trans-Id")))
		request = srv.SetLogger(request, logr)
		next.ServeHTTP(newWriter, request)
		forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
		lvl, _ := server.logLevel.MarshalText()
		if (request.Method != "REPLICATE" && request.Method != "REPCONN") || strings.ToUpper(string(lvl)) == "DEBUG" {
			extraInfo := "-"
			if forceAcquire {
				extraInfo = "FA"
			}
			logr.Info("Request log",
				zap.String("remoteAddr", request.RemoteAddr),
				zap.String("eventTime", time.Now().Format("02/Jan/2006:15:04:05 -0700")),
				zap.String("method", request.Method),
				zap.String("urlPath", common.Urlencode(request.URL.Path)),
				zap.Int("status", newWriter.Status),
				zap.String("contentLength", common.GetDefault(newWriter.Header(), "Content-Length", "-")),
				zap.String("referer", common.GetDefault(request.Header, "Referer", "-")),
				zap.String("userAgent", common.GetDefault(request.Header, "User-Agent", "-")),
				zap.Float64("requestTimeSeconds", time.Since(start).Seconds()),
				zap.Float64("requestTimeToHeaderSeconds", newWriter.ResponseStarted.Sub(start).Seconds()),
				zap.String("extraInfo", extraInfo))
		}
	}
	return http.HandlerFunc(fn)
}

// AcquireDevice is a middleware that makes sure the device is available - mounted and not beyond its max concurrency.
func (server *AccountServer) AcquireDevice(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		vars := srv.GetVars(request)
		if device, ok := vars["device"]; ok && device != "" {
			devicePath := filepath.Join(server.driveRoot, device)
			if server.checkMounts {
				if mounted, err := fs.IsMount(devicePath); err != nil || !mounted {
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

func (server *AccountServer) updateDeviceLocks(seconds int64) {
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

// GetHandler returns the server's http handler - it sets up routes and instantiates middleware.
func (server *AccountServer) GetHandler(config conf.Config, metricsPrefix string) http.Handler {
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
	router.Get("/recon/:method/:recon_type", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/recon/:method", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	router.Put("/:device/tmp/:filename", commonHandlers.ThenFunc(server.TmpUploadHandler))
	router.Put("/:device/:partition/:account/:container", commonHandlers.ThenFunc(server.ContainerPutHandler))
	router.Put("/:device/:partition/:account", commonHandlers.ThenFunc(server.AccountPutHandler))
	router.Delete("/:device/:partition/:account", commonHandlers.ThenFunc(server.AccountDeleteHandler))
	router.Get("/:device/:partition/:account", commonHandlers.ThenFunc(server.AccountGetHandler))
	router.Head("/:device/:partition/:account", commonHandlers.ThenFunc(server.AccountGetHandler))
	router.Post("/:device/:partition/:account", commonHandlers.ThenFunc(server.AccountPostHandler))
	router.Replicate("/:device/:partition/:hash", commonHandlers.ThenFunc(server.AccountReplicateHandler))
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("Invalid path: %s", r.URL.Path), http.StatusBadRequest)
	})
	return alice.New(middleware.Metrics(metricsScope)).Append(middleware.GrepObject).Then(router)
}

// GetServer parses configs and command-line flags, returning a configured server object and the ip and port it should bind on.
func GetServer(serverconf conf.Config, flags *flag.FlagSet) (bindIP string, bindPort int, serv srv.Server, logger srv.LowLevelLogger, err error) {
	server := &AccountServer{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "", policyList: conf.LoadPolicies()}
	server.hashPathPrefix, server.hashPathSuffix, err = GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}
	server.autoCreatePrefix = serverconf.GetDefault("app:account-server", "auto_create_account_prefix", ".")
	server.driveRoot = serverconf.GetDefault("app:account-server", "devices", "/srv/node")
	server.checkMounts = serverconf.GetBool("app:account-server", "mount_check", true)
	server.diskInUse = common.NewKeyedLimit(serverconf.GetLimit("app:account-server", "disk_limit", 0, 0))
	bindIP = serverconf.GetDefault("app:account-server", "bind_ip", "0.0.0.0")
	bindPort = int(serverconf.GetInt("app:account-server", "bind_port", 6000))

	logLevelString := serverconf.GetDefault("app:account-server", "log_level", "INFO")
	server.logLevel = zap.NewAtomicLevel()
	server.logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	if server.logger, err = srv.SetupLogger("account-server", &server.logLevel, flags); err != nil {
		return "", 0, nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	server.accountEngine = newLRUEngine(server.driveRoot, server.hashPathPrefix, server.hashPathSuffix, 32)
	connTimeout := time.Duration(serverconf.GetFloat("app:account-server", "conn_timeout", 1.0) * float64(time.Second))
	nodeTimeout := time.Duration(serverconf.GetFloat("app:account-server", "node_timeout", 10.0) * float64(time.Second))
	server.updateClient = &http.Client{
		Timeout:   nodeTimeout,
		Transport: &http.Transport{Dial: (&net.Dialer{Timeout: connTimeout}).Dial},
	}
	return bindIP, bindPort, server, server.logger, nil
}
