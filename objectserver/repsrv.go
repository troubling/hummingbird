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
	"bufio"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/justinas/alice"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
)

func (r *Replicator) incomingBegin(device string, timeout time.Duration) bool {
	r.incomingSemLock.Lock()
	devSem, ok := r.incomingSem[device]
	if !ok {
		devSem = make(chan struct{}, r.incomingLimitPerDev)
		r.incomingSem[device] = devSem
	}
	r.incomingSemLock.Unlock()
	select {
	case devSem <- struct{}{}:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (r *Replicator) incomingDone(device string) {
	r.incomingSemLock.Lock()
	sem := r.incomingSem[device]
	r.incomingSemLock.Unlock()
	<-sem
}

// ProgressReportHandler handles HTTP requests for current replication progress
func (r *Replicator) ProgressReportHandler(w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)
	data, err := json.Marshal(r.getDeviceProgress(vars["name"]))
	if err != nil {
		r.logger.Error("Error Marshaling device progress", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
	return
}

// priorityRepHandler handles HTTP requests for priority replications jobs.
func (r *Replicator) priorityRepHandler(w http.ResponseWriter, req *http.Request) {
	var pri PriorityRepJob
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	if err := json.Unmarshal(data, &pri); err != nil {
		w.WriteHeader(400)
		return
	}
	if r.checkMounts {
		if mounted, err := fs.IsMount(filepath.Join(r.deviceRoot, pri.FromDevice.Device)); err != nil || mounted == false {
			w.WriteHeader(507)
			return
		}
	}
	r.runningDevicesLock.Lock()
	rd, ok := r.runningDevices[deviceKeyId(pri.FromDevice.Device, pri.Policy)]
	r.runningDevicesLock.Unlock()
	if ok {
		rd.PriorityReplicate(w, pri)
	} else {
		w.WriteHeader(404)
	}
}

// priorityRepHandler handles HTTP requests for priority replications jobs.
func (r *Replicator) stabilizeHandler(w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)
	policy, err := strconv.Atoi(req.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}
	engine, ok := r.objEngines[policy]
	if !ok {
		r.logger.Error("invalid policy to stabilizeHandler")
		w.WriteHeader(500)
		return
	}
	oring, ok := r.objectRings[policy]
	if !ok {
		r.logger.Error("ring not found for policy")
		w.WriteHeader(500)
		return
	}
	ringDevices, err := oring.LocalDevices(r.port)
	if err != nil {
		r.logger.Error("Error getting local devices from ring", zap.Error(err))
		w.WriteHeader(500)
		return
	}
	var rdev *ring.Device
	for _, dev := range ringDevices {
		if dev.Device == vars["device"] {
			rdev = dev
			break
		}
	}
	if rdev == nil {
		r.logger.Error("could not find device in ring", zap.String("device", vars["device"]))
		w.WriteHeader(500)
		return
	}

	o, err := engine.New(vars, false, &r.asyncWG)
	if err != nil {
		r.logger.Error("could not build stabilizeHandler Object", zap.Error(err))
		w.WriteHeader(500)
		return
	}
	key := deviceKeyId(vars["device"], policy)
	if rd, ok := r.runningDevices[key]; ok {
		if os, ok := o.(ObjectStabilizer); ok {
			if err = os.Stabilize(rdev); err == nil {
				rd.UpdateStat("objectsPeerStabilized", 1)
			}
		}
		w.WriteHeader(200)
		return
	}
	w.WriteHeader(404)
}

func (r *Replicator) objReplicateHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)

	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}
	policy, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}
	hashes, err := GetHashes(r.deviceRoot, vars["device"], vars["partition"], recalculate, r.reclaimAge, policy, srv.GetLogger(request))
	if err != nil {
		srv.GetLogger(request).Error("Unable to get hashes",
			zap.String("Device", vars["device"]),
			zap.String("Partition", vars["partition"]), zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(pickle.PickleDumps(hashes))
}

func (r *Replicator) objRepConnHandler(writer http.ResponseWriter, request *http.Request) {
	var conn net.Conn
	var rw *bufio.ReadWriter
	var err error
	var brr BeginReplicationRequest

	vars := srv.GetVars(request)
	startTime := time.Now()

	policy, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}

	writer.WriteHeader(http.StatusOK)
	if hijacker, ok := writer.(http.Hijacker); !ok {
		srv.GetLogger(request).Error("[ObjRepConnHandler] Writer not a Hijacker")
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	} else if conn, rw, err = hijacker.Hijack(); err != nil {
		srv.GetLogger(request).Error("[ObjRepConnHandler] Hijack failed", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	rc := NewIncomingRepConn(rw, conn, r.rcTimeout)
	if err := rc.RecvMessage(&brr); err != nil {
		srv.GetLogger(request).Error("[ObjRepConnHandler] Error receiving BeginReplicationRequest", zap.Error(err))
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if request.Header.Get("X-Force-Acquire") != "true" {
		if !r.incomingBegin(brr.Device, replicateIncomingTimeout) {
			srv.GetLogger(request).Error("[ObjRepConnHandler] Timed out waiting for concurrency slot")
			writer.WriteHeader(503)
			return
		}
		defer r.incomingDone(brr.Device)
	}
	var hashes map[string]string
	if brr.NeedHashes {
		hashes, err = GetHashes(r.deviceRoot, brr.Device, brr.Partition, nil, r.reclaimAge, policy, srv.GetLogger(request))
		if err != nil {
			srv.GetLogger(request).Error("[ObjRepConnHandler] Error getting hashes", zap.Error(err))
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	if err := rc.SendMessage(BeginReplicationResponse{Hashes: hashes}); err != nil {
		srv.GetLogger(request).Error("[ObjRepConnHandler] Error sending BeginReplicationResponse", zap.Duration("connectionTime", time.Since(startTime)), zap.Error(err))
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	sfrsProcessed := int64(0)
	startTime = time.Now()
	for {
		errType, err := func() (string, error) { // this is a closure so we can use defers inside
			var sfr SyncFileRequest
			sfrsProcessed++
			if err := rc.RecvMessage(&sfr); err != nil {
				return "receiving SyncFileRequest", err
			}
			if sfr.Done {
				return "", replicationDone
			}
			if sfr.Ping {
				return "ping", rc.SendMessage(SyncFileResponse{Msg: "pong"})
			}
			tempDir := TempDirPath(r.deviceRoot, vars["device"])
			fileName := filepath.Join(r.deviceRoot, sfr.Path)
			hashDir := filepath.Dir(fileName)

			if ext := filepath.Ext(fileName); (ext != ".data" && ext != ".ts" && ext != ".meta") || len(filepath.Base(filepath.Dir(fileName))) != 32 {
				return "invalid file path", rc.SendMessage(SyncFileResponse{Msg: "bad file path"})
			}
			if fs.Exists(fileName) {
				return "file exists", rc.SendMessage(SyncFileResponse{Exists: true, Msg: "exists"})
			}
			dataFile, metaFile := ObjectFiles(hashDir)
			if filepath.Base(fileName) < filepath.Base(dataFile) || filepath.Base(fileName) < filepath.Base(metaFile) {
				return "newer file exists", rc.SendMessage(SyncFileResponse{NewerExists: true, Msg: "newer exists"})
			}
			if sfr.Check {
				return "just check", rc.SendMessage(SyncFileResponse{Exists: false, Msg: "doesn't exist"})
			}
			tempFile, err := fs.NewAtomicFileWriter(tempDir, hashDir)
			if err != nil {
				return "creating file writer", err
			}
			defer tempFile.Abandon()
			if err := tempFile.Preallocate(sfr.Size, r.reserve); err != nil {
				return "preallocating space", err
			}
			if xattrs, err := hex.DecodeString(sfr.Xattrs); err != nil || len(xattrs) == 0 {
				return "parsing xattrs", rc.SendMessage(SyncFileResponse{Msg: "bad xattrs"})
			} else if err := common.SwiftObjectRawWriteMetadata(tempFile.Fd(), xattrs); err != nil {
				return "writing metadata", err
			}
			if err := rc.SendMessage(SyncFileResponse{GoAhead: true, Msg: "go ahead"}); err != nil {
				return "sending go ahead", err
			}
			if _, err := common.CopyN(rc, sfr.Size, tempFile); err != nil {
				return "copying data", err
			}
			if err := tempFile.Save(fileName); err != nil {
				return "saving file", err
			}
			if dataFile != "" || metaFile != "" {
				HashCleanupListDir(hashDir, r.reclaimAge)
			}
			InvalidateHash(hashDir)
			err = rc.SendMessage(FileUploadResponse{Success: true, Msg: "YAY"})
			return "file done", err
		}()
		if err == replicationDone {
			return
		} else if err != nil {
			srv.GetLogger(request).Error("[ObjRepConnHandler] Error replicating",
				zap.String("errType", errType),
				zap.Int64("sfrsProcessed", sfrsProcessed),
				zap.Duration("sfrProcessTime", time.Since(startTime)),
				zap.Error(err))
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func (r *Replicator) LogRequest(next http.Handler) http.Handler {
	return srv.LogRequest(r.logger, next)
}

func (r *Replicator) GetHandler(config conf.Config, metricsPrefix string) http.Handler {
	var metricsScope tally.Scope
	metricsScope, r.metricsCloser = tally.NewRootScope(tally.ScopeOptions{
		Prefix:         metricsPrefix,
		Tags:           map[string]string{},
		CachedReporter: promreporter.NewReporter(promreporter.Options{}),
		Separator:      promreporter.DefaultSeparator,
	}, time.Second)
	commonHandlers := alice.New(
		middleware.NewDebugResponses(config.GetBool("debug", "debug_x_source_code", false)),
		r.LogRequest,
		middleware.RecoverHandler,
		middleware.ValidateRequest,
	)
	router := srv.NewRouter()
	router.Get("/metrics", prometheus.Handler())
	router.Get("/loglevel", r.logLevel)
	router.Put("/loglevel", r.logLevel)
	router.Get("/healthcheck", commonHandlers.ThenFunc(r.HealthcheckHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/priorityrep", commonHandlers.ThenFunc(r.priorityRepHandler))
	router.Post("/stabilize/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(r.stabilizeHandler))
	router.Get("/progress/:name", commonHandlers.ThenFunc(r.ProgressReportHandler))
	for _, policy := range r.policies {
		router.HandlePolicy("REPCONN", "/:device/:partition", policy.Index, commonHandlers.ThenFunc(r.objRepConnHandler))
		router.HandlePolicy("REPLICATE", "/:device/:partition/:suffixes", policy.Index, commonHandlers.ThenFunc(r.objReplicateHandler))
		router.HandlePolicy("REPLICATE", "/:device/:partition", policy.Index, commonHandlers.ThenFunc(r.objReplicateHandler))
	}
	router.Get("/debug/*_", http.DefaultServeMux)
	for policy, objEngine := range r.objEngines {
		if rhoe, ok := objEngine.(PolicyHandlerRegistrator); ok {
			rhoe.RegisterHandlers(func(method, path string, handler http.HandlerFunc) {
				router.HandlePolicy(method, path, policy, commonHandlers.ThenFunc(handler))
			})
		}
	}
	return alice.New(middleware.Metrics(metricsScope), middleware.ServerTracer(r.tracer)).Then(router)
}
