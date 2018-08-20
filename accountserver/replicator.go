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

// NOTE: Replication requests use the standard ip and port as the
// account-replicator web service is just for metrics at this time.

package accountserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/justinas/alice"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/tracing"
	"github.com/troubling/hummingbird/middleware"
	"github.com/troubling/nectar"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

var (
	errDeviceNotMounted = errors.New("Remove drive was unmounted")
	deviceLockupTimeout = time.Hour
	// GetRing is a local pointer to the hummingbird function, for overriding in tests.
	GetRing = ring.GetRing
)

// Replicator is the account replicator daemon object
type Replicator struct {
	checkMounts       bool
	deviceRoot        string
	reconCachePath    string
	logger            srv.LowLevelLogger
	serverPort        int
	Ring              ring.Ring
	perUsync          int64
	maxUsyncs         int
	concurrencySem    chan struct{}
	sendStat          chan statUpdate
	checkin           chan string
	startRun          chan string
	client            common.HTTPClient
	certFile          string
	keyFile           string
	runningDevices    map[string]*replicationDevice
	reclaimAge        int64
	logLevel          zap.AtomicLevel
	metricsCloser     io.Closer
	traceCloser       io.Closer
	tracer            opentracing.Tracer
	clientTracer      opentracing.Tracer
	clientTraceCloser io.Closer
	reaperLock        sync.Mutex
	reaperLastCheckin time.Time
	reaperCanceler    chan struct{}
	reaperCheckin     chan struct{}
}

type statUpdate struct {
	device string
	stat   string
	value  int64
}

type replicationDevice struct {
	i interface {
		sendReplicationMessage(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error)
		sync(dev *ring.Device, part uint64, ringHash string, info *AccountInfo) (*AccountInfo, error)
		rsync(dev *ring.Device, c ReplicableAccount, part uint64, op string) error
		usync(dev *ring.Device, c ReplicableAccount, part uint64, localID string, point int64) error
		chooseReplicationStrategy(localInfo, remoteInfo *AccountInfo, usyncThreshold int64) string
		replicateDatabaseToDevice(dev *ring.Device, c ReplicableAccount, part uint64) error
		replicateDatabase(dbFile string) error
		checkForReaping(dbFile string) error
		findAccountDbs(devicePath string, results chan string)
		incrementStat(stat string)
	}
	r             *Replicator
	cancel        chan struct{}
	dev           *ring.Device
	stats         map[string]int64
	lifetimeStats map[string]int64
	lastCheckin   time.Time
	runStarted    time.Time
	deviceStarted time.Time
}

func (rd *replicationDevice) sendReplicationMessage(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
	body, err := json.Marshal(args)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequest("REPLICATE", fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme,
		dev.Ip, dev.Port, dev.Device, part, ringHash), bytes.NewBuffer(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-Backend-Suppress-2xx-Logging", "t")
	req.Cancel = rd.cancel
	resp, err := rd.r.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, respBody, nil
}

func (rd *replicationDevice) sync(dev *ring.Device, part uint64, ringHash string, info *AccountInfo) (*AccountInfo, error) {
	var remoteInfo AccountInfo
	status, body, err := rd.i.sendReplicationMessage(dev, part, ringHash, "sync", info.MaxRow, info.Hash,
		info.ID, info.CreatedAt, info.PutTimestamp, info.DeleteTimestamp, info.RawMetadata)
	if err != nil {
		return nil, fmt.Errorf("sending sync request to %s/%s: %v", dev.Ip, dev.Device, err)
	} else if status == http.StatusNotFound {
		return nil, nil
	} else if status == http.StatusInsufficientStorage {
		return nil, errDeviceNotMounted
	} else if status/100 != 2 {
		return nil, fmt.Errorf("bad status code %d", status)
	}
	if err := json.Unmarshal(body, &remoteInfo); err != nil {
		return nil, fmt.Errorf("unmarshalling sync response: %v", err)
	}
	return &remoteInfo, nil
}

func (rd *replicationDevice) rsync(dev *ring.Device, c ReplicableAccount, part uint64, op string) error {
	tmpFilename := common.UUID()
	fp, release, err := c.OpenDatabaseFile()
	if err != nil {
		return fmt.Errorf("Error opening databae: %v", err)
	}
	defer release()
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/%s/tmp/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, tmpFilename), fp)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}
	req.Cancel = rd.cancel
	resp, err := rd.r.client.Do(req)
	if err != nil {
		return fmt.Errorf("putting database to %s/%s: %v", dev.Ip, dev.Device, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad status code %d rsyncing file with %s/%s", resp.StatusCode, dev.Ip, dev.Device)
	}
	status, _, err := rd.i.sendReplicationMessage(dev, part, c.RingHash(), op, tmpFilename)
	if err != nil || status/100 != 2 {
		return fmt.Errorf("sending %s message to %s/%s: status %d: %v", op, dev.Ip, dev.Device, status, err)
	}
	return nil
}

func (rd *replicationDevice) usync(dev *ring.Device, c ReplicableAccount, part uint64, localID string, point int64) error {
	objects, err := c.ItemsSince(point, int(rd.r.perUsync))
	if err != nil {
		return fmt.Errorf("getting object records from %s: %v", c.RingHash(), err)
	}
	usyncs := 0
	syncTable, err := c.SyncTable()
	if err != nil {
		return fmt.Errorf("Error getting sync table: %v", err)
	}
	for len(objects) != 0 && usyncs < rd.r.maxUsyncs {
		status, _, err := rd.i.sendReplicationMessage(dev, part, c.RingHash(), "merge_items", objects, localID)
		if err != nil || status/100 != 2 {
			return fmt.Errorf("Bad response to merge_items with %s/%s: status %d: %v", dev.Ip, dev.Device, status, err)
		}
		point = objects[len(objects)-1].Rowid
		usyncs++
		if objects, err = c.ItemsSince(point, int(rd.r.perUsync)); err != nil {
			return fmt.Errorf("getting object records from database: %s, %v", c.RingHash(), err)
		}
	}
	if usyncs >= rd.r.maxUsyncs {
		rd.i.incrementStat("diff_capped")
		return fmt.Errorf("capping usync at %d requests", usyncs)
	}
	status, _, err := rd.i.sendReplicationMessage(dev, part, c.RingHash(), "merge_syncs", syncTable)
	if err != nil {
		return err
	}
	if status/100 != 2 {
		return fmt.Errorf("Invalid status code from merge_syncs: %d", status)
	}
	return nil
}

func (rd *replicationDevice) chooseReplicationStrategy(localInfo, remoteInfo *AccountInfo, usyncThreshold int64) string {
	switch {
	case remoteInfo == nil:
		return "complete_rsync"
	case localInfo.MaxRow == -1:
		return "empty"
	case localInfo.MaxRow == remoteInfo.Point:
		return "no_change"
	case localInfo.Hash == remoteInfo.Hash:
		return "hashmatch"
	default:
		return "diff"
	}
}

func (rd *replicationDevice) replicateDatabaseToDevice(dev *ring.Device, c ReplicableAccount, part uint64) error {
	rd.i.incrementStat("attempted")
	info, err := c.GetInfo()
	if err != nil {
		return fmt.Errorf("getting local info from %s: %v", c.RingHash(), err)
	}
	remoteInfo, err := rd.i.sync(dev, part, c.RingHash(), info)
	if err != nil {
		return err
	}
	strategy := rd.i.chooseReplicationStrategy(info, remoteInfo, rd.r.perUsync*3)
	rd.i.incrementStat(strategy)
	switch strategy {
	case "empty", "hashmatch", "no_change":
		rd.r.logger.Debug("Not replicating anything.",
			zap.String("strategy", strategy),
			zap.String("RingHash", c.RingHash()))
	case "complete_rsync":
		rd.r.logger.Debug("Replicating ringhash",
			zap.String("RingHash", c.RingHash()),
			zap.String("Ip", dev.Ip),
			zap.String("Device", dev.Device),
			zap.String("strategy", strategy))
		return rd.i.rsync(dev, c, part, strategy)
	case "diff":
		rd.r.logger.Debug("Replicating ringhash",
			zap.String("RingHash", c.RingHash()),
			zap.String("Ip", dev.Ip),
			zap.String("Device", dev.Device),
			zap.String("strategy", strategy))
		return rd.i.usync(dev, c, part, info.ID, remoteInfo.Point)
	}
	return nil
}

func (rd *replicationDevice) checkForReaping(dbFile string) error {
	c, err := sqliteOpenAccount(dbFile)
	if err != nil {
		return err
	}
	defer c.Close()
	if d, err := c.IsDeleted(); err != nil {
		return err
	} else if !d {
		return nil
	}
	parts := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(dbFile))))
	part, err := strconv.ParseUint(parts, 10, 64)
	if err != nil {
		return fmt.Errorf("Bad partition: %s", parts)
	}
	devs := rd.r.Ring.GetNodes(part)
	if len(devs) == 0 || devs[0].Id != rd.dev.Id {
		return nil
	}
	info, err := c.GetInfo()
	if err != nil {
		return err
	}
	doDelete := false
	if info.DeleteTimestamp > info.PutTimestamp {
		if dti, err := strconv.ParseFloat(info.DeleteTimestamp, 64); err == nil {
			dt := time.Unix(int64(dti), 0)
			cutOff := time.Now().Add(time.Second * time.Duration(-rd.r.reclaimAge))

			if dt.Before(cutOff) {
				doDelete = true
			}
		} else {
			return err
		}
	}
	if doDelete {
		rd.r.TryToReapAccount(dbFile)
	}
	return nil
}

func (rd *replicationDevice) replicateDatabase(dbFile string) error {
	rd.r.logger.Debug("Replicating database.", zap.String("dbFile", filepath.Base(dbFile)))
	parts := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(dbFile))))
	part, err := strconv.ParseUint(parts, 10, 64)
	if err != nil {
		return fmt.Errorf("Bad partition: %s", parts)
	}
	devices, handoff := rd.r.Ring.GetJobNodes(part, rd.dev.Id)
	moreNodes := rd.r.Ring.GetMoreNodes(part)
	c, err := sqliteOpenAccount(dbFile)
	if err != nil {
		return err
	}
	if isD, err := c.IsDeleted(); err == nil && isD {
		doDelete := false
		if info, err := c.GetInfo(); err == nil && info.ObjectCount == 0 && info.DeleteTimestamp > info.PutTimestamp {
			if dti, err := strconv.ParseFloat(info.DeleteTimestamp, 64); err == nil {
				dt := time.Unix(int64(dti), 0)
				cutOff := time.Now().Add(time.Second * time.Duration(-rd.r.reclaimAge))
				if dt.Before(cutOff) {
					doDelete = true
				}
			}
		}
		if doDelete {
			c.Close()
			return c.Remove()
		}
	}
	defer c.Close()
	if err := c.CleanupTombstones(rd.r.reclaimAge); err != nil {
		return err
	}
	successes := 0
	for i := 0; i < len(devices); i++ {
		if err := rd.i.replicateDatabaseToDevice(devices[i], c, part); err == nil {
			rd.i.incrementStat("success")
			rd.r.logger.Debug("Succeeded replicating database.",
				zap.String("dbFile", dbFile),
				zap.String("Ip", devices[i].Ip),
				zap.String("Device", devices[i].Device))
			successes++
		} else {
			rd.i.incrementStat("failure")
			rd.r.logger.Error("Error replicating database.",
				zap.String("dbFile", dbFile),
				zap.String("Ip", devices[i].Ip),
				zap.String("Device", devices[i].Device),
				zap.Error(err))
			if err == errDeviceNotMounted && !handoff {
				next := moreNodes.Next()
				if next == nil {
					rd.r.logger.Error("Ran out of handoffs to talk to.",
						zap.String("dbFile", dbFile))
				} else {
					devices = append(devices, moreNodes.Next())
				}
			}
		}
	}
	if handoff && successes == len(devices) {
		lock, err := fs.LockPath(filepath.Dir(dbFile), dirLockTimeout)
		if err != nil {
			return err
		}
		defer lock.Close()
		rd.i.incrementStat("remove")
		return os.RemoveAll(filepath.Dir(dbFile))
	}
	return nil
}

func (rd *replicationDevice) findAccountDbs(devicePath string, results chan string) {
	defer close(results)
	accountsDir := filepath.Join(devicePath, "accounts")
	partitions, err := filepath.Glob(filepath.Join(accountsDir, "[0-9]*"))
	if err != nil {
		rd.r.logger.Error("Error getting partitions.",
			zap.String("accountsDir", accountsDir),
			zap.Error(err))
		return
	}
	for _, part := range partitions {
		suffixes, err := filepath.Glob(filepath.Join(part, "[a-f0-9][a-f0-9][a-f0-9]"))
		if err != nil {
			rd.r.logger.Error("Error getting suffixes.",
				zap.String("part", part),
				zap.Error(err))
			return
		}
		for _, suff := range suffixes {
			hashes, err := filepath.Glob(filepath.Join(suff, "????????????????????????????????"))
			if err != nil {
				rd.r.logger.Error("Error getting hashes",
					zap.String("suff", suff),
					zap.Error(err))
				return
			}
			for _, hash := range hashes {
				dbFile := filepath.Join(hash, filepath.Base(hash)+".db")
				if fs.Exists(dbFile) {
					select {
					case results <- dbFile:
					case <-rd.cancel:
						return
					}
				}
			}
		}
	}
}

func (rd *replicationDevice) replicate() {
	rd.r.logger.Info("Beginning replication for device.",
		zap.String("device", rd.dev.Device))
	rd.r.startRun <- rd.dev.Device
	devicePath := filepath.Join(rd.r.deviceRoot, rd.dev.Device)
	stat, err := os.Stat(devicePath)
	if err != nil || !stat.IsDir() {
		rd.r.logger.Error("Device doesn't exist.",
			zap.String("devicePath", devicePath), zap.Error(err))
		return
	}
	if mount, err := fs.IsMount(devicePath); rd.r.checkMounts && (err != nil || !mount) {
		rd.r.logger.Error("Device not mounted.",
			zap.String("devicePath", devicePath), zap.Error(err))
		return
	}
	results := make(chan string, 100)
	go rd.i.findAccountDbs(devicePath, results)
	for dbFile := range results {
		select {
		case <-rd.cancel:
			return
		case rd.r.concurrencySem <- struct{}{}:
			rd.r.checkin <- rd.dev.Device
			if err := rd.i.replicateDatabase(dbFile); err != nil {
				rd.r.logger.Error("Error replicating database file.",
					zap.String("dbFile", dbFile),
					zap.Error(err))
			}
			<-rd.r.concurrencySem
		}
		if err := rd.i.checkForReaping(dbFile); err != nil {
			rd.r.logger.Error("Error checking for reaping database file.",
				zap.String("dbFile", dbFile),
				zap.Error(err))
		}
	}
}

func (rd *replicationDevice) replicateLoop() {
	for {
		select {
		case <-rd.cancel:
			return
		default:
			rd.replicate()
		}
		time.Sleep(time.Second * 30)
	}
}

func (rd *replicationDevice) incrementStat(stat string) {
	rd.r.sendStat <- statUpdate{rd.dev.Device, stat, 1}
}

func newReplicationDevice(dev *ring.Device, r *Replicator) *replicationDevice {
	rd := &replicationDevice{
		r:             r,
		cancel:        make(chan struct{}),
		lastCheckin:   time.Now(),
		deviceStarted: time.Now(),
		dev:           dev,
		stats: map[string]int64{
			"attempted":    0,
			"success":      0,
			"failure":      0,
			"no_change":    0,
			"hashmatch":    0,
			"rsync":        0,
			"diff":         0,
			"remove":       0,
			"empty":        0,
			"remote_merge": 0,
			"diff_capped":  0,
		},
		lifetimeStats: map[string]int64{
			"attempted":    0,
			"success":      0,
			"failure":      0,
			"no_change":    0,
			"hashmatch":    0,
			"rsync":        0,
			"diff":         0,
			"remove":       0,
			"empty":        0,
			"remote_merge": 0,
			"diff_capped":  0,
			"passes":       0,
		},
	}
	rd.i = rd
	return rd
}

func (server *Replicator) Type() string {
	return "account-replicator"
}

func (server *Replicator) Background(flags *flag.FlagSet) chan struct{} {
	once := false
	if f := flags.Lookup("once"); f != nil {
		once = f.Value.(flag.Getter).Get() == true
	}
	if once {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			server.Run()
		}()
		return ch
	}
	go server.RunForever()
	return nil
}

func (server *Replicator) GetHandler(config conf.Config, metricsPrefix string) http.Handler {
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
	)
	router := srv.NewRouter()
	router.Get("/metrics", prometheus.Handler())
	router.Get("/loglevel", server.logLevel)
	router.Put("/loglevel", server.logLevel)
	router.Get("/healthcheck", commonHandlers.ThenFunc(server.HealthcheckHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	return alice.New(middleware.Metrics(metricsScope), middleware.ServerTracer(server.tracer)).Then(router)
}

func (server *Replicator) Finalize() {
	if server.metricsCloser != nil {
		server.metricsCloser.Close()
	}
	if server.traceCloser != nil {
		server.traceCloser.Close()
	}
	if server.clientTraceCloser != nil {
		server.clientTraceCloser.Close()
	}
}

func (server *Replicator) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
}

func (server *Replicator) LogRequest(next http.Handler) http.Handler {
	return srv.LogRequest(server.logger, next)
}

func (r *Replicator) verifyDevices() {
	// kill devices that haven't checked in for a while
	for key, rd := range r.runningDevices {
		if time.Since(rd.lastCheckin) > deviceLockupTimeout {
			close(rd.cancel)
			delete(r.runningDevices, key)
		}
	}
	if !r.reaperLastCheckin.IsZero() && time.Since(r.reaperLastCheckin) > deviceLockupTimeout {
		r.reaperLock.Lock()
		close(r.reaperCanceler)
		r.reaperCanceler = make(chan struct{})
		r.reaperLastCheckin = time.Time{}
		r.reaperLock.Unlock()
	}
	ringDevices, err := r.Ring.LocalDevices(r.serverPort)
	if err != nil {
		r.logger.Error("Error getting local devices from ring.",
			zap.Error(err))
		return
	}
	// look for devices that aren't running but should be
	for _, dev := range ringDevices {
		if _, ok := r.runningDevices[dev.Device]; !ok {
			r.runningDevices[dev.Device] = newReplicationDevice(dev, r)
			go r.runningDevices[dev.Device].replicateLoop()
		}
	}
	// look for devices that are running but shouldn't be
	for key, rd := range r.runningDevices {
		found := false
		for _, dev := range ringDevices {
			if dev.Device == key {
				found = true
			}
		}
		if !found {
			close(rd.cancel)
			delete(r.runningDevices, key)
		}
	}
}

func (r *Replicator) reportStats() {
	var totalTime time.Duration
	aggStats := map[string]int64{"attempted": 0, "success": 0, "failure": 0, "remove": 0}
	for _, device := range r.runningDevices {
		totalTime += time.Since(device.runStarted)
		aggStats["attempted"] += device.stats["attempted"]
		aggStats["success"] += device.stats["success"]
		aggStats["failure"] += device.stats["failure"]
		aggStats["remove"] += device.stats["remove"]
	}
	// there's no longer the concept of a single pass, so we report the average running time.
	if len(r.runningDevices) > 0 {
		rate := 0.0
		runningTime := (totalTime / time.Duration(len(r.runningDevices))).Seconds()
		if runningTime > 0 {
			rate = float64(aggStats["attempted"]) / runningTime
		}
		r.logger.Info("Attempted to replicate dbs",
			zap.Int64("aggStats['attempted']", aggStats["attempted"]),
			zap.Float64("runningTime", runningTime),
			zap.Float64("rate", rate))

		r.logger.Info("Removed dbs",
			zap.Int64("aggStats['remove']", aggStats["remove"]))
		r.logger.Info("Sucess & Failure",
			zap.Int64("success", aggStats["success"]),
			zap.Int64("failure", aggStats["failure"]))
	} else {
		r.logger.Info("No devices replicating.")
	}
}

func (r *Replicator) runLoopCheck(reportTimer <-chan time.Time) {
	select {
	case device := <-r.checkin:
		if rd, ok := r.runningDevices[device]; ok {
			rd.lastCheckin = time.Now() //TODO: add locking around this
		}
	case <-r.reaperCheckin:
		r.reaperLock.Lock()
		r.reaperLastCheckin = time.Now()
		r.reaperLock.Unlock()
	case device := <-r.startRun:
		if rd, ok := r.runningDevices[device]; ok {
			rd.runStarted = time.Now()
			rd.lastCheckin = time.Now()
			for k, v := range rd.stats {
				rd.stats[k] = 0
				rd.lifetimeStats[k] += v
			}
			rd.lifetimeStats["passes"]++
		}
	case update := <-r.sendStat:
		if rd, ok := r.runningDevices[update.device]; ok {
			rd.stats[update.stat] += update.value
		}
	case <-reportTimer:
		r.reportStats()
		r.verifyDevices()
	}
}

// RunForever runs the replicator in a forever-loop.
func (r *Replicator) RunForever() {
	reportTimer := time.NewTimer(time.Minute * 15)
	r.verifyDevices()
	for {
		r.runLoopCheck(reportTimer.C)
	}
}

// Run runs a pass of the replicator once.
func (r *Replicator) Run() {
	done := make(chan struct{})
	devices, err := r.Ring.LocalDevices(r.serverPort)
	if err != nil {
		r.logger.Error("Error getting local devices from ring.",
			zap.Error(err))
		return
	}
	for _, dev := range devices {
		r.runningDevices[dev.Device] = newReplicationDevice(dev, r)
		go func(rd *replicationDevice) {
			rd.replicate()
			done <- struct{}{}
		}(r.runningDevices[dev.Device])
	}
	waitingFor := len(devices)
	for waitingFor > 0 {
		select {
		case <-r.checkin:
		case <-r.reaperCheckin:
		case <-r.startRun:
		case update := <-r.sendStat:
			if ctx, ok := r.runningDevices[update.device]; ok {
				ctx.stats[update.stat] += update.value
			}
		case <-done:
			waitingFor--
		}
	}
	r.reportStats()
}

// try to reap account for given database. will spin off a go routine to do
// actual deleting
func (r *Replicator) TryToReapAccount(dbFile string) {
	r.reaperLock.Lock()
	defer r.reaperLock.Unlock()

	if r.reaperLastCheckin.IsZero() {
		r.reaperLastCheckin = time.Now()
		go r.reapAccount(dbFile, r.reaperCanceler)
	} else if time.Since(r.reaperLastCheckin) > deviceLockupTimeout {
		close(r.reaperCanceler)
		r.reaperCanceler = make(chan struct{})
		r.reaperLastCheckin = time.Now()
		go r.reapAccount(dbFile, r.reaperCanceler)
	} else {
		r.logger.Debug("Wanted to reap an account but one is already running", zap.String("dbFile", dbFile))
	}
}
func (r *Replicator) reapContainer(cont string, dc nectar.Client, contObjChan chan *contObj, canceler chan struct{}) error {
	// TODO: add something here that will delete the container if it is empty
	// the reaper will just make 2 passes to get all the way done
	marker := ""
	objs, resp := dc.GetContainer(cont, marker, "", 10000, "", "", false, map[string]string{})
	var obj *nectar.ObjectRecord
	hdrs := map[string]string{}
	for len(objs) > 0 {
		obj, objs = objs[0], objs[1:]
		contObjChan <- &contObj{cont, obj.Name}
		marker = obj.Name
		if len(objs) == 0 {
			objs, resp = dc.GetContainer(cont, marker, "", 10000, "", "", false, map[string]string{})
		}
		select {
		case <-canceler:
			return nil
		default:
		}
	}
	hdrs["X-Timestamp"] = common.GetTimestamp()
	if resp = dc.DeleteContainer(cont, hdrs); resp == nil || resp.StatusCode/100 != 2 {
		r.logger.Debug("invalid reap cont resp", zap.String("container", cont))
	}
	return nil
}

type contObj struct {
	cont string
	obj  string
}

// in case the db didn't get completely deleted- try again tomorrow
func (r *Replicator) pushBackDeleteIfNeeded(dbFile string) {
	db, err := sqliteOpenAccount(dbFile)
	if err != nil {
		r.logger.Error("error on opening dbfile", zap.String("dbFile", dbFile), zap.Error(err))
		return
	}
	defer db.Close()
	if d, err := db.IsDeleted(); err != nil {
		r.logger.Error("error on checking IsDeleted", zap.String("dbFile", dbFile), zap.Error(err))
		return
	} else if !d {
		r.logger.Error("pushBackDeleteIfNeeded was call on active account", zap.String("dbFile", dbFile))
		return
	}
	info, err := db.GetInfo()
	if err != nil {
		r.logger.Error("pushBackDeleteIfNeeded getInfo error", zap.String("dbFile", dbFile), zap.Error(err))
		return
	}
	if info.ObjectCount > 0 {
		if dti, err := strconv.ParseFloat(info.DeleteTimestamp, 64); err == nil {
			dti += common.ONE_DAY
			db.Delete(common.CanonicalTimestamp(dti))
		} else {
			r.logger.Error("invalid timestamp error", zap.String("dbFile", dbFile), zap.Error(err))
			return
		}
	}
}

func (r *Replicator) reapAccount(dbFile string, canceler chan struct{}) {
	defer r.pushBackDeleteIfNeeded(dbFile)

	db, err := sqliteOpenAccount(dbFile)
	if err != nil {
		r.logger.Error("error on opening dbfile", zap.String("dbFile", dbFile), zap.Error(err))
		return
	}
	defer db.Close()
	if d, err := db.IsDeleted(); err != nil {
		r.logger.Error("error on checking IsDeleted", zap.String("dbFile", dbFile), zap.Error(err))
		return
	} else if !d {
		r.logger.Error("reapAccount was call on active account", zap.String("dbFile", dbFile))
		return
	}
	info, err := db.GetInfo()
	if err != nil {
		r.logger.Error("reapAccount getInfo errpr", zap.String("dbFile", dbFile), zap.Error(err))
		return
	}
	dc, err := client.NewDirectClient(info.Account, srv.DefaultConfigLoader{}, r.certFile, r.keyFile, r.logger)
	if err != nil {
		r.logger.Error("Could not create client to reap account.", zap.String("account", info.Account), zap.Error(err))
		return
	}
	wg := sync.WaitGroup{}
	conc := 20 // TODO: make config
	wg.Add(conc)
	contObjChan := make(chan *contObj, conc)
	var objsDeleted int64

	for i := 0; i < conc; i++ {
		go func() {
			defer wg.Done()
			for {
				co := <-contObjChan
				if co == nil {
					return
				}
				if resp := dc.DeleteObject(co.cont, co.obj, map[string]string{"X-Timestamp": common.GetTimestamp()}); resp == nil || resp.StatusCode/100 != 2 {
					r.logger.Debug("invalid reap object resp", zap.String("container", co.cont), zap.String("obj", co.obj))
				} else {
					atomic.AddInt64(&objsDeleted, 1)
				}
				r.reaperCheckin <- struct{}{}
			}
		}()
	}
	marker := ""
	conts, err := db.ListContainers(1000, marker, "", "", "", false)
	if err != nil {
		r.logger.Error("ListContainers error", zap.Error(err))
		conts = nil // should already be nil
	}
	var contr interface{}
ContLoop:
	for len(conts) > 0 {
		contr, conts = conts[0], conts[1:]
		cont, ok := contr.(*ContainerListingRecord)
		if ok {
			if err = r.reapContainer(cont.Name, dc, contObjChan, canceler); err != nil {
				r.logger.Error("error reaping container", zap.String("account", info.Account), zap.String("container", cont.Name), zap.Error(err))
			}
			marker = cont.Name
		} else {
			r.logger.Error("invalid listing", zap.String("record", fmt.Sprintf("%v", contr)))
			break ContLoop
		}
		if len(conts) == 0 {
			conts, err = db.ListContainers(1000, marker, "", "", "", false)
			if err != nil {
				r.logger.Error("ListContainers error", zap.Error(err))
				break ContLoop
			}
		}
		select {
		case <-canceler:
			break ContLoop
		default:
		}
	}
	close(contObjChan)
	wg.Wait()
	r.logger.Info("reaped account", zap.String("account", info.Account), zap.Int64("objectsDeleted", objsDeleted), zap.Bool("Errored Out", err != nil), zap.Error(err))
}

// NewReplicator uses the config settings and command-line flags to configure and return a replicator daemon struct.
func NewReplicator(serverconf conf.Config, flags *flag.FlagSet, cnf srv.ConfigLoader) (*srv.IpPort, srv.Server, srv.LowLevelLogger, error) {
	var ipPort *srv.IpPort
	var err error
	var logger srv.LowLevelLogger
	if !serverconf.HasSection("account-replicator") {
		return ipPort, nil, nil, fmt.Errorf("Unable to find account-replicator config section")
	}
	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		return ipPort, nil, nil, fmt.Errorf("Unable to get hash prefix and suffix: %s", err)
	}
	ring, err := cnf.GetRing("account", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return ipPort, nil, nil, fmt.Errorf("Error loading account ring: %s", err)
	}
	concurrency := int(serverconf.GetInt("account-replicator", "concurrency", 4))

	logLevelString := serverconf.GetDefault("account-replicator", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))

	if logger, err = srv.SetupLogger("account-replicator", &logLevel, flags); err != nil {
		return ipPort, nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	ip := serverconf.GetDefault("account-replicator", "bind_ip", "0.0.0.0")
	port := int(serverconf.GetInt("account-replicator", "bind_port", common.DefaultAccountReplicatorPort))
	certFile := serverconf.GetDefault("account-replicator", "cert_file", "")
	keyFile := serverconf.GetDefault("account-replicator", "key_file", "")

	transport := &http.Transport{
		Dial:                (&net.Dialer{Timeout: time.Second}).Dial,
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
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
	c := &http.Client{
		Timeout:   time.Minute * 15,
		Transport: transport,
	}
	server := &Replicator{
		runningDevices: make(map[string]*replicationDevice),
		perUsync:       3000,
		maxUsyncs:      25,
		sendStat:       make(chan statUpdate),
		checkin:        make(chan string),
		reaperCheckin:  make(chan struct{}),
		reaperCanceler: make(chan struct{}),
		startRun:       make(chan string),
		reconCachePath: serverconf.GetDefault("account-replicator", "recon_cache_path", "/var/cache/swift"),
		checkMounts:    serverconf.GetBool("account-replicator", "mount_check", true),
		deviceRoot:     serverconf.GetDefault("account-replicator", "devices", "/srv/node"),
		serverPort:     port,
		reclaimAge:     serverconf.GetInt("account-replicator", "reclaim_age", 604800),
		logger:         logger,
		concurrencySem: make(chan struct{}, concurrency),
		Ring:           ring,
		client:         c,
		certFile:       certFile,
		keyFile:        keyFile,
		logLevel:       logLevel,
	}
	if serverconf.HasSection("tracing") {
		server.tracer, server.traceCloser, err = tracing.Init("account-replicator", server.logger, serverconf.GetSection("tracing"))
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up tracer: %v", err)
		}
		server.clientTracer, server.clientTraceCloser, err = tracing.Init("account-replicator-client", server.logger, serverconf.GetSection("tracing"))
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up tracer: %v", err)
		}
		enableHTTPTrace := serverconf.GetBool("tracing", "enable_httptrace", true)
		server.client, err = client.NewTracingClient(server.clientTracer, c, enableHTTPTrace)
		if err != nil {
			return ipPort, nil, nil, fmt.Errorf("Error setting up tracing client: %v", err)
		}
	}
	ipPort = &srv.IpPort{Ip: ip, Port: port, CertFile: certFile, KeyFile: keyFile}
	return ipPort, server, logger, nil
}
