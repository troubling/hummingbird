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
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

var (
	errDeviceNotMounted = errors.New("Remove drive was unmounted")
	deviceLockupTimeout = time.Hour
	// GetRing is a local pointer to the hummingbird function, for overriding in tests.
	GetRing = ring.GetRing
)

// Replicator is the container replicator daemon object
type Replicator struct {
	checkMounts    bool
	deviceRoot     string
	reconCachePath string
	logger         srv.LowLevelLogger
	serverPort     int
	Ring           ring.Ring
	perUsync       int64
	maxUsyncs      int
	concurrencySem chan struct{}
	sendStat       chan statUpdate
	checkin        chan string
	startRun       chan string
	client         *http.Client
	runningDevices map[string]*replicationDevice
	reclaimAge     int64
}

type statUpdate struct {
	device string
	stat   string
	value  int64
}

type replicationDevice struct {
	i interface {
		sendReplicationMessage(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error)
		sync(dev *ring.Device, part uint64, ringHash string, info *ContainerInfo) (*ContainerInfo, error)
		rsync(dev *ring.Device, c ReplicableContainer, part uint64, op string) error
		usync(dev *ring.Device, c ReplicableContainer, part uint64, localID string, point int64) error
		chooseReplicationStrategy(localInfo, remoteInfo *ContainerInfo, usyncThreshold int64) string
		replicateDatabaseToDevice(dev *ring.Device, c ReplicableContainer, part uint64) error
		replicateDatabase(dbFile string) error
		findContainerDbs(devicePath string, results chan string)
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
	req, err := http.NewRequest("REPLICATE", fmt.Sprintf("http://%s:%d/%s/%d/%s",
		dev.ReplicationIp, dev.ReplicationPort, dev.Device, part, ringHash), bytes.NewBuffer(body))
	if err != nil {
		return 0, nil, err
	}
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

func (rd *replicationDevice) sync(dev *ring.Device, part uint64, ringHash string, info *ContainerInfo) (*ContainerInfo, error) {
	var remoteInfo ContainerInfo
	status, body, err := rd.i.sendReplicationMessage(dev, part, ringHash, "sync", info.MaxRow, info.Hash,
		info.ID, info.CreatedAt, info.PutTimestamp, info.DeleteTimestamp, info.RawMetadata)
	if err != nil {
		return nil, fmt.Errorf("sending sync request to %s/%s: %v", dev.ReplicationIp, dev.Device, err)
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

func (rd *replicationDevice) rsync(dev *ring.Device, c ReplicableContainer, part uint64, op string) error {
	tmpFilename := common.UUID()
	fp, release, err := c.OpenDatabaseFile()
	if err != nil {
		return fmt.Errorf("Error opening databae: %v", err)
	}
	defer release()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/tmp/%s", dev.ReplicationIp, dev.ReplicationPort, dev.Device, tmpFilename), fp)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}
	req.Cancel = rd.cancel
	resp, err := rd.r.client.Do(req)
	if err != nil {
		return fmt.Errorf("putting database to %s/%s: %v", dev.ReplicationIp, dev.Device, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad status code %d rsyncing file with %s/%s", resp.StatusCode, dev.ReplicationIp, dev.Device)
	}
	status, _, err := rd.i.sendReplicationMessage(dev, part, c.RingHash(), op, tmpFilename)
	if err != nil || status/100 != 2 {
		return fmt.Errorf("sending %s message to %s/%s: %v", op, dev.ReplicationIp, dev.Device, err)
	}
	return nil
}

func (rd *replicationDevice) usync(dev *ring.Device, c ReplicableContainer, part uint64, localID string, point int64) error {
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
			return fmt.Errorf("Bad response to merge_items with %s/%s: %v, %v", dev.ReplicationIp, dev.Device, status, err)
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

func (rd *replicationDevice) chooseReplicationStrategy(localInfo, remoteInfo *ContainerInfo, usyncThreshold int64) string {
	switch {
	case remoteInfo == nil:
		return "complete_rsync"
	case localInfo.MaxRow == -1:
		return "empty"
	case localInfo.MaxRow == remoteInfo.Point:
		return "no_change"
	case localInfo.Hash == remoteInfo.Hash:
		return "hashmatch"
	case remoteInfo.MaxRow < localInfo.MaxRow*2 && localInfo.MaxRow-remoteInfo.MaxRow > usyncThreshold:
		return "rsync_then_merge"
	default:
		return "diff"
	}
}

func (rd *replicationDevice) replicateDatabaseToDevice(dev *ring.Device, c ReplicableContainer, part uint64) error {
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
	case "complete_rsync", "rsync_then_merge":
		rd.r.logger.Debug("Replicating ringhash",
			zap.String("RingHash", c.RingHash()),
			zap.String("ReplicationIp", dev.ReplicationIp),
			zap.String("Device", dev.Device),
			zap.String("strategy", strategy))
		return rd.i.rsync(dev, c, part, strategy)
	case "diff":
		rd.r.logger.Debug("Replicating ringhash",
			zap.String("RingHash", c.RingHash()),
			zap.String("ReplicationIp", dev.ReplicationIp),
			zap.String("Device", dev.Device),
			zap.String("strategy", strategy))
		return rd.i.usync(dev, c, part, info.ID, remoteInfo.Point)
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
	c, err := sqliteOpenContainer(dbFile)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.CleanupTombstones(rd.r.reclaimAge); err != nil {
		return err
	}
	if err := c.CheckSyncLink(); err != nil {
		return err
	}
	successes := 0
	for i := 0; i < len(devices); i++ {
		if err := rd.i.replicateDatabaseToDevice(devices[i], c, part); err == nil {
			rd.i.incrementStat("success")
			rd.r.logger.Debug("Succeeded replicating database.",
				zap.String("dbFile", dbFile),
				zap.String("ReplicationIp", devices[i].ReplicationIp),
				zap.String("Device", devices[i].Device))
			successes++
		} else {
			rd.i.incrementStat("failure")
			rd.r.logger.Error("Error replicating database.",
				zap.String("dbFile", dbFile),
				zap.String("ReplicationIp", devices[i].ReplicationIp),
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
		rd.i.incrementStat("remove")
		return os.RemoveAll(filepath.Dir(dbFile))
	}
	return nil
}

func (rd *replicationDevice) findContainerDbs(devicePath string, results chan string) {
	defer close(results)
	containersDir := filepath.Join(devicePath, "containers")
	partitions, err := filepath.Glob(filepath.Join(containersDir, "[0-9]*"))
	if err != nil {
		rd.r.logger.Error("Error getting partitions.",
			zap.String("containersDir", containersDir),
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
			zap.String("devicePath", devicePath))
		return
	}
	if mount, err := fs.IsMount(devicePath); rd.r.checkMounts && (err != nil || !mount) {
		rd.r.logger.Error("Device not mounted.",
			zap.String("devicePath", devicePath))
		return
	}
	results := make(chan string, 100)
	go rd.i.findContainerDbs(devicePath, results)
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

func (r *Replicator) verifyDevices() {
	// kill devices that haven't checked in for a while
	for key, rd := range r.runningDevices {
		if time.Since(rd.lastCheckin) > deviceLockupTimeout {
			close(rd.cancel)
			delete(r.runningDevices, key)
		}
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
			rd.lastCheckin = time.Now()
		}
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
	go http.ListenAndServe("127.0.0.1:0", nil)
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

// NewReplicator uses the config settings and command-line flags to configure and return a replicator daemon struct.
func NewReplicator(serverconf conf.Config, flags *flag.FlagSet, cnf srv.ConfigLoader) (srv.Daemon, srv.LowLevelLogger, error) {
	if !serverconf.HasSection("container-replicator") {
		return nil, nil, fmt.Errorf("Unable to find container-replicator config section")
	}
	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get hash prefix and suffix")
	}
	ring, err := cnf.GetRing("container", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("Error loading container ring")
	}
	concurrency := int(serverconf.GetInt("container-replicator", "concurrency", 4))

	logLevelString := serverconf.GetDefault("container-replicator", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))

	var logger srv.LowLevelLogger
	if logger, err = srv.SetupLogger("container-replicator", &logLevel, flags); err != nil {
		return nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	return &Replicator{
		runningDevices: make(map[string]*replicationDevice),
		perUsync:       3000,
		maxUsyncs:      25,
		sendStat:       make(chan statUpdate),
		checkin:        make(chan string),
		startRun:       make(chan string),
		reconCachePath: serverconf.GetDefault("container-replicator", "recon_cache_path", "/var/cache/swift"),
		checkMounts:    serverconf.GetBool("container-replicator", "mount_check", true),
		deviceRoot:     serverconf.GetDefault("container-replicator", "devices", "/srv/node"),
		serverPort:     int(serverconf.GetInt("container-replicator", "bind_port", 6000)),
		reclaimAge:     serverconf.GetInt("container-replicator", "reclaim_age", 604800),
		logger:         logger,
		concurrencySem: make(chan struct{}, concurrency),
		Ring:           ring,
		client: &http.Client{
			Timeout:   time.Minute * 15,
			Transport: &http.Transport{Dial: (&net.Dialer{Timeout: time.Second}).Dial},
		},
	}, logger, nil
}
