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
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/http2"
)

const (
	tmpEmptyTime                 = 24 * time.Hour
	replicateStatsReportInterval = 10 * time.Minute
	replicateDeviceTimeout       = 4 * time.Hour
	replicateIncomingTimeout     = time.Minute
	replicateLoopSleepTime       = time.Second * 30
	replicatePartSleepTime       = time.Millisecond * 10
	handoffListDirFreq           = time.Minute * 10
	handoffToAllMod              = 5
	priorityReplicateTimeout     = time.Hour
)

type PriorityRepJob struct {
	Partition  uint64       `json:"partition"`
	FromDevice *ring.Device `json:"from_device"`
	ToDevice   *ring.Device `json:"to_device"`
	Policy     int          `json:"policy"`
}

func deviceKeyId(dev string, policy int) string {
	if policy == 0 {
		return dev
	}
	return fmt.Sprintf("%s-%d", dev, policy)
}

type DeviceStats struct {
	Stats              map[string]int64
	LastCheckin        time.Time
	PassStarted        time.Time
	DeviceStarted      time.Time
	LastPassFinishDate time.Time
	LastPassDuration   time.Duration
	CancelCount        int64
	FilesSent          int64
	BytesSent          int64
	PartitionsDone     int64
	PartitionsTotal    int64
	TotalPasses        int64
	PriorityRepsDone   int64
}

type statUpdate struct {
	service   string
	deviceKey string
	stat      string
	value     int64
}

// Object replicator daemon object
type Replicator struct {
	checkMounts         bool
	deviceRoot          string
	reconCachePath      string
	port                int
	bindIp              string
	CertFile            string
	KeyFile             string
	devices             map[string]bool
	partitions          map[string]bool
	quorumDelete        bool
	reclaimAge          int64
	reserve             int64
	incomingLimitPerDev int64
	policies            conf.PolicyList
	logLevel            zap.AtomicLevel
	metricsCloser       io.Closer
	auditor             *AuditorDaemon

	stats                   map[string]map[string]*DeviceStats
	runningDevices          map[string]ReplicationDevice
	updatingDevices         map[string]*updateDevice
	runningDevicesLock      sync.Mutex
	logger                  srv.LowLevelLogger
	objectRings             map[int]ring.Ring
	objEngines              map[int]ObjectEngine
	containerRing           ring.Ring
	replicateConcurrencySem chan struct{}
	updateConcurrencySem    chan struct{}
	nurseryConcurrencySem   chan struct{}
	updateStat              chan statUpdate
	onceDone                chan struct{}
	onceWaiting             int64
	client                  *http.Client
	incomingSemLock         sync.Mutex
	incomingSem             map[string]chan struct{}
	asyncWG                 sync.WaitGroup // Used to wait on async goroutines
	rcTimeout               time.Duration
}

func (server *Replicator) Type() string {
	return "object-replicator"
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
	if server.auditor != nil {
		go server.auditor.RunForever()
	}
	return nil
}

func (server *Replicator) Finalize() {
	if server.metricsCloser != nil {
		server.metricsCloser.Close()
	}
}

func (server *Replicator) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
}

func (r *Replicator) cancelStalledDevices() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	for key, rd := range r.runningDevices {
		stats, ok := r.stats["object-replicator"][key]
		if ok && time.Since(stats.LastCheckin) > replicateDeviceTimeout {
			rd.Cancel()
			delete(r.runningDevices, key)
			delete(r.stats["object-replicator"], key)
		}
	}
	for key, ud := range r.updatingDevices {
		stats, ok := r.stats["object-updater"][key]
		if ok && time.Since(stats.LastCheckin) > replicateDeviceTimeout {
			ud.cancel()
			delete(r.updatingDevices, key)
			delete(r.stats["object-updater"], key)
		}
	}
}

func (r *Replicator) verifyRunningDevices() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	expectedDevices := make(map[string]bool)
	for policy, oring := range r.objectRings {
		ringDevices, err := oring.LocalDevices(r.port)
		if err != nil {
			r.logger.Error("Error getting local devices from ring", zap.Error(err))
			return
		}
		objEngine, ok := r.objEngines[policy]
		if !ok {
			r.logger.Error("Error finding engine for policy", zap.Int("policy", policy), zap.Error(err))
			return
		}
		// look for devices that aren't running but should be
		for _, dev := range ringDevices {
			key := deviceKeyId(dev.Device, policy)
			expectedDevices[key] = true
			if len(r.devices) > 0 && !r.devices[dev.Device] {
				continue
			}
			if _, ok := r.runningDevices[key]; !ok {
				if rd, err := objEngine.GetReplicationDevice(oring, dev, policy, r); err == nil {
					r.runningDevices[key] = rd
					r.stats["object-replicator"][key] = &DeviceStats{
						LastCheckin: time.Now(), DeviceStarted: time.Now(),
						Stats: map[string]int64{},
					}
					go r.runningDevices[key].ReplicateLoop()
				} else {
					r.logger.Error("building replication device", zap.String("device", key), zap.Int("policy", policy), zap.Error(err))
				}
			}
			if _, ok := r.updatingDevices[key]; !ok {
				r.updatingDevices[key] = newUpdateDevice(dev, policy, r)
				r.stats["object-updater"][key] = &DeviceStats{
					LastCheckin: time.Now(), DeviceStarted: time.Now(),
					Stats: map[string]int64{"Success": 0, "Failure": 0},
				}
				go r.updatingDevices[key].updateLoop()
			}
		}
	}
	// look for devices that are running but shouldn't be
	for key, rd := range r.runningDevices {
		if _, found := expectedDevices[key]; !found {
			rd.Cancel()
			delete(r.runningDevices, key)
		}
	}
	for key, ud := range r.updatingDevices {
		if _, found := expectedDevices[key]; !found {
			ud.cancel()
			delete(r.updatingDevices, key)
		}
	}
}

func (r *Replicator) reportStats() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	minLastPass := time.Now()
	allHaveCompleted := true
	for key := range r.runningDevices {
		stats, ok := r.stats["object-replicator"][key]
		if !ok {
			continue
		}
		if stats.TotalPasses <= 1 {
			allHaveCompleted = false
		}
		if stats.LastPassFinishDate.Before(minLastPass) {
			minLastPass = stats.LastPassFinishDate
		}
		processingTimeSec := time.Since(stats.PassStarted).Seconds()
		doneParts := stats.Stats["PartitionsDone"]
		totalParts := stats.Stats["PartitionsTotal"]
		partsPerSecond := float64(doneParts) / processingTimeSec

		remaining := time.Duration(
			int64(float64(totalParts-doneParts)/partsPerSecond)) * time.Second
		var remainingStr string
		if remaining >= time.Hour {
			remainingStr = fmt.Sprintf("%.0fh", remaining.Hours())
		} else if remaining >= time.Minute {
			remainingStr = fmt.Sprintf("%.0fm", remaining.Minutes())
		} else {
			remainingStr = fmt.Sprintf("%.0fs", remaining.Seconds())
		}
		r.logger.Info("Partition Replicated",
			zap.String("Device", key),
			zap.Int64("doneParts", doneParts),
			zap.Int64("totalParts", totalParts),
			zap.Float64("DoneParts/TotalParts", float64(100*doneParts)/float64(totalParts)),
			zap.Float64("processingTimeSec", processingTimeSec),
			zap.Float64("partsPerSecond", partsPerSecond),
			zap.String("remainingStr", remainingStr))
	}
	if allHaveCompleted {
		// this is a mess but object_replication_time (in old way) is # minutes
		// passed since 1 complete pass of all devices started.
		// replication_last is unix time stamp when last complete pass was finished
		// now "last pass" means oldest device lastPass
		maxLastPassComplete := time.Since(minLastPass).Minutes()
		middleware.DumpReconCache(r.reconCachePath, "object",
			map[string]interface{}{
				"object_replication_time": maxLastPassComplete,
				"object_replication_last": float64(minLastPass.UnixNano()) / float64(time.Second),
			})
	}
}

func (r *Replicator) priorityReplicate(w http.ResponseWriter, pri PriorityRepJob) error {
	r.runningDevicesLock.Lock()
	rd, ok := r.runningDevices[deviceKeyId(pri.FromDevice.Device, pri.Policy)]
	r.runningDevicesLock.Unlock()
	if ok {
		return rd.PriorityReplicate(w, pri)
	}
	w.WriteHeader(404)
	return nil
}

func (r *Replicator) getDeviceProgress() map[string]*DeviceStats {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	deviceProgress := make(map[string]*DeviceStats)
	for key, stats := range r.stats["object-replicator"] {
		deviceProgress[key] = stats
	}
	return deviceProgress
}

func (r *Replicator) runLoopCheck(reportTimer <-chan time.Time) {
	select {
	case update := <-r.updateStat:
		r.runningDevicesLock.Lock()
		defer r.runningDevicesLock.Unlock()
		stats, ok := r.stats[update.service][update.deviceKey]
		if !ok {
			stats = &DeviceStats{LastCheckin: time.Now(), DeviceStarted: time.Now(),
				Stats: map[string]int64{}}
			r.stats[update.service][update.deviceKey] = stats
		}
		stats.LastCheckin = time.Now()
		switch update.stat {
		case "checkin":
		case "startRun":
			stats.PassStarted = time.Now()
			stats.PartitionsDone = 0
			stats.FilesSent = 0
			stats.BytesSent = 0
			stats.PriorityRepsDone = 0
			stats.LastPassFinishDate = time.Time{}
			for k := range stats.Stats {
				stats.Stats[k] = 0
			}
		case "FullReplicateCount":
			stats.LastPassDuration = time.Since(stats.PassStarted)
			stats.LastPassFinishDate = time.Now()
			stats.TotalPasses++
			stats.Stats[update.stat] += update.value

			lf := []zapcore.Field{
				zap.String("service", update.service),
				zap.String("device", update.deviceKey),
				zap.Duration("duration", stats.LastPassDuration),
			}
			for k, v := range stats.Stats {
				lf = append(lf, zap.Int64(k, v))
			}
			r.logger.Info("Service pass complete", lf...)
		case "cancel":
			stats.CancelCount += update.value
		case "FilesSent":
			stats.FilesSent += update.value
		case "BytesSent":
			stats.BytesSent += update.value
		case "PartitionsDone":
			stats.PartitionsDone += update.value
		case "PartitionsTotal":
			stats.PartitionsTotal = update.value
		case "PriorityRepsDone":
			stats.PriorityRepsDone += update.value
		default:
			stats.Stats[update.stat] += update.value
		}
	case <-reportTimer:
		r.cancelStalledDevices()
		r.verifyRunningDevices()
		r.reportStats()
	case <-r.onceDone:
		r.onceWaiting--
	}
}

// Run replication passes in a loop until forever.
func (r *Replicator) RunForever() {
	reportTimer := time.NewTimer(replicateStatsReportInterval)
	r.verifyRunningDevices()
	for {
		r.runLoopCheck(reportTimer.C)
	}
}

// Run a single replication pass. (NOTE: we will prob get rid of this because of priorityRepl)
func (r *Replicator) Run() {
	for policy, theRing := range r.objectRings {
		devices, err := theRing.LocalDevices(r.port)
		if err != nil {
			r.logger.Error("Error getting local devices from ring", zap.Error(err))
			return
		}
		objEngine, ok := r.objEngines[policy]
		if !ok {
			r.logger.Error("Error finding engine for policy", zap.Int("policy", policy), zap.Error(err))
			return
		}
		for _, dev := range devices {
			rd, err := objEngine.GetReplicationDevice(theRing, dev, policy, r)
			if err != nil {
				r.logger.Error("building replication device", zap.String("device", dev.Device), zap.Int("policy", policy), zap.Error(err))
				continue
			}
			key := rd.Key()
			r.runningDevices[key] = rd
			go func(rd ReplicationDevice) {
				rd.Replicate()
				r.onceDone <- struct{}{}
			}(rd)

			r.updatingDevices[key] = newUpdateDevice(dev, policy, r)
			go func(ud *updateDevice) {
				ud.update()
				r.onceDone <- struct{}{}
			}(r.updatingDevices[key])

			r.onceWaiting += 2
		}
	}
	for r.onceWaiting > 0 {
		r.runLoopCheck(make(chan time.Time))
	}
	r.reportStats()
}

func NewReplicator(serverconf conf.Config, flags *flag.FlagSet, cnf srv.ConfigLoader) (ipPort *srv.IpPort, server srv.Server, logger srv.LowLevelLogger, err error) {
	if !serverconf.HasSection("object-replicator") {
		return ipPort, nil, nil, fmt.Errorf("Unable to find object-replicator config section")
	}
	concurrency := int(serverconf.GetInt("object-replicator", "concurrency", 1))
	updaterConcurrency := int(serverconf.GetInt("object-updater", "concurrency", 2))
	nurseryConcurrency := int(serverconf.GetInt("object-nursery", "concurrency", 10))

	logLevelString := serverconf.GetDefault("object-replicator", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	certFile := serverconf.GetDefault("object-replicator", "cert_file", "")
	keyFile := serverconf.GetDefault("object-replicator", "key_file", "")
	transport := &http.Transport{
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

	replicator := &Replicator{
		reserve:             serverconf.GetInt("object-replicator", "fallocate_reserve", 0),
		reconCachePath:      serverconf.GetDefault("object-replicator", "recon_cache_path", "/var/cache/swift"),
		checkMounts:         serverconf.GetBool("object-replicator", "mount_check", true),
		deviceRoot:          serverconf.GetDefault("object-replicator", "devices", "/srv/node"),
		port:                int(serverconf.GetInt("object-replicator", "bind_port", common.DefaultObjectReplicatorPort)),
		bindIp:              serverconf.GetDefault("object-replicator", "bind_ip", "0.0.0.0"),
		CertFile:            certFile,
		KeyFile:             keyFile,
		quorumDelete:        serverconf.GetBool("object-replicator", "quorum_delete", false),
		reclaimAge:          int64(serverconf.GetInt("object-replicator", "reclaim_age", int64(common.ONE_WEEK))),
		incomingLimitPerDev: int64(serverconf.GetInt("object-replicator", "incoming_limit", 3)),

		runningDevices:          make(map[string]ReplicationDevice),
		updatingDevices:         make(map[string]*updateDevice),
		objectRings:             make(map[int]ring.Ring),
		replicateConcurrencySem: make(chan struct{}, concurrency),
		updateConcurrencySem:    make(chan struct{}, updaterConcurrency),
		nurseryConcurrencySem:   make(chan struct{}, nurseryConcurrency),
		rcTimeout:               time.Duration(serverconf.GetInt("object-replicator", "replication_timeout_sec", 0)) * time.Second,
		updateStat:              make(chan statUpdate),
		devices:                 make(map[string]bool),
		partitions:              make(map[string]bool),
		onceDone:                make(chan struct{}),
		client: &http.Client{
			Timeout:   time.Second * 60,
			Transport: transport,
		},
		incomingSem: make(map[string]chan struct{}),
		stats: map[string]map[string]*DeviceStats{
			"object-replicator": {},
			"object-updater":    {},
			"object-nursery":    {},
		},
	}
	replicator.logLevel = logLevel

	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		return ipPort, nil, nil, fmt.Errorf("Unable to get hash prefix and suffix: %s", err)
	}
	if replicator.policies, err = cnf.GetPolicies(); err != nil {
		return ipPort, nil, nil, err
	}
	for _, policy := range replicator.policies {
		if !(policy.Type == "replication" || policy.Type == "replication-nursery" || policy.Type == "hec") {
			continue
		}
		if replicator.objectRings[policy.Index], err = cnf.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index); err != nil {
			return ipPort, nil, nil, fmt.Errorf("Unable to load ring for Policy %d: %s", policy.Index, err)
		}
	}
	if replicator.containerRing, err = cnf.GetRing("container", hashPathPrefix, hashPathSuffix, 0); err != nil {
		return ipPort, nil, nil, fmt.Errorf("Error loading container ring: %v", err)
	}
	if replicator.objEngines, err = buildEngines(serverconf, flags, cnf); err != nil {
		return ipPort, nil, nil, err
	}
	if replicator.logger, err = srv.SetupLogger("object-replicator", &logLevel, flags); err != nil {
		return ipPort, nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	devices_flag := flags.Lookup("devices")
	if devices_flag != nil {
		if devices := devices_flag.Value.(flag.Getter).Get().(string); len(devices) > 0 {
			for _, devName := range strings.Split(devices, ",") {
				replicator.devices[strings.TrimSpace(devName)] = true
			}
		}
	}
	partitions_flag := flags.Lookup("partitions")
	if partitions_flag != nil {
		if partitions := partitions_flag.Value.(flag.Getter).Get().(string); len(partitions) > 0 {
			for _, part := range strings.Split(partitions, ",") {
				replicator.partitions[strings.TrimSpace(part)] = true
			}
		}
	}
	if !replicator.quorumDelete {
		quorumFlag := flags.Lookup("q")
		if quorumFlag != nil && quorumFlag.Value.(flag.Getter).Get() == true {
			replicator.quorumDelete = true
		}
	}
	if serverconf.HasSection("object-auditor") {
		replicator.auditor, err = NewAuditorDaemon(serverconf, flags, cnf)
	}
	ipPort = &srv.IpPort{Ip: replicator.bindIp, Port: replicator.port, CertFile: certFile, KeyFile: keyFile}
	return ipPort, replicator, replicator.logger, err
}
