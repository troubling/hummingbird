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
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	tmpEmptyTime                 = 24 * time.Hour
	replicateStatsReportInterval = 10 * time.Minute
	replicateDeviceTimeout       = 4 * time.Hour
	replicateIncomingTimeout     = time.Minute
	replicateLoopSleepTime       = time.Second * 30
	replicatePartSleepTime       = time.Millisecond * 10
	minimumReplicationPassTime   = time.Minute * 30
)

var (
	// GetRing is a local pointer to the hummingbird function, for overriding in tests
	GetRing = ring.GetRing
)

type PriorityRepJob struct {
	Partition  uint64         `json:"partition"`
	FromDevice *ring.Device   `json:"from_device"`
	ToDevices  []*ring.Device `json:"to_devices"`
	Policy     int            `json:"policy"`
}

// minimal ring interface for replication
type replicationRing interface {
	GetJobNodes(partition uint64, localDevice int) (response []*ring.Device, handoff bool)
	GetMoreNodes(partition uint64) ring.MoreNodes
	LocalDevices(localPort int) (devs []*ring.Device, err error)
}

type quarantineFileError struct {
	msg string
}

func (q quarantineFileError) Error() string {
	return q.msg
}

func deviceKey(dev *ring.Device, policy int) string {
	if policy == 0 {
		return dev.Device
	}
	return fmt.Sprintf("%s-%d", dev.Device, policy)
}

func getFile(filePath string) (fp *os.File, xattrs []byte, size int64, err error) {
	fp, err = os.Open(filePath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("unable to open file (%v): %s", err, filePath)
	}
	defer func() {
		if err != nil {
			fp.Close()
		}
	}()
	finfo, err := fp.Stat()
	if err != nil || !finfo.Mode().IsRegular() {
		return nil, nil, 0, quarantineFileError{"not a regular file"}
	}
	rawxattr, err := RawReadMetadata(fp.Fd())
	if err != nil || len(rawxattr) == 0 {
		return nil, nil, 0, quarantineFileError{"error reading xattrs"}
	}

	// Perform a mini-audit, since it's cheap and we can potentially avoid spreading bad data around.
	v, err := pickle.PickleLoads(rawxattr)
	if err != nil {
		return nil, nil, 0, quarantineFileError{"error unpickling xattrs"}
	}
	metadata, ok := v.(map[interface{}]interface{})
	if !ok {
		return nil, nil, 0, quarantineFileError{"invalid metadata type"}
	}
	for key, value := range metadata {
		if _, ok := key.(string); !ok {
			return nil, nil, 0, quarantineFileError{"invalid key in metadata"}
		}
		if _, ok := value.(string); !ok {
			return nil, nil, 0, quarantineFileError{"invalid value in metadata"}
		}
	}
	switch filepath.Ext(filePath) {
	case ".data":
		for _, reqEntry := range []string{"Content-Length", "Content-Type", "name", "ETag", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				return nil, nil, 0, quarantineFileError{".data missing required metadata"}
			}
		}
		if contentLength, err := strconv.ParseInt(metadata["Content-Length"].(string), 10, 64); err != nil || contentLength != finfo.Size() {
			return nil, nil, 0, quarantineFileError{"invalid content-length"}
		}
	case ".ts":
		for _, reqEntry := range []string{"name", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				return nil, nil, 0, quarantineFileError{".ts missing required metadata"}
			}
		}
	}
	return fp, rawxattr, finfo.Size(), nil
}

type DeviceStats struct {
	Stats            map[string]int64
	LastCheckin      time.Time
	RunStarted       time.Time
	DeviceStarted    time.Time
	LastPassDate     time.Time
	LastPassDuration time.Duration
	TotalPasses      int64
}

type ReplicationDevice interface {
	Replicate()
	ReplicateLoop()
	Key() string
	Cancel()
	PriorityReplicate(pri PriorityRepJob, timeout time.Duration) bool
}

type replicationDevice struct {
	// If you have a better way to make struct methods that are overridable for tests, please call my house.
	i interface {
		beginReplication(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse)
		listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool)
		syncFile(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error)
		replicateLocal(partition string, nodes []*ring.Device, moreNodes ring.MoreNodes)
		replicateHandoff(partition string, nodes []*ring.Device)
		cleanTemp()
		listPartitions() ([]string, error)
		replicatePartition(partition string)
	}
	r             *Replicator
	dev           *ring.Device
	policy        int
	cancel        chan struct{}
	priRep        chan PriorityRepJob
	partSleepTime time.Duration
}

type statUpdate struct {
	service   string
	deviceKey string
	stat      string
	value     int64
}

func (rd *replicationDevice) updateStat(stat string, amount int64) {
	rd.r.updateStat <- statUpdate{"object-replicator", rd.Key(), stat, amount}
}

type beginReplicationResponse struct {
	dev    *ring.Device
	conn   RepConn
	hashes map[string]string
	err    error
}

func (rd *replicationDevice) listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
	defer close(objChan)
	suffixDirs, err := filepath.Glob(filepath.Join(partdir, "[a-f0-9][a-f0-9][a-f0-9]"))
	if err != nil {
		rd.r.logger.Error("[listObjFiles]", zap.Error(err))
		return
	}
	if len(suffixDirs) == 0 {
		os.Remove(filepath.Join(partdir, ".lock"))
		os.Remove(filepath.Join(partdir, "hashes.pkl"))
		os.Remove(filepath.Join(partdir, "hashes.invalid"))
		os.Remove(partdir)
		return
	}
	for i := len(suffixDirs) - 1; i > 0; i-- { // shuffle suffixDirs list
		j := rand.Intn(i + 1)
		suffixDirs[j], suffixDirs[i] = suffixDirs[i], suffixDirs[j]
	}
	for _, suffDir := range suffixDirs {
		if !needSuffix(filepath.Base(suffDir)) {
			continue
		}
		hashDirs, err := filepath.Glob(filepath.Join(suffDir, "????????????????????????????????"))
		if err != nil {
			rd.r.logger.Error("[listObjFiles]", zap.Error(err))
			return
		}
		if len(hashDirs) == 0 {
			os.Remove(suffDir)
			continue
		}
		for _, hashDir := range hashDirs {
			fileList, err := filepath.Glob(filepath.Join(hashDir, "*.[tdm]*"))
			if len(fileList) == 0 {
				os.Remove(hashDir)
				continue
			}
			if err != nil {
				rd.r.logger.Error("[listObjFiles]", zap.Error(err))
				return
			}
			for _, objFile := range fileList {
				select {
				case objChan <- objFile:
				case <-cancel:
					return
				}
			}
		}
	}
}

type syncFileArg struct {
	conn RepConn
	dev  *ring.Device
}

func (rd *replicationDevice) syncFile(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error) {
	// TODO: parallelize the data transfer someday
	var wrs []*syncFileArg
	lst := strings.Split(objFile, string(os.PathSeparator))
	relPath := filepath.Join(lst[len(lst)-5:]...)
	fp, xattrs, fileSize, err := getFile(objFile)
	if _, ok := err.(quarantineFileError); ok {
		hashDir := filepath.Dir(objFile)
		rd.r.logger.Error("[syncFile] Failed audit and is being quarantined",
			zap.String("hashDir", hashDir),
			zap.Error(err))
		QuarantineHash(hashDir)
		return 0, 0, nil
	} else if err != nil {
		return 0, 0, nil
	}
	defer fp.Close()

	// are we already going to sync to this region?
	syncingRemoteRegion := make(map[int]bool)

	// ask each server if we need to sync the file
	for _, sfa := range dst {
		var sfr SyncFileResponse
		thisPath := filepath.Join(sfa.dev.Device, relPath)
		sfa.conn.SendMessage(SyncFileRequest{Path: thisPath, Xattrs: hex.EncodeToString(xattrs), Size: fileSize,
			// if we're already syncing handoffs to this remote region, just do a check
			Check: handoff && syncingRemoteRegion[sfa.dev.Region],
			// If we're not syncing handoffs, we don't care about the state. Just ping to keep the connection alive.
			Ping: !handoff && syncingRemoteRegion[sfa.dev.Region],
		})
		if err := sfa.conn.RecvMessage(&sfr); err != nil {
			continue
		} else if sfr.GoAhead {
			wrs = append(wrs, sfa)
			if sfa.dev.Region != rd.dev.Region {
				syncingRemoteRegion[sfa.dev.Region] = true
			}
		} else if sfr.NewerExists {
			insync++
			if os.Remove(objFile) == nil {
				InvalidateHash(filepath.Dir(objFile))
			}
		} else if sfr.Exists {
			insync++
		}
	}
	if len(wrs) == 0 { // nobody needed the file
		return
	}

	// send the file to servers
	scratch := make([]byte, 32768)
	var length int
	var totalRead int64
	for length, err = fp.Read(scratch); err == nil; length, err = fp.Read(scratch) {
		totalRead += int64(length)
		for index, sfa := range wrs {
			if sfa == nil {
				continue
			}
			if _, err := sfa.conn.Write(scratch[0:length]); err != nil {
				rd.r.logger.Error("Failed to write to remoteDevice",
					zap.Int("device id", sfa.dev.Id),
					zap.Error(err))
				wrs[index] = nil
			}
		}
	}
	if totalRead != fileSize {
		return 0, 0, fmt.Errorf("Failed to read the full file: %s, %v", objFile, err)
	}

	// get file upload results
	for _, sfa := range wrs {
		if sfa == nil {
			continue
		}
		var fur FileUploadResponse
		sfa.conn.Flush()
		if sfa.conn.RecvMessage(&fur) == nil {
			if fur.Success {
				syncs++
				insync++
				rd.updateStat("FilesSent", 1)
				rd.updateStat("BytesSent", fileSize)
			}
		}
	}
	return syncs, insync, nil
}

func (rd *replicationDevice) beginReplication(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse) {
	var brr BeginReplicationResponse
	if rc, err := NewRepConn(dev, partition, rd.policy); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else if err := rc.SendMessage(BeginReplicationRequest{Device: dev.Device, Partition: partition, NeedHashes: hashes}); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else if err := rc.RecvMessage(&brr); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else {
		rChan <- beginReplicationResponse{dev: dev, conn: rc, hashes: brr.Hashes}
	}
}

func (rd *replicationDevice) replicateLocal(partition string, nodes []*ring.Device, moreNodes ring.MoreNodes) {
	path := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy), partition)
	syncCount := 0
	startGetHashesRemote := time.Now()
	remoteHashes := make(map[int]map[string]string)
	remoteConnections := make(map[int]RepConn)
	rChan := make(chan beginReplicationResponse)
	for _, dev := range nodes {
		go rd.i.beginReplication(dev, partition, true, rChan)
	}
	for i := 0; i < len(nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteHashes[rData.dev.Id] = rData.hashes
			remoteConnections[rData.dev.Id] = rData.conn
		} else if rData.err == RepUnmountedError {
			if nextNode := moreNodes.Next(); nextNode != nil {
				go rd.i.beginReplication(nextNode, partition, true, rChan)
				nodes = append(nodes, nextNode)
			} else {
				break
			}
		}
	}
	if len(remoteHashes) == 0 {
		return
	}

	timeGetHashesRemote := float64(time.Now().Sub(startGetHashesRemote)) / float64(time.Second)
	startGetHashesLocal := time.Now()

	recalc := []string{}
	hashes, err := GetHashes(rd.r.deviceRoot, rd.dev.Device, partition, recalc, rd.r.reclaimAge, rd.policy, rd.r.logger)
	if err != nil {
		rd.r.logger.Error("[replicateLocal] error getting local hashes", zap.Error(err))
		return
	}
	for suffix, localHash := range hashes {
		for _, remoteHash := range remoteHashes {
			if remoteHash[suffix] != "" && localHash != remoteHash[suffix] {
				recalc = append(recalc, suffix)
				break
			}
		}
	}
	hashes, err = GetHashes(rd.r.deviceRoot, rd.dev.Device, partition, recalc, rd.r.reclaimAge, rd.policy, rd.r.logger)
	if err != nil {
		rd.r.logger.Error("[replicateLocal] error recalculating local hashes", zap.Error(err))
		return
	}
	timeGetHashesLocal := float64(time.Now().Sub(startGetHashesLocal)) / float64(time.Second)

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go rd.i.listObjFiles(objChan, cancel, path, func(suffix string) bool {
		for _, remoteHash := range remoteHashes {
			if hashes[suffix] != remoteHash[suffix] {
				return true
			}
		}
		return false
	})
	startSyncing := time.Now()
	for objFile := range objChan {
		toSync := make([]*syncFileArg, 0)
		suffix := filepath.Base(filepath.Dir(filepath.Dir(objFile)))
		for _, dev := range nodes {
			if rhashes, ok := remoteHashes[dev.Id]; ok && hashes[suffix] != rhashes[suffix] {
				if !remoteConnections[dev.Id].Disconnected() {
					toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
				}
			}
		}
		if len(toSync) == 0 {
			break
		}
		if syncs, _, err := rd.i.syncFile(objFile, toSync, false); err == nil {
			syncCount += syncs
		} else {
			rd.r.logger.Error("[syncFile]", zap.Error(err))
			return
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected() {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	timeSyncing := float64(time.Now().Sub(startSyncing)) / float64(time.Second)
	if syncCount > 0 {
		rd.r.logger.Info("[replicateLocal]",
			zap.String("Partition", path),
			zap.Any("Files Synced", syncCount),
			zap.Float64("timeGetHashesRemote", timeGetHashesRemote),
			zap.Float64("timeGetHashesLocal", timeGetHashesLocal),
			zap.Float64("timeSyncing", timeSyncing))
	}
}

func (rd *replicationDevice) replicateHandoff(partition string, nodes []*ring.Device) {
	path := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy), partition)
	syncCount := 0
	remoteConnections := make(map[int]RepConn)
	rChan := make(chan beginReplicationResponse)
	for _, dev := range nodes {
		go rd.i.beginReplication(dev, partition, false, rChan)
	}
	for i := 0; i < len(nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteConnections[rData.dev.Id] = rData.conn
		}
	}
	if len(remoteConnections) == 0 {
		return
	}

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go rd.i.listObjFiles(objChan, cancel, path, func(string) bool { return true })
	for objFile := range objChan {
		toSync := make([]*syncFileArg, 0)
		for _, dev := range nodes {
			if remoteConnections[dev.Id] != nil && !remoteConnections[dev.Id].Disconnected() {
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if len(toSync) == 0 {
			return
		}
		if syncs, insync, err := rd.i.syncFile(objFile, toSync, true); err == nil {
			syncCount += syncs

			success := insync == len(nodes)
			if rd.r.quorumDelete {
				success = insync >= len(nodes)/2+1
			}
			if success {
				os.Remove(objFile)
				os.Remove(filepath.Dir(objFile))
			}
		} else {
			rd.r.logger.Error("[syncFile]", zap.Error(err))
			return
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected() {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	if syncCount > 0 {
		rd.r.logger.Info("[replicateHandoff]", zap.String("Partition", path), zap.Any("Files Synced", syncCount))
	}
}

func (rd *replicationDevice) Key() string {
	return deviceKey(rd.dev, rd.policy)
}

func (rd *replicationDevice) cleanTemp() {
	tempDir := TempDirPath(rd.r.deviceRoot, rd.dev.Device)
	if tmpContents, err := ioutil.ReadDir(tempDir); err == nil {
		for _, tmpEntry := range tmpContents {
			if time.Since(tmpEntry.ModTime()) > tmpEmptyTime {
				os.RemoveAll(filepath.Join(tempDir, tmpEntry.Name()))
			}
		}
	}
}

func (rd *replicationDevice) replicatePartition(partition string) {
	rd.r.replicateConcurrencySem <- struct{}{}
	defer func() {
		<-rd.r.replicateConcurrencySem
	}()
	partitioni, err := strconv.ParseUint(partition, 10, 64)
	if err != nil {
		return
	}
	nodes, handoff := rd.r.objectRings[rd.policy].GetJobNodes(partitioni, rd.dev.Id)
	if handoff {
		rd.i.replicateHandoff(partition, nodes)
	} else {
		rd.i.replicateLocal(partition, nodes, rd.r.objectRings[rd.policy].GetMoreNodes(partitioni))
	}
	rd.updateStat("PartitionsDone", 1)
}

func (rd *replicationDevice) listPartitions() ([]string, error) {
	objPath := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy))
	partitions, err := filepath.Glob(filepath.Join(objPath, "[0-9]*"))
	if err != nil {
		return nil, err
	}
	partitionList := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		partition = filepath.Base(partition)
		if len(rd.r.partitions) > 0 && !rd.r.partitions[partition] {
			continue
		}
		if _, err := strconv.ParseUint(partition, 10, 64); err == nil {
			partitionList = append(partitionList, partition)
		}
	}
	for i := len(partitionList) - 1; i > 0; i-- { // shuffle partition list
		j := rand.Intn(i + 1)
		partitionList[j], partitionList[i] = partitionList[i], partitionList[j]
	}
	return partitionList, nil
}

func (rd *replicationDevice) Replicate() {
	defer srv.LogPanics(rd.r.logger, fmt.Sprintf("PANIC REPLICATING DEVICE: %s", rd.dev.Device))
	rd.updateStat("startRun", 1)
	if mounted, err := fs.IsMount(filepath.Join(rd.r.deviceRoot, rd.dev.Device)); rd.r.checkMounts && (err != nil || mounted != true) {
		rd.r.logger.Error("[replicateDevice] Drive not mounted", zap.String("Device", rd.dev.Device))
		return
	}
	if fs.Exists(filepath.Join(rd.r.deviceRoot, rd.dev.Device, "lock_device")) {
		return
	}

	rd.i.cleanTemp()

	partitionList, err := rd.i.listPartitions()
	if err != nil {
		rd.r.logger.Error("[replicateDevice] Error getting partition list",
			zap.String("Device", rd.dev.Device),
			zap.Error(err))
		return
	} else if len(partitionList) == 0 {
		rd.r.logger.Info("[replicateDevice] No partitions found",
			zap.String("filepath", filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy))))
		return
	}
	rd.updateStat("PartitionsTotal", int64(len(partitionList)))

	start := time.Now()
	for _, partition := range partitionList {
		rd.updateStat("checkin", 1)
		select {
		case <-rd.cancel:
			{
				rd.r.logger.Error("replicateDevice canceled for device", zap.String("Device", rd.dev.Device))
				return
			}
		default:
		}
		rd.processPriorityJobs()
		rd.i.replicatePartition(partition)
		time.Sleep(rd.partSleepTime)
	}
	if time.Since(start) < minimumReplicationPassTime {
		rd.partSleepTime = minimumReplicationPassTime / time.Duration(len(partitionList))
	} else {
		rd.partSleepTime = rd.partSleepTime / 2
	}
	if rd.partSleepTime < replicatePartSleepTime {
		rd.partSleepTime = replicatePartSleepTime
	}
	rd.updateStat("FullReplicateCount", 1)
}

func (rd *replicationDevice) Cancel() {
	close(rd.cancel)
}

func (rd *replicationDevice) ReplicateLoop() {
	for {
		select {
		case <-rd.cancel:
			return
		default:
			rd.Replicate()
		}
		time.Sleep(replicateLoopSleepTime)
	}
}

type NoMoreNodes struct{}

func (n *NoMoreNodes) Next() *ring.Device {
	return nil
}

func (rd *replicationDevice) PriorityReplicate(pri PriorityRepJob, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case rd.priRep <- pri:
		return true
	case <-timer.C:
		return false
	}
}

// processPriorityJobs runs any pending priority jobs given the device's id
func (rd *replicationDevice) processPriorityJobs() {
	for {
		select {
		case pri := <-rd.priRep:
			func() {
				time.Sleep(replicatePartSleepTime)
				rd.r.replicateConcurrencySem <- struct{}{}
				defer func() {
					<-rd.r.replicateConcurrencySem
				}()
				partition := strconv.FormatUint(pri.Partition, 10)
				_, handoff := rd.r.objectRings[rd.policy].GetJobNodes(pri.Partition, pri.FromDevice.Id)
				toDevicesArr := make([]string, len(pri.ToDevices))
				for i, s := range pri.ToDevices {
					toDevicesArr[i] = fmt.Sprintf("%s:%d/%s", s.Ip, s.Port, s.Device)
				}
				jobType := "local"
				if handoff {
					jobType = "handoff"
				}
				rd.r.logger.Info("PriorityReplicationJob",
					zap.Uint64("partition", pri.Partition),
					zap.String("jobType", jobType),
					zap.String("From Device", pri.FromDevice.Device),
					zap.String("To Device", strings.Join(toDevicesArr, ",")))
				if handoff {
					rd.i.replicateHandoff(partition, pri.ToDevices)
				} else {
					rd.i.replicateLocal(partition, pri.ToDevices, &NoMoreNodes{})
				}
			}()
			rd.updateStat("PriorityRepsDone", 1)
		default:
			return
		}
	}
}

var newReplicationDevice = func(dev *ring.Device, policy int, r *Replicator) *replicationDevice {
	rd := &replicationDevice{
		r:             r,
		dev:           dev,
		policy:        policy,
		cancel:        make(chan struct{}),
		priRep:        make(chan PriorityRepJob),
		partSleepTime: replicatePartSleepTime,
	}
	rd.i = rd
	return rd
}

// Object replicator daemon object
type Replicator struct {
	checkMounts         bool
	deviceRoot          string
	reconCachePath      string
	port                int
	bindIp              string
	devices             map[string]bool
	partitions          map[string]bool
	quorumDelete        bool
	reclaimAge          int64
	reserve             int64
	incomingLimitPerDev int64

	stats                   map[string]map[string]*DeviceStats
	runningDevices          map[string]ReplicationDevice
	updatingDevices         map[string]*updateDevice
	runningDevicesLock      sync.Mutex
	logger                  srv.LowLevelLogger
	objectRings             map[int]replicationRing
	containerRing           ring.Ring
	cancelCounts            map[string]int64
	replicateConcurrencySem chan struct{}
	updateConcurrencySem    chan struct{}
	updateStat              chan statUpdate
	onceDone                chan struct{}
	onceWaiting             int64
	client                  *http.Client
	incomingSemLock         sync.Mutex
	incomingSem             map[string]chan struct{}
}

func (r *Replicator) cancelStalledDevices() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	for key, rd := range r.runningDevices {
		stats, ok := r.stats["object-replicator"][key]
		if ok && time.Since(stats.LastCheckin) > replicateDeviceTimeout {
			rd.Cancel()
			r.cancelCounts[key] += 1
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
	for policy, ring := range r.objectRings {
		ringDevices, err := ring.LocalDevices(r.port)
		if err != nil {
			r.logger.Error("Error getting local devices from ring", zap.Error(err))
			return
		}
		// look for devices that aren't running but should be
		for _, dev := range ringDevices {
			key := deviceKey(dev, policy)
			expectedDevices[key] = true
			if len(r.devices) > 0 && !r.devices[dev.Device] {
				continue
			}
			if _, ok := r.runningDevices[key]; !ok {
				r.runningDevices[key] = newReplicationDevice(dev, policy, r)
				r.stats["object-replicator"][key] = &DeviceStats{
					LastCheckin: time.Now(), DeviceStarted: time.Now(),
					Stats: map[string]int64{"PartitionsDone": 0, "PartitionsTotal": 0,
						"FilesSent": 0, "BytesSent": 0, "PriorityRepsDone": 0},
				}
				go r.runningDevices[key].ReplicateLoop()
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
		if stats.LastPassDate.Before(minLastPass) {
			minLastPass = stats.LastPassDate
		}
		processingTimeSec := time.Since(stats.RunStarted).Seconds()
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

func (r *Replicator) priorityReplicate(pri PriorityRepJob, timeout time.Duration) bool {
	r.runningDevicesLock.Lock()
	rd, ok := r.runningDevices[deviceKey(pri.FromDevice, pri.Policy)]
	r.runningDevicesLock.Unlock()
	if ok {
		return rd.PriorityReplicate(pri, timeout)
	}
	return false
}

func (r *Replicator) getDeviceProgress() map[string]map[string]interface{} {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	deviceProgress := make(map[string]map[string]interface{})
	for key, stats := range r.stats["object-replicator"] {
		deviceProgress[key] = map[string]interface{}{
			"StartDate":          stats.DeviceStarted,
			"LastUpdate":         stats.LastCheckin,
			"LastPassDuration":   stats.LastPassDuration,
			"LastPassFinishDate": stats.LastPassDate,
			"LastPassUpdate":     stats.RunStarted,
			"TotalPasses":        stats.TotalPasses,
			"CancelCount":        r.cancelCounts[key],
		}
		for k, v := range stats.Stats {
			deviceProgress[key][k] = v
		}
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
			stats.RunStarted = time.Now()
			for k := range stats.Stats {
				stats.Stats[k] = 0
			}
		case "FullReplicateCount", "PassComplete":
			stats.LastPassDuration = time.Since(stats.RunStarted)
			stats.LastPassDate = time.Now()
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
	go r.startWebServer()
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
		for _, dev := range devices {
			rd := newReplicationDevice(dev, policy, r)
			key := rd.Key()
			r.runningDevices[key] = rd
			go func(rd *replicationDevice) {
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

func NewReplicator(serverconf conf.Config, flags *flag.FlagSet) (srv.Daemon, srv.LowLevelLogger, error) {
	if !serverconf.HasSection("object-replicator") {
		return nil, nil, fmt.Errorf("Unable to find object-replicator config section")
	}
	concurrency := int(serverconf.GetInt("object-replicator", "concurrency", 1))
	updaterConcurrency := int(serverconf.GetInt("object-updater", "concurrency", 2))

	logLevelString := serverconf.GetDefault("object-replicator", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))

	replicator := &Replicator{
		reserve:             serverconf.GetInt("object-replicator", "fallocate_reserve", 0),
		reconCachePath:      serverconf.GetDefault("object-replicator", "recon_cache_path", "/var/cache/swift"),
		checkMounts:         serverconf.GetBool("object-replicator", "mount_check", true),
		deviceRoot:          serverconf.GetDefault("object-replicator", "devices", "/srv/node"),
		port:                int(serverconf.GetInt("object-replicator", "bind_port", 6500)),
		bindIp:              serverconf.GetDefault("object-replicator", "bind_ip", "0.0.0.0"),
		quorumDelete:        serverconf.GetBool("object-replicator", "quorum_delete", false),
		reclaimAge:          int64(serverconf.GetInt("object-replicator", "reclaim_age", int64(common.ONE_WEEK))),
		incomingLimitPerDev: int64(serverconf.GetInt("object-replicator", "incoming_limit", 3)),

		runningDevices:          make(map[string]ReplicationDevice),
		updatingDevices:         make(map[string]*updateDevice),
		cancelCounts:            make(map[string]int64),
		objectRings:             make(map[int]replicationRing),
		replicateConcurrencySem: make(chan struct{}, concurrency),
		updateConcurrencySem:    make(chan struct{}, updaterConcurrency),
		updateStat:              make(chan statUpdate),
		devices:                 make(map[string]bool),
		partitions:              make(map[string]bool),
		onceDone:                make(chan struct{}),
		client:                  &http.Client{Timeout: time.Second * 60},
		incomingSem:             make(map[string]chan struct{}),
		stats: map[string]map[string]*DeviceStats{
			"object-replicator": {},
			"object-updater":    {},
		},
	}

	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get hash prefix and suffix")
	}
	for _, policy := range conf.LoadPolicies() {
		if policy.Type != "replication" {
			continue
		}
		if replicator.objectRings[policy.Index], err = GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index); err != nil {
			return nil, nil, fmt.Errorf("Unable to load ring for Policy %d.", policy.Index)
		}
	}
	if replicator.containerRing, err = GetRing("container", hashPathPrefix, hashPathSuffix, 0); err != nil {
		return nil, nil, fmt.Errorf("Error loading container ring: %v", err)
	}
	if replicator.logger, err = srv.SetupLogger("object-replicator", &logLevel, flags); err != nil {
		return nil, nil, fmt.Errorf("Error setting up logger: %v", err)
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
	return replicator, replicator.logger, nil
}
