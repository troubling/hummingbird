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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
)

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
	rawxattr, err := common.SwiftObjectRawReadMetadata(fp.Fd())
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

type swiftDevice struct {
	// If you have a better way to make struct methods that are overridable for tests, please call my house.
	i interface {
		beginReplication(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse, headers map[string]string)
		listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool)
		syncFile(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error)
		replicateUsingHashes(rjob replJob, moreNodes ring.MoreNodes) (int64, error)
		replicateAll(rjob replJob, isHandoff bool) (int64, error)
		cleanTemp()
		listPartitions() ([]string, []string, error)
		replicatePartition(partition string)
	}
	r      *Replicator
	dev    *ring.Device
	policy int
	cancel chan struct{}
}

type beginReplicationResponse struct {
	dev    *ring.Device
	conn   RepConn
	hashes map[string]string
	err    error
}

type syncFileArg struct {
	conn RepConn
	dev  *ring.Device
}

type replJob struct {
	partition string
	nodes     []*ring.Device
	headers   map[string]string
}

type quarantineFileError struct {
	msg string
}

func (q quarantineFileError) Error() string {
	return q.msg
}

func (rd *swiftDevice) UpdateStat(stat string, amount int64) {
	rd.r.updateStat <- statUpdate{"object-replicator", rd.Key(), stat, amount}
}

func (rd *swiftDevice) listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
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

func (rd *swiftDevice) syncFile(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error) {
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
				rd.UpdateStat("FilesSent", 1)
				rd.UpdateStat("BytesSent", fileSize)
			}
		}
	}
	return syncs, insync, nil
}

func spaceWriter(w http.ResponseWriter, c chan struct{}, d chan struct{}) {
	defer close(d)
	for {
		select {
		case <-time.After(time.Minute):
			w.Write([]byte(" "))
		case <-c:
			return
		}
	}
}

func (rd *swiftDevice) PriorityReplicate(w http.ResponseWriter, pri PriorityRepJob) {
	if !fs.Exists(filepath.Join(rd.r.deviceRoot, pri.FromDevice.Device, PolicyDir(rd.policy), strconv.FormatUint(pri.Partition, 10))) {
		w.WriteHeader(404)
		return
	}
	partition := strconv.FormatUint(pri.Partition, 10)
	_, handoff := rd.r.objectRings[rd.policy].GetJobNodes(pri.Partition, pri.FromDevice.Id)
	jobType := "local"
	if handoff {
		jobType = "handoff"
	}
	policy := rd.r.policies[rd.policy]
	if policy == nil {
		w.WriteHeader(400)
		return
	}
	rd.r.logger.Info("PriorityReplicationJob",
		zap.Uint64("partition", pri.Partition),
		zap.String("jobType", jobType),
		zap.String("From Device", pri.FromDevice.Device),
		zap.String("To Device", pri.ToDevice.Device))
	rjob := replJob{
		partition: partition, nodes: []*ring.Device{pri.ToDevice},
		headers: map[string]string{"X-Force-Acquire": "true"}}
	var synced int64
	var err error
	w.WriteHeader(200)
	swc := make(chan struct{})
	swd := make(chan struct{})
	go spaceWriter(w, swc, swd)
	if handoff || (policy.Type == "replication-nursery" &&
		!common.LooksTrue(policy.Config["cache_hash_dirs"])) {
		synced, err = rd.i.replicateAll(rjob, handoff)
	} else {
		synced, err = rd.i.replicateUsingHashes(rjob, &NoMoreNodes{})
	}
	rd.UpdateStat("PriorityRepsDone", 1)
	prr := PriorityReplicationResult{ObjectsReplicated: synced, Success: err == nil}
	if err != nil {
		prr.ErrorMsg = fmt.Sprintf("%v", err)
	}
	b, err := json.Marshal(prr)
	if err != nil {
		rd.r.logger.Error("error prirep jsoning", zap.Error(err))
		b = []byte("There was an internal server error generating JSON.")
	}
	close(swc)
	<-swd
	w.Write(b)
	w.Write([]byte("\n"))
}

func (rd *swiftDevice) beginReplication(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse, headers map[string]string) {
	var brr BeginReplicationResponse
	if headers == nil {
		headers = map[string]string{}
	}
	headers["X-Trans-Id"] = fmt.Sprintf("%s-%d", common.UUID(), dev.Id)

	if rc, err := NewRepConn(dev, partition, rd.policy, headers, rd.r.CertFile, rd.r.KeyFile, rd.r.rcTimeout); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else if err := rc.SendMessage(BeginReplicationRequest{Device: dev.Device, Partition: partition, NeedHashes: hashes}); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else if err := rc.RecvMessage(&brr); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else {
		rChan <- beginReplicationResponse{dev: dev, conn: rc, hashes: brr.Hashes}
	}
}

func (rd *swiftDevice) replicateUsingHashes(rjob replJob, moreNodes ring.MoreNodes) (int64, error) {
	path := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy), rjob.partition)
	syncCount := int64(0)
	startGetHashesRemote := time.Now()
	remoteHashes := make(map[int]map[string]string)
	remoteConnections := make(map[int]RepConn)
	rChan := make(chan beginReplicationResponse)
	for _, dev := range rjob.nodes {
		go rd.i.beginReplication(dev, rjob.partition, true, rChan, rjob.headers)
	}
	for i := 0; i < len(rjob.nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteHashes[rData.dev.Id] = rData.hashes
			remoteConnections[rData.dev.Id] = rData.conn
		} else if rData.err == RepUnmountedError {
			if nextNode := moreNodes.Next(); nextNode != nil {
				go rd.i.beginReplication(nextNode, rjob.partition, true, rChan, rjob.headers)
				rjob.nodes = append(rjob.nodes, nextNode)
			} else {
				break
			}
		}
	}
	if len(remoteHashes) == 0 {
		return 0, fmt.Errorf("replicateAll could get no remote connections")
	}

	timeGetHashesRemote := float64(time.Now().Sub(startGetHashesRemote)) / float64(time.Second)
	startGetHashesLocal := time.Now()

	recalc := []string{}
	hashes, err := GetHashes(rd.r.deviceRoot, rd.dev.Device, rjob.partition, recalc, rd.r.reclaimAge, rd.policy, rd.r.logger)
	if err != nil {
		rd.r.logger.Error("[replicateUsingHashes] error getting local hashes", zap.Error(err))
		return 0, err
	}
	for suffix, localHash := range hashes {
		for _, remoteHash := range remoteHashes {
			if remoteHash[suffix] != "" && localHash != remoteHash[suffix] {
				recalc = append(recalc, suffix)
				break
			}
		}
	}
	hashes, err = GetHashes(rd.r.deviceRoot, rd.dev.Device, rjob.partition, recalc, rd.r.reclaimAge, rd.policy, rd.r.logger)
	if err != nil {
		rd.r.logger.Error("[replicateUsingHashes] error recalculating local hashes", zap.Error(err))
		return 0, err
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
		for _, dev := range rjob.nodes {
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
			syncCount += int64(syncs)
		} else {
			rd.r.logger.Error("[syncFile]", zap.Error(err))
			return syncCount, err
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected() {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	timeSyncing := float64(time.Now().Sub(startSyncing)) / float64(time.Second)
	if syncCount > 0 {
		rd.r.logger.Info("[replicateUsingHashes]",
			zap.String("Partition", path),
			zap.Any("Files Synced", syncCount),
			zap.Float64("timeGetHashesRemote", timeGetHashesRemote),
			zap.Float64("timeGetHashesLocal", timeGetHashesLocal),
			zap.Float64("timeSyncing", timeSyncing))
	}
	return syncCount, nil
}

func (rd *swiftDevice) replicateAll(rjob replJob, isHandoff bool) (int64, error) {
	path := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy), rjob.partition)
	syncCount := int64(0)
	remoteConnections := make(map[int]RepConn)
	rChan := make(chan beginReplicationResponse)
	for _, dev := range rjob.nodes {
		go rd.i.beginReplication(dev, rjob.partition, false, rChan, rjob.headers)
	}
	for i := 0; i < len(rjob.nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteConnections[rData.dev.Id] = rData.conn
		}
	}
	if len(remoteConnections) == 0 {
		return 0, fmt.Errorf("replicateAll could get no remote connections")
	}

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go rd.i.listObjFiles(objChan, cancel, path, func(string) bool { return true })
	for objFile := range objChan {
		toSync := make([]*syncFileArg, 0)
		for _, dev := range rjob.nodes {
			if remoteConnections[dev.Id] != nil && !remoteConnections[dev.Id].Disconnected() {
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if len(toSync) == 0 {
			return 0, fmt.Errorf("replicateAll could get no remote connections to sync")
		}
		if syncs, insync, err := rd.i.syncFile(objFile, toSync, true); err == nil {
			syncCount += int64(syncs)

			success := insync == len(rjob.nodes)
			if rd.r.quorumDelete {
				success = insync >= len(rjob.nodes)/2+1
			}
			if success && isHandoff {
				os.Remove(objFile)
				os.Remove(filepath.Dir(objFile))
			}
		} else {
			rd.r.logger.Error("[syncFile]", zap.Error(err))
			return syncCount, err
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected() {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	if syncCount > 0 {
		rd.r.logger.Info("[replicateAll]", zap.String("Partition", path), zap.Any("Files Synced", syncCount))
	}
	return syncCount, nil
}

func (rd *swiftDevice) Key() string {
	return deviceKeyId(rd.dev.Device, rd.policy)
}

func (rd *swiftDevice) cleanTemp() {
	tempDir := TempDirPath(rd.r.deviceRoot, rd.dev.Device)
	if tmpContents, err := ioutil.ReadDir(tempDir); err == nil {
		for _, tmpEntry := range tmpContents {
			if time.Since(tmpEntry.ModTime()) > tmpEmptyTime {
				os.RemoveAll(filepath.Join(tempDir, tmpEntry.Name()))
			}
		}
	}
}

func (rd *swiftDevice) replicatePartition(partition string) {
	rd.r.replicateConcurrencySem <- struct{}{}
	defer func() {
		<-rd.r.replicateConcurrencySem
	}()
	partitioni, err := strconv.ParseUint(partition, 10, 64)
	if err != nil {
		return
	}
	nodes, handoff := rd.r.objectRings[rd.policy].GetJobNodes(partitioni, rd.dev.Id)
	policy := rd.r.policies[rd.policy]
	if policy == nil {
		return
	}
	rjob := replJob{partition: partition, nodes: nodes}
	if handoff || (policy.Type == "replication-nursery" &&
		!common.LooksTrue(policy.Config["cache_hash_dirs"])) {
		rd.i.replicateAll(rjob, handoff)
	} else {
		rd.i.replicateUsingHashes(rjob, rd.r.objectRings[rd.policy].GetMoreNodes(partitioni))
	}
	rd.UpdateStat("PartitionsDone", 1)
}

func (rd *swiftDevice) listPartitions() ([]string, []string, error) {
	// returns a list of all partitions and a subset of that list- just the handoffs
	objPath := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy))
	partitions, err := filepath.Glob(filepath.Join(objPath, "[0-9]*"))
	if err != nil {
		return nil, nil, err
	}
	partitionList := make([]string, 0, len(partitions))
	handoffList := []string{}
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
	for _, partition := range partitionList {
		if pi, err := strconv.ParseUint(partition, 10, 64); err == nil {
			if _, handoff := rd.r.objectRings[rd.policy].GetJobNodes(pi, rd.dev.Id); handoff {
				handoffList = append(handoffList, partition)
			}
		}
	}
	return partitionList, handoffList, nil
}

func (rd *swiftDevice) Scan() {
	defer srv.LogPanics(rd.r.logger, fmt.Sprintf("PANIC REPLICATING DEVICE: %s", rd.dev.Device))
	rd.UpdateStat("startRun", 1)
	if mounted, err := fs.IsMount(filepath.Join(rd.r.deviceRoot, rd.dev.Device)); rd.r.checkMounts && (err != nil || mounted != true) {
		rd.r.logger.Error("[replicateDevice] Drive not mounted", zap.String("Device", rd.dev.Device), zap.Error(err))
		return
	}
	if fs.Exists(filepath.Join(rd.r.deviceRoot, rd.dev.Device, "lock_device")) {
		return
	}

	rd.i.cleanTemp()

	allPartitionList, handoffPartitions, err := rd.i.listPartitions()
	if err != nil {
		rd.r.logger.Error("[replicateDevice] Error getting partition list",
			zap.String("Device", rd.dev.Device),
			zap.Error(err))
		return
	} else if len(allPartitionList) == 0 {
		rd.r.logger.Info("[replicateDevice] No partitions found",
			zap.String("filepath", filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy))))
		return
	}
	rd.UpdateStat("PartitionsTotal", int64(len(allPartitionList)))

	lastListing := time.Now()
	handoffsForLog := len(handoffPartitions)
	for i, partition := range allPartitionList {
		rd.UpdateStat("checkin", 1)
		select {
		case <-rd.cancel:
			{
				rd.r.logger.Error("replicateDevice canceled for device", zap.String("Device", rd.dev.Device))
				return
			}
		default:
		}
		rd.i.replicatePartition(partition)
		if j := common.StringInSliceIndex(partition, handoffPartitions); j >= 0 {
			handoffPartitions = append(handoffPartitions[:j], handoffPartitions[j+1:]...)
		}
		time.Sleep(replicatePartSleepTime)
		if i%handoffToAllMod == 0 && len(handoffPartitions) > 0 {
			var p string
			p, handoffPartitions = handoffPartitions[0], handoffPartitions[1:]
			rd.i.replicatePartition(p)
		}
		if len(handoffPartitions) == 0 {
			if handoffsForLog > 0 {
				rd.r.logger.Info("[replicateDevice] Completed handoff replication pass",
					zap.Int("handoffsProcessed", handoffsForLog),
					zap.Duration("handoffDuration", time.Since(lastListing)))
				handoffsForLog = 0
			}
			if time.Since(lastListing) > handoffListDirFreq {
				if _, handoffPartitions, err = rd.i.listPartitions(); err != nil {
					rd.r.logger.Error("[replicateDevice] Error getting handoff partition list",
						zap.String("Device", rd.dev.Device),
						zap.Error(err))
				}
				lastListing = time.Now()
				handoffsForLog = len(handoffPartitions)
			}
		}
	}
	rd.UpdateStat("FullReplicateCount", 1)
}

func (rd *swiftDevice) Cancel() {
	rd.UpdateStat("cancel", 1)
	close(rd.cancel)
}

func (rd *swiftDevice) ScanLoop() {
	for {
		select {
		case <-rd.cancel:
			return
		default:
			rd.Scan()
		}
		time.Sleep(replicateLoopSleepTime)
	}
}

type NoMoreNodes struct{}

func (n *NoMoreNodes) Next() *ring.Device {
	return nil
}

// SwiftObject implements an Object that is compatible with Swift's object server.
type SwiftObject struct {
	file         *os.File
	afw          fs.AtomicFileWriter
	hashDir      string
	tempDir      string
	dataFile     string
	metaFile     string
	workingClass string
	metadata     map[string]string
	reserve      int64
	reclaimAge   int64
	asyncWG      *sync.WaitGroup // Used to keep track of async goroutines
}

// Metadata returns the object's metadata.
func (o *SwiftObject) Metadata() map[string]string {
	return o.metadata
}

// ContentLength parses and returns the Content-Length for the object.
func (o *SwiftObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(o.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

// Quarantine removes the object's underlying files to the Quarantined directory on the device.
func (o *SwiftObject) Quarantine() error {
	o.Close()
	if QuarantineHash(o.hashDir) == nil {
		return InvalidateHash(o.hashDir)
	}
	return nil
}

// Exists returns true if the object exists, that is if it has a .data file.
func (o *SwiftObject) Exists() bool {
	return strings.HasSuffix(o.dataFile, ".data")
}

// Copy copies all data from the underlying .data file to the given writers.
func (o *SwiftObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if len(dsts) == 1 {
		return io.Copy(dsts[0], o.file)
	} else {
		return common.Copy(o.file, dsts...)
	}
}

// CopyRange copies data in the range of start to end from the underlying .data file to the writer.
func (o *SwiftObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	if _, err := o.file.Seek(start, os.SEEK_SET); err != nil {
		return 0, err
	}
	return common.CopyN(o.file, end-start, w)
}

// Repr returns a string that identifies the object in some useful way, used for logging.
func (o *SwiftObject) Repr() string {
	if o.dataFile != "" && o.metaFile != "" {
		return fmt.Sprintf("SwiftObject(%s, %s)", o.dataFile, o.metaFile)
	} else if o.dataFile != "" {
		return fmt.Sprintf("SwiftObject(%s)", o.dataFile)
	}
	return fmt.Sprintf("SwiftObject(%s)", o.hashDir)
}

func (o *SwiftObject) newFile(class string, size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = fs.NewAtomicFileWriter(o.tempDir, o.hashDir); err != nil {
		return nil, fmt.Errorf("Error creating temp file: %v", err)
	}
	if err := o.afw.Preallocate(size, o.reserve); err != nil {
		o.afw.Abandon()
		return nil, DriveFullError
	}
	o.workingClass = class
	return o.afw, nil
}

// SetData is called to set the object's data.  It takes a size (if available, otherwise set to zero).
func (o *SwiftObject) SetData(size int64) (io.Writer, error) {
	return o.newFile("data", size)
}

// Commit commits an open data file to disk, given the metadata.
func (o *SwiftObject) Commit(metadata map[string]string) error {
	defer o.afw.Abandon()
	timestamp, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("No timestamp in metadata")
	}
	if err := common.SwiftObjectWriteMetadata(o.afw.Fd(), metadata); err != nil {
		return fmt.Errorf("Error writing metadata: %v", err)
	}
	fileName := filepath.Join(o.hashDir, fmt.Sprintf("%s.%s", timestamp, o.workingClass))
	o.afw.Save(fileName)
	o.asyncWG.Add(1)
	go func() {
		defer o.asyncWG.Done()
		HashCleanupListDir(o.hashDir, o.reclaimAge)
		if dir, err := os.OpenFile(o.hashDir, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
		InvalidateHash(o.hashDir)
	}()
	return nil
}

func (o *SwiftObject) CommitMetadata(metadata map[string]string) error {
	if _, err := o.newFile("meta", 0); err != nil {
		return err
	}
	defer o.Close()
	return o.Commit(metadata)
}

// Delete deletes the object.
func (o *SwiftObject) Delete(metadata map[string]string) error {
	if _, err := o.newFile("ts", 0); err != nil {
		return err
	} else {
		defer o.Close()
		return o.Commit(metadata)
	}
}

// Close releases any resources used by the instance of SwiftObject
func (o *SwiftObject) Close() error {
	if o.afw != nil {
		defer o.afw.Abandon()
		o.afw = nil
	}
	if o.file != nil {
		defer o.file.Close()
		o.file = nil
	}
	return nil
}

type SwiftEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	reclaimAge     int64
	policy         int
}

// New returns an instance of SwiftObject with the given parameters. Metadata is read in and if needData is true, the file is opened.  AsyncWG is a waitgroup if the object spawns any async operations
func (f *SwiftEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (Object, error) {
	var err error
	sor := &SwiftObject{reclaimAge: f.reclaimAge, reserve: f.reserve, asyncWG: asyncWG}
	sor.hashDir = ObjHashDir(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix, f.policy)
	sor.tempDir = TempDirPath(f.driveRoot, vars["device"])
	sor.dataFile, sor.metaFile = ObjectFiles(sor.hashDir)
	if sor.Exists() {
		var stat os.FileInfo
		if needData {
			if sor.file, err = os.Open(sor.dataFile); err != nil {
				return nil, err
			}
			if sor.metadata, err = OpenObjectMetadata(sor.file.Fd(), sor.metaFile); err != nil {
				sor.Quarantine()
				return nil, fmt.Errorf("Error getting metadata: %v", err)
			}
		} else {
			if sor.metadata, err = ObjectMetadata(sor.dataFile, sor.metaFile); err != nil {
				sor.Quarantine()
				return nil, fmt.Errorf("Error getting metadata: %v", err)
			}
		}
		if sor.file != nil {
			if stat, err = sor.file.Stat(); err != nil {
				sor.Close()
				return nil, fmt.Errorf("Error statting file: %v", err)
			}
		} else if stat, err = os.Stat(sor.dataFile); err != nil {
			return nil, fmt.Errorf("Error statting file: %v", err)
		}
		if contentLength, err := strconv.ParseInt(sor.metadata["Content-Length"], 10, 64); err != nil {
			sor.Quarantine()
			return nil, fmt.Errorf("Unable to parse content-length: %s", sor.metadata["Content-Length"])
		} else if stat.Size() != contentLength {
			sor.Quarantine()
			return nil, fmt.Errorf("File size doesn't match content-length: %d vs %d", stat.Size(), contentLength)
		}
	} else {
		sor.metadata, _ = ObjectMetadata(sor.dataFile, sor.metaFile) // ignore errors if deleted
	}
	return sor, nil
}

func (f *SwiftEngine) GetReplicationDevice(oring ring.Ring, dev *ring.Device, policy int, r *Replicator) (ReplicationDevice, error) {
	rd := &swiftDevice{
		r:      r,
		dev:    dev,
		policy: policy,
		cancel: make(chan struct{}),
	}
	rd.i = rd
	return rd, nil
}

var replicationDone = fmt.Errorf("Replication done")

// SwiftEngineConstructor creates a SwiftEngine given the object server configs.
func SwiftEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	reclaimAge := int64(config.GetInt("app:object-server", "reclaim_age", int64(common.ONE_WEEK)))
	return &SwiftEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		reclaimAge:     reclaimAge,
		policy:         policy.Index}, nil
}

func init() {
	RegisterObjectEngine("replication", SwiftEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = SwiftEngineConstructor
var _ Object = &SwiftObject{}
var _ ObjectEngine = &SwiftEngine{}
