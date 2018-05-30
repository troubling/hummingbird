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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/middleware"
	"go.uber.org/zap"
)

// AuditForeverInterval represents how often a auditor check should be performed.
var AuditForeverInterval = 30 * time.Second
var expiredObject = fmt.Errorf("Expired Object")

// AuditorDaemon keeps track of object specific audit data.
type AuditorDaemon struct {
	checkMounts       bool
	driveRoot         string
	policies          conf.PolicyList
	idbAuditors       map[int]IndexDBAuditor
	logger            srv.LowLevelLogger
	bytesPerSecond    int64
	logTime           int64
	regFilesPerSecond int64
	zbFilesPerSecond  int64
	reconCachePath    string
	hashPathPrefix    string
	hashPathSuffix    string
}

// Auditor keeps track of general audit data.
type Auditor struct {
	*AuditorDaemon
	auditorType                   string
	mode                          string
	filesPerSecond                int64
	passStart, lastLog            time.Time
	passes, totalPasses           int64
	bytesProcessed, totalBytes    int64
	quarantines, totalQuarantines int64
	errors, totalErrors           int64
}

func slowCopyMd5(file *os.File, bps int64) (int64, string, error) {
	h := md5.New()
	st := time.Now()
	bytesRead := int64(0)
	for {
		if b, err := io.CopyN(h, file, 64*1024); err != nil {
			if err != io.EOF {
				return bytesRead, "", err
			}
			bytesRead += b
			break
		} else {
			bytesRead += b
			rateLimitSleep(st, bytesRead, bps)
		}
	}
	return bytesRead, hex.EncodeToString(h.Sum(nil)), nil
}

type IndexDBAuditor interface {
	AuditItem(path string, item *IndexDBItem, md5BytesPerSec int64) (int64, error)
}

type ecAuditor struct{}

func (ecAuditor) AuditItem(path string, item *IndexDBItem, md5BytesPerSec int64) (int64, error) {
	finfo, err := os.Stat(path)
	if err != nil || !finfo.Mode().IsRegular() {
		if item.Nursery {
			// We're not going to do any quarantining here. It's likely the object
			// simply got stabilized and is gone.
			return 0, nil
		} else {
			return 0, fmt.Errorf("Object file isn't a normal file: %s", err)
		}
	}
	var hsh string
	var fBytes int64
	var ok bool
	metadata := map[string]string{}
	if err = json.Unmarshal(item.Metabytes, &metadata); err != nil {
		return 0, fmt.Errorf("Error decoding metadata: %s", err)
	}
	contentLength, err := strconv.ParseInt(metadata["Content-Length"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Error parsing content-length from metadata: %q %v", metadata["Content-Length"], err)
	}
	if item.Nursery {
		hsh, ok = metadata["ETag"]
		if !ok {
			return 0, fmt.Errorf("Metadata missing ETag: %s", metadata)
		}
		fBytes = contentLength
	} else {
		hsh = item.ShardHash
		if _, ds, _, _, err := parseECScheme(metadata["Ec-Scheme"]); err == nil {
			fBytes = ecShardLength(contentLength, ds)
		} else {
			return 0, fmt.Errorf("Error decoding ec-scheme: %s", err)
		}
	}
	if fBytes != finfo.Size() {
		return 0, fmt.Errorf("File size (%d) doesn't match metadata (%d)", finfo.Size(), fBytes)
	}
	if md5BytesPerSec > 0 {
		file, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("Error opening file: %s", err)
		}
		defer file.Close()
		bytesRead, calcHsh, err := slowCopyMd5(file, md5BytesPerSec)
		if err != nil {
			return bytesRead, fmt.Errorf("Error calc md5 file: %s", err)
		}
		if bytesRead != fBytes {
			return bytesRead, fmt.Errorf("did not read in entire file")
		}
		if calcHsh != hsh {
			return bytesRead, fmt.Errorf("File contents don't match object hash")
		}
		return bytesRead, nil
	}
	return 0, nil
}

type repAuditor struct{}

func (repAuditor) AuditItem(path string, item *IndexDBItem, md5BytesPerSec int64) (int64, error) {
	finfo, err := os.Stat(path)
	if err != nil || !finfo.Mode().IsRegular() {
		if item.Nursery {
			// We're not going to do any quarantining here. It's likely the object
			// simply got stabilized and is gone.
			return 0, nil
		} else {
			return 0, fmt.Errorf("Object file isn't a normal file: %s", err)
		}
	}
	metadata := map[string]string{}
	if err = json.Unmarshal(item.Metabytes, &metadata); err != nil {
		return 0, fmt.Errorf("Error decoding metadata: %s", err)
	}
	fBytes, err := strconv.ParseInt(metadata["Content-Length"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Error parsing content-length from metadata: %q %v", metadata["Content-Length"], err)
	}
	hsh, ok := metadata["ETag"]
	if !ok {
		return 0, fmt.Errorf("Metadata missing ETag: %s", metadata)
	}
	if fBytes != finfo.Size() {
		return 0, fmt.Errorf("File size (%d) doesn't match metadata (%d)", finfo.Size(), fBytes)
	}
	if md5BytesPerSec > 0 {
		file, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("Error opening file: %s", err)
		}
		defer file.Close()
		bytesRead, calcHsh, err := slowCopyMd5(file, md5BytesPerSec)
		if err != nil {
			return bytesRead, fmt.Errorf("Error calc md5 file: %s", err)
		}
		if bytesRead != fBytes {
			return bytesRead, fmt.Errorf("did not read in entire file")
		}
		if calcHsh != hsh {
			return bytesRead, fmt.Errorf("File contents don't match object hash")
		}
		return bytesRead, nil
	}
	return 0, nil
}

func QuarantineItem(db *IndexDB, item *IndexDBItem) error {
	itemPath, err := db.WholeObjectPath(item.Hash, item.Shard, item.Timestamp, item.Nursery)
	if err != nil {
		return err
	}
	itemName := filepath.Base(itemPath)
	objsDir := filepath.Dir(filepath.Dir(filepath.Dir(itemPath)))
	driveDir := filepath.Dir(objsDir)
	quarantineDir := filepath.Join(driveDir, "quarantined", filepath.Base(objsDir), itemName)
	if err := os.MkdirAll(quarantineDir, 0755); err != nil {
		return err
	}
	dest := filepath.Join(quarantineDir, itemName)
	var rerr error
	if err = os.Rename(itemPath, dest); err != nil && !os.IsNotExist(err) {
		rerr = err
	}
	metaName := filepath.Join(quarantineDir, itemName+".idbmeta")
	f, err := os.OpenFile(metaName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(item.Metabytes)
	f.Close()
	if err != nil {
		if rmErr := os.Remove(metaName); rmErr != nil {
			return rmErr
		}
		return err
	}
	err = db.Remove(item.Hash, item.Shard, item.Timestamp, item.Nursery)
	if err == nil && rerr != nil {
		return rerr
	}
	return err
}

// OneTimeChan returns a channel that will yield the current time once, then is closed.
func OneTimeChan() chan time.Time {
	c := make(chan time.Time, 1)
	c <- time.Now()
	close(c)
	return c
}

// rateLimitSleep long enough to achieve the target rate limit.
func rateLimitSleep(startTime time.Time, done int64, rate int64) {
	shouldHaveDone := int64(time.Since(startTime)/time.Second) * rate
	if done > shouldHaveDone {
		time.Sleep(time.Second * time.Duration((done-shouldHaveDone)/rate))
	}
}

// auditHash of object hash dir. if md5BytesPerSec == 0 then it will not calc md5
func auditHash(hashPath string, md5BytesPerSec int64) (bytesProcessed int64, err error) {
	objFiles, err := fs.ReadDirNames(hashPath)
	if err != nil {
		return 0, fmt.Errorf("Error reading hash dir")
	}
	for _, file := range objFiles {
		filePath := filepath.Join(hashPath, file)

		ext := filepath.Ext(filePath)
		if ext != ".data" && ext != ".ts" && ext != ".meta" {
			return bytesProcessed, fmt.Errorf("Object file has invalid extension %s", ext)
		}

		finfo, err := os.Stat(filePath)
		if err != nil || !finfo.Mode().IsRegular() {
			return bytesProcessed, fmt.Errorf("Object file isn't a normal file: %s", err)
		}

		metadata, err := common.SwiftObjectReadMetadata(filePath)
		if err != nil {
			return bytesProcessed, fmt.Errorf("Error getting file metadata: %v", err)
		}
		if Expired(metadata) {
			return 0, expiredObject
		}

		if ext == ".data" {
			for _, reqEntry := range []string{"Content-Length", "Content-Type", "name", "ETag", "X-Timestamp"} {
				if _, ok := metadata[reqEntry]; !ok {
					return bytesProcessed, fmt.Errorf("Required metadata entry %s not found", reqEntry)
				}
			}
			contentLength, err := strconv.ParseInt(metadata["Content-Length"], 10, 64)
			if err != nil {
				return bytesProcessed, fmt.Errorf("Error parsing content-length from metadata: %q %v", metadata["Content-Length"], err)
			}
			if contentLength != finfo.Size() {
				return bytesProcessed, fmt.Errorf("File size (%d) doesn't match metadata (%d)", finfo.Size(), contentLength)
			}
			if md5BytesPerSec > 0 {
				file, err := os.Open(filePath)
				if err != nil {
					return bytesProcessed, fmt.Errorf("Error opening file: %s", err)
				}
				bytesRead, calcHsh, err := slowCopyMd5(file, md5BytesPerSec)
				if err != nil {
					return bytesRead, fmt.Errorf("Error calc md5 file: %s", err)
				}
				bytesProcessed += bytesRead
				if calcHsh != metadata["ETag"] {
					return bytesProcessed, fmt.Errorf("File contents don't match etag")
				}
			}
		} else if ext == ".ts" {
			for _, reqEntry := range []string{"name", "X-Timestamp"} {
				if _, ok := metadata[reqEntry]; !ok {
					return bytesProcessed, fmt.Errorf("Required metadata entry %s not found", reqEntry)
				}
			}
		}
	}
	return bytesProcessed, nil
}

// auditDB.  Runs auditFunc on all objects in the given DB.
func (a *Auditor) auditDB(devPath string, objRing ring.Ring, policy *conf.Policy) {
	dbpath := filepath.Join(devPath, PolicyDir(policy.Index), fmt.Sprintf("%s.db", policy.Type))
	if _, err := os.Stat(dbpath); os.IsNotExist(err) {
		a.logger.Debug("policy dbpath not found ", zap.String("policytype", policy.Type))
		return
	} else if err != nil {
		a.errors++
		a.totalErrors++
		a.logger.Error("Couldn't open indexdb", zap.String("dbBasePath", dbpath), zap.Error(err))
		return
	}
	policyDir := filepath.Dir(dbpath)
	path := filepath.Join(policyDir, policy.Type)
	temppath := filepath.Join(filepath.Dir(policyDir), "tmp")
	ringPartPower := bits.Len64(objRing.PartitionCount() - 1)
	zapLogger, ok := a.logger.(*zap.Logger)
	if !ok {
		a.logger.Error("Logger type assertion failed")
		zapLogger = zap.L()
	}
	dbPartPower, err := policy.GetDbPartPower()
	if err != nil {
		a.logger.Error("Could not GetDbPartPower", zap.Error(err))
		return
	}
	subdirs, err := policy.GetDbSubDirs()
	if err != nil {
		a.logger.Error("could not GetDbSubDirs", zap.Error(err))
		return
	}
	if _, ok := a.idbAuditors[policy.Index]; !ok {
		a.logger.Error("No auditor set policy", zap.String("policy-type", policy.Type), zap.Int("policy-index", policy.Index))
		return
	}
	db, err := NewIndexDB(dbpath, path, temppath, ringPartPower, int(dbPartPower), subdirs, 0, zapLogger, a.idbAuditors[policy.Index])
	if err != nil {
		a.errors++
		a.totalErrors++
		a.logger.Error("Couldn't open indexdb", zap.String("dbBasePath", dbpath), zap.Error(err))
		return
	}
	defer db.Close()

	marker := ""
	for {
		items, err := db.List("", "", marker, 1000)
		if err != nil {
			a.logger.Error("db.List failed", zap.String("dbpath", dbpath), zap.Error(err))
			return
		}
		for _, item := range items {
			itemPath, err := db.WholeObjectPath(item.Hash, item.Shard, item.Timestamp, item.Nursery)
			if err != nil {
				a.logger.Error("Error getting indexdb path for hash",
					zap.String("hash", item.Hash), zap.Error(err))
				continue
			}
			a.passes++
			a.totalPasses++
			var bytesPerSecond int64
			if a.auditorType != "ZBF" {
				bytesPerSecond = a.bytesPerSecond
			}
			bytes, err := a.idbAuditors[policy.Index].AuditItem(itemPath, item, bytesPerSecond)
			if err != nil {
				a.logger.Error("Failed audit and is being quarantined",
					zap.String("itemPath", itemPath), zap.String("auditorType", a.auditorType), zap.Error(err))
				err = QuarantineItem(db, item)
				if err != nil {
					a.logger.Error("Failed to quarantine indexdb item", zap.String("auditorType", a.auditorType), zap.String("itemPath", itemPath), zap.Error(err))
					continue
				}
				a.quarantines++
				a.totalQuarantines++
			}
			a.bytesProcessed += bytes
			a.totalBytes += bytes
			rateLimitSleep(a.passStart, a.totalPasses, a.filesPerSecond)
			rateLimitSleep(a.passStart, a.totalBytes, a.bytesPerSecond)

			if time.Since(a.lastLog) > (time.Duration(a.logTime) * time.Second) {
				a.statsReport()
			}
		}
		if len(items) == 0 {
			return
		}
		marker = items[len(items)-1].Hash
	}
}

// auditSuffix directory.  Lists hash dirs, calls auditHash() for each, and quarantines any with errors.
func (a *Auditor) auditSuffix(suffixDir string) {
	hashes, err := fs.ReadDirNames(suffixDir)
	if err != nil {
		a.errors++
		a.totalErrors++
		a.logger.Error("Error reading suffix dir", zap.String("suffixDir", suffixDir), zap.Error(err))
		return
	}
	for _, hash := range hashes {
		_, hexErr := hex.DecodeString(hash)
		hashDir := filepath.Join(suffixDir, hash)
		if finfo, err := os.Stat(hashDir); err != nil || len(hash) != 32 || hexErr != nil || !finfo.Mode().IsDir() {
			a.logger.Error("Skipping invalid file in suffix", zap.String("hashDir", hashDir), zap.Error(err))
			continue
		}
		a.passes++
		a.totalPasses++
		var bps int64
		if a.auditorType != "ZBF" {
			bps = a.bytesPerSecond
		}
		bytesProcessed, err := auditHash(hashDir, bps)
		a.bytesProcessed += bytesProcessed
		a.totalBytes += bytesProcessed
		rateLimitSleep(a.passStart, a.totalPasses, a.filesPerSecond)
		rateLimitSleep(a.passStart, a.totalBytes, a.bytesPerSecond)
		if err == expiredObject {
			a.logger.Debug("Removing expired object", zap.String("hashDir", hashDir))
			os.RemoveAll(hashDir)
		} else if err != nil {
			a.logger.Error("Failed audit and is being quarantined",
				zap.String("hashDir", hashDir),
				zap.Error(err))
			QuarantineHash(hashDir)
			InvalidateHash(hashDir)
			a.quarantines++
			a.totalQuarantines++
		}
	}
}

// auditPartition directory.  Lists suffixes in the partition and calls auditSuffix() for each.
func (a *Auditor) auditPartition(partitionDir string) {
	suffixes, err := fs.ReadDirNames(partitionDir)
	if err != nil {
		a.errors++
		a.totalErrors++
		a.logger.Error("Error reading partition dir ", zap.String("partitionDir", partitionDir), zap.Error(err))
		return
	}
	for _, suffix := range suffixes {
		suffixDir := filepath.Join(partitionDir, suffix)
		if suffix == ".lock" || suffix == "hashes.pkl" || suffix == "hashes.invalid" {
			continue
		}
		_, hexErr := strconv.ParseInt(suffix, 16, 64)
		if finfo, err := os.Stat(suffixDir); err != nil || len(suffix) != 3 || hexErr != nil || !finfo.Mode().IsDir() {
			a.logger.Error("Skipping invalid file in partition.", zap.String("suffixDir", suffixDir), zap.Error(err))
			continue
		}
		a.auditSuffix(suffixDir)
		if time.Since(a.lastLog) > (time.Duration(a.logTime) * time.Second) {
			a.statsReport()
		}
	}
}

// auditDevice, checking for mount, list partitions, then call auditPartition() for each.
func (a *Auditor) auditDevice(devPath string) {
	defer srv.LogPanics(a.logger, "PANIC WHILE AUDITING DEVICE")

	if mounted, err := fs.IsMount(devPath); a.checkMounts && (err != nil || mounted != true) {
		a.logger.Error("Skipping unmounted device", zap.String("devPath", devPath), zap.Error(err))
		return
	}

	for _, policy := range a.policies {
		if policy.Type == "replication" {
			objPath := filepath.Join(devPath, PolicyDir(policy.Index))
			partitions, err := fs.ReadDirNames(objPath)
			if err != nil {
				if !os.IsNotExist(err) {
					a.errors++
					a.totalErrors++
					a.logger.Error("Error reading objects dir", zap.String("objPath", objPath), zap.Error(err))
				}
				continue
			}
			for _, partition := range partitions {
				_, intErr := strconv.ParseInt(partition, 10, 64)
				partitionDir := filepath.Join(objPath, partition)
				if finfo, err := os.Stat(partitionDir); err != nil || intErr != nil || !finfo.Mode().IsDir() {
					a.logger.Error("Skipping invalid file in objects directory",
						zap.String("partitionDir", partitionDir), zap.Error(err))
					continue
				}
				a.auditPartition(partitionDir)
			}
		} else {
			r, err := ring.GetRing("object", a.hashPathPrefix, a.hashPathSuffix, policy.Index)
			if err != nil {
				a.logger.Error("Error getting object ring", zap.Int("policyindex", policy.Index))
				continue
			}
			a.auditDB(devPath, r, policy)
		}
	}
}

// statsReport logs auditing stats and dump recon cache.  Called periodically by auditPartition().
func (a *Auditor) statsReport() {
	now := time.Now()
	total := float64(now.Sub(a.passStart)) / float64(time.Second)
	sinceLast := float64(now.Sub(a.lastLog)) / float64(time.Second)
	frate := float64(a.passes) / sinceLast
	brate := float64(a.bytesProcessed) / sinceLast
	audit := 0.0      // TODO maybe
	audit_rate := 0.0 // TODO maybe
	a.logger.Info("statsReport",
		zap.String("Object audit", a.auditorType),
		zap.String("Since", a.lastLog.Format(time.ANSIC)),
		zap.Int64("Locally passed", a.passes),
		zap.Int64("Locally quarantined", a.quarantines),
		zap.Int64("Locally errored", a.errors),
		zap.Float64("files/sec", frate),
		zap.Float64("bytes/sec", brate),
		zap.Float64("Total time", total),
		zap.Float64("Auditing Time", audit),
		zap.Float64("Auditing Rate", audit_rate))

	middleware.DumpReconCache(a.reconCachePath, "object",
		map[string]interface{}{"object_auditor_stats_" + a.auditorType: map[string]interface{}{
			"errors":          a.errors,
			"passes":          a.passes,
			"quarantined":     a.quarantines,
			"bytes_processed": a.bytesProcessed,
			"start_time":      float64(a.passStart.UnixNano()) / float64(time.Second), //???
			"audit_time":      audit,
		}})
	a.passes = 0
	a.quarantines = 0
	a.errors = 0
	a.bytesProcessed = 0
	a.lastLog = now
}

// finalLog final stats summary for the audit pass.
func (a *Auditor) finalLog() {
	elapsed := float64(time.Since(a.passStart)) / float64(time.Second)
	frate := float64(a.totalPasses) / elapsed
	brate := float64(a.totalBytes) / elapsed
	audit := 0.0      // TODO maybe
	audit_rate := 0.0 // TODO maybe
	a.logger.Info("Object Audit",
		zap.String("Auditor type", a.auditorType),
		zap.String("Mode", a.mode),
		zap.Float64("completed", elapsed),
		zap.Int64("Total quarantined", a.totalQuarantines),
		zap.Int64("Total errors", a.totalErrors),
		zap.Float64("Total files/sec", frate),
		zap.Float64("Total bytes/sec", brate),
		zap.Float64("Auditing time", audit),
		zap.Float64("Auditing rate", audit_rate))
}

// run audit passes of the whole server until c is closed.
func (a *Auditor) run(c <-chan time.Time) {
	for a.passStart = range c {
		middleware.DumpReconCache(a.reconCachePath, "object",
			map[string]interface{}{"object_auditor_stats_" + a.auditorType: nil})
		a.passes = 0
		a.bytesProcessed = 0
		a.quarantines = 0
		a.errors = 0
		a.totalPasses = 0
		a.totalBytes = 0
		a.totalQuarantines = 0
		a.totalErrors = 0
		a.logger.Info("Begin object audit",
			zap.String("mode", a.mode),
			zap.String("auditorType", a.auditorType),
			zap.String("driveRoot", a.driveRoot))
		devices, err := fs.ReadDirNames(a.driveRoot)
		if err != nil {
			a.logger.Error("Unable to list devices", zap.String("driveRoot", a.driveRoot), zap.Error(err))
			continue
		}
		for _, dev := range devices {
			a.auditDevice(filepath.Join(a.driveRoot, dev))
		}
		a.finalLog()
	}
}

// Run a single audit pass.
func (d *AuditorDaemon) Run() {
	wg := sync.WaitGroup{}
	if d.zbFilesPerSecond > 0 {
		wg.Add(1)
		go func() {
			zba := Auditor{AuditorDaemon: d, auditorType: "ZBF", mode: "once", filesPerSecond: d.zbFilesPerSecond}
			zba.run(OneTimeChan())
			wg.Done()
		}()
	}
	reg := Auditor{AuditorDaemon: d, auditorType: "ALL", mode: "once", filesPerSecond: d.regFilesPerSecond}
	reg.run(OneTimeChan())
	wg.Wait()
}

// RunForever triggering audit passes every time AuditForeverInterval has passed.
func (d *AuditorDaemon) RunForever() {
	if d.zbFilesPerSecond > 0 {
		zba := Auditor{AuditorDaemon: d, auditorType: "ZBF", mode: "forever", filesPerSecond: d.zbFilesPerSecond}
		go zba.run(time.Tick(AuditForeverInterval))
	}
	reg := Auditor{AuditorDaemon: d, auditorType: "ALL", mode: "forever", filesPerSecond: d.regFilesPerSecond}
	reg.run(time.Tick(AuditForeverInterval))
}

// NewAuditor returns a new AuditorDaemon with the given conf.
func NewAuditorDaemon(serverconf conf.Config, flags *flag.FlagSet, cnf srv.ConfigLoader) (*AuditorDaemon, error) {
	var err error
	if !serverconf.HasSection("object-auditor") {
		return nil, fmt.Errorf("Unable to find object-auditor config section")
	}
	d := &AuditorDaemon{}
	if d.policies, err = cnf.GetPolicies(); err != nil {
		return nil, err
	}
	d.idbAuditors = map[int]IndexDBAuditor{}
	for _, policy := range d.policies {
		switch policy.Type {
		case "hec":
			d.idbAuditors[policy.Index] = ecAuditor{}
		case "rep":
			d.idbAuditors[policy.Index] = repAuditor{}
		}
	}
	d.hashPathPrefix, d.hashPathSuffix, err = cnf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, fmt.Errorf("Unable to load hashpath prefix and suffix")
	}
	d.driveRoot = serverconf.GetDefault("object-auditor", "devices", "/srv/node")
	d.checkMounts = serverconf.GetBool("object-auditor", "mount_check", true)

	logLevelString := serverconf.GetDefault("object-auditor", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	if d.logger, err = srv.SetupLogger("object-auditor", &logLevel, flags); err != nil {
		return nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	d.bytesPerSecond = serverconf.GetInt("object-auditor", "bytes_per_second", 10000000)
	d.regFilesPerSecond = serverconf.GetInt("object-auditor", "files_per_second", 20)
	d.zbFilesPerSecond = serverconf.GetInt("object-auditor", "zero_byte_files_per_second", 50)
	d.reconCachePath = serverconf.GetDefault("object-auditor", "recon_cache_path", "/var/cache/swift")
	d.logTime = serverconf.GetInt("object-auditor", "log_time", 3600)
	return d, nil
}
