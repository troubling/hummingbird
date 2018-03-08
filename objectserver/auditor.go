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

// AuditorDaemon keeps track of object specific audit data.
type AuditorDaemon struct {
	checkMounts       bool
	driveRoot         string
	policies          conf.PolicyList
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
	ecfuncs                       ECAuditFuncs
}

type ECAuditFuncs interface {
	AuditShard(path string, hash string, skipMd5 bool) (int64, error)
	AuditNurseryObject(path string, metabytes []byte, skipMd5 bool) (int64, error)
}

type RealECAuditFuncs struct{}

// AuditNurseryObject of indexdb shard, does nothing
func (RealECAuditFuncs) AuditNurseryObject(path string, metabytes []byte, skipMd5 bool) (int64, error) {
	finfo, err := os.Stat(path)
	if err != nil || !finfo.Mode().IsRegular() {
		return 0, fmt.Errorf("Object file isn't a normal file: %s", err)
	}

	bytes := int64(0)
	if !skipMd5 {
		metadata := map[string]string{}
		if err = json.Unmarshal(metabytes, &metadata); err != nil {
			return bytes, fmt.Errorf("Error decding metadata: %s", err)
		}
		hash, ok := metadata["ETag"]
		if !ok {
			return bytes, fmt.Errorf("Metadata missing ETag: %s", string(metabytes))
		}

		file, err := os.Open(path)
		if err != nil {
			return bytes, fmt.Errorf("Error opening file: %s", err)
		}
		h := md5.New()
		bytes, err = common.Copy(file, h)
		if err != nil {
			return bytes, fmt.Errorf("Error reading file: %s", err)
		}
		if hex.EncodeToString(h.Sum(nil)) != hash {
			return bytes, fmt.Errorf("File contents don't match object hash")
		}
	}
	return bytes, nil
}

// AuditShardHash of indexdb shard
func (RealECAuditFuncs) AuditShard(path string, hash string, skipMd5 bool) (int64, error) {
	finfo, err := os.Stat(path)
	if err != nil || !finfo.Mode().IsRegular() {
		return 0, fmt.Errorf("Object file isn't a normal file: %s", err)
	}

	bytes := int64(0)
	if !skipMd5 {
		file, err := os.Open(path)
		if err != nil {
			return bytes, fmt.Errorf("Error opening file: %s", err)
		}
		h := md5.New()
		bytes, err = common.Copy(file, h)
		if err != nil {
			return bytes, fmt.Errorf("Error reading file: %s", err)
		}
		if hex.EncodeToString(h.Sum(nil)) != hash {
			return bytes, fmt.Errorf("File contents don't match shard hash")
		}
	}
	return bytes, nil
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

// auditHash of object hash dir.
func auditHash(hashPath string, skipMd5 bool) (bytesProcessed int64, err error) {
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
			if !skipMd5 {
				file, err := os.Open(filePath)
				if err != nil {
					return bytesProcessed, fmt.Errorf("Error opening file: %s", err)
				}
				h := md5.New()
				bytes, err := common.Copy(file, h)
				if err != nil {
					return bytesProcessed, fmt.Errorf("Error reading file: %s", err)
				}
				bytesProcessed += bytes
				if hex.EncodeToString(h.Sum(nil)) != metadata["ETag"] {
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

func quarantineShard(db *IndexDB, hash string, shard int, timestamp int64, nursery bool) error {
	shardPath, err := db.WholeObjectPath(hash, shard, timestamp, nursery)
	if err != nil {
		return err
	}
	shardName := filepath.Base(shardPath)
	objsDir := filepath.Dir(filepath.Dir(filepath.Dir(shardPath)))
	driveDir := filepath.Dir(objsDir)
	quarantineDir := filepath.Join(driveDir, "quarantined", filepath.Base(objsDir))
	if err := os.MkdirAll(quarantineDir, 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(quarantineDir, shardName), os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	f.Close()
	return db.Remove(hash, shard, timestamp, nursery)
}

// auditDB.  Runs auditFunc on all objects in the given DB.
func (a *Auditor) auditDB(dbpath string, objRing ring.Ring) {
	policyDir := filepath.Dir(dbpath)
	path := filepath.Join(policyDir, "hec")
	temppath := filepath.Join(filepath.Dir(policyDir), "tmp")
	ringPartPower := bits.Len64(objRing.PartitionCount() - 1)
	zapLogger, ok := a.logger.(*zap.Logger)
	if !ok {
		a.logger.Error("Logger type assertion failed")
		zapLogger = zap.L()
	}
	db, err := NewIndexDB(dbpath, path, temppath, ringPartPower, 1, 32, zapLogger)
	if err != nil {
		a.errors++
		a.totalErrors++
		a.logger.Error("Couldn't open indexdb", zap.String("dbpath", dbpath), zap.Error(err))
		return
	}

	marker := ""
	for {
		items, err := db.List("", "", marker, 1000)
		if err != nil {
			a.logger.Error("db.List failed", zap.String("dbpath", dbpath), zap.Error(err))
			return
		}
		for _, item := range items {
			shardPath, err := db.WholeObjectPath(item.Hash, item.Shard, item.Timestamp, item.Nursery)
			if err != nil {
				a.logger.Error("Error getting indexdb path for hash",
					zap.String("hash", item.Hash), zap.Error(err))
				continue
			}
			a.passes++
			a.totalPasses++
			var bytes int64
			if item.Nursery {
				bytes, err = a.ecfuncs.AuditNurseryObject(shardPath, item.Metabytes, a.auditorType == "ZBF")
			} else {
				bytes, err = a.ecfuncs.AuditShard(shardPath, item.ShardHash, a.auditorType == "ZBF")
			}
			if err != nil {
				a.logger.Error("Failed audit and is being quarantined",
					zap.String("shardPath", shardPath), zap.Error(err))
				err = quarantineShard(db, item.Hash, item.Shard, item.Timestamp, item.Nursery)
				if err != nil {
					a.logger.Error("Failed to quarantine shard", zap.String("shardPath", shardPath), zap.Error(err))
					continue
				}
				a.quarantines++
				a.totalQuarantines++
			}
			a.bytesProcessed += bytes
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
		bytesProcessed, err := auditHash(hashDir, a.auditorType == "ZBF")
		a.bytesProcessed += bytesProcessed
		a.totalBytes += bytesProcessed
		rateLimitSleep(a.passStart, a.totalPasses, a.filesPerSecond)
		rateLimitSleep(a.passStart, a.totalBytes, a.bytesPerSecond)
		if err != nil {
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
		switch policy.Type {
		case "replication":
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
		case "hec":
			r, err := ring.GetRing("object", a.hashPathPrefix, a.hashPathSuffix, policy.Index)
			if err != nil {
				a.logger.Error("Error getting object ring", zap.Int("policyindex", policy.Index))
				continue
			}
			hecdbPath := filepath.Join(devPath, PolicyDir(policy.Index), "hec.db")
			if _, err := os.Stat(hecdbPath); os.IsNotExist(err) {
				continue
			}
			a.auditDB(hecdbPath, r)
		default:
			a.logger.Error("Unknown policy type", zap.String("policytype", policy.Type))
			continue
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
		zap.Int64("Locally quarantied", a.quarantines),
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
			zba := Auditor{AuditorDaemon: d, auditorType: "ZBF", mode: "once", filesPerSecond: d.zbFilesPerSecond, ecfuncs: RealECAuditFuncs{}}
			zba.run(OneTimeChan())
			wg.Done()
		}()
	}
	reg := Auditor{AuditorDaemon: d, auditorType: "ALL", mode: "once", filesPerSecond: d.regFilesPerSecond, ecfuncs: RealECAuditFuncs{}}
	reg.run(OneTimeChan())
	wg.Wait()
}

// RunForever triggering audit passes every time AuditForeverInterval has passed.
func (d *AuditorDaemon) RunForever() {
	if d.zbFilesPerSecond > 0 {
		zba := Auditor{AuditorDaemon: d, auditorType: "ZBF", mode: "forever", filesPerSecond: d.zbFilesPerSecond, ecfuncs: RealECAuditFuncs{}}
		go zba.run(time.Tick(AuditForeverInterval))
	}
	reg := Auditor{AuditorDaemon: d, auditorType: "ALL", mode: "forever", filesPerSecond: d.regFilesPerSecond, ecfuncs: RealECAuditFuncs{}}
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
