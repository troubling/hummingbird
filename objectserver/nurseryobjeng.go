//  Copyright (c) 2017 Rackspace
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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/srv"
)

// NurseryObject implements an Object that is compatible with Swift's object
// server. The difference with this object is that on initial write objects are
// written to a nursery (the existing 'objects' dir) and on write they do not
// have any affect on the hashes.pkl.  After they are written a
// nursery-stabilizer will walk the dirs and for each object HEAD the other
// primaries looking for that object. If all primaries have this object (based
// on x-timestamp) it will rename the object to a new 'stable-objects' dir. On
// subsequent GETs to the object both the nursery and stable locations will be
// listdired to find the most recent data.

//TODO: figure out rest of this and explain it :p
// What this does is divide the
// responsibilies of the replicator. These are as follows: 1. Handoff
// objects: during PUT a device was down and object was put somewhere else
// 2. Out-of-place OOP object: Object was written to a primary but the ring
// was changed and is now not on a primary. These objects are out of place
// due to an "ring event" 3. Missing object: Object was lost due to a disk
// "failure event".

// Now instead of one very resource intensive and slow process that handles
// all possibilities of problems we will have one daemon scan for handoff
// objects and use a centralized admin daemon kick off coordinated jobs for
// cleaning up after ring changes and disk failures.

type nurseryObject struct {
	file             *os.File
	afw              fs.AtomicFileWriter
	nurseryHashDir   string
	stableHashDir    string
	tempDir          string
	dataFile         string
	metaFile         string
	stabilized       bool
	workingClass     string
	metadata         map[string]string
	reserve          int64
	reclaimAge       int64
	nurseryMigration bool
	asyncWG          *sync.WaitGroup // Used to keep track of async goroutines
}

// Metadata returns the object's metadata.
func (o *nurseryObject) Metadata() map[string]string {
	return o.metadata
}

// ContentLength parses and returns the Content-Length for the object.
func (o *nurseryObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(o.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

// Quarantine removes the object's underlying files to the Quarantined directory on the device.
func (o *nurseryObject) Quarantine() error {
	o.Close()
	hashDir := o.nurseryHashDir
	if o.stabilized {
		hashDir = o.stableHashDir
	}
	if QuarantineHash(hashDir) == nil {
		return InvalidateHash(hashDir)
	}
	return nil
}

// Exists returns true if the object exists, that is if it has a .data file.
func (o *nurseryObject) Exists() bool {
	return strings.HasSuffix(o.dataFile, ".data")
}

// Copy copies all data from the underlying .data file to the given writers.
func (o *nurseryObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if len(dsts) == 1 {
		return io.Copy(dsts[0], o.file)
	} else {
		return common.Copy(o.file, dsts...)
	}
}

// CopyRange copies data in the range of start to end from the underlying .data file to the writer.
func (o *nurseryObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	if _, err := o.file.Seek(start, os.SEEK_SET); err != nil {
		return 0, err
	}
	return common.CopyN(o.file, end-start, w)
}

// Repr returns a string that identifies the object in some useful way, used for logging.
func (o *nurseryObject) Repr() string {
	if o.dataFile != "" && o.metaFile != "" {
		return fmt.Sprintf("NurseryObject(%s, %s)", o.dataFile, o.metaFile)
	} else if o.dataFile != "" {
		return fmt.Sprintf("NurseryObject(%s)", o.dataFile)
	}
	return fmt.Sprintf("NurseryObject(%s,%s)", o.nurseryHashDir, o.stableHashDir)
}

func (o *nurseryObject) newFile(class string, size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = fs.NewAtomicFileWriter(o.tempDir, o.nurseryHashDir); err != nil {
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
func (o *nurseryObject) SetData(size int64) (io.Writer, error) {
	return o.newFile("data", size)
}

// CommitMetadata updates the object's metadata (e.g. POST).
func (o *nurseryObject) CommitMetadata(metadata map[string]string) error {
	if _, err := o.newFile("meta", 0); err != nil {
		return err
	}
	defer o.Close()
	return o.Commit(metadata)
}

// Commit commits an open data file to disk, given the metadata.
func (o *nurseryObject) Commit(metadata map[string]string) error {
	defer o.afw.Abandon()
	timestamp, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("No timestamp in metadata")
	}
	if err := WriteMetadata(o.afw.Fd(), metadata); err != nil {
		return fmt.Errorf("Error writing metadata: %v", err)
	}
	fileName := filepath.Join(o.nurseryHashDir, fmt.Sprintf("%s.%s", timestamp, o.workingClass))
	o.afw.Save(fileName)
	if o.asyncWG != nil {
		o.asyncWG.Add(1)
	}
	go func() {
		if o.asyncWG != nil {
			defer o.asyncWG.Done()
		}
		HashCleanupListDir(o.nurseryHashDir, o.reclaimAge)
		if dir, err := os.OpenFile(o.nurseryHashDir, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
		if o.nurseryMigration {
			InvalidateHash(o.nurseryHashDir)
		}
	}()
	return nil
}

// Delete deletes the object.
func (o *nurseryObject) Delete(metadata map[string]string) error {
	if _, err := o.newFile("ts", 0); err != nil {
		return err
	} else {
		defer o.Close()
		return o.Commit(metadata)
	}
}

// Close releases any resources used by the instance of ecObject
func (o *nurseryObject) Close() error {
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

func (o *nurseryObject) Stabilize() error {
	if fs.Exists(o.stableHashDir) {
		plock, err := fs.LockPath(o.stableHashDir, 10*time.Second)
		defer plock.Close()
		if err != nil {
			return err
		}
	} else {
		if err := os.MkdirAll(o.stableHashDir, 0755); err != nil {
			return err
		}
		plock, err := fs.LockPath(o.stableHashDir, 10*time.Second)
		defer plock.Close()
		if err != nil {
			return err
		}
	}
	sObjDataPath := filepath.Join(o.stableHashDir, filepath.Base(o.dataFile))

	if err := os.Rename(o.dataFile, sObjDataPath); err != nil {
		return err
	}
	if o.metaFile != "" {
		sObjMetaPath := filepath.Join(o.stableHashDir, filepath.Base(o.metaFile))
		if err := os.Rename(o.metaFile, sObjMetaPath); err != nil {
			return err
		}
	}
	HashCleanupListDir(o.stableHashDir, o.reclaimAge)
	os.Remove(o.nurseryHashDir)
	return nil
}

type nurseryEngine struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	reserve          int64
	reclaimAge       int64
	logger           srv.LowLevelLogger
	policy           int
	nurseryMigration bool
}

func neObjectFiles(nurseryDir, stableDir string) (string, string, bool) {
	fileList := []string{}
	nurseryFileList, dErr := fs.ReadDirNames(nurseryDir)
	if dErr == nil && len(nurseryFileList) > 0 {
		fileList = append(fileList, nurseryFileList...)
	}
	stableFileList, sErr := fs.ReadDirNames(stableDir)
	if !(dErr == nil || sErr == nil) {
		return "", "", false
	}
	if sErr == nil && len(stableFileList) > 0 {
		fileList = append(fileList, stableFileList...)
		if len(fileList) > 1 {
			sort.Strings(fileList)
		}
	}
	metaFile := ""
	for index := len(fileList) - 1; index >= 0; index-- {
		filename := fileList[index]
		if strings.HasSuffix(filename, ".meta") {
			metaFile = filename
		}
		if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
			directory := nurseryDir
			isStable := common.StringInSlice(filename, stableFileList)
			if isStable {
				directory = stableDir
			}
			if metaFile != "" {
				return filepath.Join(directory, filename), filepath.Join(directory, metaFile), isStable
			} else {
				return filepath.Join(directory, filename), "", isStable
			}
		}
	}
	return "", "", false
}

func nurseryHashDirs(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string, policy int) (string, string) {
	h := md5.New()
	io.WriteString(h, hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+hashPathSuffix)
	hexHash := hex.EncodeToString(h.Sum(nil))
	suffix := hexHash[29:32]
	nurseryDir := filepath.Join(driveRoot, vars["device"], PolicyDir(policy), vars["partition"], suffix, hexHash)
	stableDir := filepath.Join(driveRoot, vars["device"],
		fmt.Sprintf("stable-%s", PolicyDir(policy)), vars["partition"], hexHash)
	return nurseryDir, stableDir
}

// New returns an instance of NurseryObject with the given parameters. Metadata is read in and if needData is true, the file is opened.  AsyncWG is a waitgroup if the object spawns any async operations
func (f *nurseryEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (Object, error) {
	//TODO the vars map is lame. it expects there to always be "account", "container", etc. this should be moved into a separate struct. this struct will handle the hashDir, suffix crap. until them i'm going to make this a little worse :p
	var err error
	sor := &nurseryObject{reclaimAge: f.reclaimAge, reserve: f.reserve, asyncWG: asyncWG, nurseryMigration: f.nurseryMigration}
	sor.nurseryHashDir, sor.stableHashDir = nurseryHashDirs(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix, f.policy)
	if vars["nurseryHashDir"] != "" {
		// this is being built by walking the disk- the nurseryHashDir is where it was found. not necessarily where it should be.
		sor.nurseryHashDir = vars["nurseryHashDir"]
	}
	sor.tempDir = TempDirPath(f.driveRoot, vars["device"])
	sor.dataFile, sor.metaFile, sor.stabilized = neObjectFiles(sor.nurseryHashDir, sor.stableHashDir)
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

func (f *nurseryEngine) GetNurseryObjects(device string, c chan ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	objDirPath := filepath.Join(f.driveRoot, device, PolicyDir(f.policy))
	partitions, err := fs.ReadDirNames(objDirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			f.logger.Error("Error reading objects dir", zap.String("objDirPath", objDirPath))
		}
		return
	}
	for _, partition := range partitions {
		partitionDir := filepath.Join(objDirPath, partition)
		suffixes, err := fs.ReadDirNames(partitionDir)
		if err != nil {
			f.logger.Error("Error reading partition dir ", zap.String("worker", "nursery"), zap.String("partitionDir", partitionDir))
			continue
		}
		for _, suffix := range suffixes {
			suffixDir := filepath.Join(partitionDir, suffix)
			if suffix == ".lock" || suffix == "hashes.pkl" || suffix == "hashes.invalid" {
				continue
			}
			if _, hexErr := strconv.ParseInt(suffix, 16, 64); len(suffix) != 3 || hexErr != nil {
				f.logger.Error("Skipping invalid file in partition.", zap.String("worker", "nursery"), zap.String("suffixDir", suffixDir))
				continue
			}
			hashes, err := fs.ReadDirNames(suffixDir)
			if err != nil {
				f.logger.Error("Error reading suffix dir", zap.String("worker", "nursery"), zap.String("suffixDir", suffixDir))
				continue
			}
			for _, hash := range hashes {
				_, hexErr := hex.DecodeString(hash)
				hashDir := filepath.Join(suffixDir, hash)
				if len(hash) != 32 || hexErr != nil {
					f.logger.Error("Skipping invalid file in suffix", zap.String("worker", "nursery"), zap.String("worker", "nursery"), zap.String("hashDir", hashDir))
					continue
				}
				dataFile, metaFile := ObjectFiles(hashDir)
				if dataFile == "" {
					if _, err := auditHash(hashDir, true); err != nil {
						f.logger.Error("Failed stabilize and is being quarantined", zap.String("worker", "nursery"),
							zap.String("hashDir", hashDir),
							zap.Error(err))
						QuarantineHash(hashDir)
						InvalidateHash(hashDir)
					}
					continue
				}
				metadata, err := ObjectMetadata(dataFile, metaFile)
				ns := strings.SplitN(metadata["name"], "/", 4)
				if len(ns) != 4 {
					f.logger.Error("invalid metadata name", zap.String("name", metadata["name"]))
				}
				// TODO make vars a struct- this is lame
				vars := map[string]string{
					"nurseryHashDir": hashDir,
					"account":        ns[1], "container": ns[2],
					"obj": ns[3], "partition": partition,
					"device": device}

				p, err := f.New(vars, true, nil)
				if err != nil {
					f.logger.Error("could not build object", zap.Error(err))

				}
				if os, ok := p.(ObjectStabilizer); ok {
					select {
					case c <- os:
					case <-cancel:
						return
					}
				} else {
					f.logger.Error("could not cast to ObjectStabilizer", zap.String("object", p.Repr()))
				}
			}
		}
	}
}

// nurseryEngineConstructor creates a nurseryEngine given the object server configs.
func nurseryEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	reclaimAge := int64(config.GetInt("app:object-server", "reclaim_age", int64(common.ONE_WEEK)))
	logLevelString := config.GetDefault("app:object-server", "log_level", "INFO")
	logLevel := zap.NewAtomicLevel()
	logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	logger, err := srv.SetupLogger("app:object-server", &logLevel, flags)
	if err != nil {
		return nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	nurseryMigration := config.GetBool("app:object-server", "nursery_migration", false)
	return &nurseryEngine{
		driveRoot:        driveRoot,
		hashPathPrefix:   hashPathPrefix,
		hashPathSuffix:   hashPathSuffix,
		reserve:          reserve,
		reclaimAge:       reclaimAge,
		logger:           logger,
		nurseryMigration: nurseryMigration,
		policy:           policy.Index}, nil
}

func init() {
	RegisterObjectEngine("replication-nursery", nurseryEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = nurseryEngineConstructor
var _ Object = &nurseryObject{}
var _ ObjectEngine = &nurseryEngine{}
