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
	"net"
	"net/http"
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
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"golang.org/x/net/http2"
)

// NurseryObject implements an Object that is compatible with Swift's object
// server. The difference with this object is that on initial write objects are
// written to a nursery (the existing 'objects' dir) but after they are written
// a nursery-stabilizer will walk the dirs and for each object HEAD the other
// primaries looking for that object. If all primaries have this object (based
// on x-timestamp) it will rename the object to a new 'stable-objects' dir.

// from /srv/node/c1u0/objects/12345/abc/aabbcc...abc/1009988.data
// to   /srv/node/c1u0/stable-objects/12345/aabbcc...abc/1009988.data

// On subsequent GETs to the object both the nursery and stable locations will be
// listdired to find the most recent data.

// if the object does not get a match for all other primaries, nothing is done.
// It waits for replication to "fix" the object

// What is the point of this? First it keeps the 'objects' dir small. In the objects
// dir there is a hashes.pkl file that has a map {'abc':
// md5-of-all-file-names-in-tree}. As it stands, on every write the suffix for
// the object's suffix in cleared. It is then the job of the replicator to
// listdir all the object-dirs under that suffix to regenerate the md5. The
// purpose of this is to validate that every local object under that hash-suffix has
// its other primaries populated. It sends the hash to the remote node, if it
// matches then you are guaranteed they have the same files. If it does not
// match it sends the file it has to the remote. The problem is as the # of
// objects in on the device increases the replicator will have to do
// many listdirs for each write (one for each object-dir within the suffix).
// All of this work is, in many cases, completely redundant as in a properly
// running cluster almost all writes will get 201s from all primaries. This
// extra work has 2 main consequences. First, it causes a very high load on the
// cluster doing mostly nothing. Second- objects that did not 201 on every
// primary, which are in a more fragile state, have to wait longer for the
// replicator to deal with them.

// The idea with this patch is once objects get fully written (.data or .ts is
// on all primaries) they are moved to stable-objects. the replicator daemon
// will not walk this directory (which should hold the vast majority of the
// data on the server.) When there is a drive lost it relies on priority
// replication calls (spawned from andrewd) to repopulate the data. the
// priority replication itself will be much faster because it will not have to
// populate hashes.pkls at all- there are none in the stable-objects dir. This
// should ease stress on XFS and prevent it from being as much of a bottleneck.

// The normal replicator is left to only walk the 'objects' dir. By default
// hashes.pkl is no longer used (writes will not invalidate a hash, hashes
// generated for remote calls are thrown away). Everytime the replicator
// processes a partition it will treat it like a handoff partition and try to
// verify / send every file in the tree.  If replication is running too slow,
// you can set the cache_hash_dirs setting to true. This will force it to use
// the hashes.pkl the way it was originally intended. All writes will
// invalidate the hash, the replicator will use it and only run listdirs on the
// missing suffixes.

// Migrations. If you have an existing swift cluster and wish to migrate to
// this policy there is a migration path. just change the policy_type in your
// swift.conf from "replication" to "replication_nursery". You will also want
// to add "cache_hash_dirs = true" as well. This will continue the use of the
// hashes.pkl files to keep replication moving until the object stabilizer has
// moved the majority of objects to the stable-objects dir. Once the
// nursery-stabilizer can complete a pass within about a day (depending on
// cluster) you can set "cache_hash_dirs = false" (or remove). You will need
// to keep an eye on replication pass times and nursery pass times - similar to
// keeping an eye replication currently. At a later date this will hopefully be
// more automated.

type nurseryObject struct {
	file           *os.File
	afw            fs.AtomicFileWriter
	nurseryHashDir string
	stableHashDir  string
	tempDir        string
	dataFile       string
	metaFile       string
	stabilized     bool
	workingClass   string
	metadata       map[string]string
	reserve        int64
	reclaimAge     int64
	cacheHashDirs  bool
	asyncWG        *sync.WaitGroup // Used to keep track of async goroutines
	client         *http.Client
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
		if o.cacheHashDirs {
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

func (o *nurseryObject) canStabilize(ring ring.Ring, dev *ring.Device, policy int) (bool, error) {
	metadata := o.Metadata()
	ns := strings.SplitN(metadata["name"], "/", 4)
	if len(ns) != 4 {
		return false, fmt.Errorf("invalid metadata name: %s", metadata["name"])
	}
	partition := ring.GetPartition(ns[1], ns[2], ns[3])
	if _, handoff := ring.GetJobNodes(partition, dev.Id); handoff {
		return false, nil
	}
	nodes := ring.GetNodes(partition)
	goodNodes := uint64(0)
	for _, device := range nodes {
		if device.Ip == dev.Ip && device.Port == dev.Port && device.Device == device.Device {
			continue
		}
		url := fmt.Sprintf("%s://%s:%d/%s/%d%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(metadata["name"]))
		req, err := http.NewRequest("HEAD", url, nil)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.FormatInt(int64(policy), 10))
		req.Header.Set("User-Agent", "nursery-stabilizer")
		resp, err := o.client.Do(req)

		if err == nil && (resp.StatusCode/100 == 2 || resp.StatusCode == 404) &&
			resp.Header.Get("X-Backend-Data-Timestamp") != "" &&
			resp.Header.Get("X-Backend-Data-Timestamp") ==
				metadata["X-Backend-Data-Timestamp"] &&
			resp.Header.Get("X-Backend-Meta-Timestamp") ==
				metadata["X-Backend-Meta-Timestamp"] {
			goodNodes++
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return goodNodes+1 == ring.ReplicaCount(), nil
}

func (o *nurseryObject) notifyPeers(ring ring.Ring, dev *ring.Device, policy int) error {
	metadata := o.Metadata()
	ns := strings.SplitN(metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", metadata["name"])
	}
	partition := ring.GetPartition(ns[1], ns[2], ns[3])
	if _, handoff := ring.GetJobNodes(partition, dev.Id); handoff {
		return nil
	}
	nodes := ring.GetNodes(partition)
	for _, device := range nodes {
		if device.Ip == dev.Ip && device.Port == dev.Port && device.Device == device.Device {
			continue
		}
		url := fmt.Sprintf("%s://%s:%d/stabilize/%s/%d%s", device.Scheme, device.Ip, device.ReplicationPort, device.Device, partition, common.Urlencode(metadata["name"]))
		if req, err := http.NewRequest("POST", url, nil); err == nil {
			req.Header.Set("X-Backend-Storage-Policy-Index", strconv.FormatInt(int64(policy), 10))
			req.Header.Set("X-Backend-Nursery-Stabilize", "true")
			req.Header.Set("X-Timestamp", metadata["X-Backend-Data-Timestamp"])
			req.Header.Set("User-Agent", "nursery-peer-stabilizer")
			resp, _ := o.client.Do(req)

			if resp != nil {
				resp.Body.Close()
			}
		} else {
			return err
		}
	}
	return nil
}

func (o *nurseryObject) Stabilize(ring ring.Ring, dev *ring.Device, policy int) error {
	if o.stabilized {
		return nil
	}
	if cs, err := o.canStabilize(ring, dev, policy); err != nil {
		return err
	} else if !cs {
		return nil
	}
	if !fs.Exists(o.stableHashDir) {
		if err := os.MkdirAll(o.stableHashDir, 0755); err != nil {
			return err
		}
	}
	plock, err := fs.LockPath(o.stableHashDir, 10*time.Second)
	defer plock.Close()
	if err != nil {
		return err
	}
	sObjDataPath := filepath.Join(o.stableHashDir, filepath.Base(o.dataFile))
	if fs.Exists(sObjDataPath) {
		os.Remove(o.dataFile)
	} else {
		if err := os.Rename(o.dataFile, sObjDataPath); err != nil {
			return err
		}
	}
	if o.metaFile != "" {
		sObjMetaPath := filepath.Join(o.stableHashDir, filepath.Base(o.metaFile))
		if fs.Exists(sObjMetaPath) {
			os.Remove(o.metaFile)
		} else {
			if err := os.Rename(o.metaFile, sObjMetaPath); err != nil {
				return err
			}
		}
	}
	HashCleanupListDir(o.stableHashDir, o.reclaimAge)
	os.Remove(o.nurseryHashDir)
	os.Remove(filepath.Dir(o.nurseryHashDir)) // try to remove suffix dir
	if err := o.notifyPeers(ring, dev, policy); err != nil {
		return err
	}
	return nil
}

type nurseryEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	reclaimAge     int64
	logger         srv.LowLevelLogger
	policy         int
	cacheHashDirs  bool
	client         *http.Client
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
			directory := ""
			isStable := false
			if common.StringInSlice(filename, nurseryFileList) {
				directory = nurseryDir
			} else {
				if common.StringInSlice(filename, stableFileList) {
					directory = stableDir
					isStable = true
				}
			}
			if directory == "" {
				return "", "", false
			}
			if metaFile == "" {
				return filepath.Join(directory, filename), "", isStable
			}
			mDirectory := ""
			if common.StringInSlice(metaFile, nurseryFileList) {
				mDirectory = nurseryDir
			} else {
				if common.StringInSlice(metaFile, stableFileList) {
					mDirectory = stableDir
					// TODO: dfg: what happens when .data is stable as .meta is in nursery?
				}
			}
			if mDirectory == "" {
				return filepath.Join(directory, filename), "", isStable
			}
			return filepath.Join(directory, filename), filepath.Join(mDirectory, metaFile), isStable
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
	sor := &nurseryObject{reclaimAge: f.reclaimAge, reserve: f.reserve, asyncWG: asyncWG, cacheHashDirs: f.cacheHashDirs, client: f.client}
	sor.nurseryHashDir, sor.stableHashDir = nurseryHashDirs(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix, f.policy)
	if vars["nurseryHashDir"] != "" {
		// this is being built by walking the disk- the nurseryHashDir is where it was found. not necessarily where it should be.
		sor.nurseryHashDir = vars["nurseryHashDir"]
	}
	sor.tempDir = TempDirPath(f.driveRoot, vars["device"])
	var stat os.FileInfo
	loadAttrs := func() error {
		sor.dataFile, sor.metaFile, sor.stabilized = neObjectFiles(sor.nurseryHashDir, sor.stableHashDir)
		if sor.Exists() {
			if needData {
				if sor.file, err = os.Open(sor.dataFile); err != nil {
					return err
				}
				if sor.metadata, err = OpenObjectMetadata(sor.file.Fd(), sor.metaFile); err != nil {
					//sor.Quarantine()
					return fmt.Errorf("Error getting OpenObjectMetadata: %v", err)
				}
			} else {
				if sor.metadata, err = ObjectMetadata(sor.dataFile, sor.metaFile); err != nil {
					//sor.Quarantine()
					return fmt.Errorf("Error getting ObjectMetadata: %v", err)
				}
			}
			if sor.file != nil {
				if stat, err = sor.file.Stat(); err != nil {
					sor.Close()
					return fmt.Errorf("Error file statting nurseryEngine New file: %v", err)
				}
			} else if stat, err = os.Stat(sor.dataFile); err != nil {
				return fmt.Errorf("Error os statting nurseryEngine New file: %v", err)
			}
		} else {
			sor.metadata, _ = ObjectMetadata(sor.dataFile, sor.metaFile) // ignore errors if deleted
		}
		return nil
	}
	if err := loadAttrs(); err != nil {
		// if file gets stabilized between listdir and loading, allow one more check
		err = loadAttrs()
		if err != nil {
			// this quarantines in more places than before
			sor.Quarantine()
			return nil, err
		}
	}
	if err == nil && sor.Exists() {
		if contentLength, err := strconv.ParseInt(sor.metadata["Content-Length"], 10, 64); err != nil {
			sor.Quarantine()
			return nil, fmt.Errorf("Unable to parse content-length: %s", sor.metadata["Content-Length"])
		} else if stat.Size() != contentLength {
			sor.Quarantine()
			return nil, fmt.Errorf("File size doesn't match content-length: %d vs %d", stat.Size(), contentLength)
		}
	}
	return sor, nil
}

func (f *nurseryEngine) GetNurseryObjects(device string, c chan ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	objDirPath := filepath.Join(f.driveRoot, device, PolicyDir(f.policy))
	partitions, err := fs.ReadDirNames(objDirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			f.logger.Error("Error reading objects dir", zap.String("objDirPath", objDirPath), zap.Error(err))
		}
		return
	}
	for _, partition := range partitions {
		partitionDir := filepath.Join(objDirPath, partition)
		suffixes, err := fs.ReadDirNames(partitionDir)
		if err != nil {
			f.logger.Error("Error reading partition dir ", zap.String("worker", "nursery"), zap.String("partitionDir", partitionDir), zap.Error(err))
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
				f.logger.Error("Error reading suffix dir", zap.String("worker", "nursery"), zap.String("suffixDir", suffixDir), zap.Error(err))
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
	cacheHashDirs := common.LooksTrue(policy.Config["cache_hash_dirs"])
	certFile := config.GetDefault("app:object-server", "cert_file", "")
	keyFile := config.GetDefault("app:object-server", "key_file", "")
	transport := &http.Transport{
		MaxIdleConnsPerHost: 256,
		MaxIdleConns:        0,
		IdleConnTimeout:     5 * time.Second,
		DisableCompression:  true,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
		ExpectContinueTimeout: 10 * time.Minute,
	}
	if certFile != "" && keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			return nil, err
		}
	}
	return &nurseryEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		reclaimAge:     reclaimAge,
		logger:         logger,
		cacheHashDirs:  cacheHashDirs,
		client: &http.Client{
			Timeout:   120 * time.Minute,
			Transport: transport,
		},
		policy: policy.Index}, nil
}

func init() {
	RegisterObjectEngine("replication-nursery", nurseryEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = nurseryEngineConstructor
var _ Object = &nurseryObject{}
var _ ObjectEngine = &nurseryEngine{}
