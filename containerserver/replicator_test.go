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
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
)

type patchableReplicationDevice struct {
	rd                         *replicationDevice
	_sendReplicationMessage    func(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error)
	_sync                      func(dev *ring.Device, part uint64, ringHash string, info *ContainerInfo) (*ContainerInfo, error)
	_rsync                     func(dev *ring.Device, c ReplicableContainer, part uint64, op string) error
	_usync                     func(dev *ring.Device, c ReplicableContainer, part uint64, localID string, point int64) error
	_chooseReplicationStrategy func(localInfo, remoteInfo *ContainerInfo, usyncThreshold int64) string
	_replicateDatabaseToDevice func(dev *ring.Device, c ReplicableContainer, part uint64) error
	_replicateDatabase         func(dbFile string) error
	_findContainerDbs          func(devicePath string, results chan string)
	_incrementStat             func(stat string)
}

func (d *patchableReplicationDevice) sendReplicationMessage(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
	if d._sendReplicationMessage != nil {
		return d._sendReplicationMessage(dev, part, ringHash, args...)
	}
	return d.rd.sendReplicationMessage(dev, part, ringHash, args...)
}
func (d *patchableReplicationDevice) sync(dev *ring.Device, part uint64, ringHash string, info *ContainerInfo) (*ContainerInfo, error) {
	if d._sync != nil {
		return d._sync(dev, part, ringHash, info)
	}
	return d.rd.sync(dev, part, ringHash, info)
}
func (d *patchableReplicationDevice) rsync(dev *ring.Device, c ReplicableContainer, part uint64, op string) error {
	if d._rsync != nil {
		return d._rsync(dev, c, part, op)
	}
	return d.rd.rsync(dev, c, part, op)
}
func (d *patchableReplicationDevice) usync(dev *ring.Device, c ReplicableContainer, part uint64, localID string, point int64) error {
	if d._usync != nil {
		return d._usync(dev, c, part, localID, point)
	}
	return d.rd.usync(dev, c, part, localID, point)
}
func (d *patchableReplicationDevice) chooseReplicationStrategy(localInfo, remoteInfo *ContainerInfo, usyncThreshold int64) string {
	if d._chooseReplicationStrategy != nil {
		return d._chooseReplicationStrategy(localInfo, remoteInfo, usyncThreshold)
	}
	return d.rd.chooseReplicationStrategy(localInfo, remoteInfo, usyncThreshold)
}
func (d *patchableReplicationDevice) replicateDatabaseToDevice(dev *ring.Device, c ReplicableContainer, part uint64) error {
	if d._replicateDatabaseToDevice != nil {
		return d._replicateDatabaseToDevice(dev, c, part)
	}
	return d.rd.replicateDatabaseToDevice(dev, c, part)
}
func (d *patchableReplicationDevice) replicateDatabase(dbFile string) error {
	if d._replicateDatabase != nil {
		return d._replicateDatabase(dbFile)
	}
	return d.rd.replicateDatabase(dbFile)
}
func (d *patchableReplicationDevice) findContainerDbs(devicePath string, results chan string) {
	if d._findContainerDbs != nil {
		d._findContainerDbs(devicePath, results)
		return
	}
	d.rd.findContainerDbs(devicePath, results)
}
func (d *patchableReplicationDevice) incrementStat(stat string) {
	if d._incrementStat != nil {
		d._incrementStat(stat)
		return
	}
	d.rd.incrementStat(stat)
}
func (d *patchableReplicationDevice) replicate() {
	d.rd.replicate()
}

func newTestReplicationDevice(dev *ring.Device, r *Replicator) *patchableReplicationDevice {
	if r.sendStat == nil {
		r.sendStat = make(chan statUpdate, 100)
	}
	if r.logger == nil {
		r.logger = fakeLowLevelLogger{}
	}
	if r.Ring == nil {
		r.Ring = &fakeRing{}
	}
	if r.concurrencySem == nil {
		r.concurrencySem = make(chan struct{}, 1)
	}
	if r.startRun == nil {
		r.startRun = make(chan string, 100)
	}
	if r.checkin == nil {
		r.checkin = make(chan string, 100)
	}
	rd := newReplicationDevice(dev, r)
	p := &patchableReplicationDevice{rd: rd}
	rd.i = p
	return p
}

func testServer(t *testing.T, h http.Handler) (*ring.Device, func()) {
	ts := httptest.NewServer(h)
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, portstring, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(portstring)
	require.Nil(t, err)
	dev := &ring.Device{
		ReplicationIp:   host,
		ReplicationPort: port,
		Device:          "sdb",
	}
	return dev, ts.Close
}

func TestReplicatorSync(t *testing.T) {
	info := &ContainerInfo{
		MaxRow:          -1,
		Hash:            "00000000000000000000000000000000",
		ID:              "12345",
		CreatedAt:       "1470586890.28563",
		PutTimestamp:    "1470586890.28563",
		DeleteTimestamp: "",
		RawMetadata:     "{}",
	}
	rinfo := &ContainerInfo{
		MaxRow:          10,
		Hash:            "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0",
		ID:              "67890",
		CreatedAt:       "1410586890.28563",
		PutTimestamp:    "1410586890.28563",
		DeleteTimestamp: "",
		RawMetadata:     "{}",
	}
	dev, cleanup := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, r.Method, "REPLICATE")
		require.Equal(t, r.URL.Path, "/sdb/1/00000000000000000000000000000000")
		body, err := ioutil.ReadAll(r.Body)
		require.Nil(t, err)
		var args []interface{}
		require.Nil(t, json.Unmarshal(body, &args))
		require.Equal(t, args[0], "sync")
		require.Equal(t, int64(args[1].(float64)), info.MaxRow)
		require.Equal(t, args[2], info.Hash)
		require.Equal(t, args[3], info.ID)
		require.Equal(t, args[4], info.CreatedAt)
		require.Equal(t, args[5], info.PutTimestamp)
		require.Equal(t, args[6], info.DeleteTimestamp)
		require.Equal(t, args[7], info.RawMetadata)
		resp, err := json.Marshal(rinfo)
		require.Nil(t, err)
		w.Write(resp)
	}))
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient})
	newrinfo, err := rd.sync(dev, 1, "00000000000000000000000000000000", info)
	require.Nil(t, err)
	require.Equal(t, rinfo, newrinfo)
}

func TestReplicatorSyncNotFound(t *testing.T) {
	dev, cleanup := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient})
	rinfo, err := rd.sync(dev, 1, "00000000000000000000000000000000", &ContainerInfo{})
	require.Nil(t, rinfo)
	require.Nil(t, err)
}

func TestReplicatorSyncUnmounted(t *testing.T) {
	dev, cleanup := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInsufficientStorage)
	}))
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient})
	_, err := rd.sync(dev, 1, "00000000000000000000000000000000", &ContainerInfo{})
	require.Equal(t, err, errDeviceNotMounted)
}

func TestReplicatorRsync(t *testing.T) {
	tmpFileName := ""
	dev, cleanup1 := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			written, err := io.Copy(ioutil.Discard, r.Body)
			require.Nil(t, err)
			require.True(t, written > 0)
			tmpFileName = filepath.Base(r.URL.Path)
		} else if r.Method == "REPLICATE" {
			var args []interface{}
			body, err := ioutil.ReadAll(r.Body)
			require.Nil(t, err)
			require.Nil(t, json.Unmarshal(body, &args))
			require.Equal(t, args[0], "complete_rsync")
			require.Equal(t, args[1], tmpFileName)
		}
	}))
	defer cleanup1()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient})
	c, _, cleanup2, err := createTestDatabase("1410586890.28563")
	require.Nil(t, err)
	require.Nil(t, mergeItemsByName(c, []string{"a", "b", "c"}))
	defer cleanup2()
	err = rd.rsync(dev, c, 1, "complete_rsync")
	require.Nil(t, err)
}

func TestReplicatorUsync(t *testing.T) {
	requestNumber := 0
	dev, cleanup1 := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var args []interface{}
		body, err := ioutil.ReadAll(r.Body)
		require.Nil(t, err)
		require.Nil(t, json.Unmarshal(body, &args))
		if requestNumber < 3 {
			require.Equal(t, args[0], "merge_items")
		} else {
			require.Equal(t, args[0], "merge_syncs")
		}
		requestNumber++
	}))
	defer cleanup1()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient, perUsync: 1, maxUsyncs: 5})
	c, _, cleanup2, err := createTestDatabase("1410586890.28563")
	require.Nil(t, err)
	require.Nil(t, mergeItemsByName(c, []string{"a", "b", "c"}))
	defer cleanup2()
	err = rd.usync(dev, c, 1, "12345", -1)
	require.Nil(t, err)
}

func TestReplicatorChooseReplicationStrategy(t *testing.T) {
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient})
	require.Equal(t, "complete_rsync", rd.chooseReplicationStrategy(&ContainerInfo{}, nil, 100))
	require.Equal(t, "empty", rd.chooseReplicationStrategy(&ContainerInfo{MaxRow: -1}, &ContainerInfo{}, 100))
	require.Equal(t, "no_change", rd.chooseReplicationStrategy(&ContainerInfo{MaxRow: 123}, &ContainerInfo{Point: 123}, 100))
	require.Equal(t, "hashmatch", rd.chooseReplicationStrategy(
		&ContainerInfo{Hash: "somehash", MaxRow: 10},
		&ContainerInfo{Hash: "somehash", Point: 9},
		100))
	require.Equal(t, "rsync_then_merge", rd.chooseReplicationStrategy(
		&ContainerInfo{Hash: "somehash1", MaxRow: 1000},
		&ContainerInfo{Hash: "somehash2", Point: 9},
		100))
	require.Equal(t, "diff", rd.chooseReplicationStrategy(
		&ContainerInfo{Hash: "somehash1", MaxRow: 10},
		&ContainerInfo{Hash: "somehash2", Point: 9},
		100))
}

func TestReplicatorReplicateDatabaseToDevice(t *testing.T) {
	c, _, cleanup, err := createTestDatabase("1410586890.28563")
	require.Nil(t, err)
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{})
	currentMethod := "complete_rsync"
	rsyncCalled := false
	usyncCalled := false
	rd._sync = func(dev *ring.Device, part uint64, ringHash string, info *ContainerInfo) (*ContainerInfo, error) {
		return &ContainerInfo{}, nil
	}
	rd._chooseReplicationStrategy = func(localInfo, remoteInfo *ContainerInfo, usyncThreshold int64) string {
		return currentMethod
	}
	rd._rsync = func(dev *ring.Device, c ReplicableContainer, part uint64, op string) error {
		rsyncCalled = true
		return nil
	}
	rd._usync = func(dev *ring.Device, c ReplicableContainer, part uint64, localID string, point int64) error {
		usyncCalled = true
		return nil
	}
	require.Nil(t, rd.replicateDatabaseToDevice(&ring.Device{}, c, 1))
	require.True(t, rsyncCalled)
	require.False(t, usyncCalled)
	rsyncCalled = false
	currentMethod = "diff"
	require.Nil(t, rd.replicateDatabaseToDevice(&ring.Device{}, c, 1))
	require.True(t, usyncCalled)
	require.False(t, rsyncCalled)
	usyncCalled = false
	currentMethod = "no_change"
	require.Nil(t, rd.replicateDatabaseToDevice(&ring.Device{}, c, 1))
	require.False(t, usyncCalled)
	require.False(t, rsyncCalled)
}

func TestFindContainers(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	files := []string{
		"containers/1/123/f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0/f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0.db",
		"containers/2/456/49f68a5c8493ec2c0bf489821c21fc3b/49f68a5c8493ec2c0bf489821c21fc3b.db",
		"containers/3/789/d850f04cdb48312a9be171e214c0b4ee/d850f04cdb48312a9be171e214c0b4ee.db",
	}
	for _, f := range files {
		os.MkdirAll(filepath.Join(tempDir, filepath.Dir(f)), 0777)
		ioutil.WriteFile(filepath.Join(tempDir, f), []byte(""), 0666)
	}
	results := make(chan string, 3)
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{client: http.DefaultClient})
	rd.findContainerDbs(tempDir, results)
	for _, f := range files {
		require.Equal(t, filepath.Join(tempDir, f), <-results)
	}

	results = make(chan string, 3)
	close(rd.rd.cancel)
	rd.findContainerDbs(tempDir, results)
}

func TestReplicateDatabase(t *testing.T) {
	_, dbFile, cleanup, err := createTestDatabase("1410586890.28563")
	require.Nil(t, err)
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{})
	called := 0
	rd._replicateDatabaseToDevice = func(dev *ring.Device, c ReplicableContainer, part uint64) error {
		require.Equal(t, part, uint64(1))
		if called == 0 {
			require.Equal(t, "sda", dev.Device)
		} else if called == 1 {
			require.Equal(t, "sdb", dev.Device)
		}
		called++
		return nil
	}
	rd.replicateDatabase(dbFile)
	require.Equal(t, 2, called)
}

type handoffJobRing struct {
	fakeRing
}

func (r *handoffJobRing) GetJobNodes(partition uint64, localDevice int) (response []*ring.Device, handoff bool) {
	return []*ring.Device{
		&ring.Device{Device: "sda", ReplicationIp: "127.0.0.1", ReplicationPort: 20000},
		&ring.Device{Device: "sdb", ReplicationIp: "127.0.0.2", ReplicationPort: 2000},
	}, true
}

func TestReplicateDatabaseDeletesHandoff(t *testing.T) {
	_, dbFile, cleanup, err := createTestDatabase("1410586890.28563")
	require.Nil(t, err)
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{Ring: &handoffJobRing{}})
	called := 0
	rd._replicateDatabaseToDevice = func(dev *ring.Device, c ReplicableContainer, part uint64) error {
		require.Equal(t, part, uint64(1))
		if called == 0 {
			require.Equal(t, "sda", dev.Device)
		} else if called == 1 {
			require.Equal(t, "sdb", dev.Device)
		}
		called++
		return nil
	}
	rd.replicateDatabase(dbFile)
	require.Equal(t, 2, called)
	require.False(t, common.Exists(dbFile))
}

func TestReplicateDatabaseDeletesHandoffOnSuccess(t *testing.T) {
	_, dbFile, cleanup, err := createTestDatabase("1410586890.28563")
	require.Nil(t, err)
	defer cleanup()
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{Ring: &handoffJobRing{}})
	called := 0
	rd._replicateDatabaseToDevice = func(dev *ring.Device, c ReplicableContainer, part uint64) error {
		require.Equal(t, part, uint64(1))
		if called == 0 {
			require.Equal(t, "sda", dev.Device)
		} else if called == 1 {
			require.Equal(t, "sdb", dev.Device)
		}
		called++
		return errors.New("I HAVE FAILED YOU")
	}
	rd.replicateDatabase(dbFile)
	require.Equal(t, 2, called)
	require.True(t, common.Exists(dbFile))
}

func TestReplicate(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	require.Nil(t, os.MkdirAll(filepath.Join(tempDir, "sdb"), 0777))

	rep := &Replicator{deviceRoot: tempDir}
	rd := newTestReplicationDevice(&ring.Device{Device: "sdb"}, rep)
	dbFiles := []string{
		"/path/to/a/db.db",
		"/path/to/another/db.db",
		"/path/to/yetanother/db.db",
	}
	rd._findContainerDbs = func(devicePath string, results chan string) {
		for _, dbFile := range dbFiles {
			results <- dbFile
		}
		close(results)
	}
	call := 0
	rd._replicateDatabase = func(dbFile string) error {
		require.Equal(t, dbFiles[call], dbFile)
		call++
		return nil
	}
	rd.replicate()
	require.Equal(t, "sdb", <-rep.startRun)
	for i := 0; i < len(dbFiles); i++ {
		require.Equal(t, "sdb", <-rep.checkin)
	}
	require.Equal(t, call, len(dbFiles))
}

type localDevicesRing struct {
	fakeRing
	localDevs []*ring.Device
}

func (r *localDevicesRing) LocalDevices(localPort int) (devs []*ring.Device, err error) {
	return r.localDevs, nil
}

func TestVerifyDevicesRemoveStuck(t *testing.T) {
	r := &Replicator{
		Ring:   &localDevicesRing{localDevs: []*ring.Device{}},
		logger: fakeLowLevelLogger{},
		runningDevices: map[string]*replicationDevice{
			"sda": &replicationDevice{lastCheckin: time.Now().Add(time.Hour * -2), cancel: make(chan struct{})},
			"sdb": &replicationDevice{lastCheckin: time.Now().Add(time.Hour * -2), cancel: make(chan struct{})},
		},
	}
	r.verifyDevices()
	require.Nil(t, r.runningDevices["sda"])
	require.Nil(t, r.runningDevices["sdb"])
}

func TestVerifyDevicesLaunchMissing(t *testing.T) {
	r := &Replicator{
		Ring: &localDevicesRing{
			localDevs: []*ring.Device{
				&ring.Device{Device: "sda"},
				&ring.Device{Device: "sdb"},
			},
		},
		logger:         fakeLowLevelLogger{},
		runningDevices: map[string]*replicationDevice{},
	}
	r.verifyDevices()
	require.NotNil(t, r.runningDevices["sda"])
	require.NotNil(t, r.runningDevices["sdb"])
}

func TestVerifyDevicesRemoveMissing(t *testing.T) {
	r := &Replicator{
		Ring: &localDevicesRing{
			localDevs: []*ring.Device{},
		},
		logger: fakeLowLevelLogger{},
		runningDevices: map[string]*replicationDevice{
			"sda": &replicationDevice{lastCheckin: time.Now(), cancel: make(chan struct{})},
			"sdb": &replicationDevice{lastCheckin: time.Now(), cancel: make(chan struct{})},
		},
	}
	r.verifyDevices()
	require.Equal(t, 0, len(r.runningDevices))
}

func TestGetReplicator(t *testing.T) {
	oldGetRing := GetRing
	oldGetHashes := GetHashPrefixAndSuffix
	defer func() {
		GetHashPrefixAndSuffix = oldGetHashes
		GetRing = oldGetRing
	}()
	GetHashPrefixAndSuffix = func() (pfx string, sfx string, err error) {
		return "changeme", "changeme", nil
	}
	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &fakeRing{}, nil
	}
	config, err := conf.StringConfig("[container-replicator]\nmount_check=false\nbind_port=1000")
	require.Nil(t, err)
	r, err := GetReplicator(config, &flag.FlagSet{})
	require.Nil(t, err)
	replicator, ok := r.(*Replicator)
	require.True(t, ok)
	require.Equal(t, 1000, replicator.serverPort)
	require.Equal(t, "/srv/node", replicator.deviceRoot)
	require.NotNil(t, replicator.client)
	require.NotNil(t, replicator.sendStat)
	require.NotNil(t, replicator.checkin)
	require.NotNil(t, replicator.concurrencySem)
	require.False(t, replicator.checkMounts)

	config, err = conf.StringConfig("")
	require.Nil(t, err)
	r, err = GetReplicator(config, &flag.FlagSet{})
	require.NotNil(t, err)

	GetHashPrefixAndSuffix = func() (pfx string, sfx string, err error) {
		return "", "", errors.New("I AM A BAD CONFIGURATION")
	}
	config, err = conf.StringConfig("[container-replicator]\nmount_check=false\nbind_port=1000")
	require.Nil(t, err)
	r, err = GetReplicator(config, &flag.FlagSet{})
	require.NotNil(t, err)
}

type savingLowLevelLogger struct {
	fakeLowLevelLogger
	logs []string
}

func (s *savingLowLevelLogger) Info(l string) error {
	s.logs = append(s.logs, l)
	return nil
}

func TestReportStats(t *testing.T) {
	logger := &savingLowLevelLogger{logs: make([]string, 0)}
	r := &Replicator{
		logger: logger,
		runningDevices: map[string]*replicationDevice{
			"sda": &replicationDevice{
				runStarted: time.Now().Add(-5 * time.Minute),
				cancel:     make(chan struct{}),
				stats: map[string]int64{
					"attempted": 10, "success": 5, "failure": 5,
					"no_change": 5, "hashmatch": 4,
					"rsync": 1, "diff": 0, "remove": 7,
					"empty": 0, "remote_merge": 0, "diff_capped": 0,
				},
			},
			"sdb": &replicationDevice{
				runStarted: time.Now().Add(-15 * time.Minute),
				stats: map[string]int64{
					"attempted": 10, "success": 9, "failure": 1,
					"no_change": 8, "hashmatch": 0,
					"rsync": 0, "diff": 1, "remove": 3,
					"empty": 0, "remote_merge": 0, "diff_capped": 0,
				},
			},
		},
	}
	r.reportStats()
	require.True(t, strings.HasPrefix(logger.logs[0], "Attempted to replicate 20 dbs in 600."))
	require.Equal(t, "Removed 10 dbs", logger.logs[1])
	require.Equal(t, "14 successes, 6 failures", logger.logs[2])
}

func TestRunLoop(t *testing.T) {
	logger := &savingLowLevelLogger{logs: make([]string, 0)}
	r := &Replicator{
		logger: logger,
		Ring: &localDevicesRing{
			localDevs: []*ring.Device{
				&ring.Device{Device: "sda"},
				&ring.Device{Device: "sdb"},
			},
		},
		runningDevices: map[string]*replicationDevice{
			"sda": &replicationDevice{
				runStarted:  time.Now().Add(-5 * time.Minute),
				lastCheckin: time.Now().Add(-5 * time.Minute),
				stats: map[string]int64{
					"attempted": 0, "success": 0, "failure": 0, "no_change": 0, "hashmatch": 0,
					"rsync": 0, "diff": 0, "remove": 0, "empty": 0, "remote_merge": 0, "diff_capped": 0,
				},
			},
			"sdb": &replicationDevice{
				runStarted:  time.Now().Add(-15 * time.Minute),
				lastCheckin: time.Now().Add(-15 * time.Minute),
				stats: map[string]int64{
					"attempted": 0, "success": 0, "failure": 0, "no_change": 0, "hashmatch": 0,
					"rsync": 0, "diff": 0, "remove": 0, "empty": 0, "remote_merge": 0, "diff_capped": 0,
				},
			},
		},
		sendStat: make(chan statUpdate, 1),
		startRun: make(chan string, 1),
		checkin:  make(chan string, 1),
	}
	rpTimer := make(chan time.Time, 1)
	rpTimer <- time.Now()
	r.runLoopCheck(rpTimer)
	require.True(t, strings.HasPrefix(logger.logs[0], "Attempted to replicate "))

	r.sendStat <- statUpdate{"sda", "attempted", 1}
	r.runLoopCheck(rpTimer)
	require.Equal(t, int64(1), r.runningDevices["sda"].stats["attempted"])

	r.startRun <- "sda"
	r.runLoopCheck(rpTimer)
	require.Equal(t, int64(0), r.runningDevices["sda"].stats["attempted"])
	lastCheckin := r.runningDevices["sda"].lastCheckin

	r.checkin <- "sda"
	r.runLoopCheck(rpTimer)
	require.NotEqual(t, lastCheckin, r.runningDevices["sda"].lastCheckin)
}

func TestReplicatorRun(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda"), 0777)
	os.MkdirAll(filepath.Join(dir, "sdb"), 0777)
	r := &Replicator{
		Ring: &localDevicesRing{localDevs: []*ring.Device{
			&ring.Device{Device: "sda"},
			&ring.Device{Device: "sdb"},
		}},
		logger:         fakeLowLevelLogger{},
		deviceRoot:     dir,
		sendStat:       make(chan statUpdate, 10),
		startRun:       make(chan string, 10),
		checkin:        make(chan string, 10),
		runningDevices: map[string]*replicationDevice{},
	}
	r.Run()
	require.Equal(t, 2, len(r.runningDevices))
	_, ok := r.runningDevices["sda"]
	require.True(t, ok)
	_, ok = r.runningDevices["sdb"]
	require.True(t, ok)
}

func TestReplicateSyncOnMessageError(t *testing.T) {
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{Ring: &handoffJobRing{}})
	rd._sendReplicationMessage = func(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
		return 0, nil, errors.New("SOME ERROR")
	}
	rinfo, err := rd.sync(&ring.Device{}, 1, "SOMEHASH", &ContainerInfo{})
	require.Nil(t, rinfo)
	require.NotNil(t, err)

	rd._sendReplicationMessage = func(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
		return 404, []byte{}, nil
	}
	rinfo, err = rd.sync(&ring.Device{}, 1, "SOMEHASH", &ContainerInfo{})
	require.Nil(t, rinfo)
	require.Nil(t, err)

	rd._sendReplicationMessage = func(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
		return 200, []byte("BAD JSON"), nil
	}
	rinfo, err = rd.sync(&ring.Device{}, 1, "SOMEHASH", &ContainerInfo{})
	require.Nil(t, rinfo)
	require.NotNil(t, err)

	rd._sendReplicationMessage = func(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
		return http.StatusInsufficientStorage, []byte{}, nil
	}
	rinfo, err = rd.sync(&ring.Device{}, 1, "SOMEHASH", &ContainerInfo{})
	require.Nil(t, rinfo)
	require.Equal(t, err, errDeviceNotMounted)

	rd._sendReplicationMessage = func(dev *ring.Device, part uint64, ringHash string, args ...interface{}) (int, []byte, error) {
		return 500, []byte{}, nil
	}
	rinfo, err = rd.sync(&ring.Device{}, 1, "SOMEHASH", &ContainerInfo{})
	require.Nil(t, rinfo)
	require.NotNil(t, err)
}

func TestSomeDatabaseFailures(t *testing.T) {
	rd := newTestReplicationDevice(&ring.Device{}, &Replicator{Ring: &handoffJobRing{}})
	require.NotNil(t, rd.rsync(&ring.Device{}, fakeDatabase{}, 1, "complete_rsync"))
	require.NotNil(t, rd.usync(&ring.Device{}, fakeDatabase{}, 1, "123", 3))
}
