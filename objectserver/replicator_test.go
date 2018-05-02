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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func newTestReplicator(confLoader *srv.TestConfigLoader, settings ...string) (*Replicator, conf.Config, error) {
	return newTestReplicatorWithFlags(confLoader, settings, &flag.FlagSet{})
}

func newTestReplicatorWithFlags(confLoader *srv.TestConfigLoader, settings []string, flags *flag.FlagSet) (*Replicator, conf.Config, error) {
	configString := "[object-replicator]\nmount_check=false\n"
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, _ := conf.StringConfig(configString)
	_, replicator, _, err := NewReplicator(conf, flags, confLoader)
	if err != nil {
		return nil, conf, err
	}
	rep := replicator.(*Replicator)
	rep.replicateConcurrencySem = make(chan struct{}, 1)
	rep.updateConcurrencySem = make(chan struct{}, 1)
	rep.updateStat = make(chan statUpdate, 100)
	return rep, conf, nil
}

type mockRepConn struct {
	_SendMessage    func(v interface{}) error
	_RecvMessage    func(v interface{}, sfrq *SyncFileRequest) error
	_Write          func(data []byte) (l int, err error)
	_Flush          func() error
	_Read           func(data []byte) (l int, err error)
	_Disconnected   func() bool
	_Close          func()
	lastSentMessage *SyncFileRequest
}

func (f *mockRepConn) SendMessage(v interface{}) error {
	if sfr, ok := v.(SyncFileRequest); ok {
		f.lastSentMessage = &sfr
	}
	if f._SendMessage != nil {
		return f._SendMessage(v)
	}
	return nil
}
func (f *mockRepConn) RecvMessage(v interface{}) error {
	if f._RecvMessage != nil {
		return f._RecvMessage(v, f.lastSentMessage)
	}
	return nil
}
func (f *mockRepConn) Write(data []byte) (l int, err error) {
	if f._Write != nil {
		return f._Write(data)
	}
	return len(data), nil
}
func (f *mockRepConn) Flush() error {
	if f._Flush != nil {
		return f._Flush()
	}
	return nil
}
func (f *mockRepConn) Read(data []byte) (l int, err error) {
	if f._Read != nil {
		return f._Read(data)
	}
	return len(data), nil
}

func (f *mockRepConn) Disconnected() bool {
	if f._Disconnected != nil {
		return f._Disconnected()
	}
	return false
}
func (f *mockRepConn) Close() {
	if f._Close != nil {
		f._Close()
	}
}

type mockReplicationDevice struct {
	_Replicate         func()
	_ReplicateLoop     func()
	_Key               func() string
	_Cancel            func()
	_PriorityReplicate func(w http.ResponseWriter, pri PriorityRepJob) error
}

func (d *mockReplicationDevice) Replicate() {
	if d._Replicate != nil {
		d._Replicate()
	}
}
func (d *mockReplicationDevice) ReplicateLoop() {
	if d._ReplicateLoop != nil {
		d._ReplicateLoop()
	}
}
func (d *mockReplicationDevice) Key() string {
	if d._Key != nil {
		return d._Key()
	}
	return ""
}
func (d *mockReplicationDevice) Cancel() {
	if d._Cancel != nil {
		d._Cancel()
	}
}
func (d *mockReplicationDevice) UpdateStat(s string, v int64) {
}

func (d *mockReplicationDevice) PriorityReplicate(w http.ResponseWriter, pri PriorityRepJob) {
	if d._PriorityReplicate != nil {
		d._PriorityReplicate(w, pri)
	}
	return
}

type patchableReplicationDevice struct {
	*swiftDevice
	_beginReplication     func(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse, headers map[string]string)
	_listObjFiles         func(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool)
	_syncFile             func(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error)
	_replicateUsingHashes func(rjob replJob, moreNodes ring.MoreNodes)
	_replicateAll         func(rjob replJob, isHandoff bool)
	_cleanTemp            func()
	_listPartitions       func() ([]string, []string, error)
	_replicatePartition   func(partition string)
	_Replicate            func()
}

func (d *patchableReplicationDevice) replicatePartition(partition string) {
	if d._replicatePartition != nil {
		d._replicatePartition(partition)
		return
	}
	d.swiftDevice.replicatePartition(partition)
}
func (d *patchableReplicationDevice) listPartitions() ([]string, []string, error) {
	if d._listPartitions != nil {
		return d._listPartitions()
	}
	return d.swiftDevice.listPartitions()
}
func (d *patchableReplicationDevice) beginReplication(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse, headers map[string]string) {
	if d._beginReplication != nil {
		d._beginReplication(dev, partition, hashes, rChan, headers)
		return
	}
	d.swiftDevice.beginReplication(dev, partition, hashes, rChan, headers)
}
func (d *patchableReplicationDevice) listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
	if d._listObjFiles != nil {
		d._listObjFiles(objChan, cancel, partdir, needSuffix)
		return
	}
	d.swiftDevice.listObjFiles(objChan, cancel, partdir, needSuffix)
}
func (d *patchableReplicationDevice) syncFile(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error) {
	if d._syncFile != nil {
		return d._syncFile(objFile, dst, handoff)
	}
	return d.swiftDevice.syncFile(objFile, dst, handoff)
}
func (d *patchableReplicationDevice) replicateUsingHashes(rjob replJob, moreNodes ring.MoreNodes) (int64, error) {
	if d._replicateUsingHashes != nil {
		d._replicateUsingHashes(rjob, moreNodes)
		return 0, nil
	}
	return d.swiftDevice.replicateUsingHashes(rjob, moreNodes)
}
func (d *patchableReplicationDevice) replicateAll(rjob replJob, isHandoff bool) (int64, error) {
	if d._replicateAll != nil {
		d._replicateAll(rjob, isHandoff)
		return 0, nil
	}
	return d.swiftDevice.replicateAll(rjob, isHandoff)
}
func (d *patchableReplicationDevice) cleanTemp() {
	if d._cleanTemp != nil {
		d._cleanTemp()
		return
	}
	d.swiftDevice.cleanTemp()
}

func newPatchableReplicationDevice(rng ring.Ring, r *Replicator) *patchableReplicationDevice {
	rd := &swiftDevice{
		r:      r,
		dev:    &ring.Device{},
		policy: 0,
		cancel: make(chan struct{}),
	}
	prd := &patchableReplicationDevice{swiftDevice: rd}
	rd.i = prd
	return prd
}

type TestReplicatorWebServer struct {
	*httptest.Server
	host       string
	port       int
	root       string
	replicator *Replicator
}

func (t *TestReplicatorWebServer) Close() {
	os.RemoveAll(t.root)
	t.Server.Close()
}

func (t *TestReplicatorWebServer) Do(method string, path string, body io.ReadCloser) (*http.Response, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("http://%s:%d%s", t.host, t.port, path), body)
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func makeReplicatorWebServer(confLoader *srv.TestConfigLoader, settings ...string) (*TestReplicatorWebServer, error) {
	return makeReplicatorWebServerWithFlags(confLoader, settings, &flag.FlagSet{})
}

var nonce int32

func makeReplicatorWebServerWithFlags(confLoader *srv.TestConfigLoader, settings []string, flags *flag.FlagSet) (*TestReplicatorWebServer, error) {
	deviceRoot, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(deviceRoot)
	replicator, conf, err := newTestReplicatorWithFlags(confLoader, settings, flags)
	if err != nil {
		return nil, err
	}
	ts := httptest.NewServer(replicator.GetHandler(conf, fmt.Sprintf("hb_metrics_%d", atomic.AddInt32(&nonce, 1))))
	u, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	host, ports, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(ports)
	if err != nil {
		return nil, err
	}
	return &TestReplicatorWebServer{Server: ts, host: host, port: port, root: deviceRoot, replicator: replicator}, nil
}

func TestGetFile(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	assert.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1234567890.12345",
		"Content-Length": "9",
		"name":           "some name",
	})

	fp, xattrs, size, err := getFile(file.Name())
	fp.Close()
	require.Equal(t, size, int64(9))
	require.True(t, len(xattrs) > 0)
	assert.Nil(t, err)
}

func TestGetFileBadFile(t *testing.T) {
	_, _, _, err := getFile("somenonexistentfile")
	require.NotNil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	_, _, _, err = getFile(dir)
	require.NotNil(t, err)

	file, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)
}

func TestGetFileBadMetadata(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())

	require.Nil(t, common.SwiftObjectRawWriteMetadata(file.Fd(), []byte("HI")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, common.SwiftObjectRawWriteMetadata(file.Fd(), []byte("\x80\x02U\x02HIq\x01.")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, common.SwiftObjectRawWriteMetadata(file.Fd(), []byte("\x80\x02}q\x01K\x00U\x02hiq\x02s.")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, common.SwiftObjectRawWriteMetadata(file.Fd(), []byte("\x80\x02}q\x01U\x02hiq\x02K\x00s.")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	dfile, err := os.Create(file.Name() + ".data")
	require.Nil(t, err)
	defer dfile.Close()
	defer os.RemoveAll(dfile.Name())
	require.Nil(t, common.SwiftObjectWriteMetadata(dfile.Fd(), nil))
	_, _, _, err = getFile(dfile.Name())
	require.NotNil(t, err)

	tfile, err := os.Create(file.Name() + ".ts")
	require.Nil(t, err)
	defer tfile.Close()
	defer os.RemoveAll(tfile.Name())
	require.Nil(t, common.SwiftObjectWriteMetadata(tfile.Fd(), nil))
	_, _, _, err = getFile(tfile.Name())
	require.NotNil(t, err)

	dfile, err = os.Create(file.Name() + ".data")
	require.Nil(t, err)
	defer dfile.Close()
	defer os.RemoveAll(dfile.Name())
	require.Nil(t, common.SwiftObjectWriteMetadata(dfile.Fd(), nil))
	_, _, _, err = getFile(dfile.Name())
	require.NotNil(t, err)
}

func TestListObjFiles(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	repl, _, err := newTestReplicator(confLoader)
	require.Nil(t, err)
	rd := &swiftDevice{
		r:      repl,
		dev:    &ring.Device{},
		policy: 0,
		cancel: make(chan struct{}),
	}
	rd.i = rd
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e"), 0777)
	fp, err := os.Create(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	require.Nil(t, err)
	defer fp.Close()
	objChan := make(chan string)
	cancel := make(chan struct{})
	var files []string
	go rd.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.Equal(t, 1, len(files))
	require.Equal(t, filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"), files[0])

	os.RemoveAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	objChan = make(chan string)
	files = nil
	go rd.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.False(t, fs.Exists(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e")))
	require.True(t, fs.Exists(filepath.Join(dir, "objects", "1", "abc")))

	objChan = make(chan string)
	files = nil
	go rd.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.False(t, fs.Exists(filepath.Join(dir, "objects", "1", "abc")))
	require.True(t, fs.Exists(filepath.Join(dir, "objects", "1")))

	objChan = make(chan string)
	files = nil
	go rd.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.False(t, fs.Exists(filepath.Join(dir, "objects", "1")))
	require.True(t, fs.Exists(filepath.Join(dir, "objects")))
}

func TestCancelListObjFiles(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	repl, _, err := newTestReplicator(confLoader)
	require.Nil(t, err)
	rd := &swiftDevice{
		r:      repl,
		dev:    &ring.Device{},
		policy: 0,
		cancel: make(chan struct{}),
	}
	rd.i = rd
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e"), 0777)
	fp, err := os.Create(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	require.Nil(t, err)
	fp.Close()
	objChan := make(chan string)
	cancel := make(chan struct{})
	// Oh no, nobody is reading from your channel and you are stuck!
	go rd.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	// so we cancel you and make sure you closed your channel, which you do on exit.
	close(cancel)
	time.Sleep(time.Millisecond)
	_, ok := <-objChan
	require.False(t, ok)
}

func TestPriorityRepHandler404(t *testing.T) {
	t.Parallel()
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.deviceRoot = deviceRoot
	w := httptest.NewRecorder()
	job := &PriorityRepJob{
		Partition:  0,
		FromDevice: &ring.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000, ReplicationIp: "127.0.0.1", ReplicationPort: 5000},
		ToDevice:   &ring.Device{Id: 2, Device: "sdb"},
	}
	jsonned, _ := json.Marshal(job)
	req, _ := http.NewRequest("POST", "/priorityrep", bytes.NewBuffer(jsonned))
	replicator.priorityRepHandler(w, req)
	require.EqualValues(t, 404, w.Code)
}

func TestSyncFile(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			if sfr, ok := v.(*SyncFileResponse); ok {
				sfr.GoAhead = true
			} else if fur, ok := v.(*FileUploadResponse); ok {
				fur.Success = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, false)
	require.Nil(t, err)
	require.Equal(t, 1, syncs)
	require.Equal(t, 1, insync)
	require.Equal(t, 9, dataReceived)
}

func TestSyncFileExists(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			if sfr, ok := v.(*SyncFileResponse); ok {
				sfr.Exists = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, false)
	require.Nil(t, err)
	require.Equal(t, 0, syncs)
	require.Equal(t, 1, insync)
	require.Equal(t, 0, dataReceived)
}

func TestSyncFileNewerExists(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	defer file.Close()
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			if sfr, ok := v.(*SyncFileResponse); ok {
				sfr.NewerExists = true
			}
			return nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, false)
	require.Nil(t, err)
	require.False(t, fs.Exists(filename))
	require.Equal(t, 0, syncs)
	require.Equal(t, 1, insync)
}

func TestReplicateUsingHashes(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	objPath := filepath.Join(deviceRoot, "objects")
	partition := "1"
	remoteDev := &ring.Device{Device: "sda"}
	filename := filepath.Join(objPath, partition, "aaa", "00000000000000000000000000000000", "1472940619.68559")
	syncFileCalled := false
	rd := newPatchableReplicationDevice(testRing, replicator)
	rd._beginReplication = func(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse, headers map[string]string) {
		fakeHashes := make(map[string]string)
		fakeHashes["aaa"] = "hey"
		rChan <- beginReplicationResponse{dev: remoteDev, hashes: fakeHashes, conn: &mockRepConn{}}
	}
	rd._listObjFiles = func(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
		objChan <- filename
		close(objChan)
	}
	rd._syncFile = func(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error) {
		syncFileCalled = true
		require.Equal(t, filename, objFile)
		return 0, 0, nil
	}
	nodes := []*ring.Device{remoteDev}
	rd.replicateUsingHashes(replJob{partition, nodes, nil}, &NoMoreNodes{})
	require.True(t, syncFileCalled)
}

func TestReplicateAll(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	partition := "1"
	objPath := filepath.Join(deviceRoot, "objects")
	remoteDev := &ring.Device{Device: "sda"}
	filename := filepath.Join(objPath, partition, "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	defer file.Close()
	syncFileCalled := false
	rd := newPatchableReplicationDevice(testRing, replicator)
	rd._beginReplication = func(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse, headers map[string]string) {
		rChan <- beginReplicationResponse{dev: remoteDev, hashes: make(map[string]string), conn: &mockRepConn{}}
	}
	rd._listObjFiles = func(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
		objChan <- filename
		close(objChan)
	}
	rd._syncFile = func(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error) {
		syncFileCalled = true
		require.Equal(t, filename, objFile)
		return 1, 1, nil
	}
	nodes := []*ring.Device{remoteDev}
	rd.replicateAll(replJob{partition, nodes, nil}, true)
	require.True(t, syncFileCalled)
	require.False(t, fs.Exists(filename))
}

func TestCleanTemp(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no", "devices", deviceRoot)
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(testRing, replicator)
	rd.dev.Device = "sda"
	tmpDir := filepath.Join(deviceRoot, "sda", "tmp")
	require.Nil(t, os.MkdirAll(tmpDir, 0777))
	file, err := os.Create(filepath.Join(tmpDir, "testfile1"))
	require.Nil(t, err)
	file.Close()
	file, err = os.Create(filepath.Join(tmpDir, "testfile2"))
	require.Nil(t, err)
	file.Close()
	oldTime := time.Now().Add(-(time.Hour * 24 * 14))
	require.Nil(t, os.Chtimes(filepath.Join(tmpDir, "testfile2"), oldTime, oldTime))
	rd.cleanTemp()
	require.False(t, fs.Exists(filepath.Join(tmpDir, "testfile2")))
	require.True(t, fs.Exists(filepath.Join(tmpDir, "testfile1")))
}

func TestReplicate(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(testRing, replicator)
	rd._listPartitions = func() ([]string, []string, error) {
		return []string{"1", "2", "3"}, []string{"2"}, nil
	}
	calledWith := []string{}
	rd._replicatePartition = func(partition string) {
		calledWith = append(calledWith, partition)
	}
	rd.Replicate()
	require.Equal(t, []string{"1", "2", "2", "3"}, calledWith)
}

func TestCancelReplicate(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(testRing, replicator)
	rd._listPartitions = func() ([]string, []string, error) {
		return []string{"1", "2", "3"}, nil, nil
	}
	calledWith := []string{}
	rd._replicatePartition = func(partition string) {
		calledWith = append(calledWith, partition)
	}
	rd.cancel = make(chan struct{}, 1)
	rd.cancel <- struct{}{}
	rd.Replicate()
	require.Equal(t, 0, len(calledWith))
}

func TestListPartitions(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no", "devices", deviceRoot)
	require.Nil(t, err)
	objPath := filepath.Join(deviceRoot, "sda", "objects")
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "1"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "2"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "3"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "X"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "Y"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "Z"), 0777))
	rd := newPatchableReplicationDevice(testRing, replicator)
	rd.dev = &ring.Device{Device: "sda"}
	partitions, _, err := rd.listPartitions()
	require.Nil(t, err)
	require.Equal(t, 3, len(partitions))

	replicator.partitions = map[string]bool{"2": true}
	partitions, _, err = rd.listPartitions()
	require.Nil(t, err)
	require.Equal(t, 1, len(partitions))
	require.Equal(t, "2", partitions[0])
}

func TestReplicatePartition(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{MockGetMoreNodes: &NoMoreNodes{}}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no", "devices", deviceRoot)
	require.Nil(t, err)
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(testRing, replicator)
	replicateUsingHashesCalled := false
	replicateAllCalled := false
	rd._replicateUsingHashes = func(rjob replJob, moreNodes ring.MoreNodes) {
		require.Equal(t, "1", rjob.partition)
		replicateUsingHashesCalled = true
	}
	rd._replicateAll = func(rjob replJob, isHandoff bool) {
		replicateAllCalled = true
	}
	rd.replicatePartition("1")
	require.True(t, replicateUsingHashesCalled)
	require.False(t, replicateAllCalled)

	testRing.MockGetJobNodesHandoff = true
	replicateUsingHashesCalled = false
	replicateAllCalled = false
	rd.replicatePartition("1")
	require.False(t, replicateUsingHashesCalled)
	require.True(t, replicateAllCalled)
}

func TestProcessPriorityJobs(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{MockGetMoreNodes: &NoMoreNodes{}}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader)
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(testRing, replicator)
	repJob := PriorityRepJob{
		Partition:  1,
		FromDevice: &ring.Device{Device: "sda"},
		ToDevice:   &ring.Device{Device: "sdb"},
		Policy:     0,
	}
	replicateUsingHashesCalled := false
	replicateAllCalled := false
	rd._replicateUsingHashes = func(rjob replJob, moreNodes ring.MoreNodes) {
		require.Equal(t, "1", rjob.partition)
		replicateUsingHashesCalled = true
	}
	rd._replicateAll = func(rjob replJob, isHandoff bool) {
		require.Equal(t, "1", rjob.partition)
		replicateAllCalled = true
	}
	replicator.runningDevices["sda"] = rd
	replicator.deviceRoot = deviceRoot
	os.MkdirAll(filepath.Join(deviceRoot, "sda", "objects", "1"), os.ModeDir)
	data, err := json.Marshal(repJob)
	require.Nil(t, err)
	req, err := http.NewRequest("POST", "/", bytes.NewReader(data))
	require.Nil(t, err)
	replicator.priorityRepHandler(&httptest.ResponseRecorder{}, req)
	require.True(t, replicateUsingHashesCalled)

	repJob = PriorityRepJob{
		Partition:  1,
		FromDevice: &ring.Device{Device: "sda"},
		ToDevice:   &ring.Device{Device: "sdb"},
		Policy:     0,
	}
	testRing.MockGetJobNodesHandoff = true
	replicateUsingHashesCalled = false
	replicateAllCalled = false
	data, err = json.Marshal(repJob)
	require.Nil(t, err)
	req, err = http.NewRequest("POST", "/", bytes.NewReader(data))
	require.Nil(t, err)
	replicator.priorityRepHandler(&httptest.ResponseRecorder{}, req)
	require.True(t, replicateAllCalled)
}

func TestCancelStalledDevices(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	type repDev struct {
		mockReplicationDevice
		index   int
		running bool
	}
	replicator.stats = map[string]map[string]*DeviceStats{
		"object-replicator": {
			"sda": {LastCheckin: time.Now()},
			"sdb": {LastCheckin: time.Now()},
			"sdc": {LastCheckin: time.Now()},
		},
	}
	mockDevices := []*repDev{
		{index: 0, running: true},
		{index: 1, running: true},
		{index: 2, running: true},
	}
	runningDevices := map[string]ReplicationDevice{
		"sda": mockDevices[0], "sdb": mockDevices[1], "sdc": mockDevices[2],
	}
	for _, v := range mockDevices {
		w := v
		w._Cancel = func() {
			w.running = false
		}
	}
	replicator.runningDevices = runningDevices
	replicator.cancelStalledDevices()
	for _, v := range mockDevices {
		require.True(t, v.running)
	}

	replicator.stats["object-replicator"]["sda"].LastCheckin = time.Now().Add(-replicateDeviceTimeout)
	replicator.runningDevices = runningDevices
	replicator.cancelStalledDevices()
	require.False(t, mockDevices[0].running)
	require.True(t, mockDevices[1].running)
	require.True(t, mockDevices[2].running)
}

func TestVerifyDevices(t *testing.T) {
	testRing := &test.FakeRing{MockLocalDevices: []*ring.Device{{Device: "sda"}}}
	confLoader := srv.NewTestConfigLoader(testRing)
	confLoader.GetPoliciesFunc = func() (conf.PolicyList, error) {
		return conf.PolicyList(map[int]*conf.Policy{0: {Index: 0, Type: "replication", Name: "gold", Aliases: []string{}, Default: true, Deprecated: false, Config: map[string]string{"policy_type": "replication", "default": "yes", "name": "gold"}}}), nil
	}
	canceled := false
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.runningDevices = map[string]ReplicationDevice{
		"sdb": &mockReplicationDevice{
			_Cancel: func() {
				canceled = true
			},
		},
	}
	require.Equal(t, 0, len(replicator.stats["object-replicator"]))
	replicator.verifyRunningDevices()
	_, ok := replicator.stats["object-replicator"]["sda"]
	require.True(t, ok)
	require.Equal(t, 1, len(replicator.runningDevices))
	require.True(t, canceled)
}

func TestReportStats(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.stats = map[string]map[string]*DeviceStats{
		"object-replicator": {
			"sda": &DeviceStats{
				LastPassDuration: time.Hour,
				PassStarted:      time.Now().Add(-time.Hour),
				Stats: map[string]int64{
					"PartitionsTotal": 1000,
					"PartitionsDone":  500,
				},
			},

			"sdb": &DeviceStats{
				LastPassDuration: time.Hour,
				PassStarted:      time.Now().Add(-time.Hour),
				Stats: map[string]int64{
					"PartitionsTotal": 100,
					"PartitionsDone":  40,
				},
			},
		},
	}
	replicator.runningDevices = map[string]ReplicationDevice{
		"sda": &mockReplicationDevice{
			_Key: func() string { return "1.1:10/sda" },
		},
		"sdb": &mockReplicationDevice{
			_Key: func() string { return "1.1:10/sdb" },
		},
	}
	obs, logs := observer.New(zap.InfoLevel)
	replicator.logger = zap.New(obs)
	replicator.reportStats()
	want := []observer.LoggedEntry{{
		Entry: zapcore.Entry{Level: zap.InfoLevel, Message: "Partition Replicated"},
		Context: []zapcore.Field{zap.String("Device", "sda"),
			zap.Int64("doneParts", 500),
			zap.Int64("totalParts", 1000),
			zap.Float64("DoneParts/TotalParts", 50.00),
			zap.Float64("processingTimeSec", 0.00),
			zap.Float64("partsPerSecond", 0.00),
			zap.String("remainingStr", "1h")},
	}, {
		Entry: zapcore.Entry{Level: zap.InfoLevel, Message: "Partition Replicated"},
		Context: []zapcore.Field{zap.String("Device", "sdb"),
			zap.Int64("doneParts", 40),
			zap.Int64("totalParts", 100),
			zap.Float64("DoneParts/TotalParts", 40.00),
			zap.Float64("processingTimeSec", 0.00),
			zap.Float64("partsPerSecond", 0.00),
			zap.String("remainingStr", "2h")},
	}}
	require.Equal(t, 2, logs.Len())
	var obslog1, obslog2 observer.LoggedEntry
	if logs.AllUntimed()[0].Context[1].Integer == 500 {
		obslog1 = logs.AllUntimed()[0]
		obslog2 = logs.AllUntimed()[1]
	} else {
		obslog2 = logs.AllUntimed()[0]
		obslog1 = logs.AllUntimed()[1]
	}

	require.Equal(t, want[0].Message, obslog1.Message)
	require.Equal(t, want[0].Context[0], obslog1.Context[0])
	require.Equal(t, want[0].Context[1], obslog1.Context[1])
	require.Equal(t, want[0].Context[2], obslog1.Context[2])
	//unable to do float comparison
	//require.Equal(t, want[0].Context[3], obslog1.Context[3])
	//require.Equal(t, want[0].Context[4], obslog1.Context[4])
	//require.Equal(t, want[0].Context[5], obslog1.Context[5])
	require.Equal(t, want[0].Context[6], obslog1.Context[6])

	require.Equal(t, want[1].Message, obslog2.Message)
	require.Equal(t, want[1].Context[0], obslog2.Context[0])
	require.Equal(t, want[1].Context[1], obslog2.Context[1])
	require.Equal(t, want[1].Context[2], obslog2.Context[2])
	//unable to do float comparison
	//require.Equal(t, want[1].Context[3], obslog2.Context[3])
	//require.Equal(t, want[1].Context[4], obslog2.Context[4])
	//require.Equal(t, want[1].Context[5], obslog2.Context[5])
	require.Equal(t, want[1].Context[6], obslog2.Context[6])
}

func TestPriorityReplicate(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	priorityReplicateCalled := false
	replicator.runningDevices = map[string]ReplicationDevice{
		"sda": &mockReplicationDevice{
			_PriorityReplicate: func(w http.ResponseWriter, pri PriorityRepJob) error {
				priorityReplicateCalled = true
				return nil
			},
		},
	}
	repJob := PriorityRepJob{
		Partition:  1,
		FromDevice: &ring.Device{Device: "sda"},
		ToDevice:   nil,
		Policy:     0,
	}
	data, err := json.Marshal(repJob)
	require.Nil(t, err)
	req, err := http.NewRequest("POST", "/", bytes.NewReader(data))
	require.Nil(t, err)
	replicator.priorityRepHandler(&httptest.ResponseRecorder{}, req)
	require.True(t, priorityReplicateCalled)
}

func TestGetDeviceProgress(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.stats = map[string]map[string]*DeviceStats{
		"object-replicator": {
			"sda": &DeviceStats{
				LastPassDuration: time.Hour,
				PassStarted:      time.Now().Add(-time.Hour),
				PartitionsTotal:  1000,
				PartitionsDone:   500,
			},

			"sdb": &DeviceStats{
				LastPassDuration: time.Hour,
				PassStarted:      time.Now().Add(-time.Hour),
				PartitionsTotal:  100,
				PartitionsDone:   50,
			},
		},
	}
	progress := replicator.getDeviceProgress()
	sdb, ok := progress["sdb"]
	require.True(t, ok)
	require.Equal(t, int64(100), sdb.PartitionsTotal)
	require.Equal(t, int64(50), sdb.PartitionsDone)
}

func TestRunLoopOnceDone(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.onceWaiting = 10
	replicator.onceDone = make(chan struct{}, 1)
	replicator.onceDone <- struct{}{}
	replicator.runLoopCheck(make(chan time.Time))
	require.Equal(t, int64(9), replicator.onceWaiting)
}

func TestRunLoopStatUpdate(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.stats = map[string]map[string]*DeviceStats{
		"object-replicator": {
			"sda": &DeviceStats{
				LastPassDuration: time.Hour,
				PassStarted:      time.Now().Add(-time.Hour),
				PartitionsTotal:  1000,
				PartitionsDone:   500,
			},
		},
	}
	st := replicator.stats["object-replicator"]["sda"]
	rd := &mockReplicationDevice{}
	replicator.runningDevices = map[string]ReplicationDevice{"sda": rd}
	replicator.updateStat = make(chan statUpdate, 1)
	replicator.updateStat <- statUpdate{"object-replicator", "sda", "PartitionsTotal", 1001}
	replicator.runLoopCheck(make(chan time.Time))
	require.Equal(t, int64(1001), st.PartitionsTotal)
	require.Equal(t, int64(500), st.PartitionsDone)
	replicator.updateStat <- statUpdate{"object-replicator", "sda", "PartitionsDone", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.Equal(t, int64(1001), st.PartitionsTotal)
	require.Equal(t, int64(501), st.PartitionsDone)
	replicator.updateStat <- statUpdate{"object-replicator", "sda", "checkin", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.True(t, time.Since(st.LastCheckin) < time.Second)
	replicator.updateStat <- statUpdate{"object-replicator", "sda", "startRun", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.True(t, time.Since(st.PassStarted) < time.Second)
	require.Equal(t, int64(0), st.PartitionsDone)
}

func TestReplicationLocal(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	trs1, err := makeReplicatorWebServer(confLoader)
	require.Nil(t, err)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot

	trs2, err := makeReplicatorWebServer(confLoader)
	require.Nil(t, err)
	defer trs2.Close()
	trs2.replicator.deviceRoot = ts2.objServer.driveRoot
	ldev := &ring.Device{ReplicationIp: trs1.host, ReplicationPort: trs1.port, Device: "sda", Scheme: "http"}
	rdev := &ring.Device{ReplicationIp: trs2.host, ReplicationPort: trs2.port, Device: "sda", Scheme: "http"}
	testRing.MockLocalDevices = []*ring.Device{ldev}
	testRing.MockGetJobNodes = []*ring.Device{rdev}

	trs1.replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

func TestReplicationHandoff(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)

	trs1, err := makeReplicatorWebServer(confLoader)
	require.Nil(t, err)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot
	trs2, err := makeReplicatorWebServer(confLoader)
	require.Nil(t, err)
	defer trs2.Close()
	trs2.replicator.deviceRoot = ts2.objServer.driveRoot

	ldev := &ring.Device{ReplicationIp: trs1.host, ReplicationPort: trs1.port, Device: "sda", Scheme: "http"}
	rdev := &ring.Device{ReplicationIp: trs2.host, ReplicationPort: trs2.port, Device: "sda", Scheme: "http"}
	testRing.MockLocalDevices = []*ring.Device{ldev}
	testRing.MockGetJobNodes = []*ring.Device{rdev}
	testRing.MockGetJobNodesHandoff = true

	trs1.replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 404, resp.StatusCode)
}

func TestReplicationHandoffQuorumDelete(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	ts, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer(confLoader)
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	flags := flag.NewFlagSet("hbird flags", flag.ContinueOnError)
	flags.Bool("q", false, "boolean value")
	flags.Parse([]string{})
	trs1, err := makeReplicatorWebServerWithFlags(confLoader, []string{}, flags)
	require.Nil(t, err)
	require.False(t, trs1.replicator.quorumDelete)
	trs1.Close()

	flags.Parse([]string{"-q"})
	trs1, err = makeReplicatorWebServerWithFlags(confLoader, []string{}, flags)
	require.Nil(t, err)
	require.True(t, trs1.replicator.quorumDelete)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot

	trs2, err := makeReplicatorWebServer(confLoader)
	require.Nil(t, err)
	defer trs2.Close()
	trs2.replicator.deviceRoot = ts2.objServer.driveRoot

	ldev := &ring.Device{ReplicationIp: trs1.host, ReplicationPort: trs1.port, Device: "sda", Scheme: "http"}
	rdev := &ring.Device{ReplicationIp: trs2.host, ReplicationPort: trs2.port, Device: "sda", Scheme: "http"}
	testRing.MockLocalDevices = []*ring.Device{ldev}
	testRing.MockGetJobNodes = []*ring.Device{rdev}

	trs1.replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

func TestAllDifferentRegionsSync(t *testing.T) {
	// syncing a file in non handoff partition to 3 devs in separate remote regions
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			if sfr, ok := v.(*SyncFileResponse); ok {
				sfr.GoAhead = true
			} else if fur, ok := v.(*FileUploadResponse); ok {
				fur.Success = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda1"), Region: 1}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda2"), Region: 2}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda3"), Region: 3}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, false)
	require.Nil(t, err)
	require.Equal(t, 3, syncs)
	require.Equal(t, 3, insync)
	require.Equal(t, 27, dataReceived)
}

func TestAllSameRegionsSync(t *testing.T) {
	// syncing a file in non handoff partition to 3 devs in local region
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			if sfr, ok := v.(*SyncFileResponse); ok {
				sfr.GoAhead = true
			} else if fur, ok := v.(*FileUploadResponse); ok {
				fur.Success = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda1"), Region: 0}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda2"), Region: 0}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda3"), Region: 0}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, false)
	require.Nil(t, err)
	require.Equal(t, 3, syncs)
	require.Equal(t, 3, insync)
	require.Equal(t, 27, dataReceived)
}

func TestHalfSameRegionsSync(t *testing.T) {
	// syncing a file in non handoff part in reg0 to dev in reg1 and 2 devs in reg2
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			sfr, ok := v.(*SyncFileResponse)
			if ok {
				if sfrq != nil && sfrq.Ping {
					sfr.Msg = "pong"
				} else {
					sfr.GoAhead = true
				}
			} else if fur, ok := v.(*FileUploadResponse); ok {
				fur.Success = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda1"), Region: 1}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda2"), Region: 2}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda3"), Region: 2}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, false)
	require.Nil(t, err)
	require.Equal(t, 2, syncs)
	require.Equal(t, 2, insync)
	require.Equal(t, 18, dataReceived)
}

func TestHalfSameRegionsSyncHandoffNotExists(t *testing.T) {
	// syncing a file in handoff part in reg0 to dev in reg1 and
	// 2 devs in reg2. file is on neither remote dev

	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	gotACheck := false
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			sfr, ok := v.(*SyncFileResponse)
			if ok {
				if sfrq != nil && sfrq.Ping {
					sfr.Msg = "pong"
				} else if sfrq != nil && sfrq.Check {
					sfr.Msg = "just check"
					sfr.Exists = false
					gotACheck = true
				} else {
					sfr.GoAhead = true
				}
			} else if fur, ok := v.(*FileUploadResponse); ok {
				fur.Success = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda1"), Region: 1}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda2"), Region: 2}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda3"), Region: 2}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, true)
	require.Nil(t, err)
	require.Equal(t, 2, syncs)
	require.Equal(t, 2, insync)
	require.Equal(t, 18, dataReceived)
	require.True(t, gotACheck)
}

func TestHalfSameRegionsSyncHandoffYesExists(t *testing.T) {
	// syncing a file in handoff part in reg0 to dev in reg1 and
	// 2 devs in reg2. file already on remote that you are only checking
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	replicator, _, err := newTestReplicator(confLoader, "bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	common.SwiftObjectWriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	gotACheck := false
	rd := newPatchableReplicationDevice(testRing, replicator)
	rc := &mockRepConn{
		_RecvMessage: func(v interface{}, sfrq *SyncFileRequest) error {
			sfr, ok := v.(*SyncFileResponse)
			if ok {
				if sfrq != nil && sfrq.Ping {
					sfr.Msg = "pong"
				} else if sfrq != nil && sfrq.Check {
					sfr.Msg = "file exists"
					sfr.Exists = true
					gotACheck = true
				} else {
					sfr.GoAhead = true
				}
			} else if fur, ok := v.(*FileUploadResponse); ok {
				fur.Success = true
			}
			return nil
		},
		_Write: func(data []byte) (l int, err error) {
			dataReceived += len(data)
			return len(data), nil
		},
	}
	dsts := []*syncFileArg{
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda1"), Region: 1}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda2"), Region: 2}},
		{conn: rc, dev: &ring.Device{Device: fmt.Sprintf("sda3"), Region: 2}},
	}
	syncs, insync, err := rd.syncFile(file.Name(), dsts, true)
	require.Nil(t, err)
	require.True(t, gotACheck)
	require.Equal(t, 2, syncs)
	require.Equal(t, 3, insync)
	require.Equal(t, 18, dataReceived)
}
