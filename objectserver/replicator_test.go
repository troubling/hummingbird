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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func newTestReplicator(settings ...string) (*Replicator, error) {
	return newTestReplicatorWithFlags(settings, &flag.FlagSet{})
}

func newTestReplicatorWithFlags(settings []string, flags *flag.FlagSet) (*Replicator, error) {
	configString := "[object-replicator]\nmount_check=false\n"
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, _ := conf.StringConfig(configString)
	replicator, _, err := NewReplicator(conf, flags)
	if err != nil {
		return nil, err
	}
	rep := replicator.(*Replicator)
	rep.concurrencySem = make(chan struct{}, 1)
	rep.loopSleepTime = 0
	rep.updateStat = make(chan statUpdate, 100)
	rep.partSleepTime = 0
	return rep, nil
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
	_PriorityReplicate func(pri PriorityRepJob, timeout time.Duration) bool
	_Stats             func() *ReplicationDeviceStats
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
func (d *mockReplicationDevice) PriorityReplicate(pri PriorityRepJob, timeout time.Duration) bool {
	if d._PriorityReplicate != nil {
		return d._PriorityReplicate(pri, timeout)
	}
	return true
}
func (d *mockReplicationDevice) Stats() *ReplicationDeviceStats {
	if d._Stats != nil {
		return d._Stats()
	}
	return &ReplicationDeviceStats{}
}

type patchableReplicationDevice struct {
	*replicationDevice
	_beginReplication   func(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse)
	_listObjFiles       func(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool)
	_syncFile           func(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error)
	_replicateLocal     func(partition string, nodes []*ring.Device, moreNodes ring.MoreNodes)
	_replicateHandoff   func(partition string, nodes []*ring.Device)
	_cleanTemp          func()
	_listPartitions     func() ([]string, error)
	_replicatePartition func(partition string)
	_Replicate          func()
}

func (d *patchableReplicationDevice) replicatePartition(partition string) {
	if d._replicatePartition != nil {
		d._replicatePartition(partition)
		return
	}
	d.replicationDevice.replicatePartition(partition)
}
func (d *patchableReplicationDevice) listPartitions() ([]string, error) {
	if d._listPartitions != nil {
		return d._listPartitions()
	}
	return d.replicationDevice.listPartitions()
}
func (d *patchableReplicationDevice) beginReplication(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse) {
	if d._beginReplication != nil {
		d._beginReplication(dev, partition, hashes, rChan)
		return
	}
	d.replicationDevice.beginReplication(dev, partition, hashes, rChan)
}
func (d *patchableReplicationDevice) listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
	if d._listObjFiles != nil {
		d._listObjFiles(objChan, cancel, partdir, needSuffix)
		return
	}
	d.replicationDevice.listObjFiles(objChan, cancel, partdir, needSuffix)
}
func (d *patchableReplicationDevice) syncFile(objFile string, dst []*syncFileArg, handoff bool) (syncs int, insync int, err error) {
	if d._syncFile != nil {
		return d._syncFile(objFile, dst, handoff)
	}
	return d.replicationDevice.syncFile(objFile, dst, handoff)
}
func (d *patchableReplicationDevice) replicateLocal(partition string, nodes []*ring.Device, moreNodes ring.MoreNodes) {
	if d._replicateLocal != nil {
		d._replicateLocal(partition, nodes, moreNodes)
		return
	}
	d.replicationDevice.replicateLocal(partition, nodes, moreNodes)
}
func (d *patchableReplicationDevice) replicateHandoff(partition string, nodes []*ring.Device) {
	if d._replicateHandoff != nil {
		d._replicateHandoff(partition, nodes)
		return
	}
	d.replicationDevice.replicateHandoff(partition, nodes)
}
func (d *patchableReplicationDevice) cleanTemp() {
	if d._cleanTemp != nil {
		d._cleanTemp()
		return
	}
	d.replicationDevice.cleanTemp()
}

func newPatchableReplicationDevice(r *Replicator) *patchableReplicationDevice {
	rd := newReplicationDevice(&ring.Device{}, 0, r)
	prd := &patchableReplicationDevice{replicationDevice: rd}
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

func makeReplicatorWebServer(settings ...string) (*TestReplicatorWebServer, error) {
	return makeReplicatorWebServerWithFlags(settings, &flag.FlagSet{})
}

func makeReplicatorWebServerWithFlags(settings []string, flags *flag.FlagSet) (*TestReplicatorWebServer, error) {
	deviceRoot, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	replicator, err := newTestReplicatorWithFlags(settings, flags)
	if err != nil {
		return nil, err
	}
	ts := httptest.NewServer(replicator.GetHandler())
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
	replicator.partSleepTime = 0
	return &TestReplicatorWebServer{Server: ts, host: host, port: port, root: deviceRoot, replicator: replicator}, nil
}

func TestGetFile(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	assert.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
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

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("HI")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("\x80\x02U\x02HIq\x01.")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("\x80\x02}q\x01K\x00U\x02hiq\x02s.")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("\x80\x02}q\x01U\x02hiq\x02K\x00s.")))
	_, _, _, err = getFile(file.Name())
	require.NotNil(t, err)

	dfile, err := os.Create(file.Name() + ".data")
	require.Nil(t, err)
	defer dfile.Close()
	defer os.RemoveAll(dfile.Name())
	require.Nil(t, WriteMetadata(dfile.Fd(), nil))
	_, _, _, err = getFile(dfile.Name())
	require.NotNil(t, err)

	tfile, err := os.Create(file.Name() + ".ts")
	require.Nil(t, err)
	defer tfile.Close()
	defer os.RemoveAll(tfile.Name())
	require.Nil(t, WriteMetadata(tfile.Fd(), nil))
	_, _, _, err = getFile(tfile.Name())
	require.NotNil(t, err)

	dfile, err = os.Create(file.Name() + ".data")
	require.Nil(t, err)
	defer dfile.Close()
	defer os.RemoveAll(dfile.Name())
	require.Nil(t, WriteMetadata(dfile.Fd(), nil))
	_, _, _, err = getFile(dfile.Name())
	require.NotNil(t, err)
}

func TestListObjFiles(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	repl, err := newTestReplicator()
	require.Nil(t, err)
	rd := newReplicationDevice(&ring.Device{}, 0, repl)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	repl, err := newTestReplicator()
	require.Nil(t, err)
	rd := newReplicationDevice(&ring.Device{}, 0, repl)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	t.Parallel()
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.deviceRoot = deviceRoot
	w := httptest.NewRecorder()
	job := &PriorityRepJob{
		Partition:  0,
		FromDevice: &ring.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000, ReplicationIp: "127.0.0.1", ReplicationPort: 5000},
		ToDevices: []*ring.Device{
			{Id: 2, Device: "sdb"},
		},
	}
	jsonned, _ := json.Marshal(job)
	req, _ := http.NewRequest("POST", "/priorityrep", bytes.NewBuffer(jsonned))
	replicator.priorityRepHandler(w, req)
	require.EqualValues(t, 404, w.Code)
}

func TestSyncFile(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(replicator)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(replicator)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	defer file.Close()
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	rd := newPatchableReplicationDevice(replicator)
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

func TestReplicateLocal(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	objPath := filepath.Join(deviceRoot, "objects")
	partition := "1"
	remoteDev := &ring.Device{Device: "sda"}
	filename := filepath.Join(objPath, partition, "aaa", "00000000000000000000000000000000", "1472940619.68559")
	syncFileCalled := false
	rd := newPatchableReplicationDevice(replicator)
	rd._beginReplication = func(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse) {
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
	rd.replicateLocal(partition, nodes, &NoMoreNodes{})
	require.True(t, syncFileCalled)
}

func TestReplicateHandoff(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
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
	rd := newPatchableReplicationDevice(replicator)
	rd._beginReplication = func(dev *ring.Device, partition string, hashes bool, rChan chan beginReplicationResponse) {
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
	rd.replicateHandoff(partition, nodes)
	require.True(t, syncFileCalled)
	require.False(t, fs.Exists(filename))
}

func TestCleanTemp(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no", "devices", deviceRoot)
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(replicator)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(replicator)
	rd._listPartitions = func() ([]string, error) {
		return []string{"1", "2", "3"}, nil
	}
	calledWith := []string{}
	rd._replicatePartition = func(partition string) {
		calledWith = append(calledWith, partition)
	}
	rd.Replicate()
	require.Equal(t, []string{"1", "2", "3"}, calledWith)
}

func TestCancelReplicate(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(replicator)
	rd._listPartitions = func() ([]string, error) {
		return []string{"1", "2", "3"}, nil
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no", "devices", deviceRoot)
	require.Nil(t, err)
	objPath := filepath.Join(deviceRoot, "sda", "objects")
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "1"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "2"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "3"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "X"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "Y"), 0777))
	require.Nil(t, os.MkdirAll(filepath.Join(objPath, "Z"), 0777))
	rd := newPatchableReplicationDevice(replicator)
	rd.dev = &ring.Device{Device: "sda"}
	partitions, err := rd.listPartitions()
	require.Nil(t, err)
	require.Equal(t, 3, len(partitions))

	replicator.partitions = map[string]bool{"2": true}
	partitions, err = rd.listPartitions()
	require.Nil(t, err)
	require.Equal(t, 1, len(partitions))
	require.Equal(t, "2", partitions[0])
}

func TestReplicatePartition(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	testRing := &test.FakeRing{MockGetMoreNodes: &NoMoreNodes{}}
	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return testRing, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no", "devices", deviceRoot)
	require.Nil(t, err)
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(replicator)
	replicateLocalCalled := false
	replicateHandoffCalled := false
	rd._replicateLocal = func(partition string, nodes []*ring.Device, moreNodes ring.MoreNodes) {
		require.Equal(t, "1", partition)
		replicateLocalCalled = true
	}
	rd._replicateHandoff = func(partition string, nodes []*ring.Device) {
		replicateHandoffCalled = true
	}
	rd.replicatePartition("1")
	require.True(t, replicateLocalCalled)
	require.False(t, replicateHandoffCalled)

	testRing.MockGetJobNodesHandoff = true
	replicateLocalCalled = false
	replicateHandoffCalled = false
	rd.replicatePartition("1")
	require.False(t, replicateLocalCalled)
	require.True(t, replicateHandoffCalled)
}

func TestProcessPriorityJobs(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	testRing := &test.FakeRing{MockGetMoreNodes: &NoMoreNodes{}}
	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return testRing, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator()
	require.Nil(t, err)
	rd := newPatchableReplicationDevice(replicator)
	rd.priRep = make(chan PriorityRepJob, 1)
	rd.priRep <- PriorityRepJob{
		Partition:  1,
		FromDevice: &ring.Device{},
		ToDevices:  []*ring.Device{{}},
		Policy:     0,
	}
	replicateLocalCalled := false
	replicateHandoffCalled := false
	rd._replicateLocal = func(partition string, nodes []*ring.Device, moreNodes ring.MoreNodes) {
		require.Equal(t, "1", partition)
		replicateLocalCalled = true
	}
	rd._replicateHandoff = func(partition string, nodes []*ring.Device) {
		require.Equal(t, "1", partition)
		replicateHandoffCalled = true
	}
	rd.processPriorityJobs()
	require.True(t, replicateLocalCalled)

	rd.priRep <- PriorityRepJob{
		Partition:  1,
		FromDevice: &ring.Device{},
		ToDevices:  []*ring.Device{{}},
		Policy:     0,
	}
	testRing.MockGetJobNodesHandoff = true
	replicateLocalCalled = false
	replicateHandoffCalled = false
	rd.processPriorityJobs()
	require.True(t, replicateHandoffCalled)
}

func TestCancelStalledDevices(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	type repDev struct {
		mockReplicationDevice
		index   int
		running bool
	}
	stats := []*ReplicationDeviceStats{
		{LastCheckin: time.Now()},
		{LastCheckin: time.Now()},
		{LastCheckin: time.Now()},
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
		w._Stats = func() *ReplicationDeviceStats {
			return stats[w.index]
		}
		w._Cancel = func() {
			w.running = false
		}
	}
	replicator.runningDevices = runningDevices
	replicator.cancelStalledDevices()
	for _, v := range mockDevices {
		require.True(t, v.running)
	}

	stats[0].LastCheckin = time.Now().Add(-ReplicateDeviceTimeout)
	replicator.runningDevices = runningDevices
	replicator.cancelStalledDevices()
	require.False(t, mockDevices[0].running)
	require.True(t, mockDevices[1].running)
	require.True(t, mockDevices[2].running)
}

func TestVerifyDevices(t *testing.T) {
	oldGetRing := GetRing
	oldLoadPolicies := conf.LoadPolicies
	defer func() {
		GetRing = oldGetRing
		conf.LoadPolicies = oldLoadPolicies
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{MockLocalDevices: []*ring.Device{{Device: "sda"}}}, nil
	}
	conf.LoadPolicies = func() conf.PolicyList {
		return conf.PolicyList(map[int]*conf.Policy{0: {Index: 0, Type: "replication", Name: "gold", Aliases: []string{}, Default: true, Deprecated: false, Config: map[string]string{"policy_type": "replication", "default": "yes", "name": "gold"}}})
	}
	oldNewReplicationDevice := newReplicationDevice
	defer func() {
		newReplicationDevice = oldNewReplicationDevice
	}()
	newrdcalled := false
	newReplicationDevice = func(dev *ring.Device, policy int, r *Replicator) *replicationDevice {
		newrdcalled = true
		require.Equal(t, "sda", dev.Device)
		return oldNewReplicationDevice(dev, policy, r)
	}
	canceled := false
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.runningDevices = map[string]ReplicationDevice{
		"sdb": &mockReplicationDevice{
			_Cancel: func() {
				canceled = true
			},
		},
	}
	replicator.verifyRunningDevices()
	require.True(t, newrdcalled)
	require.Equal(t, 1, len(replicator.runningDevices))
	require.True(t, canceled)
}

func TestReportStats(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.runningDevices = map[string]ReplicationDevice{
		"sda": &mockReplicationDevice{
			_Stats: func() *ReplicationDeviceStats {
				return &ReplicationDeviceStats{
					LastPassDuration: time.Hour,
					RunStarted:       time.Now().Add(-time.Hour),
					Stats: map[string]int64{
						"PartitionsTotal": 1000,
						"PartitionsDone":  500,
					},
				}
			},
			_Key: func() string { return "1.1:10/sda" },
		},
		"sdb": &mockReplicationDevice{
			_Stats: func() *ReplicationDeviceStats {
				return &ReplicationDeviceStats{
					LastPassDuration: time.Hour,
					RunStarted:       time.Now().Add(-time.Hour),
					Stats: map[string]int64{
						"PartitionsTotal": 100,
						"PartitionsDone":  40,
					},
				}
			},
			_Key: func() string { return "1.1:10/sdb" },
		},
	}
	obs, logs := observer.New(zap.InfoLevel)
	replicator.logger = zap.New(obs)
	replicator.reportStats()
	want := []observer.LoggedEntry{{
		Entry: zapcore.Entry{Level: zap.InfoLevel, Message: "Partition Replicated"},
		Context: []zapcore.Field{zap.String("Device", "1.1:10/sda"),
			zap.Int64("doneParts", 500),
			zap.Int64("totalParts", 1000),
			zap.Float64("DoneParts/TotalParts", 50.00),
			zap.Float64("processingTimeSec", 0.00),
			zap.Float64("partsPerSecond", 0.00),
			zap.String("remainingStr", "1h")},
	}, {
		Entry: zapcore.Entry{Level: zap.InfoLevel, Message: "Partition Replicated"},
		Context: []zapcore.Field{zap.String("Device", "1.1:10/sdb"),
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	priorityReplicateCalled := false
	replicator.runningDevices = map[string]ReplicationDevice{
		"sda": &mockReplicationDevice{
			_PriorityReplicate: func(pri PriorityRepJob, timeout time.Duration) bool {
				priorityReplicateCalled = true
				return true
			},
		},
	}
	replicator.priorityReplicate(PriorityRepJob{
		Partition:  1,
		FromDevice: &ring.Device{Device: "sda"},
		ToDevices:  []*ring.Device{},
		Policy:     0,
	}, time.Minute)
	require.True(t, priorityReplicateCalled)
}

func TestGetDeviceProgress(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.runningDevices = map[string]ReplicationDevice{
		"sda": &mockReplicationDevice{
			_Stats: func() *ReplicationDeviceStats {
				return &ReplicationDeviceStats{
					LastPassDuration: time.Hour,
					RunStarted:       time.Now().Add(-time.Hour),
					Stats: map[string]int64{
						"PartitionsTotal": 1000,
						"PartitionsDone":  500,
					},
				}
			},
		},
		"sdb": &mockReplicationDevice{
			_Stats: func() *ReplicationDeviceStats {
				return &ReplicationDeviceStats{
					LastPassDuration: time.Hour,
					RunStarted:       time.Now().Add(-time.Hour),
					Stats: map[string]int64{
						"PartitionsTotal": 100,
						"PartitionsDone":  50,
					},
				}
			},
		},
	}
	progress := replicator.getDeviceProgress()
	sdb, ok := progress["sdb"]
	require.True(t, ok)
	require.Equal(t, int64(100), sdb["PartitionsTotal"])
	require.Equal(t, int64(50), sdb["PartitionsDone"])
}

func TestRunLoopOnceDone(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.onceWaiting = 10
	replicator.onceDone = make(chan struct{}, 1)
	replicator.onceDone <- struct{}{}
	replicator.runLoopCheck(make(chan time.Time))
	require.Equal(t, int64(9), replicator.onceWaiting)
}

func TestRunLoopStatUpdate(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	stats := &ReplicationDeviceStats{
		LastPassDuration: time.Hour,
		RunStarted:       time.Now().Add(-time.Hour),
		Stats: map[string]int64{
			"PartitionsTotal": 1000,
			"PartitionsDone":  500,
		},
	}
	rd := &mockReplicationDevice{
		_Stats: func() *ReplicationDeviceStats {
			return stats
		},
	}
	replicator.runningDevices = map[string]ReplicationDevice{"sda": rd}
	replicator.updateStat = make(chan statUpdate, 1)
	replicator.updateStat <- statUpdate{"sda", "PartitionsTotal", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.Equal(t, int64(1001), rd.Stats().Stats["PartitionsTotal"])
	require.Equal(t, int64(500), rd.Stats().Stats["PartitionsDone"])
	replicator.updateStat <- statUpdate{"sda", "PartitionsDone", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.Equal(t, int64(1001), rd.Stats().Stats["PartitionsTotal"])
	require.Equal(t, int64(501), rd.Stats().Stats["PartitionsDone"])
	replicator.updateStat <- statUpdate{"sda", "checkin", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.True(t, time.Since(stats.LastCheckin) < time.Second)
	replicator.updateStat <- statUpdate{"sda", "startRun", 1}
	replicator.runLoopCheck(make(chan time.Time))
	require.True(t, time.Since(stats.RunStarted) < time.Second)
	require.Equal(t, int64(0), rd.Stats().Stats["PartitionsTotal"])
}

func TestReplicationLocal(t *testing.T) {
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	testRing := &test.FakeRing{}
	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return testRing, nil
	}
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
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

	trs1, err := makeReplicatorWebServer()
	require.Nil(t, err)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot

	trs2, err := makeReplicatorWebServer()
	require.Nil(t, err)
	defer trs2.Close()
	trs2.replicator.deviceRoot = ts2.objServer.driveRoot
	ldev := &ring.Device{ReplicationIp: trs1.host, ReplicationPort: trs1.port, Device: "sda"}
	rdev := &ring.Device{ReplicationIp: trs2.host, ReplicationPort: trs2.port, Device: "sda"}
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	testRing := &test.FakeRing{}
	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return testRing, nil
	}
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
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

	trs1, err := makeReplicatorWebServer()
	require.Nil(t, err)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot
	trs2, err := makeReplicatorWebServer()
	require.Nil(t, err)
	defer trs2.Close()
	trs2.replicator.deviceRoot = ts2.objServer.driveRoot

	ldev := &ring.Device{ReplicationIp: trs1.host, ReplicationPort: trs1.port, Device: "sda"}
	rdev := &ring.Device{ReplicationIp: trs2.host, ReplicationPort: trs2.port, Device: "sda"}
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	testRing := &test.FakeRing{}
	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return testRing, nil
	}
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
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
	trs1, err := makeReplicatorWebServerWithFlags([]string{}, flags)
	require.Nil(t, err)
	require.False(t, trs1.replicator.quorumDelete)
	trs1.Close()

	flags.Parse([]string{"-q"})
	trs1, err = makeReplicatorWebServerWithFlags([]string{}, flags)
	require.Nil(t, err)
	require.True(t, trs1.replicator.quorumDelete)
	defer trs1.Close()
	trs1.replicator.deviceRoot = ts.objServer.driveRoot

	trs2, err := makeReplicatorWebServer()
	require.Nil(t, err)
	defer trs2.Close()
	trs2.replicator.deviceRoot = ts2.objServer.driveRoot

	ldev := &ring.Device{ReplicationIp: trs1.host, ReplicationPort: trs1.port, Device: "sda"}
	rdev := &ring.Device{ReplicationIp: trs2.host, ReplicationPort: trs2.port, Device: "sda"}
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(replicator)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(replicator)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	rd := newPatchableReplicationDevice(replicator)
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

	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	gotACheck := false
	rd := newPatchableReplicationDevice(replicator)
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
	oldGetRing := GetRing
	defer func() {
		GetRing = oldGetRing
	}()

	GetRing = func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
		return &test.FakeRing{}, nil
	}
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	replicator, err := newTestReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)

	filename := filepath.Join(deviceRoot, "objects", "1", "aaa", "00000000000000000000000000000000", "1472940619.68559")
	require.Nil(t, os.MkdirAll(filepath.Dir(filename), 0777))
	file, err := os.Create(filename)
	require.Nil(t, err)
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1472940619.68559",
		"Content-Length": "9",
		"name":           "/a/c/o",
	})
	dataReceived := 0
	gotACheck := false
	rd := newPatchableReplicationDevice(replicator)
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
