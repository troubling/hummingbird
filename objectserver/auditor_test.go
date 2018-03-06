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
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type FakeECAuditFuncs struct {
	paths  []string
	shards []string
}

// AuditNurseryObject of indexdb shard, does nothing
func (f *FakeECAuditFuncs) AuditNurseryObject(path string, metabytes []byte, skipMd5 bool) (int64, error) {
	return 0, nil
}

// AuditShardHash of indexdb shard
func (f *FakeECAuditFuncs) AuditShard(path string, hash string, skipMd5 bool) (int64, error) {
	fmt.Printf("HELLO? f: %p\n", f)
	f.paths = append(f.paths, path)
	f.shards = append(f.shards, hash)
	fmt.Printf("paths: %p %+v\n", f.paths, f.paths)
	return 0, nil
}

func TestAuditHashPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	f, _ = os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12346.ts"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"name": "somename", "X-Timestamp": ""})
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.Nil(t, err)
	assert.Equal(t, bytesProcessed, int64(12))
}

func TestAuditNonStringKey(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	metadata := map[interface{}]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": "", "name": "", 3: "hi"}
	common.SwiftObjectRawWriteMetadata(f.Fd(), pickle.PickleDumps(metadata))
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditNonStringValue(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	metadata := map[string]interface{}{"Content-Length": 12, "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": "", "name": ""}
	common.SwiftObjectRawWriteMetadata(f.Fd(), pickle.PickleDumps(metadata))
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashDataMissingMetadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashTSMissingMetadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.ts"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"name": "somename"})
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashIncorrectContentLength(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "0", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashInvalidContentLength(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "X", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashBadHash(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "f3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(12))
}

func TestAuditHashBadFilename(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.xxx"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashNonfileInDir(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"), 0777)
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashNoMetadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

// type auditLogSaver struct {
// 	logged []string
// }

// func (s *auditLogSaver) Err(line string) error {
// 	s.logged = append(s.logged, line)
// 	return nil
// }

// func (s *auditLogSaver) Info(line string) error {
// 	s.logged = append(s.logged, line)
// 	return nil
// }

// func (s *auditLogSaver) Debug(line string) error {
// 	s.logged = append(s.logged, line)
// 	return nil
// }
var obs zapcore.Core
var logs *observer.ObservedLogs

func makeAuditor(t *testing.T, confLoader *srv.TestConfigLoader, settings ...string) *Auditor {
	configString := "[object-auditor]\n"
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, _ := conf.StringConfig(configString)
	auditorDaemon, err := NewAuditorDaemon(conf, &flag.FlagSet{}, confLoader)
	require.Nil(t, err)
	obs, logs = observer.New(zap.InfoLevel)
	auditorDaemon.logger = zap.New(obs)
	return &Auditor{AuditorDaemon: auditorDaemon, filesPerSecond: 1, ecfuncs: RealECAuditFuncs{}}
}

func TestFailsWithoutSection(t *testing.T) {
	conf, err := conf.StringConfig("")
	require.Nil(t, err)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditorDaemon, err := NewAuditorDaemon(conf, &flag.FlagSet{}, confLoader)
	require.NotNil(t, err)
	assert.Nil(t, auditorDaemon)
	assert.True(t, strings.HasPrefix(err.Error(), "Unable to find object-auditor"))
}

func TestAuditSuffixNotDir(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditSuffix(file.Name())
	assert.Equal(t, logs.TakeAll()[0].Message, "Error reading suffix dir")
	assert.True(t, auditor.errors > errors)
}

func TestAuditSuffixPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	totalPasses := auditor.totalPasses
	auditor.auditSuffix(filepath.Join(dir, "abc"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditSuffixQuarantine(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.derp"))
	defer f.Close()
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	totalQuarantines := auditor.totalQuarantines
	auditor.auditSuffix(filepath.Join(dir, "objects", "1", "abc"))
	assert.Equal(t, totalQuarantines+1, auditor.totalQuarantines)
	quarfiles, err := ioutil.ReadDir(filepath.Join(dir, "quarantined", "objects"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(quarfiles))
}

func TestAuditSuffixSkipsBad(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "notavalidhash"), 0777)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	auditor.auditSuffix(filepath.Join(dir, "objects", "1", "abc"))
	assert.Equal(t, logs.TakeAll()[0].Message, "Skipping invalid file in suffix")
}

func TestAuditPartitionNotDir(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditPartition(file.Name())
	assert.Equal(t, logs.TakeAll()[0].Message, "Error reading partition dir ")
	assert.True(t, auditor.errors > errors)
}

func TestAuditPartitionPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	totalPasses := auditor.totalPasses
	auditor.auditPartition(filepath.Join(dir, "1"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditPartitionSkipsBadData(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(dir, "1", "xyz"), 0777)
	f, _ := os.Create(filepath.Join(dir, "1", ".lock"))
	f.Close()
	f, _ = os.Create(filepath.Join(dir, "1", "hashes.pkl"))
	f.Close()
	f, _ = os.Create(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	totalPasses := auditor.totalPasses
	auditor.auditPartition(filepath.Join(dir, "1"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceNotDir(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader, "mount_check", "false")
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditDevice(file.Name())
	// Two replication policies, 1 HEC policy. All fail.
	assert.Equal(t, errors+3, auditor.errors)
	var logmsgs []string
	for _, log := range logs.TakeAll() {
		logmsgs = append(logmsgs, log.Message)
	}
	sort.Strings(logmsgs)
	assert.Equal(t, []string{"Couldn't open indexdb", "Error reading objects dir", "Error reading objects dir"}, logmsgs)
}

func TestAuditDevicePasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader, "mount_check", "false")
	totalPasses := auditor.totalPasses
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceSkipsBadData(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "X"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader, "mount_check", "false")
	totalPasses := auditor.totalPasses
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceUnmounted(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1"), 0777)
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader, "mount_check", "true")
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, logs.TakeAll()[0].Message, "Skipping unmounted device")
}

func TestFinalLog(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	auditor.passStart = time.Now().Add(-60 * time.Second)
	auditor.totalQuarantines = 5
	auditor.totalErrors = 3
	auditor.totalPasses = 120
	auditor.totalBytes = 120000
	auditor.auditorType = "ALL"
	auditor.mode = "forever"
	auditor.finalLog()
	want := []observer.LoggedEntry{{
		Entry: zapcore.Entry{Level: zap.InfoLevel, Message: "Object Audit"},
		Context: []zapcore.Field{zap.String("Auditor type", "ALL"),
			zap.String("Mode", "forever"),
			zap.Float64("completed", 60.00),
			zap.Int64("Total quarantined", 5),
			zap.Int64("Total errors", 3),
			zap.Float64("Total files/sec", 2.00),
			zap.Float64("Total bytes/sec", 2000.00),
			zap.Float64("Auditing time", 0.00),
			zap.Float64("Auditing rate", 0.00)},
	}}
	obslog := logs.AllUntimed()[0]
	require.Equal(t, want[0].Message, obslog.Message)
	require.Equal(t, want[0].Context[0], obslog.Context[0])
	require.Equal(t, want[0].Context[1], obslog.Context[1])
	//TODO: Figure out how to do float comparison.
	//require.Equal(t, want[0].Context[2], obslog.Context[2])
	require.Equal(t, want[0].Context[3], obslog.Context[3])
	require.Equal(t, want[0].Context[4], obslog.Context[4])
	//require.Equal(t, want[0].Context[5], obslog.Context[5])
	//require.Equal(t, want[0].Context[6], obslog.Context[6])
	require.Equal(t, want[0].Context[7], obslog.Context[7])
	require.Equal(t, want[0].Context[8], obslog.Context[8])
}

func TestAuditRun(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	common.SwiftObjectWriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader, "mount_check", "false")
	auditor.driveRoot = dir
	totalPasses := auditor.totalPasses
	auditor.run(OneTimeChan())
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestStatReport(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader, "mount_check", "false")
	auditor.passStart = time.Now().Add(-120 * time.Second)
	auditor.lastLog = time.Now().Add(-120 * time.Second)
	auditor.passes = 120
	auditor.bytesProcessed = 120000
	auditor.quarantines = 17
	auditor.errors = 41
	auditor.statsReport()
	want := []observer.LoggedEntry{{
		Entry: zapcore.Entry{Level: zap.InfoLevel, Message: "statsReport"},
		Context: []zapcore.Field{zap.String("Object audit", ""),
			zap.String("Since", "Tue May  2 05:48:19 2017"),
			zap.Int64("Locally passed", 120),
			zap.Int64("Locally quarantied", 17),
			zap.Int64("Locally errored", 41),
			zap.Float64("files/sec", 1),
			zap.Float64("bytes/sec", 2),
			zap.Float64("Total time", 3),
			zap.Float64("Auditing Time", 0.00),
			zap.Float64("Auditing Rate", 0.00)},
	}}
	obslog := logs.AllUntimed()[0]
	require.Equal(t, want[0].Message, obslog.Message)
	require.Equal(t, want[0].Context[0], obslog.Context[0])
	//unable to do time string comparison.
	//require.Equal(t, want[0].Context[1], obslog.Context[1])
	require.Equal(t, want[0].Context[2], obslog.Context[2])
	require.Equal(t, want[0].Context[3], obslog.Context[3])
	require.Equal(t, want[0].Context[4], obslog.Context[4])
	//require.Equal(t, want[0].Context[5], obslog.Context[5])
	//require.Equal(t, want[0].Context[6], obslog.Context[6])
	//require.Equal(t, want[0].Context[7], obslog.Context[7])
	require.Equal(t, want[0].Context[8], obslog.Context[8])
	require.Equal(t, want[0].Context[9], obslog.Context[9])
}

func TestAuditDB(t *testing.T) {
	testRing := &test.FakeRing{}
	confLoader := srv.NewTestConfigLoader(testRing)
	auditor := makeAuditor(t, confLoader)
	fakes := &FakeECAuditFuncs{}
	auditor.ecfuncs = fakes
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	policydir := filepath.Join(dir, "objects")
	dbdir := filepath.Join(policydir, "hec.db")
	hecdir := filepath.Join(policydir, "hec")
	db, err := NewIndexDB(dbdir, hecdir, dir, 2, 1, 32, zap.L())
	assert.Nil(t, err)
	body := "some shard content nonsense"
	shardHash := "d3ac5112fe464b81184352ccba743001"
	hash := "00000000000000000000000000000000"
	timestamp := time.Now().UnixNano()
	f, err := db.TempFile(hash, 0, timestamp, int64(len(body)), false)
	assert.Nil(t, err)
	f.Write([]byte(body))
	err = db.Commit(f, hash, 0, timestamp, false, "", nil, false, shardHash)
	assert.Nil(t, err)
	hash1 := "00000000000000000000000000000001"
	f, err = db.TempFile(hash1, 0, timestamp, int64(len(body)), false)
	assert.Nil(t, err)
	f.Write([]byte(body))
	err = db.Commit(f, hash1, 0, timestamp, false, "", nil, false, shardHash)
	assert.Nil(t, err)
	hash2 := "00000000000000000000000000000002"
	f, err = db.TempFile(hash2, 0, timestamp, int64(len(body)), false)
	assert.Nil(t, err)
	f.Write([]byte(body))
	err = db.Commit(f, hash2, 0, timestamp, false, "", nil, false, shardHash)
	assert.Nil(t, err)

	shardPath, err := db.WholeObjectPath(hash, 0, timestamp, false)

	auditor.auditDB(db.dbpath, testRing)

	assert.Equal(t, 3, len(fakes.paths))
	assert.Equal(t, shardPath, fakes.paths[0])
	assert.Equal(t, shardHash, fakes.shards[0])
}

func TestAuditShardPasses(t *testing.T) {
	auditFuncs := RealECAuditFuncs{}
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	fName := filepath.Join(dir, "12345")
	f, _ := os.Create(fName)
	hash := "d3ac5112fe464b81184352ccba743001"
	f.Write([]byte("testcontents"))
	f.Close()
	bytes, err := auditFuncs.AuditShard(fName, hash, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(12), bytes)
}

func TestAuditShardFails(t *testing.T) {
	auditFuncs := RealECAuditFuncs{}
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	fName := filepath.Join(dir, "12345")
	f, _ := os.Create(filepath.Join(dir, "12345"))
	hash := "d3ac5112fe464b81184352ccba743001"
	f.Write([]byte("asdftestcontents"))
	f.Close()
	bytes, err := auditFuncs.AuditShard(fName, hash, false)
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), bytes)
}

func TestQuarantineShard(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	policydir := filepath.Join(dir, "objects")
	dbdir := filepath.Join(policydir, "hec.db")
	hecdir := filepath.Join(policydir, "hec")
	db, err := NewIndexDB(dbdir, hecdir, dir, 2, 1, 32, zap.L())
	timestamp := time.Now().UnixNano()
	hash := "00000000000000000000000000000000"
	body := "nonsense"
	f, err := db.TempFile(hash, 0, timestamp, int64(len(body)), false)
	assert.Nil(t, err)
	f.Write([]byte(body))
	err = db.Commit(f, hash, 0, timestamp, false, "", nil, false, "unused")
	assert.Nil(t, err)

	err = quarantineShard(db, hash, 0, timestamp, false)
	assert.Nil(t, err)

	shardPath, err := db.WholeObjectPath(hash, 0, timestamp, false)
	assert.Nil(t, err)
	shard := filepath.Base(shardPath)
	quarfiles, err := ioutil.ReadDir(filepath.Join(dir, "quarantined", "objects"))
	assert.Nil(t, err)
	if len(quarfiles) != 1 {
		t.Fatal(1, len(quarfiles))
	}
	assert.Equal(t, shard, quarfiles[0].Name())
	dbitem, err := db.Lookup(hash, 0, false)
	assert.Nil(t, err)
	assert.Nil(t, dbitem)
}
