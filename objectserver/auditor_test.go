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
	"strings"
	"testing"
	"time"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditHashPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	f, _ = os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12346.ts"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"name": "somename", "X-Timestamp": ""})
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
	RawWriteMetadata(f.Fd(), pickle.PickleDumps(metadata))
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
	RawWriteMetadata(f.Fd(), pickle.PickleDumps(metadata))
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": ""})
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
	WriteMetadata(f.Fd(), map[string]string{"name": "somename"})
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "0", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "X", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "f3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
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

func makeAuditor(settings ...string) *Auditor {
	configString := "[object-auditor]\n"
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	configString += test.GetLogPathConfLine("objectauditor.log")
	conf, _ := conf.StringConfig(configString)
	auditorDaemon, _, _ := NewAuditor(conf, &flag.FlagSet{})
	obs, logs = observer.New(zap.InfoLevel)
	auditorDaemon.(*AuditorDaemon).logger = zap.New(obs)
	return &Auditor{AuditorDaemon: auditorDaemon.(*AuditorDaemon), filesPerSecond: 1}
}

func TestFailsWithoutSection(t *testing.T) {
	conf, err := conf.StringConfig("")
	require.Nil(t, err)
	auditorDaemon, logger, err := NewAuditor(conf, &flag.FlagSet{})
	require.NotNil(t, err)
	require.Nil(t, logger)
	assert.Nil(t, auditorDaemon)
	assert.True(t, strings.HasPrefix(err.Error(), "Unable to find object-auditor"))
}

func TestAuditSuffixNotDir(t *testing.T) {
	auditor := makeAuditor()
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor()
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
	auditor := makeAuditor()
	totalQuarantines := auditor.totalQuarantines
	auditor.auditSuffix(filepath.Join(dir, "objects", "1", "abc"))
	assert.Equal(t, totalQuarantines+1, auditor.totalQuarantines)
	quarfiles, err := ioutil.ReadDir(filepath.Join(dir, "quarantined"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(quarfiles))
}

func TestAuditSuffixSkipsBad(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "notavalidhash"), 0777)
	auditor := makeAuditor()
	auditor.auditSuffix(filepath.Join(dir, "objects", "1", "abc"))
	assert.Equal(t, logs.TakeAll()[0].Message, "Skipping invalid file in suffix")
}

func TestAuditPartitionNotDir(t *testing.T) {
	auditor := makeAuditor()
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor()
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor()
	totalPasses := auditor.totalPasses
	auditor.auditPartition(filepath.Join(dir, "1"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceNotDir(t *testing.T) {
	auditor := makeAuditor("mount_check", "false")
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditDevice(file.Name())
	assert.Equal(t, logs.TakeAll()[0].Message, "Error reading objects dir")
	assert.True(t, auditor.errors > errors)
}

func TestAuditDevicePasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor("mount_check", "false")
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor("mount_check", "false")
	totalPasses := auditor.totalPasses
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceUnmounted(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1"), 0777)
	auditor := makeAuditor("mount_check", "true")
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, logs.TakeAll()[0].Message, "Skipping unmounted device")
}

func TestFinalLog(t *testing.T) {
	auditor := makeAuditor()
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
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor("mount_check", "false")
	auditor.driveRoot = dir
	totalPasses := auditor.totalPasses
	auditor.run(OneTimeChan())
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestStatReport(t *testing.T) {
	auditor := makeAuditor("mount_check", "false")
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
