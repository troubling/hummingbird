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

package test

import (
	"bytes"
	"net/http"

	"github.com/troubling/hummingbird/common/ring"
)

// a place for utility functions and interface satisifiers that are used across tests

type CaptureResponse struct {
	Status int
	header http.Header
	Body   *bytes.Buffer
}

func (w *CaptureResponse) WriteHeader(status int) {
	w.Status = status
}

func (w *CaptureResponse) Header() http.Header {
	return w.header
}

func (w *CaptureResponse) Write(b []byte) (int, error) {
	return w.Body.Write(b)
}

func MakeCaptureResponse() *CaptureResponse {
	return &CaptureResponse{
		Status: 0,
		header: make(http.Header),
		Body:   new(bytes.Buffer),
	}
}

type FakeLowLevelLogger struct{}

func (FakeLowLevelLogger) Err(s string) error {
	return nil
}

func (FakeLowLevelLogger) Info(s string) error {
	return nil
}

func (FakeLowLevelLogger) Debug(s string) error {
	return nil
}

type FakeRing struct{}

func (r *FakeRing) GetNodes(partition uint64) (response []*ring.Device) {
	return nil
}
func (r *FakeRing) GetNodesInOrder(partition uint64) (response []*ring.Device) {
	return nil
}
func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*ring.Device, handoff bool) {
	return []*ring.Device{
		&ring.Device{Device: "sda", ReplicationIp: "127.0.0.1", ReplicationPort: 20000},
		&ring.Device{Device: "sdb", ReplicationIp: "127.0.0.2", ReplicationPort: 2000},
	}, false
}
func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 1
}
func (r *FakeRing) LocalDevices(localPort int) (devs []*ring.Device, err error) {
	return nil, nil
}
func (r *FakeRing) AllDevices() (devs []ring.Device) {
	return nil
}
func (r *FakeRing) GetMoreNodes(partition uint64) ring.MoreNodes {
	return nil
}
func (r *FakeRing) PartitionCount() uint64 {
	return 1
}
func (r *FakeRing) ReplicaCount() uint64 {
	return 3
}

type FakeLogger struct{}

func (s FakeLogger) LogError(format string, args ...interface{}) {}
func (s FakeLogger) LogInfo(format string, args ...interface{})  {}
func (s FakeLogger) LogDebug(format string, args ...interface{}) {}
func (s FakeLogger) LogPanics(format string)                     {}
