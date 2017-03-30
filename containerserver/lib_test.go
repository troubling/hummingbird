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
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/test"
)

// a place for utility functions and interface satisifiers that are used across tests

func mergeItemsByName(c Container, names []string) error {
	items := []*ObjectRecord{}
	for _, name := range names {
		items = append(items, &ObjectRecord{Name: name, CreatedAt: "10000000.00001"})
	}
	return c.(ReplicableContainer).MergeItems(items, "")
}

func createTestDatabase(timestamp string) (*sqliteContainer, string, func(), error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, "", nil, err
	}
	dbFile := filepath.Join(dir, "device", "containers", "1", "000", "db", "db.db")
	if err := os.MkdirAll(filepath.Dir(dbFile), 0777); err != nil {
		return nil, "", nil, err
	}
	err = sqliteCreateContainer(dbFile, "a", "c", timestamp, nil, 0)
	if err != nil {
		os.RemoveAll(dir)
		return nil, "", nil, err
	}
	db, err := sqliteOpenContainer(dbFile)
	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db.(*sqliteContainer), dbFile, cleanup, nil
}

func makeTestServer() (http.Handler, func(), error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, nil, err
	}
	if err := os.Mkdir(filepath.Join(dir, "device"), 0777); err != nil {
		return nil, nil, err
	}
	server := &ContainerServer{
		driveRoot:        dir,
		hashPathPrefix:   "changeme",
		hashPathSuffix:   "changeme",
		logLevel:         "INFO",
		logger:           test.FakeLowLevelLogger{},
		checkMounts:      false,
		updateClient:     http.DefaultClient,
		containerEngine:  newLRUEngine(dir, "changeme", "changeme", 32),
		diskInUse:        common.NewKeyedLimit(2, 2),
		autoCreatePrefix: ".",
	}
	cleanup := func() {
		os.RemoveAll(dir)
	}
	return server.GetHandler(*new(conf.Config)), cleanup, nil
}

func makeTestServer2() (*ContainerServer, http.Handler, func(), error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, nil, nil, err
	}
	if err := os.Mkdir(filepath.Join(dir, "device"), 0777); err != nil {
		return nil, nil, nil, err
	}
	server := &ContainerServer{
		driveRoot:       dir,
		hashPathPrefix:  "changeme",
		hashPathSuffix:  "changeme",
		logLevel:        "INFO",
		logger:          test.FakeLowLevelLogger{},
		checkMounts:     false,
		updateClient:    http.DefaultClient,
		containerEngine: newLRUEngine(dir, "changeme", "changeme", 32),
		diskInUse:       common.NewKeyedLimit(2, 2),
	}
	cleanup := func() {
		os.RemoveAll(dir)
	}
	return server, server.GetHandler(*new(conf.Config)), cleanup, nil
}

type fakeDatabase struct{}

func (f fakeDatabase) GetInfo() (*ContainerInfo, error) {
	return nil, errors.New("")
}
func (f fakeDatabase) IsDeleted() (bool, error) {
	return false, errors.New("")
}
func (f fakeDatabase) Delete(timestamp string) error {
	return errors.New("")
}
func (f fakeDatabase) ListObjects(limit int, marker string, endMarker string, prefix string, delimiter string, path *string, reverse bool, storagePolicyIndex int) ([]interface{}, error) {
	return nil, errors.New("")
}
func (f fakeDatabase) GetMetadata() (map[string]string, error) {
	return nil, errors.New("")
}
func (f fakeDatabase) UpdateMetadata(updates map[string][]string) error {
	return errors.New("")
}
func (f fakeDatabase) MergeItems(records []*ObjectRecord, remoteID string) error {
	return errors.New("")
}
func (f fakeDatabase) ItemsSince(start int64, count int) ([]*ObjectRecord, error) {
	return nil, errors.New("")
}
func (f fakeDatabase) MergeSyncTable(records []*SyncRecord) error {
	return errors.New("")
}
func (f fakeDatabase) SyncTable() ([]*SyncRecord, error) {
	return nil, errors.New("")
}
func (f fakeDatabase) SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*ContainerInfo, error) {
	return nil, errors.New("")
}
func (f fakeDatabase) RingHash() string {
	return ""
}
func (f fakeDatabase) ID() string {
	return ""
}
func (f fakeDatabase) NewID() error {
	return errors.New("")
}
func (f fakeDatabase) OpenDatabaseFile() (*os.File, func(), error) {
	return nil, nil, errors.New("")
}
func (f fakeDatabase) Close() error {
	return errors.New("")
}
func (f fakeDatabase) CleanupTombstones(reclaimAge int64) error {
	return errors.New("")
}
func (f fakeDatabase) CheckSyncLink() error {
	return errors.New("")
}
func (f fakeDatabase) PutObject(name string, timestamp string, size int64, contentType string, etag string, storagePolicyIndex int) error {
	return errors.New("")
}
func (f fakeDatabase) DeleteObject(name string, timestamp string, storagePolicyIndex int) error {
	return errors.New("")
}

type fakeContainerEngine struct{}

func (fakeContainerEngine) Get(vars map[string]string) (c Container, err error) {
	return nil, errors.New("")
}
func (fakeContainerEngine) GetByHash(device, hash, partition string) (c ReplicableContainer, err error) {
	return nil, errors.New("")
}
func (fakeContainerEngine) Create(vars map[string]string, putTimestamp string, metadata map[string][]string, policyIndex, defaultPolicyIndex int) (bool, Container, error) {
	return false, nil, errors.New("")
}
func (fakeContainerEngine) PutObject(vars map[string]string, timestamp string, size int64, contentType string, etag string, storagePolicyIndex int) error {
	return errors.New("")
}
func (fakeContainerEngine) DeleteObject(vars map[string]string, timestamp string, storagePolicyIndex int) error {
	return errors.New("")
}
func (fakeContainerEngine) Return(c Container) {
}
func (fakeContainerEngine) Invalidate(c Container) {
}
func (fakeContainerEngine) Close() {
}
