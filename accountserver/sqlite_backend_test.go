//  Copyright (c) 2016-2017 Rackspace
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

package accountserver

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
)

func createTestDatabase(timestamp string) (*sqliteAccount, string, func(), error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, "", nil, err
	}
	dbFile := filepath.Join(dir, "device", "accounts", "1", "000", "db", "db.db")
	if err := os.MkdirAll(filepath.Dir(dbFile), 0777); err != nil {
		return nil, "", nil, err
	}
	err = sqliteCreateAccount(dbFile, "a", timestamp, nil)
	if err != nil {
		os.RemoveAll(dir)
		return nil, "", nil, err
	}
	db, err := sqliteOpenAccount(dbFile)
	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db.(*sqliteAccount), dbFile, cleanup, nil
}

func mergeItemsByName(a Account, names []string) error {
	items := []*ContainerRecord{}
	for _, name := range names {
		items = append(items, &ContainerRecord{Name: name, PutTimestamp: common.GetTimestamp()})
	}
	return a.(ReplicableAccount).MergeItems(items, "")
}

func BenchmarkMergeItems(b *testing.B) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	if err != nil {
		panic("Error creating database")
	}
	recs := make([]*ContainerRecord, 10000)
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10000; i++ {
			recs[i] = &ContainerRecord{Name: common.UUID(), PutTimestamp: "20000000.00001"}
		}
		db.MergeItems(recs, "")
		db.MergeItems(recs, "")
	}
	cleanup()
}

func TestContainerListings(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c"}))
	records, err := db.ListContainers(10000, "", "", "", "", false)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "a", records[0].(*ContainerListingRecord).Name)
	require.Equal(t, "b", records[1].(*ContainerListingRecord).Name)
	require.Equal(t, "c", records[2].(*ContainerListingRecord).Name)

	records, err = db.ListContainers(10000, "", "", "", "", true)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "c", records[0].(*ContainerListingRecord).Name)
	require.Equal(t, "b", records[1].(*ContainerListingRecord).Name)
	require.Equal(t, "a", records[2].(*ContainerListingRecord).Name)

	records, err = db.ListContainers(10000, "a", "c", "", "", false)
	require.Nil(t, err)
	require.Equal(t, 1, len(records))
	require.Equal(t, "b", records[0].(*ContainerListingRecord).Name)

	records, err = db.ListContainers(10000, "", "", "b", "", false)
	require.Nil(t, err)
	require.Equal(t, 1, len(records))
	require.Equal(t, "b", records[0].(*ContainerListingRecord).Name)
}

func TestContainerDelimiter(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "a-b", "a-c"}))
	records, err := db.ListContainers(10000, "", "", "", "-", false)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "a", records[0].(*ContainerListingRecord).Name)
	require.Equal(t, "a-", records[1].(*SubdirListingRecord).Name)

	records, err = db.ListContainers(10000, "", "", "", "-", true)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "a-", records[0].(*SubdirListingRecord).Name)
	require.Equal(t, "a", records[1].(*ContainerListingRecord).Name)
}

func TestNewID(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()
	info, err := db.GetInfo()
	require.Nil(t, err)
	oldID := info.ID
	require.Nil(t, db.NewID())
	info, err = db.GetInfo()
	require.NotEqual(t, oldID, info.ID)
}

func TestItemsSince(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c", "d", "e", "f", "g", "h"}))

	objs, err := db.ItemsSince(-1, 1)
	require.Nil(t, err)
	require.Equal(t, 1, len(objs))

	objs, err = db.ItemsSince(objs[0].Rowid, 1000)
	require.Nil(t, err)
	require.Equal(t, 7, len(objs))
}

func TestMergeSyncTable(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()
	info, err := db.GetInfo()
	require.Nil(t, err)
	someSyncs := []*SyncRecord{
		{SyncPoint: 5, RemoteID: "some guy I guess"},
		{SyncPoint: 1, RemoteID: "some other guy"},
	}
	require.Nil(t, db.MergeSyncTable(someSyncs))
	points, err := db.SyncTable()
	expectedSyncs := map[string]int64{
		"some guy I guess": 5,
		"some other guy":   1,
		info.ID:            info.MaxRow,
	}
	actualSyncs := map[string]int64{}
	for i := range points {
		actualSyncs[points[i].RemoteID] = points[i].SyncPoint
	}
	require.Equal(t, expectedSyncs, actualSyncs)
	newSyncs := []*SyncRecord{
		{SyncPoint: 1000000, RemoteID: "some other guy"},
		{SyncPoint: 10, RemoteID: "new guy"},
	}
	require.Nil(t, db.MergeSyncTable(newSyncs))
	expectedSyncs = map[string]int64{
		"some guy I guess": 5,
		"some other guy":   1000000,
		"new guy":          10,
		info.ID:            info.MaxRow,
	}
	points, err = db.SyncTable()
	actualSyncs = map[string]int64{}
	for i := range points {
		actualSyncs[points[i].RemoteID] = points[i].SyncPoint
	}
	require.Equal(t, expectedSyncs, actualSyncs)
}

func TestDeleteRemovesMetadata(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	db.UpdateMetadata(map[string][]string{"X-Container-Meta-Key": {"Value", "200000000.00001"}})
	require.Nil(t, db.Delete("200000001.00000"))
	deleted, err := db.IsDeleted()
	require.Nil(t, err)
	require.True(t, deleted)
	metadata, err := db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, 0, len(metadata))
}

func TestIndexAfter(t *testing.T) {
	require.Equal(t, 5, indexAfter(",    ,", ",", 3))
	require.Equal(t, 4, indexAfter("    ,,", ",", 3))
	require.Equal(t, -1, indexAfter(",,,   ", ",", 3))
}

func TestCreateExisting(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	c, err := sqliteCreateExistingAccount(db, "200000001.00000", map[string][]string{})
	require.Nil(t, err)
	require.False(t, c)
	info, err := db.GetInfo()
	require.Nil(t, err)

	newMetadata := map[string][]string{
		"X-Account-Meta-Whatever": {"something", "200000002.00000"},
	}
	c, err = sqliteCreateExistingAccount(db, "200000002.00000", newMetadata)
	require.Nil(t, err)
	require.False(t, c)
	info, err = db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, info.Metadata, newMetadata)

	newMetadata = map[string][]string{
		"X-Account-Meta-Another": {"whatevs", "200000003.00000"},
	}
	c, err = sqliteCreateExistingAccount(db, "200000003.00000", newMetadata)
	require.Nil(t, err)
	require.False(t, c)
	info, err = db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, info.Metadata, map[string][]string{
		"X-Account-Meta-Another":  {"whatevs", "200000003.00000"},
		"X-Account-Meta-Whatever": {"something", "200000002.00000"},
	})
}

func TestInt64MaybeStringified(t *testing.T) {
	i, ok := int64MaybeStringified(nil)
	if ok {
		t.Fatal(i, ok)
	}
	i, ok = int64MaybeStringified(int64(0))
	if !ok || i != 0 {
		t.Fatal(i, ok)
	}
	i, ok = int64MaybeStringified(int64(1234))
	if !ok || i != 1234 {
		t.Fatal(i, ok)
	}
	i, ok = int64MaybeStringified("")
	if ok {
		t.Fatal(i, ok)
	}
	i, ok = int64MaybeStringified("0")
	if !ok || i != 0 {
		t.Fatal(i, ok)
	}
	i, ok = int64MaybeStringified("1234")
	if !ok || i != 1234 {
		t.Fatal(i, ok)
	}
	i, ok = int64MaybeStringified("garbage")
	if ok {
		t.Fatal(i, ok)
	}
}

func TestPolicyStats(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err = db.MergeItems([]*ContainerRecord{{Name: "a", PutTimestamp: common.GetTimestamp(), ObjectCount: 123, BytesUsed: 456, StoragePolicyIndex: 1}}, ""); err != nil {
		t.Fatal(err)
	}
	polstats, err := db.PolicyStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(polstats) != 1 {
		t.Fatal(len(polstats))
	}
	polstat := polstats[0]
	if polstat.StoragePolicyIndex != 1 {
		t.Fatal(polstat.StoragePolicyIndex)
	}
	if polstat.ContainerCount != 1 {
		t.Fatal(polstat.ContainerCount)
	}
	if polstat.ObjectCount != 123 {
		t.Fatal(polstat.ObjectCount)
	}
	if polstat.BytesUsed != 456 {
		t.Fatal(polstat.BytesUsed)
	}
	if err = db.MergeItems([]*ContainerRecord{{Name: "b", PutTimestamp: common.GetTimestamp(), ObjectCount: 789, BytesUsed: 1234, StoragePolicyIndex: 1}}, ""); err != nil {
		t.Fatal(err)
	}
	db.invalidateCache()
	polstats, err = db.PolicyStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(polstats) != 1 {
		t.Fatal(len(polstats))
	}
	polstat = polstats[0]
	if polstat.StoragePolicyIndex != 1 {
		t.Fatal(polstat.StoragePolicyIndex)
	}
	if polstat.ContainerCount != 2 {
		t.Fatal(polstat.ContainerCount)
	}
	if polstat.ObjectCount != 912 {
		t.Fatal(polstat.ObjectCount)
	}
	if polstat.BytesUsed != 1690 {
		t.Fatal(polstat.BytesUsed)
	}
	if err = db.MergeItems([]*ContainerRecord{{Name: "c", PutTimestamp: common.GetTimestamp(), ObjectCount: 5678, BytesUsed: 9012, StoragePolicyIndex: 2}}, ""); err != nil {
		t.Fatal(err)
	}
	db.invalidateCache()
	polstats, err = db.PolicyStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(polstats) != 2 {
		t.Fatal(len(polstats))
	}
	polstat1 := polstats[0]
	polstat2 := polstats[1]
	if polstat1.StoragePolicyIndex != 1 {
		polstat1, polstat2 = polstat2, polstat1
	}
	if polstat1.StoragePolicyIndex != 1 {
		t.Fatal(polstat1.StoragePolicyIndex)
	}
	if polstat1.ContainerCount != 2 {
		t.Fatal(polstat1.ContainerCount)
	}
	if polstat1.ObjectCount != 912 {
		t.Fatal(polstat1.ObjectCount)
	}
	if polstat1.BytesUsed != 1690 {
		t.Fatal(polstat1.BytesUsed)
	}
	if polstat2.StoragePolicyIndex != 2 {
		t.Fatal(polstat2.StoragePolicyIndex)
	}
	if polstat2.ContainerCount != 1 {
		t.Fatal(polstat2.ContainerCount)
	}
	if polstat2.ObjectCount != 5678 {
		t.Fatal(polstat2.ObjectCount)
	}
	if polstat2.BytesUsed != 9012 {
		t.Fatal(polstat2.BytesUsed)
	}
}

func TestMalformed(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	dbFile := filepath.Join(dir, "device", "accounts", "1", "000", "db", "db.db")
	if err := os.MkdirAll(filepath.Dir(dbFile), 0777); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	err = sqliteCreateAccount(dbFile, "a", "200000000.00000", nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(dbFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Seek(100, 0)
	_, err = f.Write([]byte("garbage"))
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqliteOpenAccount(dbFile)
	sa := db.(*sqliteAccount)
	err = sa.connect()
	if err == nil {
		t.Fatal(err)
	}
	if !strings.Contains(err.Error(), "database disk image is malformed; quarantined") {
		t.Fatal(err)
	}
}

func TestNotADB(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	dbFile := filepath.Join(dir, "device", "accounts", "1", "000", "db", "db.db")
	if err := os.MkdirAll(filepath.Dir(dbFile), 0777); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	err = sqliteCreateAccount(dbFile, "a", "200000000.00000", nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(dbFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Seek(10, 0)
	_, err = f.Write([]byte("garbage"))
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqliteOpenAccount(dbFile)
	sa := db.(*sqliteAccount)
	err = sa.connect()
	if err == nil {
		t.Fatal(err)
	}
	// Different library versions give slightly different errors.
	if !strings.Contains(err.Error(), "file is encrypted or is not a database; quarantined") && !strings.Contains(err.Error(), "file is not a database; quarantined") {
		t.Fatal(err)
	}
}

func TestSqliteRename(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	for _, filename := range []string{"a.db", "a.db-wal", "a.db-journal"} {
		require.Nil(t, ioutil.WriteFile(filepath.Join(dir, filename), []byte("a data"), 0777))
	}
	for _, filename := range []string{"b.db", "b.db-wal", "b.db-journal"} {
		require.Nil(t, ioutil.WriteFile(filepath.Join(dir, filename), []byte("b data"), 0777))
	}
	require.Nil(t, sqliteRename(filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")))
	for _, filename := range []string{"a.db", "a.db-wal", "a.db-journal"} {
		require.False(t, fs.Exists(filepath.Join(dir, filename)))
	}
	for _, filename := range []string{"b.db", "b.db-wal", "b.db-journal"} {
		require.True(t, fs.Exists(filepath.Join(dir, filename)))
		data, err := ioutil.ReadFile(filepath.Join(dir, filename))
		require.Nil(t, err)
		require.Equal(t, data, []byte("a data"))
	}
}
