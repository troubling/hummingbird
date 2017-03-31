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
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
)

func BenchmarkMergeItems(b *testing.B) {
	db, _, cleanup, _ := createTestDatabase("200000000.00000")
	recs := make([]*ObjectRecord, 10000)
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10000; i++ {
			recs[i] = &ObjectRecord{Name: common.UUID(), CreatedAt: "20000000.00001", Deleted: 1}
		}
		db.MergeItems(recs, "")
		db.MergeItems(recs, "")
	}
	cleanup()
}

func BenchmarkContainerListings(b *testing.B) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	if err != nil {
		panic("NON-NIL ERROR")
	}
	defer cleanup()
	names := make([]string, 8192)
	for i := range names {
		names[i] = common.UUID()
	}
	if err := mergeItemsByName(db, names); err != nil {
		panic("NON-NIL ERROR")
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		records, err := db.ListObjects(10000, "", "", "", "", nil, false, 0)
		if err != nil {
			panic("NON-NIL ERROR")
		}
		if records == nil {
			panic("NIL RECORDS")
		}
	}
}

func TestContainerListings(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c"}))
	records, err := db.ListObjects(10000, "", "", "", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "a", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "b", records[1].(*ObjectListingRecord).Name)
	require.Equal(t, "c", records[2].(*ObjectListingRecord).Name)
}

func TestContainerUpdateRecord(t *testing.T) {
	rec := &ObjectListingRecord{Name: "a", ContentType: "text/plain; swift_bytes=100", LastModified: "1.0"}
	require.Nil(t, updateRecord(rec))
	require.Equal(t, int64(100), rec.Size)

	rec = &ObjectListingRecord{Name: "a", ContentType: "text/plain; swift_bytes=100", LastModified: "X"}
	require.NotNil(t, updateRecord(rec))

	rec = &ObjectListingRecord{Name: "a", ContentType: "text/plain; swift_bytes=X", LastModified: "1.0"}
	require.NotNil(t, updateRecord(rec))
}

func TestContainerListingsLimit(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c"}))
	records, err := db.ListObjects(2, "", "", "", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "a", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "b", records[1].(*ObjectListingRecord).Name)
}

func TestContainerListingsPrefixChange(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"b10\u2603"}))
	records, err := db.ListObjects(10000, "", "", "b10", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 1, len(records))
}

func TestContainerListingsPrefix(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a1", "a2", "A3", "b1", "B2", "a10", "b10", "zz"}))
	records, err := db.ListObjects(10000, "", "", "a", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "a1", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "a10", records[1].(*ObjectListingRecord).Name)
	require.Equal(t, "a2", records[2].(*ObjectListingRecord).Name)

	records, err = db.ListObjects(10000, "", "", "b10", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 1, len(records))
	require.Equal(t, "b10", records[0].(*ObjectListingRecord).Name)
}

func TestContainerListingsPrefixLimit(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a1", "b1", "a2", "b2", "a3", "b3"}))
	records, err := db.ListObjects(2, "", "", "a", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "a1", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "a2", records[1].(*ObjectListingRecord).Name)
}

func TestContainerListingsPrefixDelim(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"US-TX-A", "US-TX-B", "US-OK-A", "US-OK-B", "US-UT-A"}))
	records, err := db.ListObjects(10000, "", "", "US-", "-", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "US-OK-", records[0].(*SubdirListingRecord).Name)
	require.Equal(t, "US-TX-", records[1].(*SubdirListingRecord).Name)
	require.Equal(t, "US-UT-", records[2].(*SubdirListingRecord).Name)
}

func TestContainerLeadingDelimiter(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"US-TX-A", "US-TX-B", "-UK", "-CH"}))
	records, err := db.ListObjects(10000, "", "", "", "-", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "-", records[0].(*SubdirListingRecord).Name)
	require.Equal(t, "US-", records[1].(*SubdirListingRecord).Name)
}

func TestContainerMarkers(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c", "d", "e", "f"}))
	records, err := db.ListObjects(10000, "b", "e", "", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "c", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "d", records[1].(*ObjectListingRecord).Name)
}

func TestContainerReverse(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c"}))
	records, err := db.ListObjects(10000, "", "", "", "", nil, true, 0)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "c", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "b", records[1].(*ObjectListingRecord).Name)
	require.Equal(t, "a", records[2].(*ObjectListingRecord).Name)
}

func TestContainerReverseMarkers(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c", "d", "e", "f"}))
	records, err := db.ListObjects(10000, "e", "b", "", "", nil, true, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "d", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "c", records[1].(*ObjectListingRecord).Name)
}

func TestContainerListingsReversePrefixDelim(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"US-TX-A", "US-TX-B", "US-OK-A", "US-OK-B", "US-UT-A"}))
	records, err := db.ListObjects(10000, "", "", "US-", "-", nil, true, 0)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "US-UT-", records[0].(*SubdirListingRecord).Name)
	require.Equal(t, "US-TX-", records[1].(*SubdirListingRecord).Name)
	require.Equal(t, "US-OK-", records[2].(*SubdirListingRecord).Name)
}

func TestContainerListingsDelimiterAndPrefix(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"bar", "bazar"}))
	records, err := db.ListObjects(10000, "", "", "ba", "a", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "bar", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "baza", records[1].(*SubdirListingRecord).Name)

	records, err = db.ListObjects(10000, "", "", "ba", "a", nil, true, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "baza", records[0].(*SubdirListingRecord).Name)
	require.Equal(t, "bar", records[1].(*ObjectListingRecord).Name)
}

func TestContainerListingsDelimiter(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, []string{"test", "test-bar", "test-foo"}))
	records, err := db.ListObjects(10000, "", "", "", "-", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "test", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "test-", records[1].(*SubdirListingRecord).Name)

	records, err = db.ListObjects(10000, "", "", "", "-", nil, true, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(records))
	require.Equal(t, "test-", records[0].(*SubdirListingRecord).Name)
	require.Equal(t, "test", records[1].(*ObjectListingRecord).Name)
}

func TestContainerListingsPaths(t *testing.T) {
	files := []string{
		"/file1",
		"/file A",
		"/dir1/",
		"/dir2/",
		"/dir1/file2",
		"/dir1/subdir1/",
		"/dir1/subdir2/",
		"/dir1/subdir1/file2",
		"/dir1/subdir1/file3",
		"/dir1/subdir1/file4",
		"/dir1/subdir1/subsubdir1/",
		"/dir1/subdir1/subsubdir1/file5",
		"/dir1/subdir1/subsubdir1/file6",
		"/dir1/subdir1/subsubdir1/file7",
		"/dir1/subdir1/subsubdir1/file8",
		"/dir1/subdir1/subsubdir2/",
		"/dir1/subdir1/subsubdir2/file9",
		"/dir1/subdir1/subsubdir2/file0",
		"file1",
		"dir1/",
		"dir2/",
		"dir1/file2",
		"dir1/subdir1/",
		"dir1/subdir2/",
		"dir1/subdir1/file2",
		"dir1/subdir1/file3",
		"dir1/subdir1/file4",
		"dir1/subdir1/subsubdir1/",
		"dir1/subdir1/subsubdir1/file5",
		"dir1/subdir1/subsubdir1/file6",
		"dir1/subdir1/subsubdir1/file7",
		"dir1/subdir1/subsubdir1/file8",
		"dir1/subdir1/subsubdir2/",
		"dir1/subdir1/subsubdir2/file9",
		"dir1/subdir1/subsubdir2/file0",
		"dir1/subdir with spaces/",
		"dir1/subdir with spaces/file B",
		"dir1/subdir+with{whatever/",
		"dir1/subdir+with{whatever/file D",
	}

	db, _, cleanup, err := createTestDatabase("100000000.00000")
	require.Nil(t, err)
	defer cleanup()
	require.Nil(t, mergeItemsByName(db, files))
	assertListing := func(path string, expected []string) {
		sort.Strings(expected)
		records, err := db.ListObjects(10000, "", "", "", "-", &path, false, 0)
		require.Nil(t, err)
		require.Equal(t, len(expected), len(records))
		for i, rec := range records {
			require.Equal(t, expected[i], rec.(*ObjectListingRecord).Name)
		}
	}

	assertListing("/", []string{"/dir1/", "/dir2/", "/file1", "/file A"})
	assertListing("/dir1",
		[]string{"/dir1/file2", "/dir1/subdir1/", "/dir1/subdir2/"})
	assertListing("/dir1/",
		[]string{"/dir1/file2", "/dir1/subdir1/", "/dir1/subdir2/"})
	assertListing("/dir1/subdir1",
		[]string{"/dir1/subdir1/subsubdir2/", "/dir1/subdir1/file2",
			"/dir1/subdir1/file3", "/dir1/subdir1/file4",
			"/dir1/subdir1/subsubdir1/"})
	assertListing("/dir1/subdir2", []string{})
	assertListing("", []string{"file1", "dir1/", "dir2/"})

	assertListing("dir1", []string{"dir1/file2", "dir1/subdir1/",
		"dir1/subdir2/", "dir1/subdir with spaces/",
		"dir1/subdir+with{whatever/"})
	assertListing("dir1/subdir1",
		[]string{"dir1/subdir1/file4", "dir1/subdir1/subsubdir2/",
			"dir1/subdir1/file2", "dir1/subdir1/file3",
			"dir1/subdir1/subsubdir1/"})
	assertListing("dir1/subdir1/subsubdir1",
		[]string{"dir1/subdir1/subsubdir1/file7",
			"dir1/subdir1/subsubdir1/file5",
			"dir1/subdir1/subsubdir1/file8",
			"dir1/subdir1/subsubdir1/file6"})
	assertListing("dir1/subdir1/subsubdir1/",
		[]string{"dir1/subdir1/subsubdir1/file7",
			"dir1/subdir1/subsubdir1/file5",
			"dir1/subdir1/subsubdir1/file8",
			"dir1/subdir1/subsubdir1/file6"})
	assertListing("dir1/subdir with spaces/",
		[]string{"dir1/subdir with spaces/file B"})

}

func TestCreateAndGetInfo(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbFile := filepath.Join(dir, "db.db")
	metadata := map[string][]string{
		"X-Container-Meta-Hi": []string{"There", "100000000.00001"},
	}
	err = sqliteCreateContainer(dbFile, "a", "c", "100000000.00000", metadata, 2)
	require.Nil(t, err)
	db, err := sqliteOpenContainer(dbFile)
	require.Nil(t, err)
	defer db.Close()
	info, err := db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, "a", info.Account)
	require.Equal(t, "c", info.Container)
	require.Equal(t, "100000000.00000", info.PutTimestamp)
	require.Equal(t, int64(0), info.ObjectCount)
	require.Equal(t, int64(0), info.BytesUsed)
	require.Equal(t, metadata, info.Metadata)
	require.Equal(t, 2, info.StoragePolicyIndex)
}

func TestDeleteIsDeleted(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	require.Nil(t, db.Delete("100000001.00000"))
	deleted, err := db.IsDeleted()
	require.Nil(t, err)
	require.False(t, deleted)

	require.Nil(t, db.Delete("200000001.00000"))
	deleted, err = db.IsDeleted()
	require.Nil(t, err)
	require.True(t, deleted)
}

func TestMergeItems(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c"}))
	info, err := db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, int64(3), info.ObjectCount)

	require.Nil(t, mergeItemsByName(db, []string{"c", "d", "e"}))
	info, err = db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, int64(5), info.ObjectCount)

	require.Nil(t, db.MergeItems([]*ObjectRecord{&ObjectRecord{Name: "a", CreatedAt: "20000000.00001", Deleted: 1}}, ""))
	info, err = db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, int64(4), info.ObjectCount)
}

func TestIndexAfter(t *testing.T) {
	require.Equal(t, 5, indexAfter(",    ,", ",", 3))
	require.Equal(t, 4, indexAfter("    ,,", ",", 3))
	require.Equal(t, -1, indexAfter(",,,   ", ",", 3))
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

func TestGetUpdateMetadata(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbFile := filepath.Join(dir, "db.db")
	metadata := map[string][]string{
		"X-Container-Meta-Hi": []string{"There", "200000000.00001"},
	}
	metadataValues := map[string]string{
		"X-Container-Meta-Hi": "There",
	}
	err = sqliteCreateContainer(dbFile, "a", "c", "200000000.00000", metadata, 0)
	require.Nil(t, err)
	db, err := sqliteOpenContainer(dbFile)
	require.Nil(t, err)
	defer db.Close()
	info, err := db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, metadata, info.Metadata)
	m, err := db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, metadataValues, m)

	require.Nil(t, db.UpdateMetadata(map[string][]string{
		"X-Container-Meta-Hi": []string{"", "100000000.00001"},
	}, "100000000.00001"))
	m, err = db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, metadataValues, m)

	require.Nil(t, db.UpdateMetadata(map[string][]string{
		"X-Container-Meta-Hi":         []string{"", "200000001.00001"},
		"X-Container-Meta-Some-Other": []string{"value", "200000001.00001"},
	}, "200000001.00001"))
	require.Nil(t, db.UpdateMetadata(map[string][]string{}, "200000001.00001"))
	m, err = db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, map[string]string{"X-Container-Meta-Some-Other": "value"}, m)
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
		&SyncRecord{SyncPoint: 5, RemoteID: "some guy I guess"},
		&SyncRecord{SyncPoint: 1, RemoteID: "some other guy"},
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
		&SyncRecord{SyncPoint: 1000000, RemoteID: "some other guy"},
		&SyncRecord{SyncPoint: 10, RemoteID: "new guy"},
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

func TestSyncRemoteData(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("1000.0001")
	require.Nil(t, err)
	defer cleanup()

	info, err := db.SyncRemoteData(1, "00000000000000000000000000000000", "your friend", "1000.0001", "1000.0001", "", "{}")
	require.Nil(t, err)
	require.Equal(t, int64(-1), info.MaxRow)
	require.Equal(t, int64(1), info.Point)

	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c", "d", "e", "f", "g", "h"}))
	info, err = db.SyncRemoteData(1, "00000000000000000000000000000000", "your friend", "1000.0001", "1000.0001", "", "{}")
	require.Nil(t, err)
	require.Equal(t, int64(8), info.MaxRow)
	require.Equal(t, int64(1), info.Point)

	info, err = db.SyncRemoteData(10, "11111111111111111111111111111111", "your friend", "1000.0001", "1000.0001", "", "{}")
	require.Nil(t, err)
	require.Equal(t, int64(8), info.MaxRow)
	require.Equal(t, int64(1), info.Point)

	info, err = db.SyncRemoteData(10, "ae856a680962e8afedde3f2d657ed5b4", "your friend", "1000.0001", "1000.0001", "", "{}")
	require.Nil(t, err)
	require.Equal(t, int64(8), info.MaxRow)
	require.Equal(t, int64(10), info.Point)

	info, err = db.SyncRemoteData(10, "ae856a680962e8afedde3f2d657ed5b4", "your friend", "1000.0001", "1000.0001", "2000.0002", "{}")
	require.Nil(t, err)
	deleted, err := db.IsDeleted()
	require.Nil(t, err)
	require.True(t, deleted)

	info, err = db.SyncRemoteData(10, "ae856a680962e8afedde3f2d657ed5b4", "your friend", "1000.0001", "3000.0003", "2000.0002", "{}")
	require.Nil(t, err)
	deleted, err = db.IsDeleted()
	require.Nil(t, err)
	require.False(t, deleted)

	meta, err := db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, 0, len(meta))

	info, err = db.SyncRemoteData(10, "ae856a680962e8afedde3f2d657ed5b4", "your friend", "1000.0001", "3000.0003", "2000.0002",
		"{\"X-Container-Meta-Hi\": [\"I'm your friend\", \"2000.0001\"]}")
	require.Nil(t, err)
	meta, err = db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, 0, len(meta))

	info, err = db.SyncRemoteData(10, "ae856a680962e8afedde3f2d657ed5b4", "your friend", "1000.0001", "3000.0003", "2000.0002",
		"{\"X-Container-Meta-Hi\": [\"I'm your friend\", \"2001.0000\"]}")
	require.Nil(t, err)
	meta, err = db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, map[string]string{"X-Container-Meta-Hi": "I'm your friend"}, meta)

	info, err = db.SyncRemoteData(10, "ae856a680962e8afedde3f2d657ed5b4", "your friend", "1000.0001", "3000.0003", "2000.0002",
		"{\"X-Container-Meta-Hi\": [\"\", \"2001.0001\"]}")
	require.Nil(t, err)
	meta, err = db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, 0, len(meta))
}

func TestCreateExistingNoPolicy(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	c, err := sqliteCreateExistingContainer(db, "200000001.00000", map[string][]string{}, -1, 0)
	require.Nil(t, err)
	require.False(t, c)
	info, err := db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, info.StoragePolicyIndex, 0)

	c, err = sqliteCreateExistingContainer(db, "200000001.00000", map[string][]string{}, 7, 0)
	require.NotNil(t, err)

	newMetadata := map[string][]string{
		"X-Container-Meta-Whatever": []string{"something", "200000002.00000"},
	}
	c, err = sqliteCreateExistingContainer(db, "200000002.00000", newMetadata, -1, 0)
	require.Nil(t, err)
	require.False(t, c)
	info, err = db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, info.Metadata, newMetadata)

	newMetadata = map[string][]string{
		"X-Container-Meta-Another": []string{"whatevs", "200000003.00000"},
	}
	c, err = sqliteCreateExistingContainer(db, "200000003.00000", newMetadata, -1, 0)
	require.Nil(t, err)
	require.False(t, c)
	info, err = db.GetInfo()
	require.Nil(t, err)
	require.Equal(t, info.Metadata, map[string][]string{
		"X-Container-Meta-Another":  []string{"whatevs", "200000003.00000"},
		"X-Container-Meta-Whatever": []string{"something", "200000002.00000"},
	})
}

func TestCleanupTombstones(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	require.Nil(t, db.MergeItems([]*ObjectRecord{&ObjectRecord{Name: "a", CreatedAt: "10000000.00000", Deleted: 0}}, ""))
	db.UpdateMetadata(map[string][]string{
		"X-Container-Meta-Old-Value": []string{"", "10000000.00000"},
	}, "10000000.00000")
	info, err := db.GetInfo()
	require.Nil(t, err)
	_, ok := info.Metadata["X-Container-Meta-Old-Value"]
	require.True(t, ok)
	require.Nil(t, db.MergeItems([]*ObjectRecord{&ObjectRecord{Name: "a", CreatedAt: "10000001.00000", Deleted: 1}}, ""))
	require.Nil(t, db.CleanupTombstones(0))
	info, err = db.GetInfo()
	require.Nil(t, err)
	_, ok = info.Metadata["X-Container-Meta-Old-Value"]
	require.False(t, ok)
	var count int
	require.Nil(t, db.QueryRow("SELECT COUNT(*) FROM object").Scan(&count))
	require.Equal(t, 0, count)
}

func TestDeleteRemovesMetadata(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()

	db.UpdateMetadata(map[string][]string{
		"X-Container-Meta-Key": []string{"Value", "200000000.00001"},
	}, "10000000.00001")
	require.Nil(t, db.Delete("200000001.00000"))
	deleted, err := db.IsDeleted()
	require.Nil(t, err)
	require.True(t, deleted)
	metadata, err := db.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, 0, len(metadata))
}

func TestCheckSyncLink(t *testing.T) {
	db, _, cleanup, err := createTestDatabase("200000000.00000")
	require.Nil(t, err)
	defer cleanup()
	link := strings.Replace(db.containerFile, "containers", "sync_containers", -1)
	db.UpdateMetadata(map[string][]string{
		"X-Container-Sync-To": []string{"//realm/cluster/a/c", "200000000.00001"},
	}, "20000000.00001")
	db.CheckSyncLink()
	require.True(t, common.Exists(link))
	db.UpdateMetadata(map[string][]string{
		"X-Container-Sync-To": []string{"", "200000001.00001"},
	}, "20000001.00001")
	db.CheckSyncLink()
	require.False(t, common.Exists(link))
}
