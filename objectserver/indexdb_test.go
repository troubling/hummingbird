package objectserver

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
)

func errnil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func md5hash(data string) string {
	b := md5.Sum([]byte(data))
	return hex.EncodeToString(b[:])
}

func newTestIndexDB(t *testing.T, pth string) *IndexDB {
	t.Helper()
	ot, err := NewIndexDB(pth, pth, pth, 2, 1, 1, zap.L())
	errnil(t, err)
	return ot
}

func TestNewIndexDB_notExistsAndAlreadyExists(t *testing.T) {
	pth := "testdata/tmp/TestNewIndexDB_notExistsAndAlreadyExists"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	ot.Close()
	ot = newTestIndexDB(t, pth)
}

func TestIndexDB_Commit(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Commit"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	shardHash := "nonsense"
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Initial commit.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, shardHash))
	pth, err = ot.WholeObjectPath(hsh, 0, timestamp, true)
	errnil(t, err)
	fi, err := os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	item, err := ot.Lookup(hsh, 0, false)
	errnil(t, err)
	if item.ShardHash != shardHash {
		t.Fatal(item.ShardHash, shardHash)
	}

	// Same commit should return (nil, nil).
	f, err = ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
	if f != nil || err != nil {
		t.Fatal(f, err)
	}
	// So we'll fake the timestamp on the TempFile call, but try to do the same
	// as the stored timestamp on the commit again, which should not fail (but
	// won't really do anything behind the scenes).
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	// FIXME? This *WILL* overwrite the shardHash, but not the object data
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, shardHash))
	pth, err = ot.WholeObjectPath(hsh, 0, timestamp, true)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	item, err = ot.Lookup(hsh, 0, false)
	errnil(t, err)
	if item.ShardHash != shardHash {
		t.Fatal(item.ShardHash, shardHash)
	}
	// Attempting an older commit should return (nil, nil).
	f, err = ot.TempFile(hsh, 0, timestamp-1, int64(len(body)), true)
	if f != nil || err != nil {
		t.Fatal(f, err)
	}
	// Doing an older commit should be discarded. We're going to fake like
	// we're doing a newer commit to get the temp file, but then actually try
	// to do an old commit.
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp-1, false, "", nil, true, ""))
	pth, err = ot.WholeObjectPath(hsh, 0, timestamp-1, true)
	fi, err = os.Stat(pth)
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// Original commit should still be there.
	pth, err = ot.WholeObjectPath(hsh, 0, timestamp, true)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	item, err = ot.Lookup(hsh, 0, false)
	errnil(t, err)
	if item.ShardHash != shardHash {
		t.Fatal(item.ShardHash, shardHash)
	}
	// Doing a newer commit should discard the original.
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	newShardHash := "morenonsense"
	errnil(t, ot.Commit(f, hsh, 0, timestamp+1, false, "", nil, true, newShardHash))
	pth, err = ot.WholeObjectPath(hsh, 0, timestamp+1, true)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	item, err = ot.Lookup(hsh, 0, false)
	errnil(t, err)
	if item.ShardHash != newShardHash {
		t.Fatal(item.ShardHash, newShardHash)
	}
	// Original commit should be gone.
	pth, err = ot.WholeObjectPath(hsh, 0, timestamp, true)
	errnil(t, err)
	fi, err = os.Stat(pth)
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestIndexDB_Lookup(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Lookup"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	shardHash := "nonsense"
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, shardHash))
	// Do the lookup.
	//lookedupTimestamp, deletion, metahash, metadata, path, err := ot.Lookup(hsh, 0)
	i, err := ot.Lookup(hsh, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if i.Timestamp != timestamp {
		t.Fatal(i.Timestamp, timestamp)
	}
	if i.Deletion {
		t.Fatal(i.Deletion)
	}
	if i.Metahash != "" || i.Metabytes != nil {
		t.Fatalf("%#v %#v\n", i.Metahash, i.Metabytes)
	}
	if i.Path == "" {
		t.Fatal(i.Path)
	}
	if i.ShardHash != shardHash {
		t.Fatal(i.ShardHash, shardHash)
	}
	// Check the file.
	b, err := ioutil.ReadFile(i.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(body) {
		t.Fatal(len(b), len(body))
	}
	if string(b) != body {
		t.Fatal(string(b), body)
	}
}

func TestIndexDB_Lookup_withOverwrite(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Lookup_withOverwrite"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	shardHash := "nonsense"
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, shardHash))
	// Commit newer file.
	timestamp = time.Now().UnixNano()
	body = "just testing newer"
	shardHash = "newer nonsense"
	f, err = ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, shardHash))
	// Do the lookup.
	//lookedupTimestamp, deletion, metahash, metadata, path, err := ot.Lookup(hsh, 0)
	i, err := ot.Lookup(hsh, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if i.Timestamp != timestamp {
		t.Fatal(i.Timestamp, timestamp)
	}
	if i.Deletion {
		t.Fatal(i.Deletion)
	}
	if i.Metahash != "" || i.Metabytes != nil {
		t.Fatalf("%#v %#v\n", i.Metahash, i.Metabytes)
	}
	if i.Path == "" {
		t.Fatal(i.Path)
	}
	if i.ShardHash != shardHash {
		t.Fatal(i.ShardHash, shardHash)
	}
	// Check the file.
	b, err := ioutil.ReadFile(i.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(body) {
		t.Fatal(len(b), len(body))
	}
	if string(b) != body {
		t.Fatal(string(b), body)
	}
}

func TestIndexDB_Lookup_withUnderwrite(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Lookup_withUnderwrite"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	shardHash := "nonsense"
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, shardHash))
	// Commit older file (should be discarded).
	timestampOlder := timestamp - 1
	bodyOlder := "just testing older"
	shardHashOlder := "older nonsense"
	// Fake newer commit, but we'll really commit with timestampOlder.
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(bodyOlder)), true)
	errnil(t, err)
	f.Write([]byte(bodyOlder))
	errnil(t, ot.Commit(f, hsh, 0, timestampOlder, false, "", nil, true, shardHashOlder))
	// Do the lookup.
	//lookedupTimestamp, deletion, metahash, metadata, path, err := ot.Lookup(hsh, 0)
	i, err := ot.Lookup(hsh, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if i.Timestamp != timestamp {
		t.Fatal(i.Timestamp, timestamp)
	}
	if i.Deletion {
		t.Fatal(i.Deletion)
	}
	if i.Metahash != "" || i.Metabytes != nil {
		t.Fatalf("%#v %#v\n", i.Metahash, i.Metabytes)
	}
	if i.Path == "" {
		t.Fatal(i.Path)
	}
	if i.ShardHash != shardHash {
		t.Fatal(i.ShardHash, shardHash)
	}
	// Check the file.
	b, err := ioutil.ReadFile(i.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(body) {
		t.Fatal(len(b), len(body))
	}
	if string(b) != body {
		t.Fatal(string(b), body)
	}
}

func TestIndexDB_List(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_List"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	matchHashes0_0 := map[string]struct{}{}
	matchHashes0_1 := map[string]struct{}{}
	matchHashes1_1 := map[string]struct{}{}
	// Create a bunch of objects.
	for i := 0; i < 32; i++ {
		hsh := md5hash(fmt.Sprintf("object%d", i))
		hshb, err := hex.DecodeString(hsh)
		if err != nil {
			t.Fatal(err)
		}
		if hshb[0]>>(8-ot.RingPartPower) == 0 {
			matchHashes0_0[hsh] = struct{}{}
		}
		if hshb[0]>>(8-ot.RingPartPower) == 0 {
			matchHashes0_1[hsh] = struct{}{}
		}
		if hshb[0]>>(8-ot.RingPartPower) == 1 {
			matchHashes1_1[hsh] = struct{}{}
		}
		timestamp := time.Now().UnixNano()
		body := "just testing"
		f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, ""))
	}
	startHash, stopHash := ot.RingPartRange(0)
	listing, err := ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listing {
		if _, ok := matchHashes0_0[item.Hash]; !ok {
			t.Error(item.Hash)
		}
		delete(matchHashes0_0, item.Hash)
	}
	if len(matchHashes0_0) != 0 {
		t.Error(matchHashes0_0)
	}
	listing, err = ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listing {
		if _, ok := matchHashes0_1[item.Hash]; !ok {
			t.Error(item.Hash)
		}
		delete(matchHashes0_1, item.Hash)
	}
	if len(matchHashes0_1) != 0 {
		t.Error(matchHashes0_1)
	}
	startHash, stopHash = ot.RingPartRange(1)
	listing, err = ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listing {
		if _, ok := matchHashes1_1[item.Hash]; !ok {
			t.Error(item.Hash)
		}
		delete(matchHashes1_1, item.Hash)
	}
	if len(matchHashes1_1) != 0 {
		t.Error(matchHashes1_1)
	}
}

func TestIndexDB_ListRange(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_List"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()

	hsh0 := md5hash("object0")
	// FYI hsh0: 18cb296143950052124d6241bd35128a
	hsh1 := md5hash("object1")
	// FYI hsh: 5b78f9689b9aab1ebc0f3c1df916dd97

	timestamp := time.Now().UnixNano()
	body := "just testing"
	f, err := ot.TempFile(hsh0, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh0, 0, timestamp, false, "", nil, true, ""))

	f, err = ot.TempFile(hsh1, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh1, 0, timestamp, false, "", nil, true, ""))

	startHash := "00000000000000000000000000000000"
	stopHash := "11111111111111111111111111111111"
	listing, err := ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 0 {
		t.Fatalf("Should be empty: %v", listing)
	}

	startHash = "11111111111111111111111111111111"
	stopHash = "51111111111111111111111111111111"
	listing, err = ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 1 {
		t.Fatalf("Should be len 1: %v", listing)
	}
	if listing[0].Hash != hsh0 {
		t.Fatal(listing[0].Hash, hsh0)
	}

	startHash = "51111111111111111111111111111111"
	stopHash = "91111111111111111111111111111111"
	listing, err = ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 1 {
		t.Fatalf("Should be len 1: %v", listing)
	}
	if listing[0].Hash != hsh1 {
		t.Fatal(listing[0].Hash, hsh0)
	}

	startHash = "11111111111111111111111111111111"
	stopHash = "91111111111111111111111111111111"
	listing, err = ot.List(startHash, stopHash, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 2 {
		t.Fatalf("Should be len 2: %v", listing)
	}
	if listing[0].Hash != hsh0 {
		t.Fatal(listing[0].Hash, hsh0)
	}
	if listing[1].Hash != hsh1 {
		t.Fatal(listing[1].Hash, hsh1)
	}

	// Test limit while we're here.
	listing, err = ot.List(startHash, stopHash, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 1 {
		t.Fatalf("Should be len 1: %v", listing)
	}
	if listing[0].Hash != hsh0 {
		t.Fatal(listing[0].Hash, hsh0)
	}
}

func TestIndexDB_ListMarker(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_ListMarker"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()

	timestamp := time.Now().UnixNano()
	for i := 0; i < 16; i++ {
		hsh := fmt.Sprintf("%x0000000000000000000000000000000", i)
		body := "just testing"
		f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)), true)
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil, true, ""))
	}

	listing, err := ot.List("", "", "e0000000000000000000000000000000", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 1 {
		t.Fatalf("Should be len 1: %v", listing)
	}
	if listing[0].Hash != "f0000000000000000000000000000000" {
		t.Fatal(listing[0].Hash, "f0000000000000000000000000000000")
	}

	listing, err = ot.List("", "", "30000000000000000000000000000000", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 12 { // 4 through f is 12
		t.Fatalf("Should be len 12: %v", listing)
	}
	if listing[0].Hash != "40000000000000000000000000000000" {
		t.Fatal(listing[0].Hash, "40000000000000000000000000000000")
	}

	listing, err = ot.List("30000000000000000000000000000000", "40000000000000000000000000000000", "30000000000000000000000000000000", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 1 {
		t.Fatalf("Should be len 1: %v", listing)
	}
	if listing[0].Hash != "40000000000000000000000000000000" {
		t.Fatal(listing[0].Hash, "40000000000000000000000000000000")
	}

	listing, err = ot.List("30000000000000000000000000000000", "40000000000000000000000000000000", "40000000000000000000000000000000", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 0 {
		t.Fatalf("Should be len 0: %v", listing)
	}
}

func TestIndexDB_ListDefaults(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_ListDefaults"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()

	hsh0 := "00000000000000000000000000000000"
	hsh1 := "ffffffffffffffffffffffffffffffff"
	timestamp := time.Now().UnixNano()
	body := "just testing"
	f, err := ot.TempFile(hsh0, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh0, 0, timestamp, false, "", nil, true, ""))

	f, err = ot.TempFile(hsh1, 0, timestamp, int64(len(body)), true)
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh1, 0, timestamp, false, "", nil, true, ""))

	listing, err := ot.List("", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 2 {
		t.Fatalf("Should be len 2: %v", listing)
	}
	if listing[0].Hash != hsh0 {
		t.Fatal(listing[0].Hash, hsh0)
	}
	if listing[1].Hash != hsh1 {
		t.Fatal(listing[1].Hash, hsh1)
	}
}

func TestIndexDB_RingPartRange(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_partitionRange"
	defer os.RemoveAll(pth)
	ot, err := NewIndexDB(pth, pth, pth, 4, 1, 1, zap.L())
	errnil(t, err)
	defer ot.Close()
	startHash, stopHash := ot.RingPartRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "0fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.RingPartRange(7)
	if startHash != "70000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.RingPartRange(15)
	if startHash != "f0000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	ot, err = NewIndexDB(pth, pth, pth, 8, 1, 1, zap.L())
	errnil(t, err)
	defer ot.Close()
	startHash, stopHash = ot.RingPartRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "00ffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.RingPartRange(127)
	if startHash != "7f000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.RingPartRange(255)
	if startHash != "ff000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
}
