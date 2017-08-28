package internal

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
	// TODO: Once we Go 1.9:
	// t.Helper()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	if err != nil {
		panic(err)
	}
}

func md5hash(data string) string {
	b := md5.Sum([]byte(data))
	return hex.EncodeToString(b[:])
}

func TestChexorFNV64a(t *testing.T) {
	var chexorFNV64a uint64
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano())
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano())
	if chexorFNV64a != 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano())
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "e4ac654ba9b61686c2dc854a1128a323", time.Date(2016, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano())
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano())
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "e4ac654ba9b61686c2dc854a1128a323", time.Date(2016, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano())
	if chexorFNV64a != 0 {
		t.Fatal(chexorFNV64a)
	}
}

func TestMetaChexorFNV64a(t *testing.T) {
	var chexorFNV64a uint64
	chexorFNV64a = updateMetaChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", "01fb28db560ceb2b")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateMetaChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", "01fb28db560ceb2b")
	if chexorFNV64a != 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateMetaChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", "01fb28db560ceb2b")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateMetaChexorFNV64a(chexorFNV64a, "e4ac654ba9b61686c2dc854a1128a323", "65b557cd2a7faa35")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateMetaChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", "01fb28db560ceb2b")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateMetaChexorFNV64a(chexorFNV64a, "e4ac654ba9b61686c2dc854a1128a323", "65b557cd2a7faa35")
	if chexorFNV64a != 0 {
		t.Fatal(chexorFNV64a)
	}
}

func newTestFileTracker(t *testing.T, pth string) *FileTracker {
	// TODO: Once we Go 1.9:
	// t.Helper()
	ft, err := NewFileTracker(pth, 2, 1, 2, zap.L())
	errnil(t, err)
	return ft
}

func TestNewFileTracker_notExistsAndAlreadyExists(t *testing.T) {
	pth := "testdata/tmp/TestNewFileTracker_notExistsAndAlreadyExists"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	ft.Close()
	ft = newTestFileTracker(t, pth)
}

func TestFileTracker_Commit(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_Commit"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Initial commit.
	f, err := ft.TempFile(hsh, 0, timestamp, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	pth, err = ft.wholeFilePath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err := os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Same commit should return (nil, nil).
	f, err = ft.TempFile(hsh, 0, timestamp, len(body))
	if f != nil || err != nil {
		t.Fatal(f, err)
	}
	// So we'll fake the timestamp on the TempFile call, but try to do the same
	// as the stored timestamp on the commit again, which should not fail (but
	// won't really do anything behind the scenes).
	f, err = ft.TempFile(hsh, 0, timestamp+1, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	pth, err = ft.wholeFilePath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Attempting an older commit should return (nil, nil).
	f, err = ft.TempFile(hsh, 0, timestamp-1, len(body))
	if f != nil || err != nil {
		t.Fatal(f, err)
	}
	// Doing an older commit should be discarded. We're going to fake like
	// we're doing a newer commit to get the temp file, but then actually try
	// to do an old commit.
	f, err = ft.TempFile(hsh, 0, timestamp+1, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp-1, "", nil))
	pth, err = ft.wholeFilePath(hsh, 0, timestamp-1)
	fi, err = os.Stat(pth)
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// Original commit should still be there.
	pth, err = ft.wholeFilePath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Doing a newer commit should discard the original.
	f, err = ft.TempFile(hsh, 0, timestamp+1, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp+1, "", nil))
	pth, err = ft.wholeFilePath(hsh, 0, timestamp+1)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Original commit should be gone.
	pth, err = ft.wholeFilePath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err = os.Stat(pth)
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestFileTracker_Lookup(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_Lookup"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.TempFile(hsh, 0, timestamp, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	// Do the lookup.
	lookedupTimestamp, metahash, metadata, path, err := ft.Lookup(hsh, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if metahash != "" || metadata != nil {
		t.Fatalf("%#v %#v\n", metahash, metadata)
	}
	if path == "" {
		t.Fatal(path)
	}
	// Check the file.
	b, err := ioutil.ReadFile(path)
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

func TestFileTracker_Lookup_withOverwrite(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_Lookup_withOverwrite"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.TempFile(hsh, 0, timestamp, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	// Commit newer file.
	timestamp = time.Now().UnixNano()
	body = "just testing newer"
	f, err = ft.TempFile(hsh, 0, timestamp, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	// Do the lookup.
	lookedupTimestamp, metahash, metadata, path, err := ft.Lookup(hsh, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if metahash != "" || metadata != nil {
		t.Fatalf("%#v %#v\n", metahash, metadata)
	}
	if path == "" {
		t.Fatal(path)
	}
	// Check the file.
	b, err := ioutil.ReadFile(path)
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

func TestFileTracker_Lookup_withUnderwrite(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_Lookup_withUnderwrite"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.TempFile(hsh, 0, timestamp, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	// Commit older file (should be discarded).
	timestampOlder := timestamp - 1
	bodyOlder := "just testing older"
	// Fake newer commit, but we'll really commit with timestampOlder.
	f, err = ft.TempFile(hsh, 0, timestamp+1, len(bodyOlder))
	errnil(t, err)
	f.Write([]byte(bodyOlder))
	errnil(t, ft.Commit(f, hsh, 0, timestampOlder, "", nil))
	// Do the lookup.
	lookedupTimestamp, metahash, metadata, path, err := ft.Lookup(hsh, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if metahash != "" || metadata != nil {
		t.Fatalf("%#v %#v\n", metahash, metadata)
	}
	if path == "" {
		t.Fatal(path)
	}
	// Check the file.
	b, err := ioutil.ReadFile(path)
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

func TestFileTracker_Chexors(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_Chexors"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 2, 1, 2, zap.L())
	errnil(t, err)
	defer ft.Close()
	timestamp := time.Date(2017, 1, 2, 3, 5, 6, 7, time.UTC).UnixNano()
	// Create a bunch of files.
	for i := 0; i < 32; i++ {
		hsh := md5hash(fmt.Sprintf("file%d", i))
		body := "just testing"
		timestamp++
		f, err := ft.TempFile(hsh, 0, timestamp, len(body))
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	}
	listing, err := ft.Chexors(0)
	errnil(t, err)
	// Golden values; just testing they don't change.
	if *listing[0] != 0x3141a949fc5058d9 || *listing[1] != 0x5f51fe797fd947d8 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.Chexors(1)
	errnil(t, err)
	if *listing[0] != 0x745a1d290f08db20 || *listing[1] != 0x51f395db668ca4c3 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.Chexors(2)
	errnil(t, err)
	if *listing[0] != 0x541d85d562e50e2e || *listing[1] != 0xf91d6f8d5903a6d1 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.Chexors(3)
	errnil(t, err)
	if *listing[0] != 0x30a31879a94294f4 || *listing[1] != 0x31ea856ac9bb368f {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
}

func TestFileTracker_MetaChexors(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_MetaChexors"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 2, 1, 2, zap.L())
	errnil(t, err)
	defer ft.Close()
	timestamp := time.Date(2017, 1, 2, 3, 5, 6, 7, time.UTC).UnixNano()
	// Create a bunch of files.
	for i := 0; i < 32; i++ {
		hsh := md5hash(fmt.Sprintf("file%d", i))
		body := "just testing"
		timestamp++
		f, err := ft.TempFile(hsh, 0, timestamp, len(body))
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ft.Commit(f, hsh, 0, timestamp, hsh[:16], nil))
	}
	listing, err := ft.MetaChexors(0)
	errnil(t, err)
	// Golden values; just testing they don't change.
	if *listing[0] != 0x1b23359c090669d9 || *listing[1] != 0xd164921a8346dd25 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.MetaChexors(1)
	errnil(t, err)
	if *listing[0] != 0xa2064b82bc598f75 || *listing[1] != 0x32a8f7115eb4ba03 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.MetaChexors(2)
	errnil(t, err)
	if *listing[0] != 0x8e1619a90fec2f94 || *listing[1] != 0xb9aa921d04d47864 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.MetaChexors(3)
	errnil(t, err)
	if *listing[0] != 0x3b87526fc361d6c7 || *listing[1] != 0x64deffa5cd2b156c {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
}

func TestFileTracker_List(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_List"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	defer ft.Close()
	matchHashes0_0 := map[string]struct{}{}
	matchHashes0_1 := map[string]struct{}{}
	matchHashes1_1 := map[string]struct{}{}
	// Create a bunch of files.
	for i := 0; i < 32; i++ {
		hsh := md5hash(fmt.Sprintf("file%d", i))
		hshb, err := hex.DecodeString(hsh)
		if err != nil {
			t.Fatal(err)
		}
		if hshb[0]>>(8-ft.ringPartPower) == 0 && int(hshb[15])%ft.chexorsMod == 0 {
			matchHashes0_0[hsh] = struct{}{}
		}
		if hshb[0]>>(8-ft.ringPartPower) == 0 && int(hshb[15])%ft.chexorsMod == 1 {
			matchHashes0_1[hsh] = struct{}{}
		}
		if hshb[0]>>(8-ft.ringPartPower) == 1 && int(hshb[15])%ft.chexorsMod == 1 {
			matchHashes1_1[hsh] = struct{}{}
		}
		timestamp := time.Now().UnixNano()
		body := "just testing"
		f, err := ft.TempFile(hsh, 0, timestamp, len(body))
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	}
	listing, err := ft.List(0, 0)
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
	listing, err = ft.List(0, 1)
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
	listing, err = ft.List(1, 1)
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

func TestFileTracker_ringPartRange(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_partitionRange"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 4, 1, 2, zap.L())
	errnil(t, err)
	defer ft.Close()
	startHash, stopHash := ft.ringPartRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "0fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.ringPartRange(7)
	if startHash != "70000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.ringPartRange(15)
	if startHash != "f0000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	ft, err = NewFileTracker(pth, 8, 1, 2, zap.L())
	errnil(t, err)
	defer ft.Close()
	startHash, stopHash = ft.ringPartRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "00ffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.ringPartRange(127)
	if startHash != "7f000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.ringPartRange(255)
	if startHash != "ff000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
}
