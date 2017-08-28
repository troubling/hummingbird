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
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", 0, time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano(), "3d6a6976bcf5dfb9")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", 0, time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano(), "3d6a6976bcf5dfb9")
	if chexorFNV64a != 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", 0, time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano(), "3d6a6976bcf5dfb9")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "e4ac654ba9b61686c2dc854a1128a323", 1, time.Date(2016, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano(), "3d670f76bcf310f4")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "eb4585ad9fe0426781ed7c49252f8225", 0, time.Date(2017, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano(), "3d6a6976bcf5dfb9")
	if chexorFNV64a == 0 {
		t.Fatal(chexorFNV64a)
	}
	chexorFNV64a = updateChexorFNV64a(chexorFNV64a, "e4ac654ba9b61686c2dc854a1128a323", 1, time.Date(2016, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano(), "3d670f76bcf310f4")
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
	if *listing[0] != 0x41911753f0b1ca97 || *listing[1] != 0x4b78ef08c9c79e78 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.Chexors(1)
	errnil(t, err)
	if *listing[0] != 0xa88cd09e155fe208 || *listing[1] != 0x6b8188dc60291219 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.Chexors(2)
	errnil(t, err)
	if *listing[0] != 0xd684658d6e22839e || *listing[1] != 0x745bf60b803b15f3 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
	listing, err = ft.Chexors(3)
	errnil(t, err)
	if *listing[0] != 0x52faeda6def196a4 || *listing[1] != 0x14765d22c063f259 {
		msg := "chexors changed"
		for _, c := range listing {
			msg += fmt.Sprintf(" %016x", *c)
		}
		t.Fatal(msg)
	}
}

/* TODO: Once we implement new List
func TestFileTracker_List(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_List"
	defer os.RemoveAll(pth)
	ft := newTestFileTracker(t, pth)
	defer ft.Close()
	countOfHashesThatStartWith02 := 0
	// Create a bunch of files.
	for i := 0; i < 32; i++ {
		hsh := md5hash(fmt.Sprintf("file%d", i))
		if hsh[:2] == "02" {
			countOfHashesThatStartWith02++
		}
		timestamp := time.Now().UnixNano()
		body := "just testing"
		f, err := ft.TempFile(hsh, 0, timestamp, len(body))
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	}
	// List all of them.
	listing, err := ft.List("00000000000000000000000000000000", "ffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 32 {
		t.Fatal(len(listing))
	}
	// List a subset.
	listing, err = ft.List("02000000000000000000000000000000", "02ffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if countOfHashesThatStartWith02 < 1 {
		t.Fatal(countOfHashesThatStartWith02)
	}
	if len(listing) != countOfHashesThatStartWith02 {
		t.Fatal(len(listing), countOfHashesThatStartWith02)
	}
}
*/

func TestFileTracker_partitionRange(t *testing.T) {
	pth := "testdata/tmp/TestFileTracker_partitionRange"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 4, 1, 2, zap.L())
	errnil(t, err)
	defer ft.Close()
	startHash, stopHash := ft.partitionRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "0fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.partitionRange(7)
	if startHash != "70000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.partitionRange(15)
	if startHash != "f0000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	ft, err = NewFileTracker(pth, 8, 1, 2, zap.L())
	errnil(t, err)
	defer ft.Close()
	startHash, stopHash = ft.partitionRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "00ffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.partitionRange(127)
	if startHash != "7f000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ft.partitionRange(255)
	if startHash != "ff000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
}
