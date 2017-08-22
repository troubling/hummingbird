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
