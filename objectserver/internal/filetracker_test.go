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
	// TODO: Call t.Helper() once we require Go 1.9.
	if err != nil {
		t.Fatal(err)
	}
}

func md5hash(data string) string {
	b := md5.Sum([]byte(data))
	return hex.EncodeToString(b[:])
}

func Test_newFileTrackerNotExistsAndAlreadyExists(t *testing.T) {
	pth := "testdata/tmp/Test_newFileTrackerNotExistsAndAlreadyExists"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
	ft.Close()
	ft, err = NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
}

func Test_fileTracker_commit(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_commit"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Initial commit.
	f, err := ft.TempFile(hsh, len(body))
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
	// Doing the same commit again should not fail.
	f, err = ft.TempFile(hsh, len(body))
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
	// Doing an older commit should be discarded.
	f, err = ft.TempFile(hsh, len(body))
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
	f, err = ft.TempFile(hsh, len(body))
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

func Test_fileTracker_lookup(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_lookup"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.TempFile(hsh, len(body))
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
		t.Fatal(metahash, metadata)
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

func Test_fileTracker_lookup_with_overwrite(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_lookup_with_overwrite"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.TempFile(hsh, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	// Commit newer file.
	timestamp = time.Now().UnixNano()
	body = "just testing newer"
	f, err = ft.TempFile(hsh, len(body))
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
		t.Fatal(metahash, metadata)
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

func Test_fileTracker_lookup_with_underwrite(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_lookup_with_underwrite"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
	defer ft.Close()
	hsh := md5hash("file1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.TempFile(hsh, len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.Commit(f, hsh, 0, timestamp, "", nil))
	// Commit older file (should be discarded).
	timestampOlder := timestamp - 1
	bodyOlder := "just testing older"
	f, err = ft.TempFile(hsh, len(bodyOlder))
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
		t.Fatal(metahash, metadata)
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

func Test_fileTracker_list(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_list"
	defer os.RemoveAll(pth)
	ft, err := NewFileTracker(pth, 1, zap.L())
	errnil(t, err)
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
		f, err := ft.TempFile(hsh, len(body))
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
