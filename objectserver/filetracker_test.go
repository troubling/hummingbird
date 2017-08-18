package objectserver

import (
	"crypto/md5"
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

func Test_newFileTrackerNotExistsAndAlreadyExists(t *testing.T) {
	pth := "testdata/tmp/Test_newFileTrackerNotExistsAndAlreadyExists"
	defer os.RemoveAll(pth)
	ft, err := newFileTracker(pth, zap.L())
	errnil(t, err)
	ft.close()
	ft, err = newFileTracker(pth, zap.L())
	errnil(t, err)
}

func Test_fileTracker_commit(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_commit"
	defer os.RemoveAll(pth)
	ft, err := newFileTracker(pth, zap.L())
	errnil(t, err)
	defer ft.close()
	hsh := md5.Sum([]byte("file1"))
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Initial commit.
	f, err := ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	fi, err := os.Stat(ft.wholeFilePath(hsh[:], 0, timestamp))
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Doing the same commit again should not fail.
	f, err = ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	fi, err = os.Stat(ft.wholeFilePath(hsh[:], 0, timestamp))
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Doing an older commit should be discarded.
	f, err = ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp-1, nil, nil))
	fi, err = os.Stat(ft.wholeFilePath(hsh[:], 0, timestamp-1))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// Original commit should still be there.
	fi, err = os.Stat(ft.wholeFilePath(hsh[:], 0, timestamp))
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Doing a newer commit should discard the original.
	f, err = ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp+1, nil, nil))
	fi, err = os.Stat(ft.wholeFilePath(hsh[:], 0, timestamp+1))
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Original commit should be gone.
	fi, err = os.Stat(ft.wholeFilePath(hsh[:], 0, timestamp))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func Test_fileTracker_lookup(t *testing.T) {
	pth := "testdata/tmp/Test_fileTracker_lookup"
	defer os.RemoveAll(pth)
	ft, err := newFileTracker(pth, zap.L())
	errnil(t, err)
	defer ft.close()
	hsh := md5.Sum([]byte("file1"))
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	// Do the lookup.
	lookedupTimestamp, metahash, metadata, path, err := ft.lookup(hsh[:], 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if metahash != nil || metadata != nil {
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
	ft, err := newFileTracker(pth, zap.L())
	errnil(t, err)
	defer ft.close()
	hsh := md5.Sum([]byte("file1"))
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	// Commit newer file.
	timestamp = time.Now().UnixNano()
	body = "just testing newer"
	f, err = ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	// Do the lookup.
	lookedupTimestamp, metahash, metadata, path, err := ft.lookup(hsh[:], 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if metahash != nil || metadata != nil {
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
	ft, err := newFileTracker(pth, zap.L())
	errnil(t, err)
	defer ft.close()
	hsh := md5.Sum([]byte("file1"))
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ft.tempFile(hsh[:], len(body))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	// Commit older file (should be discarded).
	timestampOlder := timestamp - 1
	bodyOlder := "just testing older"
	f, err = ft.tempFile(hsh[:], len(bodyOlder))
	errnil(t, err)
	f.Write([]byte(bodyOlder))
	errnil(t, ft.commit(f, hsh[:], 0, timestampOlder, nil, nil))
	// Do the lookup.
	lookedupTimestamp, metahash, metadata, path, err := ft.lookup(hsh[:], 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if metahash != nil || metadata != nil {
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
	ft, err := newFileTracker(pth, zap.L())
	errnil(t, err)
	defer ft.close()
	// Create a bunch of files.
	for i := 0; i < 256; i++ {
		hsh := md5.Sum([]byte(fmt.Sprintf("file%d", i)))
		timestamp := time.Now().UnixNano()
		body := "just testing"
		f, err := ft.tempFile(hsh[:], len(body))
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ft.commit(f, hsh[:], 0, timestamp, nil, nil))
	}
	// List all of them.
	listing, err := ft.list([]byte{0}, []byte{
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listing) != 256 {
		t.Fatal(len(listing))
	}
}
