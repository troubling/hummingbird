//  Copyright (c) 2015 Rackspace
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

package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

// AtomicFileWriter saves a new file atomically.
type AtomicFileWriter interface {
	// Write writes the data to the underlying file.
	Write([]byte) (int, error)
	// Fd returns the file's underlying file descriptor.
	Fd() uintptr
	// Save atomically writes the file to its destination.
	Save(string) error
	// Abandon removes any resources associated with this file.
	Abandon() error
	// Preallocate pre-allocates space on disk, given the expected file size and disk reserve size.
	Preallocate(int64, int64) error
	// syncs file to disk (1st half of Save)
	Sync() error
	// links synced file to correct place in filesystem (2nd half of Save)
	Finalize(string) error
}

// LockPath locks a directory with a timeout.
func LockPath(directory string, timeout time.Duration) (*os.File, error) {
	file, err := os.Open(directory)
	if err != nil {
		if os.IsNotExist(err) && os.MkdirAll(directory, 0755) == nil {
			file, err = os.Open(directory)
		}
		if err != nil {
			return nil, fmt.Errorf("Unable to lock %s: %s", directory, err)
		}
	}
	success := make(chan error)
	cancel := make(chan struct{})
	defer close(cancel)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	go func(fd int) {
		select {
		case success <- syscall.Flock(fd, syscall.LOCK_EX):
		case <-cancel:
		}
	}(int(file.Fd()))
	select {
	case err = <-success:
		if err == nil {
			return file, nil
		}
	case <-timer.C:
		err = fmt.Errorf("Flock timed out: %q: %s", directory, timeout)
	}
	file.Close()
	return nil, err
}

func IsMount(dir string) (bool, error) {
	dir = filepath.Clean(dir)
	if fileinfo, err := os.Stat(dir); err == nil {
		if parentinfo, err := os.Stat(filepath.Dir(dir)); err == nil {
			return fileinfo.Sys().(*syscall.Stat_t).Dev != parentinfo.Sys().(*syscall.Stat_t).Dev, nil
		} else {
			return false, fmt.Errorf("Unable to stat parent: %s", err)
		}
	} else {
		return false, fmt.Errorf("Unable to stat directory: %s", err)
	}
}

func IsNotDir(err error) bool {
	if se, ok := err.(*os.SyscallError); ok {
		return se.Err == syscall.ENOTDIR || se.Err == syscall.EINVAL
	}
	if se, ok := err.(*os.PathError); ok {
		return os.IsNotExist(se)
	}
	return false
}

func ReadDirNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	if len(list) > 1 {
		sort.Strings(list)
	}
	return list, nil
}

func Exists(file string) bool {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		return false
	}
	return true
}

func Inode(file string) (uint64, error) {
	fileinfo, err := os.Stat(file)
	if err != nil {
		return 0, err
	}
	stat, ok := fileinfo.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("Unable to cast stat object")
	}
	return stat.Ino, nil
}
