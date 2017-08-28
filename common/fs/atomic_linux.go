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

// +build linux

package fs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

/*
#include <fcntl.h>

#ifndef __O_TMPFILE
#define __O_TMPFILE	020000000
#endif
*/
import "C"

var (
	AT_FDCWD          = C.AT_FDCWD
	AT_SYMLINK_FOLLOW = C.AT_SYMLINK_FOLLOW
	O_TMPFILE         = C.__O_TMPFILE | syscall.O_DIRECTORY
	useOTempfile      = false
)

func init() {
	var buf syscall.Utsname
	var linuxMajorVersion = 0
	var linuxMinorVersion = 0
	if syscall.Uname(&buf) == nil {
		var release string
		for _, c := range buf.Release {
			if c == 0 {
				break
			}
			release += string(byte(c))
		}
		if c, err := fmt.Sscanf(release, "%d.%d.", &linuxMajorVersion, &linuxMinorVersion); err != nil || c != 2 {
			linuxMajorVersion = 0
			linuxMinorVersion = 0
		}
	}
	// Linux 3.15 is where XFS got O_TMPFILE support.
	useOTempfile = (linuxMajorVersion > 3 || (linuxMajorVersion == 3 && linuxMinorVersion >= 15))
}

func linkat(fd uintptr, dst string) error {
	prodFd := []byte(fmt.Sprintf("/proc/self/fd/%d\x00", fd))
	dstName := append([]byte(dst), 0)
	_, _, err := syscall.Syscall6(syscall.SYS_LINKAT,
		uintptr(AT_FDCWD), uintptr(unsafe.Pointer(&prodFd[0])),
		uintptr(AT_FDCWD), uintptr(unsafe.Pointer(&dstName[0])),
		uintptr(AT_SYMLINK_FOLLOW), 0)
	if err != 0 {
		return err
	}
	return nil
}

// Quick check for interface implementation.
var _ = AtomicFileWriter(&TempFile{})

// TempFile implements an atomic file writer using linux's O_TMPFILE/linkat mechanism if available, otherwise by writing to a temp directory and renaming.
type TempFile struct {
	*os.File
	tempDir   string
	saved     bool
	otempfile bool
}

// Abandon removes any resources associated with this file, if it hasn't already been saved.
func (o *TempFile) Abandon() error {
	if o.saved {
		return nil
	}
	if !o.otempfile {
		os.Remove(o.Name())
	}
	return o.File.Close()
}

// Save atomically writes the file to its destination.
func (o *TempFile) Save(dst string) error {
	defer o.File.Close()
	if err := o.File.Sync(); err != nil {
		return err
	}
	if o.otempfile {
		if err := linkat(o.File.Fd(), dst); err != nil {
			if err := os.MkdirAll(o.tempDir, 0770); err != nil {
				return err
			}
			tmpLocation := filepath.Join(o.tempDir, fmt.Sprintf(".%016X", rand.Int63()))
			if err := linkat(o.File.Fd(), tmpLocation); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0770); err != nil {
				return err
			}
			return os.Rename(tmpLocation, dst)
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		if err := os.Rename(o.File.Name(), dst); err != nil {
			return err
		}
	}
	o.saved = true
	return nil
}

// Preallocate pre-allocates space for the file.
func (o *TempFile) Preallocate(size int64, reserve int64) error {
	var st syscall.Statfs_t
	if reserve > 0 {
		if err := syscall.Fstatfs(int(o.Fd()), &st); err == nil {
			freeSpace := int64(st.Frsize) * int64(st.Bavail)
			if freeSpace-size < reserve {
				return errors.New("Not enough reserve space on disk.")
			}
		}
	}
	if size > 0 {
		syscall.Fallocate(int(o.Fd()), 1, 0, size)
	}
	return nil
}

// NewAtomicFileWriter returns an AtomicFileWriter, which handles atomically writing files.
func NewAtomicFileWriter(tempDir string, dstDir string) (AtomicFileWriter, error) {
	if useOTempfile {
		if err := os.MkdirAll(dstDir, 0770); err != nil {
			return nil, err
		}
		tempFile, err := os.OpenFile(dstDir, O_TMPFILE|os.O_RDWR, 0660)
		if err == nil {
			return &TempFile{File: tempFile, tempDir: tempDir, saved: false, otempfile: true}, nil
		}
	}
	if err := os.MkdirAll(tempDir, 0770); err != nil {
		return nil, err
	}
	tempFile, err := ioutil.TempFile(tempDir, "")
	if err != nil {
		return nil, err
	}
	return &TempFile{File: tempFile, tempDir: tempDir, saved: false, otempfile: false}, nil
}
