//  Copyright (c) 2017 Rackspace
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

package ec

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/objectserver"
)

const (
	locationNotFound = iota
	locationNursery
	locationStable

	updateData = iota
	updateTombstone
	updateMetadata
)

var fileExtensions = map[int]string{
	updateData:      "data",
	updateTombstone: "ts",
	updateMetadata:  "meta",
}

type ecObject struct {
	file       *os.File
	afw        fs.AtomicFileWriter
	location   int
	updateType int
	tmpDir     string
	nurseryDir string
	reserve    int64
	exists     bool
	metadata   map[string]string
	reclaimAge int64
	asyncWG    *sync.WaitGroup // Used to keep track of async goroutines
}

func (o *ecObject) Metadata() map[string]string {
	return o.metadata
}

func (o *ecObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(o.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

func (o *ecObject) Quarantine() error {
	return errors.New("Unimplemented")
}

func (o *ecObject) Exists() bool {
	return o.exists
}

func (o *ecObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if o.location == locationNursery {
		if len(dsts) == 1 {
			return io.Copy(dsts[0], o.file)
		} else {
			return common.Copy(o.file, dsts...)
		}
	} else {
		return 0, errors.New("Object doesn't exist")
	}
}

func (o *ecObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	return 0, errors.New("Unimplemented")
}

func (o *ecObject) Repr() string {
	return fmt.Sprintf("ecObject(%s)", o.nurseryDir)
}

func (o *ecObject) newFile(updateType int, size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = fs.NewAtomicFileWriter(o.tmpDir, o.nurseryDir); err != nil {
		return nil, fmt.Errorf("Error creating temp file: %v", err)
	}
	if err := o.afw.Preallocate(size, o.reserve); err != nil {
		o.afw.Abandon()
		return nil, objectserver.DriveFullError
	}
	o.updateType = updateType
	return o.afw, nil
}

func (o *ecObject) SetData(size int64) (io.Writer, error) {
	return o.newFile(updateData, size)
}

func (o *ecObject) Commit(metadata map[string]string) error {
	defer o.afw.Abandon()
	timestamp, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("No timestamp in metadata")
	}
	if err := objectserver.WriteMetadata(o.afw.Fd(), metadata); err != nil {
		return fmt.Errorf("Error writing metadata: %v", err)
	}
	fileName := filepath.Join(o.nurseryDir, fmt.Sprintf("%s.%s", timestamp, fileExtensions[o.updateType]))
	o.afw.Save(fileName)
	o.asyncWG.Add(1)
	go func() {
		defer o.asyncWG.Done()
		objectserver.HashCleanupListDir(o.nurseryDir, o.reclaimAge)
		if dir, err := os.OpenFile(o.nurseryDir, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
	}()
	return nil
}

func (o *ecObject) CommitMetadata(metadata map[string]string) error {
	return errors.New("Unimplemented")
}

func (o *ecObject) Delete(metadata map[string]string) error {
	if _, err := o.newFile(updateTombstone, 0); err != nil {
		return err
	} else {
		defer o.Close()
		return o.Commit(metadata)
	}
}

func (o *ecObject) Close() error {
	if o.afw != nil {
		defer o.afw.Abandon()
		o.afw = nil
	}
	if o.file != nil {
		defer o.file.Close()
		o.file = nil
	}
	return nil
}

// make sure these things satisfy interfaces at compile time
var _ objectserver.Object = &ecObject{}
