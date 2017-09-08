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
	"flag"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/objectserver"
)

type ecObject struct {
	metadata map[string]string
}

// Metadata returns the object's metadata.
func (o *ecObject) Metadata() map[string]string {
	return o.metadata
}

// ContentLength parses and returns the Content-Length for the object.
func (o *ecObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(o.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

// Quarantine removes the object's underlying files to the Quarantined directory on the device.
func (o *ecObject) Quarantine() error {
	return errors.New("Unimplemented")
}

// Exists returns true if the object exists, that is if it has a .data file.
func (o *ecObject) Exists() bool {
	return false
}

// Copy copies all data from the underlying .data file to the given writers.
func (o *ecObject) Copy(dsts ...io.Writer) (written int64, err error) {
	return 0, errors.New("Unimplemented")
}

// CopyRange copies data in the range of start to end from the underlying .data file to the writer.
func (o *ecObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	return 0, errors.New("Unimplemented")
}

// Repr returns a string that identifies the object in some useful way, used for logging.
func (o *ecObject) Repr() string {
	return fmt.Sprintf("ecObject(%s:%s)", o.metadata["name"], o.metadata["X-Timestamp"])
}

// SetData is called to set the object's data.  It takes a size (if available, otherwise set to zero).
func (o *ecObject) SetData(size int64) (io.Writer, error) {
	return nil, errors.New("Unimplemented")
}

// CommitMetadata updates the object's metadata (e.g. POST).
func (o *ecObject) CommitMetadata(map[string]string) error {
	return errors.New("Unimplemented")
}

// Commit commits an open data file to disk, given the metadata.
func (o *ecObject) Commit(metadata map[string]string) error {
	return errors.New("Unimplemented")
}

// Delete deletes the object.
func (o *ecObject) Delete(metadata map[string]string) error {
	return errors.New("Unimplemented")
}

// Close releases any resources used by the instance of ecObject
func (o *ecObject) Close() error {
	return errors.New("Unimplemented")
}

type ecEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	policy         int
}

// New returns an instance of ecObject with the given parameters. Metadata is read in and if needData is true, the file is opened.  AsyncWG is a waitgroup if the object spawns any async operations
func (f *ecEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (objectserver.Object, error) {
	sor := &ecObject{metadata: map[string]string{
		"name":        "/a/c/o",
		"X-Timestamp": "",
	}}
	return sor, nil
}

// ecEngineConstructor creates a ecEngine given the object server configs.
func ecEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (objectserver.ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	return &ecEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		policy:         policy.Index}, nil
}

func init() {
	objectserver.RegisterObjectEngine("hec", ecEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ objectserver.ObjectEngineConstructor = ecEngineConstructor
var _ objectserver.Object = &ecObject{}
var _ objectserver.ObjectEngine = &ecEngine{}
