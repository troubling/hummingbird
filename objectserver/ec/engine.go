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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/objectserver"
)

// ContentLength parses and returns the Content-Length for the object.
type ecEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	policy         int
	reclaimAge     int64
}

func nurseryDir(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string, policy int) string {
	h := md5.New()
	io.WriteString(h, hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+hashPathSuffix)
	digest := h.Sum(nil)
	return filepath.Join(driveRoot, vars["device"], objectserver.PolicyDir(policy), "nursery",
		strconv.Itoa(int(digest[0]>>1)), strconv.Itoa(int((digest[0]<<6|digest[1]>>2)&127)),
		strconv.Itoa(int((digest[1]<<5|digest[2]>>3)&127)), hex.EncodeToString(digest))
}

// New returns an instance of ecObject with the given parameters. Metadata is read in and if needData is true, the file is opened.  AsyncWG is a waitgroup if the object spawns any async operations
func (f *ecEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (objectserver.Object, error) {
	var err error
	obj := &ecObject{
		location:   locationNursery,
		reserve:    f.reserve,
		asyncWG:    asyncWG,
		nurseryDir: nurseryDir(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix, f.policy),
		tmpDir:     objectserver.TempDirPath(f.driveRoot, vars["device"]),
		reclaimAge: f.reclaimAge,
	}
	dataFile, metaFile := objectserver.ObjectFiles(obj.nurseryDir)
	if filepath.Ext(dataFile) == ".data" {
		obj.exists = true
		if needData {
			if obj.file, err = os.Open(dataFile); err != nil {
				return nil, err
			}
			if obj.metadata, err = objectserver.OpenObjectMetadata(obj.file.Fd(), metaFile); err != nil {
				obj.Quarantine()
				return nil, fmt.Errorf("Error getting metadata: %v", err)
			}
		} else {
			if obj.metadata, err = objectserver.ObjectMetadata(dataFile, metaFile); err != nil {
				obj.Quarantine()
				return nil, fmt.Errorf("Error getting metadata: %v", err)
			}
		}
		// baby audit
		var stat os.FileInfo
		if obj.file != nil {
			if stat, err = obj.file.Stat(); err != nil {
				obj.Close()
				return nil, fmt.Errorf("Error statting file: %v", err)
			}
		} else if stat, err = os.Stat(dataFile); err != nil {
			return nil, fmt.Errorf("Error statting file: %v", err)
		}
		if contentLength, err := strconv.ParseInt(obj.metadata["Content-Length"], 10, 64); err != nil {
			obj.Quarantine()
			return nil, fmt.Errorf("Unable to parse content-length: %s", obj.metadata["Content-Length"])
		} else if stat.Size() != contentLength {
			obj.Quarantine()
			return nil, fmt.Errorf("File size doesn't match content-length: %d vs %d", stat.Size(), contentLength)
		}
	} else {
		obj.metadata, _ = objectserver.ObjectMetadata(dataFile, metaFile) // ignore errors if deleted
	}
	return obj, nil
}

func (f *ecEngine) ecFragGetHandler(writer http.ResponseWriter, request *http.Request) {
	// vars := srv.GetVars(request)
	srv.StandardResponse(writer, http.StatusNotImplemented)
}

func (f *ecEngine) ecFragPutHandler(writer http.ResponseWriter, request *http.Request) {
	srv.StandardResponse(writer, http.StatusNotImplemented)
}

func (f *ecEngine) ecFragDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	srv.StandardResponse(writer, http.StatusNotImplemented)
}

func (f *ecEngine) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
	addRoute("GET", "/ec-frag/:device/:hash", f.ecFragGetHandler)
	addRoute("PUT", "/ec-frag/:device/:hash", f.ecFragPutHandler)
	addRoute("DELETE", "/ec-frag/:device/:hash", f.ecFragDeleteHandler)
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
		policy:         policy.Index,
		reclaimAge:     int64(config.GetInt("app:object-server", "reclaim_age", int64(common.ONE_WEEK))),
	}, nil
}

func init() {
	objectserver.RegisterObjectEngine("hec", ecEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ objectserver.ObjectEngineConstructor = ecEngineConstructor
var _ objectserver.ObjectEngine = &ecEngine{}
var _ objectserver.PolicyHandlerRegistrator = &ecEngine{}
