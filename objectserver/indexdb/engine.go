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

package indexdb

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/objectserver"
	"golang.org/x/net/http2"
)

// ContentLength parses and returns the Content-Length for the object.
type ecEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	policy         int
	ring           ring.Ring
	idbs           map[string]*IndexDB
	idbm           sync.Mutex
	logger         *zap.Logger
	dataFrags      int
	parityFrags    int
	chunkSize      int
	client         *http.Client
}

func (f *ecEngine) getDB(device string) (*IndexDB, error) {
	f.idbm.Lock()
	defer f.idbm.Unlock()
	if idb, ok := f.idbs[device]; ok && idb != nil {
		return idb, nil
	}
	var err error
	dbpath := filepath.Join(f.driveRoot, device, objectserver.PolicyDir(f.policy), "hec.db")
	path := filepath.Join(f.driveRoot, device, objectserver.PolicyDir(f.policy), "hec")
	temppath := filepath.Join(f.driveRoot, device, "tmp")
	ringPartPower := bits.Len64(f.ring.PartitionCount())
	f.idbs[device], err = NewIndexDB(dbpath, path, temppath, ringPartPower, 1, 32, f.logger)
	if err != nil {
		return nil, err
	}
	return f.idbs[device], nil
}

// New returns an instance of ecObject with the given parameters. Metadata is read in and if needData is true, the file is opened.  AsyncWG is a waitgroup if the object spawns any async operations
func (f *ecEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (objectserver.Object, error) {
	h := md5.New()
	io.WriteString(h, f.hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+f.hashPathSuffix)
	digest := h.Sum(nil)
	hash := hex.EncodeToString(digest)

	obj := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:    hash,
			Nursery: true,
		},
		dataFrags:   f.dataFrags, /* TODO: consider just putting a reference to the engine in the object */
		parityFrags: f.parityFrags,
		chunkSize:   f.chunkSize,
		reserve:     f.reserve,
		ring:        f.ring,
		logger:      f.logger,
		policy:      f.policy,
		client:      f.client,
		metadata:    map[string]string{},
	}
	if idb, err := f.getDB(vars["device"]); err == nil {
		obj.idb = idb
		if item, err := idb.Lookup(hash, shardAny, false); err == nil && item != nil {
			obj.IndexDBItem = *item
			if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
				return nil, fmt.Errorf("Error parsing metadata: %v", err)
			}
		}
		return obj, nil
	}
	return nil, errors.New("Unable to open database")
}

func (f *ecEngine) ecFragGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	fragIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	item, err := idb.Lookup(vars["hash"], fragIndex, false)
	if err != nil || item == nil || item.Deletion {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}
	metadata := map[string]string{}
	if err = json.Unmarshal(item.Metabytes, &metadata); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	writer.Header().Set("Ec-Frag-Index", metadata["Ec-Frag-Index"])
	fl, err := os.Open(item.Path)
	if err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	io.Copy(writer, fl)
}

func (f *ecEngine) ecFragPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	timestampTime, err := common.ParseDate(request.Header.Get("Meta-X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	fragIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	timestamp := timestampTime.UnixNano()
	atm, err := idb.TempFile(vars["hash"], fragIndex, timestamp, request.ContentLength, false)
	if err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	if atm == nil {
		srv.StandardResponse(writer, http.StatusCreated)
		return
	}
	defer atm.Abandon()
	metadata := make(map[string]string)
	for key := range request.Header {
		if strings.HasPrefix(key, "Meta-") {
			if key == "Meta-Name" {
				metadata["name"] = request.Header.Get(key)
			} else {
				metadata[http.CanonicalHeaderKey(key[5:])] = request.Header.Get(key)
			}
		}
	}
	io.Copy(atm, request.Body)
	metabytes, err := json.Marshal(metadata)
	if err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	if err := idb.Commit(atm, vars["hash"], fragIndex, timestamp, false, MetadataHash(metadata), metabytes, false); err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		srv.StandardResponse(writer, http.StatusCreated)
	}
}

func (f *ecEngine) ecFragDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	fragIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	item, err := idb.Lookup(vars["hash"], fragIndex, true)
	if err != nil || item == nil {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}

	if err := idb.Remove(item.Hash, item.Shard, item.Timestamp, item.Nursery); err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		srv.StandardResponse(writer, http.StatusCreated)
	}
}

func (f *ecEngine) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
	addRoute("GET", "/ec-frag/:device/:hash/:index", f.ecFragGetHandler)
	addRoute("PUT", "/ec-frag/:device/:hash/:index", f.ecFragPutHandler)
	addRoute("DELETE", "/ec-frag/:device/:hash/:index", f.ecFragDeleteHandler)
}

func (f *ecEngine) GetStabilizedObjects(device string, partition uint64, c chan objectserver.ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	idb, err := f.getDB(device)
	if err != nil {
		return
	}
	items, err := idb.List(int(partition))
	// TODO: so right now this returns nursery and stable objects. should i weed
	// out the nursery- we;d want to replicate those too right?
	if err != nil {
		return
	}
	for _, item := range items {
		obj := &ecObject{
			IndexDBItem: *item,
			idb:         idb,
			dataFrags:   f.dataFrags,
			parityFrags: f.parityFrags,
			chunkSize:   f.chunkSize,
			reserve:     f.reserve,
			ring:        f.ring,
			logger:      f.logger,
			policy:      f.policy,
			client:      f.client,
			metadata:    map[string]string{},
		}
		if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
			continue
		}
		select {
		case c <- obj:
		case <-cancel:
			return
		}
	}
}

func (f *ecEngine) GetNurseryObjects(device string, c chan objectserver.ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	idb, err := f.getDB(device)
	if err != nil {
		return
	}

	items, err := idb.ListNursery()
	if err != nil {
		return
	}

	for _, item := range items {
		obj := &ecObject{
			IndexDBItem: *item,
			idb:         idb,
			dataFrags:   f.dataFrags,
			parityFrags: f.parityFrags,
			chunkSize:   f.chunkSize,
			reserve:     f.reserve,
			ring:        f.ring,
			logger:      f.logger,
			policy:      f.policy,
			client:      f.client,
			metadata:    map[string]string{},
		}
		if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
			continue
		}
		select {
		case c <- obj:
		case <-cancel:
			return
		}
	}
}

// ecEngineConstructor creates a ecEngine given the object server configs.
func ecEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (objectserver.ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	r, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index)
	if err != nil {
		return nil, err
	}
	certFile := config.GetDefault("app:object-server", "cert_file", "")
	keyFile := config.GetDefault("app:object-server", "key_file", "")
	transport := &http.Transport{
		MaxIdleConnsPerHost: 256,
		MaxIdleConns:        0,
		IdleConnTimeout:     5 * time.Second,
		DisableCompression:  true,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
		ExpectContinueTimeout: 10 * time.Minute,
	}
	if certFile != "" && keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			return nil, err
		}
	}
	logger, _ := zap.NewProduction()
	engine := &ecEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		policy:         policy.Index,
		logger:         logger,
		ring:           r,
		idbs:           map[string]*IndexDB{},
		client: &http.Client{
			Timeout:   120 * time.Minute,
			Transport: transport,
		},
	}
	if engine.dataFrags, err = strconv.Atoi(policy.Config["data_frags"]); err != nil {
		return nil, err
	}
	if engine.parityFrags, err = strconv.Atoi(policy.Config["parity_frags"]); err != nil {
		return nil, err
	}
	if engine.chunkSize, err = strconv.Atoi(policy.Config["chunk_size"]); err != nil {
		engine.chunkSize = 1 << 20
	}
	return engine, nil
}

func init() {
	objectserver.RegisterObjectEngine("hec", ecEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ objectserver.ObjectEngineConstructor = ecEngineConstructor
var _ objectserver.ObjectEngine = &ecEngine{}
var _ objectserver.PolicyHandlerRegistrator = &ecEngine{}
