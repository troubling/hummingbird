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

package objectserver

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/tracing"
	"golang.org/x/net/http2"
)

// ContentLength parses and returns the Content-Length for the object.
type ecEngine struct {
	driveRoot       string
	hashPathPrefix  string
	hashPathSuffix  string
	reserve         int64
	policy          int
	ring            ring.Ring
	idbs            map[string]*IndexDB
	idbm            sync.Mutex
	logger          *zap.Logger
	dataShards      int
	parityShards    int
	chunkSize       int
	client          common.HTTPClient
	nurseryReplicas int
	dbPartPower     int
	numSubDirs      int
}

func (f *ecEngine) getDB(device string) (*IndexDB, error) {
	f.idbm.Lock()
	defer f.idbm.Unlock()
	if idb, ok := f.idbs[device]; ok && idb != nil {
		return idb, nil
	}
	var err error
	dbpath := filepath.Join(f.driveRoot, device, PolicyDir(f.policy), "hec.db")
	path := filepath.Join(f.driveRoot, device, PolicyDir(f.policy), "hec")
	temppath := filepath.Join(f.driveRoot, device, "tmp")
	ringPartPower := bits.Len64(f.ring.PartitionCount() - 1)
	f.idbs[device], err = NewIndexDB(dbpath, path, temppath, ringPartPower, f.dbPartPower, f.numSubDirs, f.reserve, f.logger)
	if err != nil {
		return nil, err
	}
	return f.idbs[device], nil
}

// New returns an instance of ecObject with the given parameters. Metadata is read in and if needData is true, the file is opened.  AsyncWG is a waitgroup if the object spawns any async operations
func (f *ecEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (Object, error) {
	hash := ObjHash(vars, f.hashPathPrefix, f.hashPathSuffix)

	obj := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:    hash,
			Nursery: true,
		},
		dataShards:      f.dataShards, /* TODO: consider just putting a reference to the engine in the object */
		parityShards:    f.parityShards,
		chunkSize:       f.chunkSize,
		reserve:         f.reserve,
		ring:            f.ring,
		logger:          f.logger,
		policy:          f.policy,
		client:          f.client,
		metadata:        map[string]string{},
		nurseryReplicas: f.nurseryReplicas,
		txnId:           vars["txnId"],
	}
	if idb, err := f.getDB(vars["device"]); err == nil {
		obj.idb = idb
		if item, err := idb.Lookup(hash, shardAny, false); err == nil && item != nil {
			obj.IndexDBItem = *item
			if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
				return nil, fmt.Errorf("Error parsing metadata: %v", err)
			}
		} //TODO: dfg handle lookup err here
		return obj, nil
	}
	return nil, errors.New("Unable to open database")
}

func (f *ecEngine) GetReplicationDevice(oring ring.Ring, dev *ring.Device, r *Replicator) (ReplicationDevice, error) {
	return GetNurseryDevice(oring, dev, f.policy, r, f)
}

func (f *ecEngine) ecShardGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	shardIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	item, err := idb.Lookup(vars["hash"], shardIndex, false)
	if err != nil || item == nil || item.Deletion {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}
	metadata := map[string]string{}
	if err = json.Unmarshal(item.Metabytes, &metadata); err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	writer.Header().Set("Ec-Shard-Index", metadata["Ec-Shard-Index"])
	fl, err := os.Open(item.Path)
	if err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer fl.Close()
	http.ServeContent(writer, request, item.Path, time.Unix(item.Timestamp, 0), fl)
}

func (f *ecEngine) ecShardPostHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	shardIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	rStatus, err := idb.StablePost(vars["hash"], shardIndex, request)
	if err != nil {
		srv.GetLogger(request).Error("error in StablePost", zap.Error(err))
	}
	srv.StandardResponse(writer, rStatus)
	return
}

func (f *ecEngine) ecShardPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	shardIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	rStatus, err := idb.StablePut(vars["hash"], shardIndex, request)
	if err != nil {
		srv.GetLogger(request).Error("error in StablePut", zap.Error(err))
	}
	srv.StandardResponse(writer, rStatus)
	return
}

func (f *ecEngine) ecNurseryPutHandler(writer http.ResponseWriter, request *http.Request) {
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
	timestamp := timestampTime.UnixNano()

	deletion, err := strconv.ParseBool(request.Header.Get("Deletion"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	method := "PUT"
	if deletion {
		method = "DELETE"
	}

	metadata := make(map[string]string)
	for key := range request.Header {
		if strings.HasPrefix(key, "Meta-") {
			if key == "Meta-Name" {
				metadata["name"] = request.Header.Get(key)
			} else if key == "Meta-Etag" {
				metadata["ETag"] = request.Header.Get(key)
			} else {
				metadata[http.CanonicalHeaderKey(key[5:])] = request.Header.Get(key)
			}
		}
	}

	var atm fs.AtomicFileWriter
	if !deletion {
		atm, err = idb.TempFile(vars["hash"], 0, timestamp, 0, true)
		if err != nil {
			srv.GetLogger(request).Error("Error opening file for writing", zap.Error(err))
			srv.StandardResponse(writer, http.StatusInternalServerError)
			return
		}
		if atm == nil {
			srv.StandardResponse(writer, http.StatusCreated)
			return
		}
		defer atm.Abandon()

		n, err := common.Copy(request.Body, atm)
		if err == io.ErrUnexpectedEOF || (request.ContentLength >= 0 && n != request.ContentLength) {
			srv.StandardResponse(writer, 499)
			return
		} else if err != nil {
			srv.GetLogger(request).Error("Error writing to file", zap.Error(err))
			srv.StandardResponse(writer, http.StatusInternalServerError)
			return
		}
	}
	if err := idb.Commit(atm, vars["hash"], 0, timestamp, method, metadata, true, ""); err != nil {
		srv.GetLogger(request).Error("Error committing object to index", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		srv.StandardResponse(writer, http.StatusCreated)
	}
}

func (f *ecEngine) ecReconstructHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	o, err := f.New(vars, false, nil)
	if err != nil {
		srv.GetLogger(request).Error("Unable to open object.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	eco, ok := o.(*ecObject)
	if !ok {
		srv.GetLogger(request).Error("Type assertion failed.")
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	err = eco.Reconstruct()
	if err != nil {
		srv.GetLogger(request).Error("Unable to reconstruct.", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	srv.StandardResponse(writer, http.StatusOK)
}

func (f *ecEngine) ecShardDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	shardIndex, err := strconv.Atoi(vars["index"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	item, err := idb.Lookup(vars["hash"], shardIndex, true)
	if err != nil || item == nil {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}

	timestampTime, err := common.ParseDate(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	timestamp := timestampTime.UnixNano()
	if timestamp <= item.Timestamp {
		srv.StandardResponse(writer, http.StatusConflict)
		return
	}
	if err := idb.Remove(item.Hash, item.Shard, item.Timestamp, item.Nursery); err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		srv.StandardResponse(writer, http.StatusNoContent)
	}
}

func (f *ecEngine) GetObjectsToReplicate(prirep PriorityRepJob, c chan ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	idb, err := f.getDB(prirep.FromDevice.Device)
	if err != nil {
		f.logger.Error("error getting local db", zap.Error(err))
		return
	}
	startHash, stopHash := idb.RingPartRange(int(prirep.Partition))
	items, err := idb.List(startHash, stopHash, "", 0)
	if len(items) == 0 {
		return
	}
	url := fmt.Sprintf("%s://%s:%d/ec-partition/%s/%d", prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device, prirep.Partition)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
	req.Header.Set("User-Agent", "nursery-stabilizer")
	resp, err := f.client.Do(req)

	var remoteItems []*IndexDBItem
	if err == nil && (resp.StatusCode/100 == 2 || resp.StatusCode == 404) {
		if data, err := ioutil.ReadAll(resp.Body); err == nil {
			if err = json.Unmarshal(data, &remoteItems); err != nil {
				f.logger.Error("error unmarshaling partition list", zap.Error(err))
			}
		} else {
			f.logger.Error("error reading partition list", zap.Error(err))
		}
	}
	if err != nil {
		f.logger.Error("error getting local partition list", zap.Error(err))
		return
	}
	rii := 0
	for _, item := range items {
		if item.Nursery {
			continue
		}
		sendItem := true
		for rii < len(remoteItems) {
			if remoteItems[rii].Hash > item.Hash {
				break
			}
			if remoteItems[rii].Hash < item.Hash {
				rii++
				continue
			}
			if remoteItems[rii].Hash == item.Hash &&
				remoteItems[rii].Timestamp == item.Timestamp &&
				remoteItems[rii].Nursery == item.Nursery &&
				remoteItems[rii].Deletion == item.Deletion {
				sendItem = false
			}
			rii++
			break
		}
		if sendItem {
			obj := &ecObject{
				IndexDBItem:  *item,
				idb:          idb,
				dataShards:   f.dataShards,
				parityShards: f.parityShards,
				chunkSize:    f.chunkSize,
				reserve:      f.reserve,
				ring:         f.ring,
				logger:       f.logger,
				policy:       f.policy,
				client:       f.client,
				metadata:     map[string]string{},
			}
			if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
				//TODO: this should quarantine right?
				f.logger.Error("error unmarshal metabytes", zap.Error(err))
				continue
			}
			if obj.Path, err = idb.WholeObjectPath(obj.Hash, obj.Shard, obj.Timestamp, obj.Nursery); err != nil {
				//TODO: this should quarantine right?
				f.logger.Error("error building obj path", zap.Error(err))
				continue
			}
			select {
			case c <- obj:
			case <-cancel:
				return
			}
		}
	}
}

func (f *ecEngine) listPartitionHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := f.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	part, err := strconv.Atoi(vars["partition"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	startHash, stopHash := idb.RingPartRange(part)
	items, err := idb.List(startHash, stopHash, "", 0)
	if err != nil {
		f.logger.Error("error listing idb", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	if data, err := json.Marshal(items); err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
		return
	} else {
		f.logger.Error("error marshaling listing idb", zap.Error(err))
	}
	srv.StandardResponse(writer, http.StatusInternalServerError)
	return
}

func (f *ecEngine) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
	addRoute("PUT", "/ec-nursery/:device/:hash", f.ecNurseryPutHandler)
	addRoute("GET", "/ec-shard/:device/:hash/:index", f.ecShardGetHandler)
	addRoute("PUT", "/ec-shard/:device/:hash/:index", f.ecShardPutHandler)
	addRoute("DELETE", "/ec-shard/:device/:hash/:index", f.ecShardDeleteHandler)
	addRoute("POST", "/ec-shard/:device/:hash/:index", f.ecShardPostHandler)
	addRoute("GET", "/ec-partition/:device/:partition", f.listPartitionHandler)
	addRoute("PUT", "/ec-reconstruct/:device/:account/:container/*obj", f.ecReconstructHandler)
}

func (f *ecEngine) GetObjectsToStabilize(device string, c chan ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	idb, err := f.getDB(device)
	if err != nil {
		return
	}

	idb.ExpireObjects()

	items, err := idb.ListObjectsToStabilize()
	if err != nil {
		return
	}

	for _, item := range items {
		obj := &ecObject{
			IndexDBItem:     *item,
			idb:             idb,
			policy:          f.policy,
			metadata:        map[string]string{},
			ring:            f.ring,
			logger:          f.logger,
			reserve:         f.reserve,
			dataShards:      f.dataShards,
			parityShards:    f.parityShards,
			chunkSize:       f.chunkSize,
			client:          f.client,
			nurseryReplicas: f.nurseryReplicas,
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
func ecEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
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
	dbPartPower, err := policy.GetDbPartPower()
	if err != nil {
		return nil, err
	}
	subdirs, err := policy.GetDbSubDirs()
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
	httpClient := &http.Client{
		Timeout:   120 * time.Minute,
		Transport: transport,
	}
	engine := &ecEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		policy:         policy.Index,
		logger:         logger,
		ring:           r,
		idbs:           map[string]*IndexDB{},
		dbPartPower:    int(dbPartPower),
		numSubDirs:     subdirs,
		client:         httpClient,
	}
	if config.HasSection("tracing") {
		clientTracer, _, err := tracing.Init("ecengine-client", logger, config.GetSection("tracing"))
		if err != nil {
			return nil, fmt.Errorf("Error setting up tracer: %v", err)
		}
		enableHTTPTrace := config.GetBool("tracing", "enable_httptrace", true)
		engine.client, err = client.NewTracingClient(clientTracer, httpClient, enableHTTPTrace)
		if err != nil {
			return nil, fmt.Errorf("Error setting up tracing client: %v", err)
		}
	}
	if engine.dataShards, err = strconv.Atoi(policy.Config["data_shards"]); err != nil {
		return nil, err
	}
	if engine.parityShards, err = strconv.Atoi(policy.Config["parity_shards"]); err != nil {
		return nil, err
	}
	if engine.chunkSize, err = strconv.Atoi(policy.Config["chunk_size"]); err != nil {
		engine.chunkSize = 1 << 20
	}
	if engine.nurseryReplicas, err = strconv.Atoi(policy.Config["nursery_replicas"]); err != nil {
		engine.nurseryReplicas = 3
	}
	return engine, nil
}

func init() {
	RegisterObjectEngine("hec", ecEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = ecEngineConstructor
var _ ObjectEngine = &ecEngine{}
var _ PolicyHandlerRegistrator = &ecEngine{}
