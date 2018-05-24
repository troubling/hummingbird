package objectserver

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/bits"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
)

const (
	roShard = 0
)

func init() {
	RegisterObjectEngine("rep", repEngineConstructor)
}

var _ ObjectEngineConstructor = repEngineConstructor

func repEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	rng, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index)
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
	return &repEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        config.GetInt("app:object-server", "fallocate_reserve", 0),
		policy:         policy.Index,
		ring:           rng,
		idbs:           map[string]*IndexDB{},
		dbPartPower:    int(dbPartPower),
		numSubDirs:     subdirs,
		logger:         zap.L(),
		client: &http.Client{
			Timeout:   120 * time.Minute,
			Transport: transport,
		},
	}, nil
}

var _ ObjectEngine = &repEngine{}

type repEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	policy         int
	ring           ring.Ring
	logger         *zap.Logger
	idbs           map[string]*IndexDB
	dblock         sync.Mutex
	dbPartPower    int
	numSubDirs     int
	client         *http.Client
}

func (re *repEngine) getDB(device string) (*IndexDB, error) {
	re.dblock.Lock()
	defer re.dblock.Unlock()
	if idb, ok := re.idbs[device]; ok && idb != nil {
		return idb, nil
	}
	var err error
	dbpath := filepath.Join(re.driveRoot, device, PolicyDir(re.policy), "rep.db")
	path := filepath.Join(re.driveRoot, device, PolicyDir(re.policy), "rep")
	temppath := filepath.Join(re.driveRoot, device, "tmp")
	ringPartPower := bits.Len64(re.ring.PartitionCount() - 1)
	re.idbs[device], err = NewIndexDB(dbpath, path, temppath, ringPartPower, re.dbPartPower, re.numSubDirs, re.reserve, re.logger)
	if err != nil {
		return nil, err
	}
	return re.idbs[device], nil
}

func (re *repEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (Object, error) {
	//TODO: not sure if here- but need to show x-backend timestamp on deleted objects
	hash := ObjHash(vars, re.hashPathPrefix, re.hashPathSuffix)
	obj := &repObject{
		IndexDBItem: IndexDBItem{
			Hash: hash,
		},
		ring:     re.ring,
		policy:   re.policy,
		reserve:  re.reserve,
		metadata: map[string]string{},
		asyncWG:  asyncWG,
		client:   re.client,
		txnId:    vars["txnId"],
	}
	if idb, err := re.getDB(vars["device"]); err == nil {
		obj.idb = idb
		if item, err := idb.Lookup(hash, roShard, false); err == nil && item != nil {
			obj.IndexDBItem = *item
			if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
				return nil, fmt.Errorf("Error parsing metadata: %v", err)
			}
		} //TODO: dfg handle lookup err here
		return obj, nil
	} else {
		return nil, err
	}
}

func (re *repEngine) GetReplicationDevice(oring ring.Ring, dev *ring.Device, r *Replicator) (ReplicationDevice, error) {
	return GetNurseryDevice(oring, dev, re.policy, r, re)
}

func (re *repEngine) GetObjectsToReplicate(prirep PriorityRepJob, c chan ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	idb, err := re.getDB(prirep.FromDevice.Device)
	if err != nil {
		re.logger.Error("error getting local db", zap.Error(err))
		return
	}
	startHash, stopHash := idb.RingPartRange(int(prirep.Partition))
	items, err := idb.List(startHash, stopHash, "", 0)
	if len(items) == 0 {
		return
	}
	url := fmt.Sprintf("%s://%s:%d/rep-partition/%s/%d", prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device, prirep.Partition)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
	req.Header.Set("User-Agent", "nursery-stabilizer")
	resp, err := re.client.Do(req)

	var remoteItems []*IndexDBItem
	if err == nil && (resp.StatusCode/100 == 2 || resp.StatusCode == 404) {
		if data, err := ioutil.ReadAll(resp.Body); err == nil {
			if err = json.Unmarshal(data, &remoteItems); err != nil {
				re.logger.Error("error unmarshaling partition list", zap.Error(err))
			}
		} else {
			re.logger.Error("error reading partition list", zap.Error(err))
		}
	}
	if err != nil {
		re.logger.Error("error getting local partition list", zap.Error(err))
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
		obj := &repObject{
			IndexDBItem: *item,
			reserve:     re.reserve,
			ring:        re.ring,
			policy:      re.policy,
			idb:         idb,
			metadata:    map[string]string{},
			client:      re.client,
			txnId:       fmt.Sprintf("%s-%s", common.UUID(), prirep.FromDevice.Device),
		}
		if err = json.Unmarshal(item.Metabytes, &obj.metadata); err != nil {
			//TODO: this should prob quarantine- also in ec thing that does this too
			continue
		}
		if obj.Path, err = idb.WholeObjectPath(item.Hash, item.Shard, item.Timestamp, item.Nursery); err != nil {
			continue // TODO: quarantine here too
		}
		if sendItem {
			select {
			case c <- obj:
			case <-cancel:
				return
			}
		}
	}
}

func (re *repEngine) GetObjectsToStabilize(device string, c chan ObjectStabilizer, cancel chan struct{}) {
	defer close(c)
	idb, err := re.getDB(device)
	if err != nil {
		return
	}

	items, err := idb.ListObjectsToStabilize()
	if err != nil {
		return
	}

	for _, item := range items {
		obj := &repObject{
			IndexDBItem: *item,
			reserve:     re.reserve,
			ring:        re.ring,
			policy:      re.policy,
			idb:         idb,
			metadata:    map[string]string{},
			client:      re.client,
			txnId:       fmt.Sprintf("%s-%s", common.UUID(), device),
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

func (re *repEngine) listPartitionHandler(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := re.getDB(vars["device"])
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
		re.logger.Error("error listing idb", zap.Error(err))
		srv.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	if data, err := json.Marshal(items); err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
		return
	} else {
		re.logger.Error("error marshaling listing idb", zap.Error(err))
	}
	srv.StandardResponse(writer, http.StatusInternalServerError)
	return
}

func (re *repEngine) putStableObject(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := re.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	rStatus, err := idb.StablePut(vars["hash"], roShard, request)
	if err != nil {
		re.logger.Error("error in StablePut", zap.Error(err))
	}
	srv.StandardResponse(writer, rStatus)
	return
}

func (re *repEngine) postStableObject(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := re.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	rStatus, err := idb.StablePost(vars["hash"], roShard, request)
	if err != nil {
		srv.GetLogger(request).Error("error in StablePost", zap.Error(err))
	}
	srv.StandardResponse(writer, rStatus)
	return
}

func (re *repEngine) deleteStableObject(writer http.ResponseWriter, request *http.Request) {
	vars := srv.GetVars(request)
	idb, err := re.getDB(vars["device"])
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	reqTimeStamp, err := common.ParseDate(request.Header.Get("X-Timestamp"))
	if err != nil {
		srv.StandardResponse(writer, http.StatusBadRequest)
	}
	item, err := idb.Lookup(vars["hash"], roShard, true)
	if err != nil || item == nil {
		srv.StandardResponse(writer, http.StatusNotFound)
		return
	}
	if reqTimeStamp.UnixNano() < item.Timestamp {
		srv.StandardResponse(writer, http.StatusConflict)
		return
	}

	if err := idb.Remove(item.Hash, item.Shard, item.Timestamp, item.Nursery); err != nil {
		srv.StandardResponse(writer, http.StatusInternalServerError)
	} else {
		srv.StandardResponse(writer, http.StatusNoContent)
	}
}

func (re *repEngine) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
	addRoute("GET", "/rep-partition/:device/:partition", re.listPartitionHandler)
	addRoute("PUT", "/rep-obj/:device/:hash", re.putStableObject)
	addRoute("POST", "/rep-obj/:device/:hash", re.postStableObject)
	addRoute("DELETE", "/rep-obj/:device/:hash", re.deleteStableObject)
}
