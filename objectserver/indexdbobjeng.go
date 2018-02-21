package objectserver

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

func init() {
	RegisterObjectEngine("index.db", indexDBEngineConstructor)
}

var _ ObjectEngineConstructor = indexDBEngineConstructor

func indexDBEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}
	ringg, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index)
	if err != nil {
		return nil, err
	}
	ringPartPower := bits.Len64(ringg.PartitionCount() - 1)
	dbPartPower := 0
	if policy.Config["db_part_power"] != "" {
		dbPartPowerInt64, err := strconv.ParseInt(policy.Config["db_part_power"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Could not parse db_part_power value %q: %s", policy.Config["db_part_power"], err)
		}
		dbPartPower = int(dbPartPowerInt64)
	}
	subdirs := 64
	if policy.Config["subdirs"] != "" {
		subdirsInt64, err := strconv.ParseInt(policy.Config["subdirs"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Could not parse subdirs value %q: %s", policy.Config["subdirs"], err)
		}
		subdirs = int(subdirsInt64)
	}
	devicespath := config.GetDefault("app:object-server", "devices", "/srv/node")
	dbspath := config.GetDefault("app:object-server", "dbs", "")
	d, err := os.Open(devicespath)
	if err != nil {
		return nil, err
	}
	dirs, err := d.Readdirnames(-1)
	d.Close()
	if err != nil {
		return nil, err
	}
	indexDBs := map[string]*IndexDB{}
	for _, dirname := range dirs {
		dirpath := path.Join(devicespath, dirname)
		fi, err := os.Stat(dirpath)
		if err != nil {
			return nil, err
		}
		if fi.IsDir() {
			// TODO: IsMount check based on config's mount_check.
			var dbpath string
			if dbspath == "" {
				dbpath = path.Join(dirpath, PolicyDir(policy.Index))
			} else {
				dbpath = path.Join(dbspath, dirname, PolicyDir(policy.Index))
			}
			filepath := path.Join(dirpath, PolicyDir(policy.Index))
			temppath := path.Join(dirpath, PolicyDir(policy.Index), "temp")
			indexDBs[dirname], err = NewIndexDB(
				dbpath,
				filepath,
				temppath,
				ringPartPower,
				dbPartPower,
				subdirs,
				zap.L(),
			)
			if err != nil {
				return nil, err
			}
		}
	}
	return &indexDBEngine{
		devicespath:      devicespath,
		hashPathPrefix:   hashPathPrefix,
		hashPathSuffix:   hashPathSuffix,
		fallocateReserve: config.GetInt("app:object-server", "fallocate_reserve", 0),
		reclaimAge:       int64(config.GetInt("app:object-server", "reclaim_age", int64(common.ONE_WEEK))),
		indexDBs:         indexDBs,
	}, nil
}

var _ ObjectEngine = &indexDBEngine{}

type indexDBEngine struct {
	devicespath      string
	hashPathPrefix   string
	hashPathSuffix   string
	fallocateReserve int64
	reclaimAge       int64
	indexDBs         map[string]*IndexDB
}

func (idbe *indexDBEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (Object, error) {
	indexDB := idbe.indexDBs[vars["device"]]
	if indexDB == nil {
		panic(vars["device"])
	}
	return &indexDBObject{
		fallocateReserve: idbe.fallocateReserve,
		reclaimAge:       idbe.reclaimAge,
		asyncWG:          asyncWG,
		indexDB:          indexDB,
		hash:             ObjHash(vars, idbe.hashPathPrefix, idbe.hashPathSuffix),
	}, nil
}

func (idbe *indexDBEngine) GetReplicationDevice(oring ring.Ring, dev *ring.Device, policy int, r *Replicator) (ReplicationDevice, error) {
	return nil, fmt.Errorf("not running replication here yet")
}

var _ Object = &indexDBObject{}

type indexDBObject struct {
	fallocateReserve int64
	reclaimAge       int64
	asyncWG          *sync.WaitGroup
	indexDB          *IndexDB
	hash             string
	loaded           bool
	timestamp        int64
	deletion         bool
	metadata         map[string]string
	path             string
	atomicFileWriter fs.AtomicFileWriter
}

func (idbo *indexDBObject) load() error {
	if idbo.loaded {
		return nil
	}
	var metabytes []byte
	//var err error
	dbItem, err := idbo.indexDB.Lookup(idbo.hash, 0, false)
	if err != nil {
		return err
	}
	idbo.metadata = map[string]string{}
	if dbItem != nil {
		idbo.timestamp, idbo.deletion, metabytes, idbo.path = dbItem.Timestamp, dbItem.Deletion, dbItem.Metabytes, dbItem.Path

		if err = json.Unmarshal(metabytes, &idbo.metadata); err != nil {
			return err
		}
	}
	idbo.loaded = true
	return nil
}

func (idbo *indexDBObject) Metadata() map[string]string {
	if err := idbo.load(); err != nil {
		// We have no way to return an error, so we'll just do this for now.
		// Maybe we should refactor to be able to return an error.
		return nil
	}
	return idbo.metadata
}

func (idbo *indexDBObject) ContentLength() int64 {
	if err := idbo.load(); err != nil {
		return -1
	}
	if contentLength, err := strconv.ParseInt(idbo.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

func (idbo *indexDBObject) Quarantine() error {
	// TODO: Not sure on this one. If file corruption is detected we'll need to
	// just remove the entry for an actively replicated system, but for an
	// event replicated system we'll need a way to notify "something" of the
	// event. Then again, maybe that's the responsibility of whoever calls this
	// Quarantine method; I'm just not sure.
	return nil
}

func (idbo *indexDBObject) Exists() bool {
	if err := idbo.load(); err != nil {
		// We have no way to return an error, so we'll just do this for now.
		// Maybe we should refactor to be able to return an error.
		return false
	}
	if idbo.deletion {
		return false
	}
	return idbo.path != ""
}

func (idbo *indexDBObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if err := idbo.load(); err != nil {
		return 0, err
	}
	var f *os.File
	f, err = os.Open(idbo.path)
	if err != nil {
		return 0, err
	}
	if len(dsts) == 1 {
		written, err = io.Copy(dsts[0], f)
	} else {
		written, err = common.Copy(f, dsts...)
	}
	if f != nil {
		if err == nil {
			err = f.Close()
		} else {
			f.Close()
		}
	}
	return written, err
}

func (idbo *indexDBObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	if err := idbo.load(); err != nil {
		return 0, err
	}
	f, err := os.Open(idbo.path)
	if err != nil {
		return 0, err
	}
	if _, err := f.Seek(start, os.SEEK_SET); err != nil {
		f.Close()
		return 0, err
	}
	written, err := common.CopyN(f, end-start, w)
	if err == nil {
		err = f.Close()
	} else {
		f.Close()
	}
	return written, err
}

func (idbo *indexDBObject) Repr() string {
	return fmt.Sprintf("indexDBObject<%s, %d>", idbo.hash, idbo.timestamp)
}

func (idbo *indexDBObject) SetData(size int64) (io.Writer, error) {
	if idbo.atomicFileWriter != nil {
		idbo.atomicFileWriter.Abandon()
	}
	var err error
	idbo.atomicFileWriter, err = idbo.indexDB.TempFile(idbo.hash, 0, math.MaxInt64, size, true)
	return idbo.atomicFileWriter, err
}

func (idbo *indexDBObject) commit(metadata map[string]string, deletion bool) error {
	var timestamp int64
	if idbo.atomicFileWriter != nil || deletion {
		timestampStr, ok := metadata["X-Timestamp"]
		if !ok {
			return errors.New("no timestamp in metadata")
		}
		timestampTime, err := common.ParseDate(timestampStr)
		if err != nil {
			return err
		}
		timestamp = timestampTime.UnixNano()
	}
	metabytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	err = idbo.indexDB.Commit(idbo.atomicFileWriter, idbo.hash, 0, timestamp, deletion, MetadataHash(metadata), metabytes, true, "")
	idbo.atomicFileWriter = nil
	return err
}

func (idbo *indexDBObject) Commit(metadata map[string]string) error {
	return idbo.commit(metadata, false)
}

func (idbo *indexDBObject) CommitMetadata(metadata map[string]string) error {
	return idbo.commit(metadata, false)
}

func (idbo *indexDBObject) Delete(metadata map[string]string) error {
	return idbo.commit(metadata, true)
}

func (idbo *indexDBObject) Close() error {
	if idbo.atomicFileWriter != nil {
		idbo.atomicFileWriter.Abandon()
		idbo.atomicFileWriter = nil
	}
	return nil
}
