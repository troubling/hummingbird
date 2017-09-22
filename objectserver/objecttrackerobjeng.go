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

	"github.com/gholt/kvt"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/objectserver/internal"
	"go.uber.org/zap"
)

func init() {
	RegisterObjectEngine("objecttracker", objectTrackerEngineConstructor)
}

var _ ObjectEngineConstructor = objectTrackerEngineConstructor

func objectTrackerEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}
	ring, err := GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index)
	if err != nil {
		return nil, err
	}
	return &objectTrackerEngine{
		devicesPath:      config.GetDefault("app:object-server", "devices", "/srv/node"),
		hashPathPrefix:   hashPathPrefix,
		hashPathSuffix:   hashPathSuffix,
		fallocateReserve: config.GetInt("app:object-server", "fallocate_reserve", 0),
		reclaimAge:       int64(config.GetInt("app:object-server", "reclaim_age", int64(common.ONE_WEEK))),
		ringPartPower:    uint(bits.Len64(ring.PartitionCount())),
		objectTrackers:   map[string]*internal.ObjectTracker{},
	}, nil
}

var _ ObjectEngine = &objectTrackerEngine{}

type objectTrackerEngine struct {
	devicesPath        string
	hashPathPrefix     string
	hashPathSuffix     string
	fallocateReserve   int64
	reclaimAge         int64
	ringPartPower      uint
	objectTrackersLock sync.Mutex
	objectTrackers     map[string]*internal.ObjectTracker
}

func (ote *objectTrackerEngine) New(vars map[string]string, needData bool, asyncWG *sync.WaitGroup) (Object, error) {
	ote.objectTrackersLock.Lock()
	ot := ote.objectTrackers[vars["device"]]
	if ot == nil {
		var err error
		ot, err = internal.NewObjectTracker(
			path.Join(ote.devicesPath, vars["device"]),
			ote.ringPartPower,
			6,  // diskParkPower, hardcoded for now, giving 64 databases per device
			64, // chexorsMod, hardcoded for now
			zap.L(),
		)
		if err != nil {
			ote.objectTrackersLock.Unlock()
			return nil, err
		}
		ote.objectTrackers[vars["device"]] = ot
	}
	ote.objectTrackersLock.Unlock()
	return &objectTrackerObject{
		fallocateReserve: ote.fallocateReserve,
		reclaimAge:       ote.reclaimAge,
		asyncWG:          asyncWG,
		objectTracker:    ot,
		hash:             ObjHash(vars, ote.hashPathPrefix, ote.hashPathSuffix),
	}, nil
}

var _ Object = &objectTrackerObject{}

type objectTrackerObject struct {
	fallocateReserve int64
	reclaimAge       int64
	asyncWG          *sync.WaitGroup
	objectTracker    *internal.ObjectTracker
	hash             string
	loaded           bool
	timestamp        int64
	deletion         bool
	metadata         map[string]string
	path             string
	atomicFileWriter fs.AtomicFileWriter
}

func (oto *objectTrackerObject) load() error {
	if oto.loaded {
		return nil
	}
	var metabytes []byte
	var err error
	oto.timestamp, oto.deletion, _, metabytes, oto.path, err = oto.objectTracker.Lookup(oto.hash, 0)
	if err != nil {
		return err
	}
	metastore := kvt.Store{}
	if err = json.Unmarshal(metabytes, &metastore); err != nil {
		return err
	}
	oto.metadata = make(map[string]string, len(metastore))
	for k, vt := range metastore {
		if vt.Value != nil {
			oto.metadata[k] = *vt.Value
		}
	}
	oto.loaded = true
	return nil
}

func (oto *objectTrackerObject) Metadata() map[string]string {
	if err := oto.load(); err != nil {
		// We have no way to return an error, so we'll just do this for now.
		// Maybe we should refactor to be able to return an error.
		return nil
	}
	return oto.metadata
}

func (oto *objectTrackerObject) ContentLength() int64 {
	if err := oto.load(); err != nil {
		return -1
	}
	if contentLength, err := strconv.ParseInt(oto.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

func (oto *objectTrackerObject) Quarantine() error {
	// TODO:
	return nil
}

func (oto *objectTrackerObject) Exists() bool {
	if err := oto.load(); err != nil {
		// We have no way to return an error, so we'll just do this for now.
		// Maybe we should refactor to be able to return an error.
		return false
	}
	return !oto.deletion
}

func (oto *objectTrackerObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if err := oto.load(); err != nil {
		return 0, err
	}
	f, err := os.Open(oto.path)
	if err != nil {
		return 0, err
	}
	if len(dsts) == 1 {
		written, err = io.Copy(dsts[0], f)
	} else {
		written, err = common.Copy(f, dsts...)
	}
	if err == nil {
		err = f.Close()
	} else {
		f.Close()
	}
	return written, err
}

func (oto *objectTrackerObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	if err := oto.load(); err != nil {
		return 0, err
	}
	f, err := os.Open(oto.path)
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

func (oto *objectTrackerObject) Repr() string {
	return fmt.Sprintf("objectTrackerObject<%s, %d>", oto.hash, oto.timestamp)
}

func (oto *objectTrackerObject) SetData(size int64) (io.Writer, error) {
	if oto.atomicFileWriter != nil {
		oto.atomicFileWriter.Abandon()
	}
	var err error
	oto.atomicFileWriter, err = oto.objectTracker.TempFile(oto.hash, 0, math.MaxInt64, size)
	return oto.atomicFileWriter, err
}

func (oto *objectTrackerObject) commit(metadata map[string]string, deletion bool) error {
	timestampStr, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("no timestamp in metadata")
	}
	timestampTime, err := common.ParseDate(timestampStr)
	if err != nil {
		return err
	}
    timestamp := timestampTime.UnixNano()
	metastore := make(kvt.Store, len(metadata))
	for k, v := range metadata {
		if v == "" {
			metastore.DeleteTimestamped(k, timestamp)
		} else {
			metastore.SetTimestamped(k, v, timestamp)
		}
	}
	var metabytes []byte
	if metabytes, err = json.Marshal(metastore); err != nil {
		return err
	}
	err = oto.objectTracker.Commit(oto.atomicFileWriter, oto.hash, 0, timestamp, deletion, metastore.Hash(), metabytes)
	oto.atomicFileWriter = nil
	return err
}

func (oto *objectTrackerObject) Commit(metadata map[string]string) error {
	return oto.commit(metadata, false)
}

func (oto *objectTrackerObject) CommitMetadata(metadata map[string]string) error {
	return oto.commit(metadata, false)
}

func (oto *objectTrackerObject) Delete(metadata map[string]string) error {
	return oto.commit(metadata, true)
}

func (oto *objectTrackerObject) Close() error {
	if oto.atomicFileWriter != nil {
		oto.atomicFileWriter.Abandon()
		oto.atomicFileWriter = nil
	}
	return nil
}
