package objectserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
)

var _ Object = &replObject{}

type replObject struct {
	IndexDBItem
	reserve          int64
	asyncWG          *sync.WaitGroup
	idb              *IndexDB
	rng              ring.Ring
	loaded           bool
	atomicFileWriter fs.AtomicFileWriter
	metadata         map[string]string
	client           *http.Client
}

func (ro *replObject) Metadata() map[string]string {
	return ro.metadata
}

func (ro *replObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(ro.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

func (ro *replObject) Quarantine() error {
	return errors.New("Unimplemented")
}

func (ro *replObject) Exists() bool {
	if ro.Deletion == true {
		return false
	}
	return ro.Path != ""
}

func (ro *replObject) Copy(dsts ...io.Writer) (written int64, err error) {
	var f *os.File
	f, err = os.Open(ro.Path)
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

func (ro *replObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	f, err := os.Open(ro.Path)
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

func (ro *replObject) Repr() string {
	return fmt.Sprintf("replObject<%s, %d>", ro.Hash, ro.Timestamp)
}

func (ro *replObject) SetData(size int64) (io.Writer, error) {
	if ro.atomicFileWriter != nil {
		ro.atomicFileWriter.Abandon()
	}
	var err error
	ro.atomicFileWriter, err = ro.idb.TempFile(ro.Hash, roShard, math.MaxInt64, size, true)
	return ro.atomicFileWriter, err
}

func (ro *replObject) commit(metadata map[string]string, method string) error {
	var timestamp int64
	timestampStr, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("no timestamp in metadata")
	}
	timestampTime, err := common.ParseDate(timestampStr)
	if err != nil {
		return err
	}
	timestamp = timestampTime.UnixNano()
	metabytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	err = ro.idb.Commit(ro.atomicFileWriter, ro.Hash, roShard, timestamp, method, MetadataHash(metadata), metabytes, true, "")
	ro.atomicFileWriter = nil
	return err
}

func (ro *replObject) Commit(metadata map[string]string) error {
	return ro.commit(metadata, "PUT")
}

func (ro *replObject) CommitMetadata(metadata map[string]string) error {
	return ro.commit(metadata, "POST")
}

func (ro *replObject) Delete(metadata map[string]string) error {
	return ro.commit(metadata, "DELETE")
}

func (ro *replObject) Close() error {
	if ro.atomicFileWriter != nil {
		ro.atomicFileWriter.Abandon()
		ro.atomicFileWriter = nil
	}
	return nil
}

func (ro *replObject) canStabilize(rng ring.Ring, dev *ring.Device, policy int) (bool, error) {
	if ro.Deletion {
		return false, fmt.Errorf("you just send deletions")
	}
	metadata := ro.Metadata()
	ns := strings.SplitN(metadata["name"], "/", 4)
	if len(ns) != 4 {
		return false, fmt.Errorf("invalid metadata name: %s", metadata["name"])
	}
	partition := rng.GetPartition(ns[1], ns[2], ns[3])
	if _, handoff := rng.GetJobNodes(partition, dev.Id); handoff {
		return false, nil
	}
	nodes := rng.GetNodes(partition)
	goodNodes := uint64(0)
	for _, device := range nodes {
		if device.Ip == dev.Ip && device.Port == dev.Port && device.Device == device.Device {
			continue
		}
		url := fmt.Sprintf("%s://%s:%d/%s/%d%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(metadata["name"]))
		req, err := http.NewRequest("HEAD", url, nil)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.FormatInt(int64(policy), 10))
		req.Header.Set("User-Agent", "nursery-stabilizer")
		resp, err := ro.client.Do(req)
		if err == nil && (resp.StatusCode/100 == 2) &&
			resp.Header.Get("X-Timestamp") != "" &&
			resp.Header.Get("X-Timestamp") ==
				metadata["X-Timestamp"] {
			goodNodes++
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return goodNodes+1 == rng.ReplicaCount(), nil
}

func (ro *replObject) stabilizeDelete(rng ring.Ring, dev *ring.Device, policy int) error {
	metadata := ro.Metadata()
	ns := strings.SplitN(metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", metadata["name"])
	}
	partition := rng.GetPartition(ns[1], ns[2], ns[3])
	nodes := rng.GetNodes(partition)
	var successes int64
	wg := sync.WaitGroup{}
	for _, node := range nodes {
		if node.Ip == dev.Ip && node.Port == dev.Port && node.Device == node.Device {
			continue
		}
		req, err := http.NewRequest("DELETE", fmt.Sprintf("%s://%s:%d/repl-obj/%s/%s", node.Scheme, node.Ip, node.Port, node.Device, ro.Hash), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Timestamp", ro.metadata["X-Timestamp"])
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			if resp, err := ro.client.Do(req); err == nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusConflict || resp.StatusCode == 404 {
					atomic.AddInt64(&successes, 1)
				}
			}
		}(req)
	}
	wg.Wait()
	if successes+1 != int64(len(nodes)) {
		return fmt.Errorf("could not stabilize DELETE to all primaries %d/%d", successes, len(nodes))
	}
	return ro.idb.Remove(ro.Hash, ro.Shard, ro.Timestamp, ro.Nursery)
}

func (ro *replObject) restabilize(rng ring.Ring, dev *ring.Device, policy int) error {
	ns := strings.SplitN(ro.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", ro.metadata["name"])
	}
	wg := sync.WaitGroup{}
	var successes int64
	partition := rng.GetPartition(ns[1], ns[2], ns[3])
	nodes := rng.GetNodes(partition)
	for _, node := range nodes {
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s:%d/repl-obj/%s/%s", node.Scheme, node.Ip, node.Port, node.Device, ro.Hash), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Timestamp", ro.metadata["X-Timestamp"])
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
		for k, v := range ro.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			if resp, err := ro.client.Do(req); err == nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusConflict {
					atomic.AddInt64(&successes, 1)
				}
			}
		}(req)
	}
	wg.Wait()
	if successes != int64(len(nodes)) {
		return fmt.Errorf("could not restabilize all primaries %d/%d", successes, len(nodes))
	}
	return ro.idb.SetStabilized(ro.Hash, ro.Shard, ro.Timestamp, false)
}

func (ro *replObject) Stabilize(rng ring.Ring, dev *ring.Device, policy int) error {
	if !ro.Nursery {
		return nil
	}
	if ro.Restabilize {
		return ro.restabilize(rng, dev, policy)
	}
	if ro.Deletion {
		return ro.stabilizeDelete(rng, dev, policy)
	}
	if cs, err := ro.canStabilize(rng, dev, policy); err != nil {
		return err
	} else if !cs {
		return nil
	}
	return ro.idb.SetStabilized(ro.Hash, roShard, ro.Timestamp, true)
}

func (ro *replObject) Replicate(prirep PriorityRepJob) error {
	if ro.Nursery {
		return fmt.Errorf("not replicating object in nursery")
	}
	_, isHandoff := ro.rng.GetJobNodes(prirep.Partition, prirep.FromDevice.Id)
	fp, err := os.Open(ro.Path)
	if err != nil {
		return err
	}
	defer fp.Close()
	req, err := http.NewRequest("PUT",
		fmt.Sprintf("%s://%s:%d/repl-obj/%s/%s",
			prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port,
			prirep.ToDevice.Device, ro.Hash), fp)
	if err != nil {
		return err
	}
	req.ContentLength = ro.ContentLength()
	req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
	for k, v := range ro.metadata {
		req.Header.Set("Meta-"+k, v)
	}
	resp, err := ro.client.Do(req)
	if err != nil {
		return fmt.Errorf("error syncing obj %s: %v", ro.Hash, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad status code %d syncing obj with  %s", resp.StatusCode, ro.Hash)
	}
	if isHandoff {
		return ro.idb.Remove(ro.Hash, ro.Shard, ro.Timestamp, ro.Nursery)
	}
	return nil
}
