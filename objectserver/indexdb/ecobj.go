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

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/objectserver"
)

type ecObject struct {
	IndexDBItem
	afw         fs.AtomicFileWriter
	idb         *IndexDB
	policy      int
	metadata    map[string]string
	ring        ring.Ring
	logger      *zap.Logger
	reserve     int64
	dataFrags   int
	parityFrags int
	chunkSize   int
	client      *http.Client
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
	if o.Deletion == true {
		return false
	}
	return o.Path != ""
}

func (o *ecObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if !o.Exists() {
		return 0, errors.New("Doesn't exist")
	}

	if o.Nursery {
		file, err := os.Open(o.Path)
		if err != nil {
			return 0, err
		}
		defer file.Close()
		return common.Copy(file, dsts...)
	}

	var dataFrags, parityFrags, chunkSize int
	if n, err := fmt.Sscanf(o.metadata["Ec-Scheme"], "%d/%d/%d", &dataFrags, &parityFrags, &chunkSize); err != nil || n != 3 {
		return 0, errors.New("Invalid scheme")
	}
	contentLength := o.ContentLength()
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return 0, fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	nodes := o.ring.GetNodes(o.ring.GetPartition(ns[1], ns[2], ns[3]))
	if len(nodes) < dataFrags+parityFrags {
		return 0, errors.New("Not enough nodes for scheme")
	}
	bodies := make([]io.Reader, len(nodes))
	for i, node := range nodes {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s:%d/ec-frag/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		resp, err := o.client.Do(req)
		if err != nil {
			continue
		}
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			continue
		}
		bodies[i] = resp.Body
	}
	ecGlue(dataFrags, parityFrags, bodies, chunkSize, contentLength, dsts...)
	return contentLength, nil
}

func (o *ecObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	return 0, errors.New("Unimplemented")
}

func (o *ecObject) Repr() string {
	return fmt.Sprintf("ecObject(%s)", o.Hash)
}

func (o *ecObject) SetData(size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = o.idb.TempFile(o.Hash, 0, math.MaxInt64, size, true); err != nil {
		return nil, fmt.Errorf("Error creating temp file: %v", err)
	}
	if err := o.afw.Preallocate(size, o.reserve); err != nil {
		o.afw.Abandon()
		return nil, objectserver.DriveFullError
	}
	return o.afw, nil
}

func (o *ecObject) commit(metadata map[string]string, deletion bool) error {
	var timestamp int64
	defer o.Close()
	if o.afw != nil || deletion {
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
	return o.idb.Commit(o.afw, o.Hash, 0, timestamp, deletion, MetadataHash(metadata), metabytes, true, "")
}

func (o *ecObject) Commit(metadata map[string]string) error {
	return o.commit(metadata, false)
}

func (o *ecObject) Delete(metadata map[string]string) error {
	return o.commit(metadata, true)
}

func (o *ecObject) CommitMetadata(metadata map[string]string) error {
	return errors.New("Unimplemented")
}

func (o *ecObject) Close() error {
	if o.afw != nil {
		defer o.afw.Abandon()
		o.afw = nil
	}
	return nil
}

func (o *ecObject) Replicate(prirep objectserver.PriorityRepJob) error {
	// If we are handoff, just replicate the shard and delete local shard
	if _, handoff := o.ring.GetJobNodes(prirep.Partition, prirep.FromDevice.Id); handoff {
		fp, err := os.Open(o.Path)
		if err != nil {
			return err
		}
		defer fp.Close()
		req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-frag/%s/%s/%d", prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device, o.Hash, o.Shard), fp)
		if err != nil {
			return err
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
		req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("%d/%d/%d", o.dataFrags, o.parityFrags, o.chunkSize))
		for k, v := range o.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		resp, err := o.client.Do(req)
		if err != nil {
			return fmt.Errorf("error syncing shard %s/%d: %v", o.Hash, o.Shard, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("bad status code %d syncing shard with  %s/%d", resp.StatusCode, o.Hash, o.Shard)
		}
		return o.idb.Remove(o.Hash, o.Shard, o.Timestamp, true)
	}
	// Else reconstruct the shard and copy over that
	var dataFrags, parityFrags, chunkSize int
	success := true
	if n, err := fmt.Sscanf(o.metadata["Ec-Scheme"], "%d/%d/%d", &dataFrags, &parityFrags, &chunkSize); err != nil || n != 3 {
		return errors.New("Invalid scheme")
	}
	contentLength := o.ContentLength()
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	nodes := o.ring.GetNodes(o.ring.GetPartition(ns[1], ns[2], ns[3]))
	if len(nodes) < dataFrags+parityFrags {
		return errors.New("Not enough nodes for scheme")
	}
	bodies := make([]io.Reader, len(nodes))
	toDeviceShard := -1
	for i, node := range nodes {
		if node.Id == prirep.ToDevice.Id {
			toDeviceShard = i
			continue
		}
		url := fmt.Sprintf("%s://%s:%d/ec-frag/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		resp, err := o.client.Do(req)
		if err != nil {
			continue
		}
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			continue
		}
		bodies[i] = resp.Body
	}
	if toDeviceShard == -1 {
		return fmt.Errorf("ToDevice %s:%d  %s is not in list of nodes for this object", prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device)
	}
	rp, wp := io.Pipe()
	defer wp.Close()
	defer rp.Close()
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-frag/%s/%s/%d", prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device, o.Hash, toDeviceShard), rp)
	if err != nil {
		return err
	}
	req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
	req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("%d/%d/%d", o.dataFrags, o.parityFrags, o.chunkSize))
	for k, v := range o.metadata {
		req.Header.Set("Meta-"+k, v)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(req *http.Request) {
		defer wg.Done()
		if resp, err := o.client.Do(req); err != nil {
			success = false
			return
		} else {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				success = false
				return
			}
		}
	}(req)
	var writer io.WriteCloser = wp
	err = ecReconstruct(dataFrags, parityFrags, bodies, chunkSize, contentLength, writer, toDeviceShard)
	writer.Close()
	wg.Wait()
	if !success {
		return fmt.Errorf("Failed to replicate")
	}
	return err
}

func (o *ecObject) Stabilize(ring ring.Ring, dev *ring.Device, policy int) error {
	wg := sync.WaitGroup{}
	success := true
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}

	partition := ring.GetPartition(ns[1], ns[2], ns[3])
	nodes := ring.GetNodes(partition)
	if len(nodes) != o.dataFrags+o.parityFrags {
		return fmt.Errorf("Ring doesn't match EC scheme (%d != %d).", len(nodes), o.dataFrags+o.parityFrags)
	}
	if o.Deletion {
		for i, node := range nodes {
			req, err := http.NewRequest("DELETE", fmt.Sprintf("%s://%s:%d/ec-frag/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
			if err != nil {
				return err
			}
			req.Header.Set("X-Timestamp", o.metadata["X-Timestamp"])
			req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
			wg.Add(1)
			go func(req *http.Request) {
				defer wg.Done()
				if resp, err := o.client.Do(req); err != nil {
					success = false
					return
				} else {
					io.Copy(ioutil.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode/100 != 2 && resp.StatusCode != 404 {
						success = false
						return
					}
				}
			}(req)
		}
	} else {
		fp, err := os.Open(o.Path)
		if err != nil {
			return err
		}

		contentLength := int64(0)
		if fi, err := fp.Stat(); err != nil {
			return err
		} else {
			contentLength = fi.Size()
		}

		defer fp.Close()

		writers := make([]io.WriteCloser, len(nodes))
		for i, node := range nodes {
			rp, wp := io.Pipe()
			defer rp.Close()
			defer wp.Close()
			writers[i] = wp
			req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-frag/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), rp)
			if err != nil {
				return err
			}
			req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
			req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("%d/%d/%d", o.dataFrags, o.parityFrags, o.chunkSize))
			for k, v := range o.metadata {
				req.Header.Set("Meta-"+k, v)
			}
			wg.Add(1)
			go func(req *http.Request) {
				defer wg.Done()
				if resp, err := o.client.Do(req); err != nil {
					success = false
					return
				} else {
					io.Copy(ioutil.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode/100 != 2 {
						success = false
						return
					}
				}
			}(req)
		}
		// TODO: junk to get expect 100-continue to work asynchronously before ecSplit is called
		ecSplit(o.dataFrags, o.parityFrags, fp, o.chunkSize, contentLength, writers)
		for _, w := range writers {
			w.Close()
		}
	}
	wg.Wait()
	if !success {
		return fmt.Errorf("Failed to stabilize object")
	}
	return o.idb.Remove(o.Hash, o.Shard, o.Timestamp, true)
}

// make sure these things satisfy interfaces at compile time
var _ objectserver.Object = &ecObject{}
var _ objectserver.ObjectStabilizer = &ecObject{}
