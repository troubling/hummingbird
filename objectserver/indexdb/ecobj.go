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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/objectserver"
)

type ecObject struct {
	IndexDBItem
	afw             fs.AtomicFileWriter
	idb             *IndexDB
	policy          int
	metadata        map[string]string
	ring            ring.Ring
	logger          *zap.Logger
	reserve         int64
	dataShards      int
	parityShards    int
	chunkSize       int
	client          *http.Client
	nurseryReplicas int
	txnId           string
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

func parseECScheme(scheme string) (algo string, dataShards, parityShards, chunkSize int, err error) {
	sections := strings.Split(scheme, "/")
	if len(sections) != 4 {
		return "", 0, 0, 0, fmt.Errorf("%d scheme sections", len(sections))
	}
	algo = sections[0]
	if dataShards, err = strconv.Atoi(sections[1]); err != nil {
		return "", 0, 0, 0, errors.New("Invalid data shard count")
	}
	if parityShards, err = strconv.Atoi(sections[2]); err != nil {
		return "", 0, 0, 0, errors.New("Invalid parity shard count")
	}
	if chunkSize, err = strconv.Atoi(sections[3]); err != nil {
		return "", 0, 0, 0, errors.New("Invalid chunk size")
	}
	return algo, dataShards, parityShards, chunkSize, nil
}

func (o *ecObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if !o.Exists() {
		return 0, errors.New("Doesn't exist")
	}
	contentLength := o.ContentLength()
	if contentLength == 0 {
		return 0, nil
	}
	if o.Nursery {
		file, err := os.Open(o.Path)
		if err != nil {
			return 0, err
		}
		defer file.Close()
		return common.Copy(file, dsts...)
	}

	algo, dataShards, parityShards, chunkSize, err := parseECScheme(o.metadata["Ec-Scheme"])
	if err != nil {
		return 0, fmt.Errorf("Invalid scheme: %v", err)
	}
	if algo != "reedsolomon" {
		return 0, fmt.Errorf("Attempt to read EC object with unknown algorithm '%s'", algo)
	}
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return 0, fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	nodes := o.ring.GetNodes(o.ring.GetPartition(ns[1], ns[2], ns[3]))
	if len(nodes) < dataShards+parityShards {
		return 0, fmt.Errorf("Not enough nodes (%d) for scheme (%d)", len(nodes), dataShards+parityShards)
	}
	bodies := make([]io.Reader, len(nodes))
	// TODO: This could be parallelized, and we can probably stop looking once we have dataShards bodies available.
	for i, node := range nodes {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		resp, err := o.client.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			continue
		}
		bodies[i] = resp.Body
	}
	ecGlue(dataShards, parityShards, bodies, chunkSize, contentLength, dsts...)
	return contentLength, nil
}

// CopyRange copies a range of bytes from the object to the writer.
func (o *ecObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	if !o.Exists() {
		return 0, errors.New("Doesn't exist")
	}

	if o.Nursery {
		file, err := os.Open(o.Path)
		if err != nil {
			return 0, err
		}
		defer file.Close()
		file.Seek(start, os.SEEK_SET)
		return common.Copy(io.LimitReader(file, end-start), w)
	}

	algo, dataShards, parityShards, chunkSize, err := parseECScheme(o.metadata["Ec-Scheme"])
	if err != nil {
		return 0, fmt.Errorf("Invalid scheme: %v", err)
	}
	if algo != "reedsolomon" {
		return 0, fmt.Errorf("Attempt to read EC object with unknown algorithm '%s'", algo)
	}
	contentLength := o.ContentLength()
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return 0, fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	nodes := o.ring.GetNodes(o.ring.GetPartition(ns[1], ns[2], ns[3]))
	if len(nodes) < dataShards+parityShards {
		return 0, fmt.Errorf("Not enough nodes (%d) for scheme (%d)", len(nodes), dataShards+parityShards)
	}
	// round the range start(down) and end(up) to chunk boundaries
	shardStart, shardEnd := rangeChunkAlign(start, end, int64(chunkSize), dataShards)
	if shardEnd > contentLength {
		shardEnd = contentLength
	}
	bodies := make([]io.Reader, len(nodes))
	// TODO: This could be parallelized, and we can probably stop looking once we have dataShards bodies available.
	for i, node := range nodes {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", shardStart, shardEnd))
		resp, err := o.client.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			continue
		}
		bodies[i] = resp.Body
	}
	err = ecGlue(dataShards, parityShards, bodies, chunkSize, shardEnd-shardStart,
		&rangeBytesWriter{startOffset: start % int64(chunkSize), length: end - start, writer: w})
	return end - start, nil
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

func (o *ecObject) commit(metadata map[string]string, method string, nursery bool) error {
	defer o.Close()
	timestampStr, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("no timestamp in metadata")
	}
	timestampTime, err := common.ParseDate(timestampStr)
	if err != nil {
		return err
	}
	timestamp := timestampTime.UnixNano()
	metabytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	shard := 0
	if !nursery {
		shard = o.Shard
	}
	return o.idb.Commit(o.afw, o.Hash, shard, timestamp, method, MetadataHash(metadata), metabytes, nursery, "")
}

func (o *ecObject) Commit(metadata map[string]string) error {
	return o.commit(metadata, "PUT", true)
}

func (o *ecObject) Delete(metadata map[string]string) error {
	return o.commit(metadata, "DELETE", true)
}

func (o *ecObject) CommitMetadata(metadata map[string]string) error {
	return o.commit(metadata, "POST", o.Nursery)
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
		req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device, o.Hash, o.Shard), fp)
		if err != nil {
			return err
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
		req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("reedsolomon/%d/%d/%d", o.dataShards, o.parityShards, o.chunkSize))
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
	success := true
	algo, dataShards, parityShards, chunkSize, err := parseECScheme(o.metadata["Ec-Scheme"])
	if err != nil {
		return fmt.Errorf("Invalid scheme: %v", err)
	}
	if algo != "reedsolomon" {
		return fmt.Errorf("Attempt to read EC object with unknown algorithm '%s'", algo)
	}
	contentLength := o.ContentLength()
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	nodes := o.ring.GetNodes(o.ring.GetPartition(ns[1], ns[2], ns[3]))
	if len(nodes) < dataShards+parityShards {
		return fmt.Errorf("Not enough nodes (%d) for scheme (%d)", len(nodes), dataShards+parityShards)
	}
	bodies := make([]io.Reader, len(nodes))
	toDeviceShard := -1
	for i, node := range nodes {
		if node.Id == prirep.ToDevice.Id {
			toDeviceShard = i
			continue
		}
		url := fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i)
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
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", prirep.ToDevice.Scheme, prirep.ToDevice.Ip, prirep.ToDevice.Port, prirep.ToDevice.Device, o.Hash, toDeviceShard), rp)
	if err != nil {
		return err
	}
	req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(prirep.Policy))
	req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("reedsolomon/%d/%d/%d", o.dataShards, o.parityShards, o.chunkSize))
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
	err = ecReconstruct(dataShards, parityShards, bodies, chunkSize, contentLength, writer, toDeviceShard)
	writer.Close()
	wg.Wait()
	if !success {
		return fmt.Errorf("Failed to replicate")
	}
	return err
}

func (o *ecObject) nurseryReplicate(rng ring.Ring, partition uint64, dev *ring.Device) error {
	nodes, handoff := rng.GetJobNodes(partition, dev.Id)
	more := rng.GetMoreNodes(partition)
	var node *ring.Device
	e := common.NewExpector(o.client)
	defer e.Close()
	wrs := make([]io.WriteCloser, 0)
	successReadyCount := 0
	var responses []*http.Response
	var ready []bool
	nurseryReplicaCount := o.nurseryReplicas
	if !handoff { // we don't need to replicate to ourselves.
		nurseryReplicaCount -= 1
	}
	for i := 0; successReadyCount < nurseryReplicaCount; i++ {
		if i < len(nodes) {
			node = nodes[i]
		} else if node = more.Next(); node == nil {
			break
		}

		rp, wp := io.Pipe()
		defer rp.Close()
		defer wp.Close()
		wrs = append(wrs, wp)
		req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/nursery/%s/%s",
			node.Scheme, node.ReplicationIp, node.ReplicationPort, node.Device, o.Hash), rp)
		if err != nil {
			return err
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("Deletion", strconv.FormatBool(o.Deletion))
		req.Header.Set("Content-Length", o.metadata["Content-Length"])
		for k, v := range o.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		e.AddRequest(req)

		if i >= nurseryReplicaCount-1 {
			successReadyCount = 0
			responses, ready = e.Wait(time.Second * 15)
			for i := 0; i < len(responses); i++ {
				if (responses[i] != nil && responses[i].StatusCode/100 == 2) || ready[i] == true {
					successReadyCount++
				}
			}
		}
	}

	if o.Path != "" {
		writers := make([]io.Writer, 0)
		for i, ready := range ready {
			if ready {
				writers = append(writers, wrs[i])
			}
		}
		fp, err := os.Open(o.Path)
		if err != nil {
			return err
		}
		defer fp.Close()
		common.Copy(fp, writers...)
	}
	for _, wr := range wrs {
		if wr != nil {
			wr.Close()
		}
	}

	successes := e.Successes(time.Second * 15)
	if handoff && successes >= nurseryReplicaCount {
		return o.idb.Remove(o.Hash, o.Shard, o.Timestamp, true)
	} else if successes < nurseryReplicaCount {
		return errors.New("Unable to fully nursery-replicate object.")
	}
	return nil
}

func (o *ecObject) restabilize(rng ring.Ring, dev *ring.Device, policy int) error {
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	wg := sync.WaitGroup{}
	var successes int64
	partition := rng.GetPartition(ns[1], ns[2], ns[3])
	nodes := rng.GetNodes(partition)
	if len(nodes) != o.dataShards+o.parityShards {
		return fmt.Errorf("Ring doesn't match EC scheme (%d != %d).", len(nodes), o.dataShards+o.parityShards)
	}
	for i, node := range nodes {
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Timestamp", o.metadata["X-Timestamp"])
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
		for k, v := range o.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			if resp, err := o.client.Do(req); err == nil {
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
	return o.idb.SetRestablized(o.Hash, o.Shard, o.Timestamp)
}

func (o *ecObject) Stabilize(rng ring.Ring, dev *ring.Device, policy int) error {
	if o.Restabilize {
		return o.restabilize(rng, dev, policy)
	}
	ns := strings.SplitN(o.metadata["name"], "/", 4)
	if len(ns) != 4 {
		return fmt.Errorf("invalid metadata name: %s", o.metadata["name"])
	}
	partition := rng.GetPartition(ns[1], ns[2], ns[3])
	nodes := rng.GetNodes(partition)
	if len(nodes) != o.dataShards+o.parityShards {
		return fmt.Errorf("Ring doesn't match EC scheme (%d != %d).", len(nodes), o.dataShards+o.parityShards)
	}
	wrs := make([]io.WriteCloser, len(nodes))
	e := common.NewExpector(o.client)
	defer e.Close()
	for i, node := range nodes {
		rp, wp := io.Pipe()
		defer rp.Close()
		defer wp.Close()
		wrs[i] = wp
		req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.ReplicationIp,
			node.ReplicationPort, node.Device, o.Hash, i), rp)
		if err != nil {
			return err
		}
		req.Header.Set("X-Timestamp", o.metadata["X-Timestamp"])
		req.Header.Set("Deletion", strconv.FormatBool(o.Deletion))
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
		req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("reedsolomon/%d/%d/%d", o.dataShards, o.parityShards, o.chunkSize))
		for k, v := range o.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		e.AddRequest(req)
	}

	responses, ready := e.Wait(time.Second * 15)
	writers := make([]io.WriteCloser, len(nodes))
	needUpload := false
	success := true
	for i := range responses {
		if responses[i] != nil {
			if responses[i].StatusCode/100 == 2 {
			} else {
				success = false
			}
		} else if ready[i] == true {
			needUpload = true
			writers[i] = wrs[i]
		} else {
			success = false
		}
	}
	if success && needUpload {
		fp, err := os.Open(o.Path)
		if err != nil {
			return err
		}
		defer fp.Close()

		contentLength := int64(0) // TODO: check this against metadata
		if fi, err := fp.Stat(); err != nil {
			return err
		} else {
			contentLength = fi.Size()
		}

		ecSplit(o.dataShards, o.parityShards, fp, o.chunkSize, contentLength, writers)
		for _, w := range wrs {
			w.Close()
		}
		if e.Successes(time.Second*15) < len(nodes) {
			success = false
		}
	}

	if !success {
		o.nurseryReplicate(rng, partition, dev)
		return fmt.Errorf("Failed to stabilize object")
	} else if o.idb != nil {
		return o.idb.Remove(o.Hash, o.Shard, o.Timestamp, true)
	}
	return nil
}

// rangeChunkAlign calculates the range to request of shards to get a given range of the final file.
func rangeChunkAlign(start, end, chunkSize int64, dataShards int) (int64, int64) {
	startChunk := start / (chunkSize * int64(dataShards))
	endChunk := end / (chunkSize * int64(dataShards))
	start = startChunk * chunkSize
	if end%(chunkSize*int64(dataShards)) == 0 {
		end = (endChunk) * chunkSize
	} else {
		end = (endChunk + 1) * chunkSize
	}
	return start, end
}

// rangeBytesWriter proxies a range of its received bytes to the underlying writer, discarding anything before `start` or after `length`
type rangeBytesWriter struct {
	startOffset int64
	length      int64
	writer      io.Writer
}

func (r *rangeBytesWriter) Write(b []byte) (written int, err error) {
	written = len(b)
	if r.startOffset > int64(len(b)) {
		r.startOffset -= int64(len(b))
		return written, nil
	}
	if r.length <= 0 {
		return written, nil
	}
	b = b[r.startOffset:]
	r.startOffset = 0
	if int64(len(b)) > r.length {
		b = b[:r.length]
	}
	r.length -= int64(len(b))
	_, err = r.writer.Write(b[r.startOffset:])
	return written, err
}

// make sure these things satisfy interfaces at compile time
var _ objectserver.Object = &ecObject{}
var _ objectserver.ObjectStabilizer = &ecObject{}
