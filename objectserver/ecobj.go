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
	"github.com/troubling/hummingbird/common/srv"
)

const dataShardTimeout = time.Millisecond * 25

type ecObject struct {
	IndexDBItem
	afw             fs.AtomicFileWriter
	idb             *IndexDB
	policy          int
	metadata        map[string]string
	ring            ring.Ring
	logger          srv.LowLevelLogger
	reserve         int64
	dataShards      int
	parityShards    int
	chunkSize       int
	client          common.HTTPClient
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
	return QuarantineItem(o.idb, &o.IndexDBItem)
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
	partition, err := o.ring.PartitionForHash(o.Hash)
	if err != nil {
		return 0, fmt.Errorf("invalid Hash: %s", o.Hash)
	}
	nodes := o.ring.GetNodes(partition)
	if len(nodes) < dataShards+parityShards {
		return 0, fmt.Errorf("Not enough nodes (%d) for scheme (%d)", len(nodes), dataShards+parityShards)
	}

	type bod struct {
		i   int
		bod io.ReadCloser
	}
	bods := make(chan *bod)
	errs := make(chan error)
	done := make(chan struct{})
	grabShard := func(i int, node *ring.Device) {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
		if err != nil {
			select {
			case errs <- err:
			case <-done:
			}
			return
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Shard-Timestamp", strconv.FormatInt(o.Timestamp, 10))
		req.Header.Set("X-Trans-Id", o.txnId)
		if resp, err := o.client.Do(req); err == nil && resp.StatusCode == http.StatusOK {
			select {
			case bods <- &bod{i: i, bod: resp.Body}:
			case <-done:
				resp.Body.Close()
			}
		} else {
			if err == nil {
				err = fmt.Errorf("Bad status code %d", resp.StatusCode)
				resp.Body.Close()
			}
			select {
			case errs <- err:
			case <-done:
			}
		}
	}
	bodies := make([]io.Reader, len(nodes))
	bodcount := 0
	errcount := 0
	// launch requests for the object's data shards
	nodeI := 0
	for ; nodeI < dataShards; nodeI++ {
		go grabShard(nodeI, nodes[nodeI])
	}
	ticker := time.NewTicker(dataShardTimeout)
	defer ticker.Stop()
	for {
		select {
		case b := <-bods:
			defer b.bod.Close()
			bodies[b.i] = b.bod
			bodcount++
			if bodcount >= dataShards {
				close(done)
				return contentLength, ecGlue(dataShards, parityShards, bodies, chunkSize, contentLength, dsts...)
			}
		// if we get an error or a little time passes, request a parity shard.
		case err := <-errs:
			if errcount++; errcount > parityShards {
				close(done)
				return 0, fmt.Errorf("Unable to retrieve enough shards to reconstruct: %v", err)
			} else if nodeI < len(nodes) {
				go grabShard(nodeI, nodes[nodeI])
				nodeI++
			}
		case <-ticker.C:
			if nodeI < len(nodes) {
				go grabShard(nodeI, nodes[nodeI])
				nodeI++
			}
		}
	}
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
	partition, err := o.ring.PartitionForHash(o.Hash)
	if err != nil {
		return 0, fmt.Errorf("invalid Hash: %s", o.Hash)
	}
	nodes := o.ring.GetNodes(partition)
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
		req.Header.Set("X-Trans-Id", o.txnId)
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

func (o *ecObject) Uuid() string {
	return o.Hash
}

func (o *ecObject) MetadataMd5() string {
	return o.Metahash
}

func (o *ecObject) SetData(size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = o.idb.TempFile(o.Hash, shardNursery, math.MaxInt64, size, true); err != nil {
		return nil, fmt.Errorf("Error creating temp file: %v", err)
	}
	if err := o.afw.Preallocate(size, o.reserve); err != nil {
		o.afw.Abandon()
		return nil, DriveFullError
	}
	return o.afw, nil
}

func (o *ecObject) commit(metadata map[string]string, method string, nursery bool) error {
	defer o.Close()
	timestampStr, ok := metadata["X-Timestamp"]
	if !ok {
		o.logger.Error("no timestamp in metadata")
		return errors.New("no timestamp in metadata")
	}
	timestampTime, err := common.ParseDate(timestampStr)
	if err != nil {
		o.logger.Error("invalid timestamp in metadata")
		return err
	}
	timestamp := timestampTime.UnixNano()
	shard := shardNursery
	if !nursery {
		shard = o.Shard
	}
	return o.idb.Commit(o.afw, o.Hash, shard, timestamp, method, metadata, nursery, "")
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

func (o *ecObject) Reconstruct() error {
	success := true
	algo, dataShards, parityShards, chunkSize, err := parseECScheme(o.metadata["Ec-Scheme"])
	if err != nil {
		return fmt.Errorf("Invalid scheme: %v", err)
	}
	if algo != "reedsolomon" {
		return fmt.Errorf("Attempt to read EC object with unknown algorithm '%s'", algo)
	}
	contentLength := o.ContentLength()
	partition, err := o.ring.PartitionForHash(o.Hash)
	if err != nil {
		return fmt.Errorf("invalid Hash: %s", o.Hash)
	}
	nodes := o.ring.GetNodes(partition)
	if len(nodes) < dataShards+parityShards {
		return fmt.Errorf("Not enough nodes (%d) for scheme (%d)", len(nodes), dataShards+parityShards)
	}
	bodies := make([]io.Reader, len(nodes))
	readSuccesses := 0
	readFails := 0
	failed := make([]*ring.Device, len(nodes))
	for i, node := range nodes {
		url := fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			o.logger.Error("NewRequest failed", zap.String("url", url))
			failed[i] = node
			readFails++
			continue
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		resp, err := o.client.Do(req)
		if err != nil {
			o.logger.Error("client.Do failed", zap.String("url", url))
			failed[i] = node
			readFails++
			continue
		}
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			o.logger.Error("Non OK response", zap.String("url", url), zap.Int("code", resp.StatusCode))
			failed[i] = node
			readFails++
			continue
		}
		bodies[i] = resp.Body
		readSuccesses++
	}
	if readSuccesses < dataShards {
		return fmt.Errorf("Not enough nodes (%d) to reconstruct from (%d)", readSuccesses, dataShards)
	}

	var writers []io.Writer
	var writeClosers []io.WriteCloser
	var shardsToFix []int
	writeSuccess := make(chan bool)

	nodeFails := 0
	for i, node := range failed {
		if node == nil {
			continue
		}
		rp, wp := io.Pipe()
		defer wp.Close()
		defer rp.Close()
		url := fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i)
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			nodeFails++
			o.logger.Info("PUT NewRequest failed", zap.String("url", url), zap.Error(err))
			continue
		}
		req.ContentLength = ecShardLength(o.ContentLength(), o.dataShards)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("reedsolomon/%d/%d/%d", o.dataShards, o.parityShards, o.chunkSize))
		for k, v := range o.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		writers = append(writers, io.Writer(wp))
		writeClosers = append(writeClosers, io.WriteCloser(wp))
		shardsToFix = append(shardsToFix, i)
		go func(req *http.Request) {
			if resp, err := o.client.Do(req); err != nil {
				o.logger.Error("client.Do Failed", zap.String("url", url), zap.Error(err))
				writeSuccess <- false
			} else {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
				if !(resp.StatusCode/100 == 2 || resp.StatusCode == 409) {
					o.logger.Info("PUT StatusCode failed", zap.String("url", url), zap.Int("code", resp.StatusCode))
					writeSuccess <- false
					return
				}
				writeSuccess <- true
			}
		}(req)
	}
	err = ecReconstruct(dataShards, parityShards, bodies, chunkSize, contentLength, writers, shardsToFix, o.logger)
	if err != nil {
		o.logger.Error("ecReconstruct failed", zap.Error(err))
	}
	waitingFor := dataShards + parityShards - readSuccesses - nodeFails
	for _, writer := range writeClosers {
		writer.Close()
	}
	for waitingFor > 0 {
		select {
		case result := <-writeSuccess:
			if result == false {
				success = false
			}
			waitingFor--
		}
	}
	if !success {
		return fmt.Errorf("Failed to reconstruct")
	}
	return err
}

func (o *ecObject) Replicate(prirep PriorityRepJob) error {
	// If we are handoff, just replicate the shard and delete local shard
	if o.Nursery {
		return fmt.Errorf("not replicating object in nursery")
	}
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
		req.ContentLength = ecShardLength(o.ContentLength(), o.dataShards)
		req.Header.Set("X-Timestamp", o.metadata["X-Timestamp"])
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		req.Header.Set("User-Agent", "nursery-stabilizer")
		req.Header.Set("Meta-Ec-Scheme", fmt.Sprintf("reedsolomon/%d/%d/%d", o.dataShards, o.parityShards, o.chunkSize))
		for k, v := range o.metadata {
			req.Header.Set("Meta-"+k, v)
		}
		resp, err := o.client.Do(req)
		if err != nil {
			return fmt.Errorf("error syncing shard %s/%d: %v", o.Hash, o.Shard, err)
		}
		defer resp.Body.Close()
		if !(resp.StatusCode/100 == 2 || resp.StatusCode == 409) {
			return fmt.Errorf("bad status code %d syncing shard with  %s/%d", resp.StatusCode, o.Hash, o.Shard)
		}
		_, err = o.idb.Remove(o.Hash, o.Shard, o.Timestamp, o.Nursery, o.Metahash)
		return err
	}
	return o.Reconstruct()
}

func (o *ecObject) notifyStable(partition uint64, dev *ring.Device) error {
	nodes := o.ring.GetNodes(partition)
	var successes int64
	wg := sync.WaitGroup{}
	sendNotify := func(node *ring.Device) {
		defer wg.Done()
		req, err := http.NewRequest("POST",
			fmt.Sprintf("%s://%s:%d/ec-nursery/%s/%s/%s/%d",
				node.Scheme, node.ReplicationIp, node.ReplicationPort,
				node.Device, o.Hash, o.Metahash, o.Timestamp), nil)
		if err != nil {
			return
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("User-Agent", "nursery-stabilizer")
		req.Header.Set("X-Trans-Id", o.txnId)
		resp, err := o.client.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			atomic.AddInt64(&successes, 1)
		}
	}
	i := 0
	for ; i < o.nurseryReplicas && i < len(nodes); i++ {
		if nodes[i].Id == dev.Id {
			atomic.AddInt64(&successes, 1)
			continue
		}
		wg.Add(1)
		go sendNotify(nodes[i])
	}
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()
	sendToRest := false
	select {
	case <-allDone:
		if successes < int64(o.nurseryReplicas) {
			sendToRest = true
		}
	case <-time.After(5 * time.Second):
		sendToRest = true
	}
	if sendToRest {
		for ; i < len(nodes); i++ {
			if nodes[i].Id == dev.Id {
				atomic.AddInt64(&successes, 1)
				continue
			}
			wg.Add(1)
			go sendNotify(nodes[i])
		}
	}
	<-allDone
	wg.Wait()
	o.logger.Debug("stabilize notification", zap.Bool("allNodes", successes == int64(len(nodes))))
	return nil
}

func (o *ecObject) nurseryReplicate(partition uint64, dev *ring.Device) error {
	nodes, handoff := o.ring.GetJobNodes(partition, dev.Id)
	more := o.ring.GetMoreNodes(partition)
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
		req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/ec-nursery/%s/%s",
			node.Scheme, node.ReplicationIp, node.ReplicationPort, node.Device, o.Hash), rp)
		if err != nil {
			return err
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		req.Header.Set("User-Agent", "nursery-stabilizer")
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

	sts := []int{2, 409}
	if o.Deletion {
		sts = append(sts, 404)
	}
	successes := e.Successes(time.Second*15, sts...)
	if handoff && successes >= nurseryReplicaCount && successes > 0 {
		_, err := o.idb.Remove(o.Hash, o.Shard, o.Timestamp, true, o.Metahash)
		return err
	} else if successes < nurseryReplicaCount {
		return errors.New("Unable to fully nursery-replicate object.")
	}
	return nil
}

func (o *ecObject) restabilize(dev *ring.Device) error {
	partition, err := o.ring.PartitionForHash(o.Hash)
	if err != nil {
		return fmt.Errorf("invalid Hash: %s", o.Hash)
	}
	wg := sync.WaitGroup{}
	var successes int64
	nodes := o.ring.GetNodes(partition)
	if len(nodes) != o.dataShards+o.parityShards {
		return fmt.Errorf("Ring doesn't match EC scheme (%d != %d).", len(nodes), o.dataShards+o.parityShards)
	}
	for i, node := range nodes {
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.Ip, node.Port, node.Device, o.Hash, i), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Timestamp", o.metadata["X-Timestamp"])
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		req.Header.Set("User-Agent", "nursery-stabilizer")
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
	return o.idb.SetStabilized(o.Hash, o.Shard, o.Timestamp, false)
}

func (o *ecObject) Stabilize(dev *ring.Device) error {
	if o.Restabilize {
		return o.restabilize(dev)
	}
	partition, err := o.ring.PartitionForHash(o.Hash)
	if err != nil {
		return fmt.Errorf("invalid Hash: %s", o.Hash)
	}
	nodes := o.ring.GetNodes(partition)
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
		url := fmt.Sprintf("%s://%s:%d/ec-shard/%s/%s/%d", node.Scheme, node.ReplicationIp,
			node.ReplicationPort, node.Device, o.Hash, i)
		method := "PUT"
		if o.Deletion {
			method = "DELETE"
		}
		req, err := http.NewRequest(method, url, rp)
		if err != nil {
			return err
		}
		if !o.Deletion {
			req.ContentLength = ecShardLength(o.ContentLength(), o.dataShards)
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(o.policy))
		req.Header.Set("X-Trans-Id", o.txnId)
		req.Header.Set("User-Agent", "nursery-stabilizer")
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
			if responses[i].StatusCode/100 == 2 || responses[i].StatusCode == 409 || (o.Deletion && responses[i].StatusCode == 404) {
			} else {
				success = false
				o.logger.Debug("stabilize req failed", zap.Int("status", responses[i].StatusCode), zap.String("resp", fmt.Sprintf("%v", responses[i])))
			}
		} else if ready[i] == true {
			if !o.Deletion {
				needUpload = true
				writers[i] = wrs[i]
			}
		} else {
			o.logger.Debug("stabilize req failed: nil response")
			success = false
		}
	}
	if success {
		if needUpload {
			fp, err := os.Open(o.Path)
			if err != nil {
				if os.IsNotExist(err) {
					// probably got notified stable, skip
					return nil
				}
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
		}
		for _, w := range wrs {
			w.Close()
		}
		sts := []int{2, 409}
		if o.Deletion {
			sts = append(sts, 404)
		}
		if e.Successes(time.Second*15, sts...) < len(nodes) {
			success = false
		}
	}

	if !success {
		o.nurseryReplicate(partition, dev)
		return fmt.Errorf("Failed to stabilize object: %s", o.txnId)
	} else if o.idb != nil {
		nSuccess := e.Successes(0, 409)
		if nSuccess != len(nodes) {
			st := time.Now()
			done := make(chan struct{}, 1)
			go func() {
				if err = o.notifyStable(partition, dev); err != nil {
					o.logger.Debug("stabilize notify failed", zap.Error(err), zap.Duration("dur", time.Since(st)))
				} else {
					o.logger.Debug("object notified", zap.String("hash", o.Uuid()), zap.Duration("dur", time.Since(st)))
				}
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		}
		_, err := o.idb.Remove(o.Hash, o.Shard, o.Timestamp, true, o.Metahash)
		return err
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
var _ Object = &ecObject{}
var _ ObjectStabilizer = &ecObject{}
