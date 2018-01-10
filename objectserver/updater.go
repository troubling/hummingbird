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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/pickle"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/middleware"
)

const asyncPendingSleep = 10 * time.Millisecond

type asyncPending struct {
	Headers   map[string]string `pickle:"headers"`
	Object    string            `pickle:"obj"`
	Account   string            `pickle:"account"`
	Container string            `pickle:"container"`
	Method    string            `pickle:"op"`
}

type updateDevice struct {
	r             *Replicator
	dev           *ring.Device
	canchan       chan struct{}
	policy        int
	lastReconDump time.Time
	reconLock     sync.Mutex
	reconRunning  bool
}

func (ud *updateDevice) updateStat(stat string, amount int64) {
	key := deviceKeyId(ud.dev.Device, ud.policy)
	ud.r.updateStat <- statUpdate{"object-updater", key, stat, amount}
}

func (ud *updateDevice) listAsyncs(c chan string, cancel chan struct{}) {
	defer close(c)
	suffixDirs, err := filepath.Glob(filepath.Join(ud.r.deviceRoot, ud.dev.Device, AsyncDir(ud.policy), "[a-f0-9][a-f0-9][a-f0-9]"))
	if err != nil {
		return
	}
	for i := len(suffixDirs) - 1; i > 0; i-- { // shuffle suffixDirs list
		j := rand.Intn(i + 1)
		suffixDirs[j], suffixDirs[i] = suffixDirs[i], suffixDirs[j]
	}
	for _, suffDir := range suffixDirs {
		asyncs, err := fs.ReadDirNames(suffDir)
		if err != nil {
			return
		}
		for _, async := range asyncs {
			select {
			case c <- filepath.Join(suffDir, async):
			case <-cancel:
				return
			}
		}
	}
}

func (ud *updateDevice) updateContainers(ap *asyncPending) bool {
	successes := uint64(0)
	part := ud.r.containerRing.GetPartition(ap.Account, ap.Container, "")
	header := common.Map2Headers(ap.Headers)
	header.Set("User-Agent", fmt.Sprintf("object-updater %d", os.Getpid()))
	for _, node := range ud.r.containerRing.GetNodes(part) {
		objUrl := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", node.Scheme, node.Ip, node.Port, node.Device, part,
			common.Urlencode(ap.Account), common.Urlencode(ap.Container), common.Urlencode(ap.Object))
		req, err := http.NewRequest(ap.Method, objUrl, nil)
		if err != nil {
			ud.r.logger.Error("updateContainers creating new request", zap.Error(err))
			continue
		}
		req.Header = header
		resp, err := ud.r.client.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			successes++
		}
	}
	return successes >= (ud.r.containerRing.ReplicaCount()/2)+1
}

func (ud *updateDevice) processAsync(async string) {
	data, err := ioutil.ReadFile(async)
	if err != nil {
		ud.updateStat("Error", 1)
		ud.r.logger.Error("read async_pending fail", zap.String("file", async), zap.Error(err))
		return
	}
	var ap asyncPending
	if err := pickle.Unmarshal(data, &ap); err != nil {
		ud.updateStat("Error", 1)
		ud.r.logger.Error("unmarshal async_pending fail", zap.String("file", async), zap.Error(err))
		return
	}
	if ud.updateContainers(&ap) {
		ud.updateStat("Success", 1)
		os.Remove(async)
		os.Remove(filepath.Dir(async))
	} else {
		ud.updateStat("Failure", 1)
	}
}

func (ud *updateDevice) reconReportAsync() {
	ud.reconLock.Lock()
	if ud.reconRunning {
		return
	}
	ud.reconRunning = true
	ud.reconLock.Unlock()
	defer func() {
		ud.reconLock.Lock()
		ud.reconRunning = false
		ud.reconLock.Unlock()
	}()
	c := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go ud.listAsyncs(c, cancel)
	cnt := int64(0)
	for range c {
		cnt++
	}
	if err := middleware.DumpReconCache(ud.r.reconCachePath, "object",
		map[string]interface{}{
			fmt.Sprintf("async_pending_%s", ud.dev.Device): cnt}); err != nil {
		ud.r.logger.Error("object-updater saving recon data", zap.Error(err))
	}
}

func (ud *updateDevice) update() {
	ud.updateStat("startRun", 1)
	if ud.lastReconDump.IsZero() || time.Since(ud.lastReconDump) > time.Hour {
		ud.lastReconDump = time.Now()
		go ud.reconReportAsync()
	}
	c := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go ud.listAsyncs(c, cancel)
	for async := range c {
		ud.updateStat("checkin", 1)
		func() {
			ud.r.updateConcurrencySem <- struct{}{}
			defer func() {
				<-ud.r.updateConcurrencySem
			}()
			ud.processAsync(async)
		}()
		select {
		case <-time.After(asyncPendingSleep):
		case <-ud.canchan:
			return
		}
		if time.Since(ud.lastReconDump) > time.Hour {
			ud.lastReconDump = time.Now()
			go ud.reconReportAsync()
		}
	}
	ud.updateStat("PassComplete", 1)
}

func (ud *updateDevice) updateLoop() {
	for {
		select {
		case <-ud.canchan:
			return
		default:
			ud.update()
		}
		time.Sleep(replicateLoopSleepTime)
		ud.updateStat("checkin", 1)
	}
}

func (ud *updateDevice) cancel() {
	close(ud.canchan)
}

func newUpdateDevice(dev *ring.Device, policy int, r *Replicator) *updateDevice {
	return &updateDevice{
		policy:  policy,
		dev:     dev,
		r:       r,
		canchan: make(chan struct{}),
	}
}
