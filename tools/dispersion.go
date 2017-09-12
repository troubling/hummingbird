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

package tools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/containerserver"
	"github.com/troubling/hummingbird/objectserver"
	"github.com/uber-go/tally"
)

const AdminAccount = ".admin"
const SLEEP_TIME = 100 * time.Millisecond

func getDispersionNames(container, prefix string, r ring.Ring, names chan string) {
	// if looking for container names, send container=""
	defer close(names)
	for partition := uint64(0); true; partition++ {
		devs := r.GetNodes(partition)
		if devs == nil {
			break
		}
		for i := uint64(0); true; i++ {
			c := container
			o := ""
			n := fmt.Sprintf("%s%d-%d", prefix, partition, i)
			if c == "" {
				c = n
			} else {
				o = n
			}
			genPart := r.GetPartition(AdminAccount, c, o)
			if genPart == partition {
				names <- n
				break
			}
		}
	}
}

func putDispersionAccount(hClient client.ProxyClient, logger srv.LowLevelLogger) bool {
	if resp := hClient.PutAccount(AdminAccount, common.Map2Headers(map[string]string{
		"Content-Length": "0",
		"Content-Type":   "text",
		"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix())})); resp.StatusCode/100 == 2 {
		resp.Body.Close()
		return true
	} else {
		logger.Error("[dispersion-account-init-error]",
			zap.String("account", AdminAccount),
			zap.Int("respCode", resp.StatusCode))
	}
	return false
}

func putDispersionContainers(hClient client.ProxyClient, logger srv.LowLevelLogger) bool {
	// will check for a .dispersion/container-init container
	// if not there, will populate all the dispersion containers
	// and then put the .dispersion/container-init to mark it
	resp := hClient.HeadContainer(AdminAccount, "container-init", nil)
	if resp.StatusCode/100 == 2 {
		return true
	}

	contRing := hClient.ContainerRing()
	contNames := make(chan string)
	go getDispersionNames("", "disp-conts-", contRing, contNames)

	start := time.Now()
	num := uint64(0)
	successes := uint64(0)

	for container := range contNames {
		num += 1
		if num%1000 == 0 {
			timeSpent := time.Since(start).Seconds()
			partsSec := float64(num) / timeSpent
			hoursRem := float64(contRing.PartitionCount()-num) / partsSec / 60 / 60
			logger.Info("[dispersion-container-init]",
				zap.Uint64("containersPut", num),
				zap.Float64("partitionsPerSecond", partsSec),
				zap.Float64("hoursRemaining", hoursRem))
		}

		if resp = hClient.PutContainer(AdminAccount, container, common.Map2Headers(map[string]string{
			"Content-Length": "0",
			"Content-Type":   "text",
			"X-Timestamp": fmt.Sprintf("%d",
				time.Now().Unix())})); resp.StatusCode/100 == 2 {
			successes += 1
			resp.Body.Close()
		} else {
			logger.Error("[dispersion-container-init-error]",
				zap.String("containerPath", fmt.Sprintf("/%s/%s", AdminAccount, container)),
				zap.Int("respCode", resp.StatusCode))
		}
	}
	if successes == num {
		if resp = hClient.PutContainer(AdminAccount, "container-init", common.Map2Headers(map[string]string{
			"Content-Length": "0",
			"Content-Type":   "text",
			"X-Timestamp": fmt.Sprintf("%d",
				time.Now().Unix())})); resp.StatusCode/100 == 2 {
			logger.Info("[dispersion-container-done]",
				zap.Bool("Success", true), zap.Uint64("numContainers", num))
			resp.Body.Close()
			return true
		}
	}
	logger.Error("[dispersion-container-done]",
		zap.Bool("Success", false), zap.Uint64("missingContainers", num-successes))
	return false
}

func putDispersionObjects(hClient client.ProxyClient, policy *conf.Policy, logger srv.LowLevelLogger) bool {
	// will check for a .dispersion/disp-objs-policyId/object-init object
	// if not there, will populate all the dispersion objects for the policy
	// and then put the .dispersion/disp-objs-policyId/object-init to mark it
	container := fmt.Sprintf("disp-objs-%d", policy.Index)
	resp := hClient.HeadObject(AdminAccount, container, "object-init", nil)
	if resp.StatusCode/100 == 2 {
		resp.Body.Close()
		return true
	}
	headers := map[string]string{
		"Content-Length":   "0",
		"Content-Type":     "text",
		"X-Timestamp":      fmt.Sprintf("%d", time.Now().Unix()),
		"X-Storage-Policy": policy.Name,
	}
	resp = hClient.PutContainer(AdminAccount, container, common.Map2Headers(headers))
	if resp.StatusCode/100 != 2 {
		logger.Error("[dispersion-object-init-error]",
			zap.String("containerPath", fmt.Sprintf("/%s/%s", AdminAccount, container)),
			zap.Int("respCode", resp.StatusCode))
		return false
	}
	numObjs := uint64(0)
	successes := uint64(0)
	objNames := make(chan string)
	var objRing ring.Ring
	objRing, resp = hClient.ObjectRingFor(AdminAccount, container)
	if objRing == nil || resp != nil {
		logger.Error("[dispersion-object-init-error]",
			zap.Int("errorGettingRing", resp.StatusCode))
		return false
	}
	go getDispersionNames(container, "", objRing, objNames)

	start := time.Now()

	for obj := range objNames {
		numObjs += 1
		if numObjs%1000 == 0 {
			timeSpent := time.Since(start).Seconds()
			partsSec := float64(numObjs) / timeSpent
			hoursRem := float64(objRing.PartitionCount()-numObjs) / partsSec / 60 / 60
			logger.Info("[dispersion-object-init]",
				zap.Uint64("objsPut", numObjs),
				zap.Float64("partitionsPerSecond", partsSec),
				zap.Float64("hoursRemaining", hoursRem))
		}
		if resp = hClient.PutObject(
			AdminAccount, container, obj, common.Map2Headers(map[string]string{
				"Content-Length": "0",
				"Content-Type":   "text",
				"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix())}),
			bytes.NewReader([]byte(""))); resp.StatusCode/100 == 2 {
			successes += 1
		} else {
			logger.Error("[dispersion-object-init-error]",
				zap.String("objectPath", fmt.Sprintf(
					"/%s/%s/%s", AdminAccount, container, obj)),
				zap.Int("respCode", resp.StatusCode))
		}
	}
	if successes == numObjs {
		if resp = hClient.PutObject(
			AdminAccount, container, "object-init", common.Map2Headers(map[string]string{
				"Content-Length": "0",
				"Content-Type":   "text",
				"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix())}),
			bytes.NewReader([]byte(""))); resp.StatusCode/100 == 2 {
			logger.Info("[dispersion-object-done]",
				zap.Int("policy-index", policy.Index),
				zap.Bool("Success", true), zap.Uint64("numObjs", numObjs))
			resp.Body.Close()
			return true
		}
	}
	logger.Error("[dispersion-object-done]",
		zap.Bool("Success", false), zap.Uint64("missingObjs", numObjs-successes))
	return false
}

func rescueLonelyPartition(policy int64, partition uint64, goodNode *ring.Device, toNodes []*ring.Device, moreNodes ring.MoreNodes, resultChan chan<- string) {
	if len(toNodes) == 0 {
		toNodes = append(toNodes, moreNodes.Next())
	}
	c := http.Client{Timeout: time.Hour}
	tries := 1
	for {
		res, success := objectserver.SendPriRepJob(&objectserver.PriorityRepJob{
			Partition:  partition,
			FromDevice: goodNode,
			ToDevices:  toNodes,
			Policy:     int(policy)}, &c)
		if success {
			resultChan <- res
			return
		} else {
			nextNode := moreNodes.Next()
			if nextNode == nil {
				resultChan <- fmt.Sprintf("Rescue partition %d failed after %d tries. %s", partition, tries, res)
				return
			}
			toNodes = []*ring.Device{nextNode}
		}
		tries += 1
		time.Sleep(time.Second)
	}
}

type probObj struct {
	part       int
	nodesFound int
}

type Dispersion struct {
	logger  srv.LowLevelLogger
	hClient client.ProxyClient

	metricsScope tally.Scope

	lastPartProcessed  time.Time
	dispersionCanceler chan struct{}
	partitionProcessed chan struct{}
	onceFullDispersion bool

	objDispersionGauges map[int64][]tally.Gauge
	objRescueCounters   map[int64]tally.Counter
}

func (d *Dispersion) scanDispersionObjs(cancelChan chan struct{}) {
	resp := d.hClient.GetAccount(AdminAccount, map[string]string{"format": "json", "prefix": "disp-objs-"}, http.Header{})
	var cRecords []accountserver.ContainerListingRecord
	err := json.NewDecoder(resp.Body).Decode(&cRecords)
	if err != nil {
		d.logger.Error("Could not get container listing", zap.Error(err))
		return
	}
	dirClient := http.Client{Timeout: 10 * time.Second}

	if len(cRecords) == 0 {
		d.logger.Error("No dispersion containers found")
		return
	}
	resultChan := make(chan string)
	for _, cr := range cRecords {
		var objRing ring.Ring
		probObjs := map[string]probObj{}
		goodPartitions := int64(0)
		numRescues := int64(0)
		contArr := strings.Split(cr.Name, "-")
		if len(contArr) != 3 {
			continue
		}
		policy, err := strconv.ParseInt(contArr[2], 10, 64)
		if err != nil {
			d.logger.Error("error parsing policy index", zap.Error(err))
			return
		}
		objRing, resp = d.hClient.ObjectRingFor(AdminAccount, cr.Name)
		if resp != nil {
			d.logger.Error("error getting obj ring for", zap.String("container", cr.Name))
			return
		}
		marker := ""
		for true {
			select {
			case <-cancelChan:
				return
			default:
			}
			var ors []containerserver.ObjectListingRecord
			resp := d.hClient.GetContainer(AdminAccount, cr.Name, map[string]string{"format": "json", "marker": marker}, http.Header{})
			err = json.NewDecoder(resp.Body).Decode(&ors)
			if err != nil {
				d.logger.Error("error in container listing", zap.String("container", cr.Name), zap.Error(err))
				return
			}
			if len(ors) == 0 {
				break
			}
			for _, objRec := range ors {
				if objRec.Name == "object-init" {
					marker = objRec.Name
					continue
				}
				objArr := strings.Split(objRec.Name, "-")
				if len(objArr) != 2 {
					continue
				}
				partition, e := strconv.ParseUint(objArr[0], 10, 64)
				if e != nil {
					continue
				}
				nodes := objRing.GetNodes(partition)
				goodNodes := []*ring.Device{}
				notFoundNodes := []*ring.Device{}

				for _, device := range nodes {
					url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
						common.Urlencode(AdminAccount), common.Urlencode(cr.Name), common.Urlencode(objRec.Name))
					req, err := http.NewRequest("HEAD", url, nil)
					req.Header.Set("X-Backend-Storage-Policy-Index", strconv.FormatInt(policy, 10))
					resp, err := dirClient.Do(req)

					if err == nil && resp.StatusCode/100 == 2 {
						goodNodes = append(goodNodes, device)
					} else if resp != nil && resp.StatusCode == 404 {
						notFoundNodes = append(notFoundNodes, device)
					}
					if resp != nil {
						resp.Body.Close()
					}
				}
				if len(nodes) != len(goodNodes) {
					probObjs[fmt.Sprintf("%s/%s", cr.Name, objRec.Name)] = probObj{int(partition), len(goodNodes)}
				} else {
					goodPartitions += 1
				}
				if len(nodes) > 1 && len(goodNodes) == 1 {
					numRescues += 1
					go rescueLonelyPartition(policy, partition, goodNodes[0],
						notFoundNodes, objRing.GetMoreNodes(partition), resultChan)
				}
				if len(goodNodes) == 0 {
					d.logger.Error("LOST Partition",
						zap.Uint64("partition", partition),
						zap.String("objPath", fmt.Sprintf("/%s/%s/%s", AdminAccount, cr.Name, objRec.Name)))
				}

				marker = objRec.Name
				if !d.onceFullDispersion {
					time.Sleep(SLEEP_TIME)
					d.partitionProcessed <- struct{}{}
				}
			}
		}
		d.reportObjectDispersion(policy, objRing.ReplicaCount(), goodPartitions, probObjs, numRescues)
		d.logger.Info("Dispersion report written",
			zap.Int64("policy", policy),
			zap.Int64("rescuing", numRescues),
			zap.Int64("goodPartitions", goodPartitions),
			zap.Uint64("totalPartitions", objRing.PartitionCount()))
		start := time.Now()
		for i := int64(0); i < numRescues; i++ {
			res := <-resultChan
			d.logger.Info(res)
		}
		if numRescues > 0 {
			d.logger.Info("time spent rescuing",
				zap.Float64("seconds", time.Since(start).Seconds()))
		}
	}
}

func (d *Dispersion) reportObjectDispersion(
	policy int64, replicas uint64, goodPartitions int64,
	probObjects map[string]probObj, numRescues int64) {

	if numRescues > 0 {
		if _, ok := d.objRescueCounters[policy]; !ok {
			d.objRescueCounters[policy] = d.metricsScope.Counter(
				fmt.Sprintf("rescue_object_p%d", policy))
		}
		d.objRescueCounters[policy].Inc(numRescues)
		d.logger.Info("rescue object partitions",
			zap.Int64("rescues", numRescues), zap.Int64("policy", policy))
	}
	objMap := map[uint64]int64{0: goodPartitions}
	for _, po := range probObjects {
		nodesMissing := replicas - uint64(po.nodesFound)
		objMap[nodesMissing] = objMap[nodesMissing] + 1
	}
	if _, ok := d.objDispersionGauges[policy]; !ok {
		d.objDispersionGauges[policy] = make([]tally.Gauge, replicas+1)
		for i := uint64(0); i <= replicas; i++ {
			d.objDispersionGauges[policy][i] = d.metricsScope.Gauge(
				fmt.Sprintf("dispersion_object_p%d_missing_%d", policy, i))
		}
	}
	for i := uint64(0); i <= replicas; i++ {
		cnt := objMap[i]
		d.objDispersionGauges[policy][i].Update(float64(cnt))
		d.logger.Info("object partitions missing", zap.Uint64("missing", i),
			zap.Int64("count", cnt), zap.Int64("policy", policy))
	}
}

func (d *Dispersion) dispersionMonitor(ticker <-chan time.Time) {
	for {
		select {
		case <-d.partitionProcessed:
			d.lastPartProcessed = time.Now()
		case <-ticker:
			d.checkDispersionRunner()
		}
	}
}

func (d *Dispersion) checkDispersionRunner() {
	if time.Since(d.lastPartProcessed) > 10*time.Minute {
		if d.dispersionCanceler != nil {
			close(d.dispersionCanceler)
		}
		d.dispersionCanceler = make(chan struct{})
		go d.scanDispersionObjs(d.dispersionCanceler)
	}
}

func (d *Dispersion) runDispersionForever() {
	d.onceFullDispersion = false
	d.checkDispersionRunner()
	d.dispersionMonitor(time.NewTicker(time.Second * 30).C)
}

func (d *Dispersion) runDispersionOnce() {
	d.onceFullDispersion = true
	dummyCanceler := make(chan struct{})
	defer close(dummyCanceler)
	d.scanDispersionObjs(dummyCanceler)
}

func NewDispersion(logger srv.LowLevelLogger, hClient client.ProxyClient, metricsScope tally.Scope) *Dispersion {
	return &Dispersion{
		logger:              logger,
		hClient:             hClient,
		partitionProcessed:  make(chan struct{}),
		objDispersionGauges: make(map[int64][]tally.Gauge),
		objRescueCounters:   make(map[int64]tally.Counter),
		metricsScope:        metricsScope,
	}

}
