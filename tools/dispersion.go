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
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"

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
const timeBetweenDispersionScans = 10 * time.Minute

func getDispersionNames(container, prefix string, r ring.Ring, names chan string, cancel chan struct{}) {
	// if looking for container names, send container=""
	defer close(names)
	var wg sync.WaitGroup
	numCPU := uint64(runtime.NumCPU())
	for cpu := uint64(0); cpu < numCPU; cpu++ {
		wg.Add(1)
		go func(funcCPU uint64) {
			defer wg.Done()
			for partition := funcCPU; true; partition += numCPU {
				select {
				case <-cancel:
					break
				default:
				}
				devs := r.GetNodes(partition)
				if devs == nil {
					break
				}
				for i := uint64(0); true; i++ {
					select {
					case <-cancel:
						break
					default:
					}
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
		}(cpu)
	}
	wg.Wait()
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
	contNames := make(chan string, 100)
	cancel := make(chan struct{})
	go getDispersionNames("", "disp-conts-", contRing, contNames, cancel)

	start := time.Now()
	num := uint64(0)
	successes := uint64(0)

	for container := range contNames {
		num += 1
		if num%1000 == 0 {
			timeSpent := time.Since(start).Seconds()
			partsSec := float64(num) / timeSpent
			hoursRem := float64(contRing.PartitionCount()-num) / partsSec / 60 / 60
			logger.Info(
				"[dispersion-container-init]",
				zap.Uint64("containersPut", num),
				zap.Float64("partitionsPerSecond", partsSec),
				zap.Float64("hoursRemaining", hoursRem),
			)
		}
		resp := hClient.PutContainer(
			AdminAccount,
			container,
			common.Map2Headers(map[string]string{
				"Content-Length": "0",
				"Content-Type":   "text",
				"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix()),
			}),
		)
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			successes += 1
		} else {
			logger.Error(
				"[dispersion-container-init-error]",
				zap.String("containerPath", fmt.Sprintf("/%s/%s", AdminAccount, container)),
				zap.Int("respCode", resp.StatusCode),
			)
			break
		}
	}
	close(cancel)
	if successes == num {
		resp = hClient.PutContainer(
			AdminAccount,
			"container-init",
			common.Map2Headers(map[string]string{
				"Content-Length": "0",
				"Content-Type":   "text",
				"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix()),
			}),
		)
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			logger.Info("[dispersion-container-done]",
				zap.Bool("Success", true), zap.Uint64("numContainers", num))
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
	objNames := make(chan string, 100)
	var objRing ring.Ring
	objRing, resp = hClient.ObjectRingFor(AdminAccount, container)
	if objRing == nil || resp != nil {
		logger.Error("[dispersion-object-init-error]",
			zap.Int("errorGettingRing", resp.StatusCode))
		return false
	}
	cancel := make(chan struct{})
	go getDispersionNames(container, "", objRing, objNames, cancel)

	start := time.Now()

	for obj := range objNames {
		numObjs += 1
		if numObjs%1000 == 0 {
			timeSpent := time.Since(start).Seconds()
			partsSec := float64(numObjs) / timeSpent
			hoursRem := float64(objRing.PartitionCount()-numObjs) / partsSec / 60 / 60
			logger.Info(
				"[dispersion-object-init]",
				zap.Uint64("objsPut", numObjs),
				zap.Float64("partitionsPerSecond", partsSec),
				zap.Float64("hoursRemaining", hoursRem),
			)
		}
		resp := hClient.PutObject(
			AdminAccount,
			container,
			obj,
			common.Map2Headers(map[string]string{
				"Content-Length": "0",
				"Content-Type":   "text",
				"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix()),
			}),
			bytes.NewReader([]byte("")),
		)
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			successes += 1
		} else {
			logger.Error(
				"[dispersion-object-init-error]",
				zap.String("objectPath", fmt.Sprintf("/%s/%s/%s", AdminAccount, container, obj)),
				zap.Int("respCode", resp.StatusCode),
			)
			break
		}
	}
	close(cancel)
	if successes == numObjs {
		resp = hClient.PutObject(
			AdminAccount,
			container,
			"object-init",
			common.Map2Headers(map[string]string{
				"Content-Length": "0",
				"Content-Type":   "text",
				"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix()),
			}),
			bytes.NewReader([]byte("")),
		)
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			logger.Info(
				"[dispersion-object-done]",
				zap.Int("policy-index", policy.Index),
				zap.Bool("Success", true),
				zap.Uint64("numObjs", numObjs),
			)
			return true
		}
	}
	logger.Error("[dispersion-object-done]",
		zap.Bool("Success", false), zap.Uint64("missingObjs", numObjs-successes))
	return false
}

func rescueLonelyPartition(policy int64, partition uint64, goodNode *ring.Device, toNodes []*ring.Device, moreNodes ring.MoreNodes, resultChan chan<- string, c *http.Client) {
	if len(toNodes) == 0 {
		toNodes = append(toNodes, moreNodes.Next())
	}
	tries := 1
	for {
		res, success := objectserver.SendPriRepJob(&objectserver.PriorityRepJob{
			Partition:  partition,
			FromDevice: goodNode,
			ToDevices:  toNodes,
			Policy:     int(policy)}, c)
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
	logger       srv.LowLevelLogger
	dirClient    *http.Client
	lonelyClient *http.Client
	hClient      client.ProxyClient
	dw           *driveWatch

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

	if len(cRecords) == 0 {
		d.logger.Error("No dispersion containers found")
		return
	}
	resultChan := make(chan string)
	for _, cr := range cRecords {
		var objRing ring.Ring
		probObjs := map[string]probObj{}
		goodPartitions := int64(0)
		numRescues, objsFound, objsNeed := int64(0), int64(0), int64(0)
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
					url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", device.Scheme, device.Ip, device.Port, device.Device, partition,
						common.Urlencode(AdminAccount), common.Urlencode(cr.Name), common.Urlencode(objRec.Name))
					req, err := http.NewRequest("HEAD", url, nil)
					req.Header.Set("X-Backend-Storage-Policy-Index", strconv.FormatInt(policy, 10))
					resp, err := d.dirClient.Do(req)

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
						notFoundNodes, objRing.GetMoreNodes(partition), resultChan, d.lonelyClient)
				}
				if len(goodNodes) == 0 {
					d.logger.Error("LOST Partition",
						zap.Uint64("partition", partition),
						zap.String("objPath", fmt.Sprintf("/%s/%s/%s", AdminAccount, cr.Name, objRec.Name)))
				}
				objsNeed += int64(len(nodes))
				objsFound += int64(len(goodNodes))

				marker = objRec.Name
				if !d.onceFullDispersion {
					time.Sleep(SLEEP_TIME)
					d.partitionProcessed <- struct{}{}
				}
			}
		}
		d.gaugeObjectDispersion(policy, objRing.ReplicaCount(), goodPartitions, probObjs, numRescues)
		d.makeObjectDispersionPrintableReport(policy, objRing.ReplicaCount(), goodPartitions, probObjs, objsNeed, objsFound, numRescues)
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

func PrintLastDispersionReport(serverconf conf.Config) error {
	sqlDir, ok := serverconf.Get("drive_watch", "sql_dir")
	if !ok {
		return fmt.Errorf("Invalid Config, no drive_watch sql_dir")
	}
	db, err := sql.Open("sqlite3", filepath.Join(sqlDir, DB_NAME))
	defer db.Close()
	if err != nil {
		return err
	}
	rows, err := db.Query(`
    SELECT d.policy, d.create_date, d.report_text FROM
    (SELECT id, policy, MAX(create_date) mcd FROM dispersion_report GROUP BY policy) r
    INNER JOIN dispersion_report d ON d.id = r.id`)
	defer rows.Close()
	if err != nil {
		fmt.Printf("ERROR: SELECT for dispersion_report: %v\n", err)
		return err
	}
	for rows.Next() {
		var createDate time.Time
		var policy, report string
		if err := rows.Scan(&policy, &createDate, &report); err == nil {
			fmt.Println(fmt.Sprintf(
				"Latest dispersion report for policy %s on %s",
				policy, createDate.Format(time.UnixDate)))
			fmt.Print(report)
			fmt.Println("-----------------------------------------------------")
		} else {
			fmt.Println("Error query data:", err)
		}
	}
	return nil
}

func (d *Dispersion) makeObjectDispersionPrintableReport(policy int64, replicas uint64, goodPartitions int64, probObjects map[string]probObj, objsNeed int64, objsFound int64, numRescues int64) {

	replMissing := map[uint64]int64{}
	for _, po := range probObjects {
		nodesMissing := replicas - uint64(po.nodesFound)
		replMissing[nodesMissing] = replMissing[nodesMissing] + 1
	}
	objText := []struct {
		text string
		cnt  uint64
	}{}
	for pMissing, cnt := range replMissing {
		objText = append(objText, struct {
			text string
			cnt  uint64
		}{fmt.Sprintf("There were %d partitions missing %d copies.\n", cnt, pMissing), pMissing})
	}
	sort.Slice(objText, func(i, j int) bool { return objText[i].cnt < objText[j].cnt })
	report := fmt.Sprintf("Using storage policy %d\nThere were %d partitions missing 0 copies.\n", policy, goodPartitions)
	for _, t := range objText {
		report += t.text
	}
	report += fmt.Sprintf("%.2f%% of object copies found (%d of %d)\nSample represents 100%% of the object partition space.\n", float64(objsFound*100)/float64(objsNeed), objsFound, objsNeed)

	if d.onceFullDispersion {
		fmt.Println(report)
	}
	db, err := d.dw.getDbAndLock()
	defer d.dw.dbl.Unlock()
	if err != nil {
		d.logger.Error("error lock report", zap.Error(err))
		return
	}
	tx, err := db.Begin()
	if err != nil {
		d.logger.Error("error begin report", zap.Error(err))
		return
	}
	defer tx.Rollback()
	r, err := tx.Exec("INSERT INTO dispersion_report (policy, objects, objects_found, report_text) VALUES (?,?,?,?)", policy, objsNeed, objsFound, report)
	if err != nil {
		d.logger.Error("error insert dispersion_report", zap.Error(err))
		return
	}
	rID, _ := r.LastInsertId()
	recDetail, err := tx.Prepare("INSERT INTO dispersion_report_detail (dispersion_report_id, policy, partition, partition_object_path, objects_found, objects_need) VALUES (?,?,?,?,?,?)")
	if err != nil {
		d.logger.Error("error insert prep dispersion_report_detail", zap.Error(err))
		return
	}
	for o, po := range probObjects {
		if _, err := recDetail.Exec(rID, policy, po.part, o, po.nodesFound, replicas); err != nil {
			d.logger.Error("error insert dispersion_report_detail", zap.Error(err))
			return
		}
	}
	r, err = tx.Exec("DELETE FROM dispersion_report_detail WHERE create_date < ?", time.Now().AddDate(0, -1, 0))
	if err = tx.Commit(); err != nil {
		d.logger.Error("error delete old dispersion_report_detail", zap.Error(err))
		return
	}
	return
}

func (d *Dispersion) gaugeObjectDispersion(
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
	replMissing := map[uint64]int64{0: goodPartitions}
	for _, po := range probObjects {
		nodesMissing := replicas - uint64(po.nodesFound)
		replMissing[nodesMissing] = replMissing[nodesMissing] + 1
	}
	if _, ok := d.objDispersionGauges[policy]; !ok {
		d.objDispersionGauges[policy] = make([]tally.Gauge, replicas+1)
		for i := uint64(0); i <= replicas; i++ {
			d.objDispersionGauges[policy][i] = d.metricsScope.Gauge(
				fmt.Sprintf("dispersion_object_p%d_missing_%d", policy, i))
		}
	}
	for i := uint64(0); i <= replicas; i++ {
		cnt := replMissing[i]
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
	if time.Since(d.lastPartProcessed) > timeBetweenDispersionScans {
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

func NewDispersion(logger srv.LowLevelLogger, hClient client.ProxyClient, metricsScope tally.Scope, dw *driveWatch, certFile, keyFile string) (*Dispersion, error) {
	dirTransport := &http.Transport{}
	lonelyTransport := &http.Transport{}
	if certFile != "" && keyFile != "" {
		dirTLSConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("Error getting TLS config: %v", err)
		}
		dirTransport.TLSClientConfig = dirTLSConf
		if err = http2.ConfigureTransport(dirTransport); err != nil {
			return nil, fmt.Errorf("Error setting up http2: %v", err)
		}
		lonelyTLSConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("Error getting TLS config: %v", err)
		}
		lonelyTransport.TLSClientConfig = lonelyTLSConf
		if err = http2.ConfigureTransport(lonelyTransport); err != nil {
			return nil, fmt.Errorf("Error setting up http2: %v", err)
		}
	}
	return &Dispersion{
		dirClient: &http.Client{Timeout: 10 * time.Second,
			Transport: dirTransport},
		lonelyClient: &http.Client{Timeout: time.Hour,
			Transport: lonelyTransport},
		logger:              logger,
		hClient:             hClient,
		partitionProcessed:  make(chan struct{}),
		objDispersionGauges: make(map[int64][]tally.Gauge),
		objRescueCounters:   make(map[int64]tally.Counter),
		metricsScope:        metricsScope,
		dw:                  dw,
	}, nil

}
