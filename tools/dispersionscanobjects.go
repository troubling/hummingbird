package tools

// In /etc/hummingbird/andrewd-server.conf:
// [dispersion-scan-objects]
// initial_delay = 0        # seconds to wait between requests for the first pass
// pass_time_target = 3600  # seconds to try to make subsequent passes take
// report_interval = 600    # seconds between progress reports

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/containerserver"
	"go.uber.org/zap"
)

type checkInfo struct {
	deviceID  int
	device    string
	partition uint64
	name      string
	ecShard   bool
	shard     int
}

type dispersionScanObjects struct {
	aa *AutoAdmin
	// delay between each request; adjusted each pass to try to make passes last passTimeTarget
	delay          time.Duration
	passTimeTarget time.Duration
	reportInterval time.Duration
	prefix         string
	suffix         string
}

func newDispersionScanObjects(aa *AutoAdmin) *dispersionScanObjects {
	dso := &dispersionScanObjects{
		aa:             aa,
		delay:          time.Duration(aa.serverconf.GetInt("dispersion-scan-objects", "initial_delay", 0)) * time.Second,
		passTimeTarget: time.Duration(aa.serverconf.GetInt("dispersion-scan-objects", "pass_time_target", secondsInADay)) * time.Second,
		reportInterval: time.Duration(aa.serverconf.GetInt("dispersion-scan-objects", "report_interval", 600)) * time.Second,
	}
	if dso.delay < 0 {
		dso.delay = time.Second
	}
	if dso.passTimeTarget < 0 {
		dso.passTimeTarget = time.Second
	}
	dso.prefix, dso.suffix = getAffixes()
	return dso
}

func (dso *dispersionScanObjects) runForever() {
	for {
		sleepFor := dso.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

func (dso *dispersionScanObjects) runOnce() time.Duration {
	start := time.Now()
	logger := dso.aa.logger.With(zap.String("process", "dispersion scan objects"))
	logger.Debug("starting pass")
	if err := dso.aa.db.startProcessPass("dispersion scan", "object-overall", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	delays := 0
	for _, policy := range dso.aa.policies {
		if !policy.Deprecated {
			delays += dso.scanDispersionObjects(logger, policy)
		}
	}
	if delays > 0 {
		dso.delay = dso.passTimeTarget / time.Duration(delays)
	}
	sleepFor := time.Until(start.Add(dso.passTimeTarget))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass complete", zap.String("next delay", dso.delay.String()), zap.String("sleep for", sleepFor.String()))
	if err := dso.aa.db.progressProcessPass("dispersion scan", "object-overall", 0, fmt.Sprintf("%d policies", len(dso.aa.policies))); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := dso.aa.db.completeProcessPass("dispersion scan", "object-overall", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}

type dispersionScanObjectsContext struct {
	logger    *zap.Logger
	wg        sync.WaitGroup
	policy    int
	ring      ring.Ring
	container string
	found     int64
	notFound  int64
	errored   int64
}

func (dso *dispersionScanObjects) scanDispersionObjects(logger *zap.Logger, policy *conf.Policy) int {
	start := time.Now()
	container := fmt.Sprintf("disp-objs-%d", policy.Index)
	logger = logger.With(zap.String("account", AdminAccount), zap.String("container", container), zap.Int("policy", policy.Index))
	logger.Debug("starting policy pass")
	if err := dso.aa.db.startProcessPass("dispersion scan", "object", policy.Index); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	if err := dso.aa.db.clearDispersionScanFailures("object", policy.Index); err != nil {
		logger.Error("clearDispersionScanFailures", zap.Int("policy", policy.Index), zap.Error(err))
	}
	resp := dso.aa.hClient.HeadObject(context.Background(), AdminAccount, container, "object-init", nil)
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		logger.Debug("object-init does not yet exist")
		if err := dso.aa.db.progressProcessPass("dispersion scan", "object", policy.Index, "object-init does not yet exist"); err != nil {
			logger.Error("progressProcessPass", zap.Error(err))
		}
		if err := dso.aa.db.completeProcessPass("dispersion scan", "object", policy.Index); err != nil {
			logger.Error("completeProcessPass", zap.Error(err))
		}
		return 0
	}
	var delays int64
	serviceChans := map[string]chan *checkInfo{}
	objectRing, resp := dso.aa.hClient.ObjectRingFor(context.Background(), AdminAccount, container)
	if resp != nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		logger.Error("error getting object ring", zap.Int("status", resp.StatusCode))
		if err := dso.aa.db.progressProcessPass("dispersion scan", "object", policy.Index, "error getting object ring"); err != nil {
			logger.Error("progressProcessPass", zap.Error(err))
		}
		if err := dso.aa.db.completeProcessPass("dispersion scan", "object", policy.Index); err != nil {
			logger.Error("completeProcessPass", zap.Error(err))
		}
		return 0
	}
	ctx := &dispersionScanObjectsContext{logger: logger, policy: policy.Index, ring: objectRing, container: container}
	cancel := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(dso.reportInterval):
				d := atomic.LoadInt64(&delays)
				var eta time.Duration
				if d > 0 {
					eta = time.Duration(int64(time.Since(start)) / d * (int64(ctx.ring.PartitionCount()) - d))
				}
				logger.Debug("progress", zap.Int64("partitions", d), zap.String("eta", eta.String()))
				if err := dso.aa.db.progressProcessPass("dispersion scan", "object", policy.Index, fmt.Sprintf("%d of %d partitions, %d not found, %d errored, %s eta", d, ctx.ring.PartitionCount(), atomic.LoadInt64(&ctx.notFound), atomic.LoadInt64(&ctx.errored), eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	var marker string
	for {
		resp := dso.aa.hClient.GetContainer(context.Background(), AdminAccount, container, map[string]string{
			"format": "json",
			"marker": marker,
		}, http.Header{})
		if resp.StatusCode/100 != 2 {
			logger.Error("GET", zap.String("marker", marker), zap.Int("status", resp.StatusCode))
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			break
		}
		var olrs []*containerserver.ObjectListingRecord
		if err := json.NewDecoder(resp.Body).Decode(&olrs); err != nil {
			logger.Error("GET got bad JSON", zap.String("marker", marker), zap.Error(err))
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			break
		}
		resp.Body.Close()
		for _, olr := range olrs {
			if olr.Name == "object-init" {
				continue
			}
			clrLogger := logger.With(zap.String("object", olr.Name))
			parts := strings.Split(olr.Name, "-")
			if len(parts) != 2 {
				clrLogger.Debug("odd object name")
				continue
			}
			partition, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				clrLogger.Debug("odd object name", zap.Error(err))
				continue
			}
			atomic.AddInt64(&delays, 1)
			time.Sleep(dso.delay)
			devices := objectRing.GetNodes(partition)
			for shard, device := range devices {
				service := fmt.Sprintf("%s://%s:%d", device.Scheme, device.Ip, device.Port)
				serviceChan := serviceChans[service]
				if serviceChan == nil {
					serviceChan = make(chan *checkInfo, queuedPerDevice)
					ctx.wg.Add(1)
					go dso.handleChecks(ctx, service, serviceChan)
					serviceChans[service] = serviceChan
				}
				ci := &checkInfo{deviceID: device.Id, device: device.Device, partition: partition, name: olr.Name}
				if policy.Type == "hec" {
					// Note that with hec, we query just the stabilized shards.
					// If the dispersion populate had just run and things
					// hadn't stabilized yet, we'll queue up some useless
					// replication jobs to repair missing dispersion shards.
					// Shouldn't be a big deal as it will just be when a new
					// cluster is launched and there won't be much, if any,
					// real data.
					ci.ecShard = true
					ci.shard = shard
				}
				serviceChan <- ci
			}
		}
		if len(olrs) == 0 {
			break
		}
		marker = olrs[len(olrs)-1].Name
	}
	for _, serviceChan := range serviceChans {
		close(serviceChan)
	}
	close(cancel)
	ctx.wg.Wait()
	<-progressDone
	logger.Debug("policy pass complete", zap.Int64("found", ctx.found), zap.Int64("not found", ctx.notFound), zap.Int64("errored", ctx.errored))
	if err := dso.aa.db.progressProcessPass("dispersion scan", "object", policy.Index, fmt.Sprintf("%d of %d partitions, %d not found, %d errored", delays, ctx.ring.PartitionCount(), ctx.notFound, ctx.errored)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := dso.aa.db.completeProcessPass("dispersion scan", "object", policy.Index); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return int(delays)
}

func (dso *dispersionScanObjects) handleChecks(ctx *dispersionScanObjectsContext, service string, checkChan chan *checkInfo) {
	for check := range checkChan {
		var url string
		if check.ecShard {
			h := md5.New()
			io.WriteString(h, path.Join(dso.prefix, AdminAccount, ctx.container, check.name+dso.suffix))
			url = fmt.Sprintf("%s/ec-shard/%s/%s/%d", service, check.device, hex.EncodeToString(h.Sum(nil)), check.shard)
		} else {
			url = fmt.Sprintf("%s/%s/%d/%s/%s/%s", service, check.device, check.partition, common.Urlencode(AdminAccount), common.Urlencode(ctx.container), common.Urlencode(check.name))
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			atomic.AddInt64(&ctx.errored, 1)
			ctx.logger.Error("http.NewRequest(GET, url, nil) // likely programming error", zap.String("url", url), zap.Error(err))
			if err = dso.aa.db.recordDispersionScanFailure("object", ctx.policy, check.partition, service, check.deviceID); err != nil {
				ctx.logger.Error("recordDispersionScanFailure", zap.Int("policy", ctx.policy), zap.Uint64("partition", check.partition), zap.String("service", service), zap.Int("deviceID", check.deviceID), zap.Error(err))
			}
			continue
		}
		req.Header.Set("X-Backend-Suppress-2xx-Logging", "t")
		req.Header.Set("X-Backend-Storage-Policy-Index", fmt.Sprintf("%d", ctx.policy))
		resp, err := dso.aa.client.Do(req)
		if err != nil {
			atomic.AddInt64(&ctx.errored, 1)
			ctx.logger.Debug("Do", zap.String("url", url), zap.Error(err))
			if err = dso.aa.db.recordDispersionScanFailure("object", ctx.policy, check.partition, service, -1); err != nil {
				ctx.logger.Error("recordDispersionScanFailure", zap.Int("policy", ctx.policy), zap.Uint64("partition", check.partition), zap.String("service", service), zap.Error(err))
			}
			continue
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 404 {
			atomic.AddInt64(&ctx.notFound, 1)
			if err = dso.aa.db.queuePartitionReplication("object", ctx.policy, check.partition, "dispersion", -1, check.deviceID); err != nil {
				ctx.logger.Error("queuePartitionReplication", zap.Uint64("partition", check.partition), zap.Int("deviceID", check.deviceID), zap.Error(err))
			}
			continue
		}
		if resp.StatusCode/100 != 2 {
			atomic.AddInt64(&ctx.errored, 1)
			ctx.logger.Debug("StatusCode", zap.String("url", url), zap.Int("status code", resp.StatusCode))
			if err = dso.aa.db.recordDispersionScanFailure("object", ctx.policy, check.partition, "", check.deviceID); err != nil {
				ctx.logger.Error("recordDispersionScanFailure", zap.Int("policy", ctx.policy), zap.Uint64("partition", check.partition), zap.Int("deviceID", check.deviceID), zap.Error(err))
			}
			continue
		}
		atomic.AddInt64(&ctx.found, 1)
	}
	ctx.wg.Done()
}
