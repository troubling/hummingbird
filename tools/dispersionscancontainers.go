package tools

// In /etc/hummingbird/andrewd-server.conf:
// [dispersion-scan-containers]
// initial_delay = 0        # seconds to wait between requests for the first pass
// pass_time_target = 3600  # seconds to try to make subsequent passes take
// report_interval = 600    # seconds between progress reports

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const queuedPerDevice = 10

type dispersionScanContainers struct {
	aa *AutoAdmin
	// delay between each request; adjusted each pass to try to make passes last passTimeTarget
	delay          time.Duration
	passTimeTarget time.Duration
	reportInterval time.Duration
	passesMetric   tally.Timer
	foundMetric    tally.Counter
	notFoundMetric tally.Counter
	erroredMetric  tally.Counter
}

func newDispersionScanContainers(aa *AutoAdmin) *dispersionScanContainers {
	dsc := &dispersionScanContainers{
		aa:             aa,
		delay:          time.Duration(aa.serverconf.GetInt("dispersion-scan-containers", "initial_delay", 0)) * time.Second,
		passTimeTarget: time.Duration(aa.serverconf.GetInt("dispersion-scan-containers", "pass_time_target", secondsInADay)) * time.Second,
		reportInterval: time.Duration(aa.serverconf.GetInt("dispersion-scan-containers", "report_interval", 600)) * time.Second,
		passesMetric:   aa.metricsScope.Timer("disp_scan_cont_passes"),
		foundMetric:    aa.metricsScope.Counter("disp_scan_cont_found"),
		notFoundMetric: aa.metricsScope.Counter("disp_scan_cont_notfound"),
		erroredMetric:  aa.metricsScope.Counter("disp_scan_cont_errored"),
	}
	if dsc.delay < 0 {
		dsc.delay = time.Second
	}
	if dsc.passTimeTarget < 0 {
		dsc.passTimeTarget = time.Second
	}
	return dsc
}

func (dsc *dispersionScanContainers) runForever() {
	for {
		sleepFor := dsc.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

type dispersionScanContainersContext struct {
	logger   *zap.Logger
	wg       sync.WaitGroup
	ring     ring.Ring
	found    int64
	notFound int64
	errored  int64
}

func (dsc *dispersionScanContainers) runOnce() time.Duration {
	defer dsc.passesMetric.Start().Stop()
	start := time.Now()
	logger := dsc.aa.logger.With(zap.String("process", "dispersion scan containers"), zap.String("account", AdminAccount))
	logger.Debug("starting pass")
	if err := dsc.aa.db.startProcessPass("dispersion scan", "container", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	if err := dsc.aa.db.clearDispersionScanFailures("container", 0); err != nil {
		logger.Error("clearDispersionScanFailures", zap.Error(err))
	}
	resp := dsc.aa.hClient.HeadContainer(context.Background(), AdminAccount, "container-init", nil)
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		logger.Debug("container-init does not yet exist")
		if err := dsc.aa.db.progressProcessPass("dispersion scan", "container", 0, "container-init does not yet exist"); err != nil {
			logger.Error("progressProcessPass", zap.Error(err))
		}
		if err := dsc.aa.db.completeProcessPass("dispersion scan", "container", 0); err != nil {
			logger.Error("completeProcessPass", zap.Error(err))
		}
		return dsc.passTimeTarget
	}
	var delays int64
	serviceChans := map[string]chan *checkInfo{}
	ctx := &dispersionScanContainersContext{logger: logger, ring: dsc.aa.hClient.ContainerRing()}
	cancel := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(dsc.reportInterval):
				d := atomic.LoadInt64(&delays)
				var eta time.Duration
				if d > 0 {
					eta = time.Duration(int64(time.Since(start)) / d * (int64(ctx.ring.PartitionCount()) - d))
				}
				logger.Debug("progress", zap.Int64("partitions", d), zap.String("eta", eta.String()))
				if err := dsc.aa.db.progressProcessPass("dispersion scan", "container", 0, fmt.Sprintf("%d of %d partitions, %d not found, %d errored, %s eta", d, ctx.ring.PartitionCount(), atomic.LoadInt64(&ctx.notFound), atomic.LoadInt64(&ctx.errored), eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	var marker string
	for {
		resp := dsc.aa.hClient.GetAccountRaw(context.Background(), AdminAccount, map[string]string{
			"format": "json",
			"marker": marker,
			"prefix": "disp-conts-",
		}, http.Header{})
		if resp.StatusCode/100 != 2 {
			logger.Error("GET", zap.String("marker", marker), zap.Int("status", resp.StatusCode))
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			break
		}
		var clrs []*accountserver.ContainerListingRecord
		if err := json.NewDecoder(resp.Body).Decode(&clrs); err != nil {
			logger.Error("GET got bad JSON", zap.String("marker", marker), zap.Error(err))
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			break
		}
		resp.Body.Close()
		for _, clr := range clrs {
			clrLogger := logger.With(zap.String("container", clr.Name))
			parts := strings.Split(clr.Name, "-")
			if len(parts) != 4 {
				clrLogger.Debug("odd container name")
				continue
			}
			partition, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				clrLogger.Debug("odd container name", zap.Error(err))
				continue
			}
			atomic.AddInt64(&delays, 1)
			time.Sleep(dsc.delay)
			devices := ctx.ring.GetNodes(partition)
			for _, device := range devices {
				service := fmt.Sprintf("%s://%s:%d", device.Scheme, device.Ip, device.Port)
				serviceChan := serviceChans[service]
				if serviceChan == nil {
					serviceChan = make(chan *checkInfo, queuedPerDevice)
					ctx.wg.Add(1)
					go dsc.handleChecks(ctx, service, serviceChan)
					serviceChans[service] = serviceChan
				}
				serviceChan <- &checkInfo{deviceID: device.Id, device: device.Device, partition: partition, name: clr.Name}
			}
		}
		if len(clrs) == 0 {
			break
		}
		marker = clrs[len(clrs)-1].Name
	}
	for _, serviceChan := range serviceChans {
		close(serviceChan)
	}
	close(cancel)
	ctx.wg.Wait()
	<-progressDone
	dsc.delay = dsc.passTimeTarget / time.Duration(delays)
	sleepFor := time.Until(start.Add(dsc.passTimeTarget))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass complete", zap.Int64("found", ctx.found), zap.Int64("not found", ctx.notFound), zap.Int64("errored", ctx.errored), zap.String("next delay", dsc.delay.String()), zap.String("sleep for", sleepFor.String()))
	if err := dsc.aa.db.progressProcessPass("dispersion scan", "container", 0, fmt.Sprintf("%d of %d partitions, %d not found, %d errored", delays, ctx.ring.PartitionCount(), ctx.notFound, ctx.errored)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := dsc.aa.db.completeProcessPass("dispersion scan", "container", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}

func (dsc *dispersionScanContainers) handleChecks(ctx *dispersionScanContainersContext, service string, checkChan chan *checkInfo) {
	for check := range checkChan {
		url := fmt.Sprintf("%s/%s/%d/%s/%s", service, check.device, check.partition, common.Urlencode(AdminAccount), common.Urlencode(check.name))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			atomic.AddInt64(&ctx.errored, 1)
			dsc.erroredMetric.Inc(1)
			ctx.logger.Error("http.NewRequest(HEAD, url, nil) // likely programming error", zap.String("url", url), zap.Error(err))
			if err = dsc.aa.db.recordDispersionScanFailure("container", 0, check.partition, service, check.deviceID); err != nil {
				ctx.logger.Error("recordDispersionScanFailure", zap.Uint64("partition", check.partition), zap.String("service", service), zap.Int("deviceID", check.deviceID), zap.Error(err))
			}
			continue
		}
		req.Header.Set("X-Backend-Suppress-2xx-Logging", "t")
		resp, err := dsc.aa.client.Do(req)
		if err != nil {
			atomic.AddInt64(&ctx.errored, 1)
			dsc.erroredMetric.Inc(1)
			if err = dsc.aa.db.recordDispersionScanFailure("container", 0, check.partition, service, -1); err != nil {
				ctx.logger.Error("recordDispersionScanFailure", zap.Uint64("partition", check.partition), zap.String("service", service), zap.Error(err))
			}
			continue
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 404 {
			atomic.AddInt64(&ctx.notFound, 1)
			dsc.notFoundMetric.Inc(1)
			if err = dsc.aa.db.queuePartitionReplication("container", 0, check.partition, "dispersion", -1, check.deviceID); err != nil {
				ctx.logger.Error("queuePartitionReplication", zap.Uint64("partition", check.partition), zap.Int("deviceID", check.deviceID), zap.Error(err))
			}
			continue
		}
		if resp.StatusCode/100 != 2 {
			atomic.AddInt64(&ctx.errored, 1)
			dsc.erroredMetric.Inc(1)
			if err = dsc.aa.db.recordDispersionScanFailure("container", 0, check.partition, "", check.deviceID); err != nil {
				ctx.logger.Error("recordDispersionScanFailure", zap.Uint64("partition", check.partition), zap.Int("deviceID", check.deviceID), zap.Error(err))
			}
			continue
		}
		atomic.AddInt64(&ctx.found, 1)
		dsc.foundMetric.Inc(1)
	}
	ctx.wg.Done()
}
