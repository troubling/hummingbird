package tools

// In /etc/hummingbird/andrewd-server.conf:
// [dispersion-populate-containers]
// retry_time = 3600     # seconds before retrying a failed populate pass
// report_interval = 600 # seconds between progress reports

import (
	"fmt"
	"io"
	"io/ioutil"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/common"
	"go.uber.org/zap"
)

type dispersionPopulateContainers struct {
	aa             *AutoAdmin
	retryTime      time.Duration
	reportInterval time.Duration
}

func newDispersionPopulateContainers(aa *AutoAdmin) *dispersionPopulateContainers {
	return &dispersionPopulateContainers{
		aa:             aa,
		retryTime:      time.Duration(aa.serverconf.GetInt("dispersion-populate-containers", "retry_time", 3600)) * time.Second,
		reportInterval: time.Duration(aa.serverconf.GetInt("dispersion-populate-containers", "report_interval", 600)) * time.Second,
	}
}

func (dpc *dispersionPopulateContainers) runForever() {
	for {
		sleepFor := dpc.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

func (dpc *dispersionPopulateContainers) runOnce() time.Duration {
	start := time.Now()
	logger := dpc.aa.logger.With(zap.String("process", "dispersion populate containers"))
	resp := dpc.aa.hClient.HeadContainer(AdminAccount, "container-init", nil)
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		logger.Debug("container-init already exists; no need to populate containers")
		return -1
	}
	logger.Debug("starting pass")
	if err := dpc.aa.db.startProcessPass("dispersion populate", "container", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	containerRing := dpc.aa.hClient.ContainerRing()
	containerNames := make(chan string, 100)
	cancel := make(chan struct{})
	var successes int64
	var errors int64
	go generateDispersionNames("", "disp-conts-", containerRing, containerNames, cancel)
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(dpc.reportInterval):
				s := atomic.LoadInt64(&successes)
				e := atomic.LoadInt64(&errors)
				eta := time.Duration(int64(time.Since(start)) / (s + e) * (int64(containerRing.PartitionCount()) - s - e))
				logger.Debug("progress", zap.Int64("successes", s), zap.Int64("errors", e), zap.String("eta", eta.String()))
				if err := dpc.aa.db.progressProcessPass("dispersion populate", "container", 0, fmt.Sprintf("%d of %d partitions, %d successes, %d errors, %s eta", s+e, containerRing.PartitionCount(), s, e, eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	for container := range containerNames {
		resp := dpc.aa.hClient.PutContainer(
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
			atomic.AddInt64(&successes, 1)
		} else {
			if atomic.AddInt64(&errors, 1) > 1000 {
				// After 1000 errors we'll just assume "things" are broken
				// right now and try again next pass.
				break
			}
			logger.Error("PUT", zap.String("account", AdminAccount), zap.String("container", container), zap.Int("status", resp.StatusCode))
		}
	}
	close(cancel)
	<-progressDone
	if errors == 0 {
		resp = dpc.aa.hClient.PutContainer(
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
		if resp.StatusCode/100 != 2 {
			logger.Error("PUT", zap.String("account", AdminAccount), zap.String("container", "container-init"), zap.Int("status", resp.StatusCode))
			errors++
		}
	}
	if err := dpc.aa.db.progressProcessPass("dispersion populate", "container", 0, fmt.Sprintf("%d successes, %d errors", successes, errors)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := dpc.aa.db.completeProcessPass("dispersion populate", "container", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	if errors == 0 {
		logger.Debug("pass completed successfully", zap.Int64("successes", successes), zap.Int64("errors", errors))
		return -1
	}
	sleepFor := time.Until(start.Add(dpc.retryTime))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass completed with errors", zap.Int64("successes", successes), zap.Int64("errors", errors), zap.String("next attempt", sleepFor.String()))
	return sleepFor
}
