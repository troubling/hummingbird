package tools

// In /etc/hummingbird/andrewd-server.conf:
// [dispersion-populate-objects]
// retry_time = 3600     # seconds before retrying a failed populate pass
// report_interval = 600 # seconds between progress reports

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
)

type dispersionPopulateObjects struct {
	aa             *AutoAdmin
	retryTime      time.Duration
	reportInterval time.Duration
}

func newDispersionPopulateObjects(aa *AutoAdmin) *dispersionPopulateObjects {
	return &dispersionPopulateObjects{
		aa:             aa,
		retryTime:      time.Duration(aa.serverconf.GetInt("dispersion-populate-objects", "retry_time", 3600)) * time.Second,
		reportInterval: time.Duration(aa.serverconf.GetInt("dispersion-populate-objects", "report_interval", 600)) * time.Second,
	}
}

func (dpo *dispersionPopulateObjects) runForever() {
	for {
		sleepFor := dpo.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

func (dpo *dispersionPopulateObjects) runOnce() time.Duration {
	start := time.Now()
	logger := dpo.aa.logger.With(zap.String("process", "dispersion populate objects"))
	logger.Debug("starting pass")
	if err := dpo.aa.db.startProcessPass("dispersion populate", "object-overall", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	failed := false
	for _, policy := range dpo.aa.policies {
		if !policy.Deprecated {
			if !dpo.putDispersionObjects(logger, policy) {
				failed = true
			}
		}
	}
	if err := dpo.aa.db.progressProcessPass("dispersion populate", "object-overall", 0, fmt.Sprintf("%d policies", len(dpo.aa.policies))); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := dpo.aa.db.completeProcessPass("dispersion populate", "object-overall", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	if !failed {
		logger.Debug("pass completed successfully")
		return -1
	}
	sleepFor := time.Until(start.Add(dpo.retryTime))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass complete but with errors", zap.String("next attempt", sleepFor.String()))
	return sleepFor
}

func (dpo *dispersionPopulateObjects) putDispersionObjects(logger *zap.Logger, policy *conf.Policy) bool {
	start := time.Now()
	logger = logger.With(zap.Int("policy", policy.Index))
	container := fmt.Sprintf("disp-objs-%d", policy.Index)
	resp := dpo.aa.hClient.HeadObject(AdminAccount, container, "object-init", nil)
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		logger.Debug("object-init already exists; no need to populate objects")
		return true
	}
	resp = dpo.aa.hClient.PutContainer(
		AdminAccount,
		container,
		common.Map2Headers(map[string]string{
			"Content-Length":   "0",
			"Content-Type":     "text",
			"X-Timestamp":      fmt.Sprintf("%d", time.Now().Unix()),
			"X-Storage-Policy": policy.Name,
		}),
	)
	if resp.StatusCode/100 != 2 {
		logger.Error("PUT", zap.String("account", AdminAccount), zap.String("container", container), zap.Int("status", resp.StatusCode))
		return false
	}
	objectRing, resp := dpo.aa.hClient.ObjectRingFor(AdminAccount, container)
	if objectRing == nil || resp != nil {
		if resp == nil {
			logger.Error("no ring")
		} else {
			logger.Error("no ring", zap.Int("status", resp.StatusCode))
		}
		return false
	}
	logger.Debug("starting policy pass")
	if err := dpo.aa.db.startProcessPass("dispersion populate", "object", policy.Index); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	objectNames := make(chan string, 100)
	cancel := make(chan struct{})
	var successes int64
	var errors int64
	go generateDispersionNames(container, "", objectRing, objectNames, cancel)
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(dpo.reportInterval):
				s := atomic.LoadInt64(&successes)
				e := atomic.LoadInt64(&errors)
				eta := time.Duration(int64(time.Since(start)) / (s + e) * (int64(objectRing.PartitionCount()) - s - e))
				logger.Debug("progress", zap.Int64("successes", s), zap.Int64("errors", e), zap.String("eta", eta.String()))
				if err := dpo.aa.db.progressProcessPass("dispersion populate", "object", policy.Index, fmt.Sprintf("%d of %d partitions, %d successes, %d errors, %s eta", s+e, objectRing.PartitionCount(), s, e, eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	for object := range objectNames {
		xtimestamp := time.Now()
		resp := dpo.aa.hClient.PutObject(
			AdminAccount,
			container,
			object,
			common.Map2Headers(map[string]string{
				"Content-Length":         "0",
				"Content-Type":           "text",
				"X-Timestamp":            common.CanonicalTimestampFromTime(xtimestamp),
				"X-Object-Meta-Populate": common.CanonicalTimestampFromTime(xtimestamp), // GLH Just for debugging, remove at some point.
			}),
			bytes.NewReader([]byte{}),
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
			logger.Error("PUT", zap.String("account", AdminAccount), zap.String("container", container), zap.String("object", object), zap.Int("status", resp.StatusCode))
		}
	}
	close(cancel)
	<-progressDone
	if errors == 0 {
		xtimestamp := time.Now()
		resp = dpo.aa.hClient.PutObject(
			AdminAccount,
			container,
			"object-init",
			common.Map2Headers(map[string]string{
				"Content-Length":         "0",
				"Content-Type":           "text",
				"X-Timestamp":            common.CanonicalTimestampFromTime(xtimestamp),
				"X-Object-Meta-Populate": common.CanonicalTimestampFromTime(xtimestamp), // GLH Just for debugging, remove at some point.
			}),
			bytes.NewReader([]byte{}),
		)
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			logger.Error("PUT", zap.String("account", AdminAccount), zap.String("container", container), zap.String("object", "object-init"), zap.Int("status", resp.StatusCode))
			errors++
		}
	}
	if err := dpo.aa.db.progressProcessPass("dispersion populate", "object", policy.Index, fmt.Sprintf("%d successes, %d errors", successes, errors)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := dpo.aa.db.completeProcessPass("dispersion populate", "object", policy.Index); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	if errors == 0 {
		logger.Debug("policy pass completed successfully", zap.Int64("successes", successes), zap.Int64("errors", errors))
		return true
	}
	logger.Debug("policy pass completed with errors - will try again later", zap.Int64("successes", successes), zap.Int64("errors", errors))
	return false
}
