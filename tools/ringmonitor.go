package tools

// In /etc/hummingbird/andrewd-server.conf:
// [ring-monitor]
// initial_delay = 1      # seconds to wait between ring checks for the first pass
// pass_time_target = 60  # seconds to try to make subsequent passes take
// report_interval = 600  # seconds between progress reports

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

type ringMonitor struct {
	aa *AutoAdmin
	// delay between each ring check; adjusted each pass to try to make passes last passTimeTarget
	delay          time.Duration
	passTimeTarget time.Duration
	reportInterval time.Duration
	prefix         string
	suffix         string
}

func newRingMonitor(aa *AutoAdmin) *ringMonitor {
	rm := &ringMonitor{
		aa:             aa,
		delay:          time.Duration(aa.serverconf.GetInt("ring-monitor", "initial_delay", 1)) * time.Second,
		passTimeTarget: time.Duration(aa.serverconf.GetInt("ring-monitor", "pass_time_target", 60)) * time.Second,
		reportInterval: time.Duration(aa.serverconf.GetInt("ring-monitor", "report_interval", 600)) * time.Second,
	}
	if rm.delay < 0 {
		rm.delay = time.Second
	}
	if rm.passTimeTarget < 0 {
		rm.passTimeTarget = time.Second
	}
	if rm.reportInterval < 0 {
		rm.reportInterval = time.Second
	}
	rm.prefix, rm.suffix = getAffixes()
	return rm
}

func (rm *ringMonitor) runForever() {
	for {
		sleepFor := rm.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

func (rm *ringMonitor) runOnce() time.Duration {
	start := time.Now()
	logger := rm.aa.logger.With(zap.String("process", "ring monitor"))
	logger.Debug("starting pass")
	if err := rm.aa.db.startProcessPass("ring monitor", "", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	var delays int64
	var errors int64
	var partitionCopiesChanged int64
	type ringTask struct {
		typ         string
		policy      int
		ring        ring.RingMD5
		previousMD5 string
	}
	var ringTasks []*ringTask
	ryng, err := ring.GetRingMD5("account", rm.prefix, rm.suffix, 0)
	if err != nil {
		errors++
		logger.Error("could not load ring", zap.String("type", "account"), zap.Int("policy", 0), zap.Error(err))
	} else {
		ringTasks = append(ringTasks, &ringTask{
			typ:  "account",
			ring: ryng,
		})
	}
	ryng, err = ring.GetRingMD5("container", rm.prefix, rm.suffix, 0)
	if err != nil {
		errors++
		logger.Error("could not load ring", zap.String("type", "container"), zap.Int("policy", 0), zap.Error(err))
	} else {
		ringTasks = append(ringTasks, &ringTask{
			typ:  "container",
			ring: ryng,
		})
	}
	for _, policy := range rm.aa.policies {
		if !policy.Deprecated {
			ryng, err = ring.GetRingMD5("object", rm.prefix, rm.suffix, policy.Index)
			if err != nil {
				errors++
				logger.Error("could not load ring", zap.String("type", "object"), zap.Int("policy", policy.Index), zap.Error(err))
				continue
			}
			ringTasks = append(ringTasks, &ringTask{
				typ:    "object",
				policy: policy.Index,
				ring:   ryng,
			})
		}
	}
	cancel := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(rm.reportInterval):
				d := atomic.LoadInt64(&delays)
				e := atomic.LoadInt64(&errors)
				p := atomic.LoadInt64(&partitionCopiesChanged)
				eta := time.Duration(int64(time.Since(start)) / d * (int64(len(ringTasks)) - d))
				logger.Debug("progress", zap.Int64("rings so far", d), zap.Int("total rings", len(ringTasks)), zap.Int64("errors", e), zap.Int64("partition copies changed", p), zap.String("eta", eta.String()))
				if err := rm.aa.db.progressProcessPass("ring monitor", "", 0, fmt.Sprintf("%d of %d rings, %d errors, %d partition copies changed, eta %s", d, len(ringTasks), e, p, eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	for _, ringTask := range ringTasks {
		atomic.AddInt64(&delays, 1)
		time.Sleep(rm.delay)
		taskLogger := logger.With(zap.String("type", ringTask.typ), zap.Int("policy", ringTask.policy))
		if err := ringTask.ring.Reload(); err != nil {
			atomic.AddInt64(&errors, 1)
			taskLogger.Error("couldn't not reload", zap.Error(err))
			continue
		}
		if ringTask.previousMD5 == "" {
			ringTask.previousMD5, err = rm.aa.db.ringHash(ringTask.typ, ringTask.policy)
			if err != nil {
				atomic.AddInt64(&errors, 1)
				taskLogger.Error("couldn't retreive previous hash", zap.Error(err))
				continue
			}
		}
		if ringTask.ring.MD5() != ringTask.previousMD5 {
			rm.aa.fastRingScan <- struct{}{}
			if ringTask.previousMD5 == "" {
				// First time seeing this ring
				rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5())
				continue
			}
			changeTaskLogger := taskLogger.With(zap.String("previous md5", ringTask.previousMD5), zap.String("current md5", ringTask.ring.MD5()))
			changeTaskLogger.Debug("ring md5 changed")
			previousRing := ringTask.ring.RingMatching(ringTask.previousMD5)
			if previousRing == nil {
				atomic.AddInt64(&errors, 1)
				changeTaskLogger.Error("can't find previous ring; assuming nothing changed")
				rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5())
				continue
			}
			partitionCount := previousRing.PartitionCount()
			if partitionCount != ringTask.ring.PartitionCount() {
				atomic.AddInt64(&errors, 1)
				changeTaskLogger.Error(
					"cannot handle changing partition counts; assuming new ring as if no previous ring ever existed",
					zap.Uint64("previous partition count", previousRing.PartitionCount()),
					zap.Uint64("current partition count", ringTask.ring.PartitionCount()),
				)
				rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5())
				continue
			}
			replicaCount := previousRing.ReplicaCount()
			if replicaCount != ringTask.ring.ReplicaCount() {
				atomic.AddInt64(&errors, 1)
				changeTaskLogger.Error(
					"cannot handle changing replica counts; assuming new ring as if no previous ring ever existed",
					zap.Uint64("previous replica count", previousRing.ReplicaCount()),
					zap.Uint64("current replica count", ringTask.ring.ReplicaCount()),
				)
				rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5())
				continue
			}
			failed := false
			for partition := uint64(0); partition < partitionCount && !failed; partition++ {
				previousDev := previousRing.GetNodes(partition)
				currentDev := ringTask.ring.GetNodes(partition)
				for replica := uint64(0); replica < replicaCount; replica++ {
					if previousDev[replica].Id != currentDev[replica].Id {
						if err := rm.aa.db.queuePartitionReplication(
							ringTask.typ,
							ringTask.policy,
							partition,
							"ring change",
							previousDev[replica].Id,
							currentDev[replica].Id,
						); err != nil {
							atomic.AddInt64(&errors, 1)
							changeTaskLogger.Error(
								"could not queue ring change; will try again next pass",
								zap.Uint64("partition", partition),
								zap.Uint64("replica", replica),
								zap.Int("previous id", previousDev[replica].Id),
								zap.Int("current id", currentDev[replica].Id),
								zap.Error(err),
							)
							failed = true
							break
						} else {
							atomic.AddInt64(&partitionCopiesChanged, 1)
						}
					}
				}
			}
			if !failed {
				if err = rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5()); err != nil {
					atomic.AddInt64(&errors, 1)
					changeTaskLogger.Error(
						"could not record the new ring hash; will try again next pass",
						zap.Error(err),
					)
				}
			}
		}
	}
	close(cancel)
	<-progressDone
	if delays > 0 {
		rm.delay = rm.passTimeTarget / time.Duration(delays)
	}
	sleepFor := time.Until(start.Add(rm.passTimeTarget))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass complete", zap.Int("total rings", len(ringTasks)), zap.Int64("errors", errors), zap.Int64("partition copies changed", partitionCopiesChanged), zap.String("next delay", rm.delay.String()), zap.String("sleep for", sleepFor.String()))
	if err := rm.aa.db.progressProcessPass("ring monitor", "", 0, fmt.Sprintf("%d of %d rings, %d errors, %d partition copies changed", delays, len(ringTasks), errors, partitionCopiesChanged)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := rm.aa.db.completeProcessPass("ring monitor", "", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}
