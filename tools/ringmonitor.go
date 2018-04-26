package tools

// The ring monitor will constantly scan the on-disk rings to detect when they
// change or need to be rebalanced. It will queue replication jobs when it
// detects changes by comparing the new and old rings. It will also trigger a
// fast-scan for the ring scan process to send out new rings.
//
// In /etc/hummingbird/andrewd-server.conf:
// [ring-monitor]
// initial_delay = 1      # seconds to wait between ring checks for the first pass
// pass_time_target = 60  # seconds to try to make subsequent passes take
// report_interval = 600  # seconds between progress reports

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

type ringMonitor struct {
	aa *AutoAdmin
	// delay between each ring check; adjusted each pass to try to make passes last passTimeTarget
	delay            time.Duration
	passTimeTarget   time.Duration
	reportInterval   time.Duration
	prefix           string
	suffix           string
	ringMD5CacheLock sync.Mutex
	ringMD5Cache     map[string]map[int]ring.RingMD5
}

func newRingMonitor(aa *AutoAdmin) *ringMonitor {
	rm := &ringMonitor{
		aa:             aa,
		delay:          time.Duration(aa.serverconf.GetInt("ring-monitor", "initial_delay", 1)) * time.Second,
		passTimeTarget: time.Duration(aa.serverconf.GetInt("ring-monitor", "pass_time_target", 60)) * time.Second,
		reportInterval: time.Duration(aa.serverconf.GetInt("ring-monitor", "report_interval", 600)) * time.Second,
		ringMD5Cache:   map[string]map[int]ring.RingMD5{},
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
	rm.ringMD5Cache["account"] = map[int]ring.RingMD5{}
	rm.ringMD5Cache["container"] = map[int]ring.RingMD5{}
	rm.ringMD5Cache["object"] = map[int]ring.RingMD5{}
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

type ringTaskInstance struct {
	typ           string
	policy        int
	ring          ring.RingMD5
	previousMD5   string
	nextRebalance time.Time
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
	var ringTasks []*ringTaskInstance
	ryng, err := rm.GetRingMD5("account", 0)
	if err != nil {
		errors++
		logger.Error("could not load ring", zap.String("type", "account"), zap.Int("policy", 0), zap.Error(err))
	} else {
		ringTasks = append(ringTasks, &ringTaskInstance{
			typ:  "account",
			ring: ryng,
		})
	}
	ryng, err = rm.GetRingMD5("container", 0)
	if err != nil {
		errors++
		logger.Error("could not load ring", zap.String("type", "container"), zap.Int("policy", 0), zap.Error(err))
	} else {
		ringTasks = append(ringTasks, &ringTaskInstance{
			typ:  "container",
			ring: ryng,
		})
	}
	for _, policy := range rm.aa.policies {
		if !policy.Deprecated {
			ryng, err = rm.GetRingMD5("object", policy.Index)
			if err != nil {
				errors++
				logger.Error("could not load ring", zap.String("type", "object"), zap.Int("policy", policy.Index), zap.Error(err))
				continue
			}
			ringTasks = append(ringTasks, &ringTaskInstance{
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
		if ringTask.previousMD5 == "" {
			ringTask.previousMD5, ringTask.nextRebalance, err = rm.aa.db.ringHash(ringTask.typ, ringTask.policy)
			if err != nil {
				atomic.AddInt64(&errors, 1)
				taskLogger.Error("couldn't retreive previous hash", zap.Error(err))
				continue
			}
		}
		if ringTask.ring.MD5() == ringTask.previousMD5 && !ringTask.nextRebalance.IsZero() && time.Now().After(ringTask.nextRebalance) {
			if !rm.rebalance(taskLogger, ringTask) {
				atomic.AddInt64(&errors, 1)
				continue
			}
		}
		if ringTask.ring.MD5() == ringTask.previousMD5 {
			continue
		}
		rm.aa.fastRingScan <- struct{}{}
		if ringTask.previousMD5 == "" {
			// First time seeing this ring
			rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5(), time.Now().Add(randomDuration(time.Minute*30, time.Hour)))
			continue
		}
		changeTaskLogger := taskLogger.With(zap.String("previous md5", ringTask.previousMD5), zap.String("current md5", ringTask.ring.MD5()))
		changeTaskLogger.Debug("ring md5 changed")
		previousRing := ringTask.ring.RingMatching(ringTask.previousMD5)
		if previousRing == nil {
			atomic.AddInt64(&errors, 1)
			changeTaskLogger.Error("can't find previous ring; assuming nothing changed")
			rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5(), time.Now().Add(randomDuration(time.Minute*30, time.Hour)))
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
			rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5(), time.Now().Add(randomDuration(time.Minute*30, time.Hour)))
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
			rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5(), time.Now().Add(randomDuration(time.Minute*30, time.Hour)))
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
			if err = rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, ringTask.ring.MD5(), time.Now().Add(randomDuration(time.Minute*30, time.Hour))); err != nil {
				atomic.AddInt64(&errors, 1)
				changeTaskLogger.Error(
					"could not record the new ring hash; will try again next pass",
					zap.Error(err),
				)
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

func (rm *ringMonitor) GetRingMD5(typ string, policy int) (ring.RingMD5, error) {
	rm.ringMD5CacheLock.Lock()
	defer rm.ringMD5CacheLock.Unlock()
	if ryng, ok := rm.ringMD5Cache[typ][policy]; ok {
		return ryng, ryng.Reload()
	}
	ryng, err := ring.GetRingMD5(typ, rm.prefix, rm.suffix, policy)
	if err != nil {
		return nil, err
	}
	rm.ringMD5Cache[typ][policy] = ryng
	return ryng, nil
}

func (rm *ringMonitor) rebalance(logger *zap.Logger, ringTask *ringTaskInstance) bool {
	ringBuilder, ringBuilderFilePath, err := ring.GetRingBuilder(ringTask.typ, ringTask.policy)
	if err != nil {
		logger.Error("Could not find builder", zap.Error(err))
		return false
	}
	ringBuilderLock, err := ring.LockBuilderPath(ringBuilderFilePath)
	if err != nil {
		logger.Error("Could not lock builder path", zap.String("ring builder file path", ringBuilderFilePath), zap.Error(err))
		return false
	}
	defer ringBuilderLock.Close()
	ringBuilder, ringBuilderFilePath, err = ring.GetRingBuilder(ringTask.typ, ringTask.policy)
	if err != nil {
		logger.Error("Could not find builder after lock", zap.Error(err))
		return false
	}
	var changedReplicas int
	changedReplicas, _, _, err = ring.Rebalance(ringBuilderFilePath, false, false, true)
	if err != nil {
		logger.Error("Error while rebalancing", zap.String("path", ringBuilderFilePath), zap.Error(err))
		return false
	}
	md5BeforeRebalance := ringTask.ring.MD5()
	if ringTask.ring, err = rm.GetRingMD5(ringTask.typ, ringTask.policy); err != nil {
		logger.Error("could not load modified ring", zap.Error(err))
		return false
	}
	// So we don't get stuck rebalancing a ring by tiny amounts for forever:
	settled := float64(changedReplicas)/(float64(ringBuilder.Parts)*ringBuilder.Replicas) < 0.01
	if settled {
		for _, dev := range ringTask.ring.AllDevices() {
			if dev == nil {
				continue
			}
			if dev.Weight == 0 && ringTask.ring.AssignmentCount(dev.Id) > 0 {
				settled = false
				break
			}
		}
	}
	if settled {
		rm.aa.db.addRingLog(ringTask.typ, ringTask.policy, fmt.Sprintf("rebalanced due to schedule; now settled"))
		rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, md5BeforeRebalance, time.Time{})
	} else {
		rm.aa.db.addRingLog(ringTask.typ, ringTask.policy, fmt.Sprintf("rebalanced due to schedule"))
		rm.aa.db.setRingHash(ringTask.typ, ringTask.policy, md5BeforeRebalance, time.Now().Add(time.Hour*time.Duration(ringBuilder.MinPartHours)+randomDuration(time.Minute, 15*time.Minute)))
	}
	return true
}

func randomDuration(min, max time.Duration) time.Duration {
	return time.Duration(int64(min) + rand.Int63n(int64(max)-int64(min)+1))
}
