package tools

// In /etc/hummingbird/andrewd-server.conf:
// [replication]
// jobs_per_device = 1      # max jobs per device
// minimum_pass_time = 60   # seconds each pass needs to take, at a minimum
// report_interval = 600    # seconds between progress reports
// db_poll_interval = 10    # seconds between database polls

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/containerserver"
	"github.com/troubling/hummingbird/objectserver"
	"go.uber.org/zap"
)

type replication struct {
	aa              *AutoAdmin
	jobsPerDevice   int
	minimumPassTime time.Duration
	reportInterval  time.Duration
	dbPollInterval  time.Duration
}

func newReplication(aa *AutoAdmin) *replication {
	r := &replication{
		aa:              aa,
		jobsPerDevice:   int(aa.serverconf.GetInt("replication", "jobs_per_device", 1)),
		minimumPassTime: time.Duration(aa.serverconf.GetInt("replication", "minimum_pass_time", 60)) * time.Second,
		reportInterval:  time.Duration(aa.serverconf.GetInt("replication", "report_interval", 600)) * time.Second,
		dbPollInterval:  time.Duration(aa.serverconf.GetInt("replication", "db_poll_interval", 10)) * time.Second,
	}
	return r
}

func (r *replication) runForever() {
	for {
		sleepFor := r.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

type replicationContext struct {
	start           time.Time
	logger          *zap.Logger
	wg              sync.WaitGroup
	cancel          chan struct{}
	progressDone    chan struct{}
	countLeft       int64
	countDone       int64
	ipDevsInUseLock sync.Mutex
	ipDevsInUse     map[string]int
}

func (r *replication) runOnce() time.Duration {
	ctx := &replicationContext{
		start:        time.Now(),
		logger:       r.aa.logger.With(zap.String("process", "replication")),
		cancel:       make(chan struct{}),
		progressDone: make(chan struct{}),
	}
	ctx.logger.Debug("starting pass")
	if err := r.aa.db.startProcessPass("replication", "", 0); err != nil {
		ctx.logger.Error("startProcessPass", zap.Error(err))
	}
	ctx.ipDevsInUse = map[string]int{}
	go r.progress(ctx)
	for {
		// We gather jobs from the db and keep trying to have them handled
		// until there are no more left or the ones left have all been requeued
		// due to errors and shouldn't be retried until next pass.
		dbQRs, err := r.aa.db.queuedReplications("", -1, "")
		if err != nil {
			ctx.logger.Error("queuedReplications", zap.Error(err))
			break
		}
		qrs := make([]*queuedReplication, 0, len(dbQRs))
		now := time.Now()
		for _, qr := range dbQRs {
			// The After(now) is just to catch anything that queued with a
			// strange time; it'll get reset to a regular time later on with a
			// updateQueuedReplication call in handleQueuedReplication/runJob.
			if qr.updated.Before(ctx.start) || qr.updated.After(now) {
				qrs = append(qrs, qr)
			}
		}
		if len(qrs) == 0 {
			break
		}
		atomic.StoreInt64(&ctx.countLeft, int64(len(qrs)))
		for _, qr := range qrs {
			r.handleQueuedReplication(ctx, qr)
		}
		time.Sleep(r.dbPollInterval)
	}
	ctx.wg.Wait()
	close(ctx.cancel)
	<-ctx.progressDone
	sleepFor := time.Until(ctx.start.Add(r.minimumPassTime))
	if sleepFor < 0 {
		sleepFor = 0
	}
	ctx.logger.Debug("pass complete")
	if err := r.aa.db.progressProcessPass("replication", "", 0, fmt.Sprintf("%d jobs done", ctx.countDone)); err != nil {
		ctx.logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := r.aa.db.completeProcessPass("replication", "", 0); err != nil {
		ctx.logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}

func (r *replication) progress(ctx *replicationContext) {
	for {
		select {
		case <-ctx.cancel:
			close(ctx.progressDone)
			return
		case <-time.After(r.reportInterval):
			activeDevs := 0
			ctx.ipDevsInUseLock.Lock()
			for _, count := range ctx.ipDevsInUse {
				if count > 0 {
					activeDevs++
				}
			}
			ctx.ipDevsInUseLock.Unlock()
			countLeft := atomic.LoadInt64(&ctx.countLeft)
			countDone := atomic.LoadInt64(&ctx.countDone)
			eta := time.Duration(int64(time.Since(ctx.start)) / countDone * countLeft)
			ctx.logger.Debug("progress", zap.Int64("jobs done", countDone), zap.Int64("jobs left", countLeft), zap.Int("active devices", activeDevs), zap.String("eta", eta.String()))
			if err := r.aa.db.progressProcessPass("replication", "", 0, fmt.Sprintf("%d jobs done, %d jobs left, %d active devices, %s eta", countDone, countLeft, activeDevs, eta)); err != nil {
				ctx.logger.Error("progressProcessPass", zap.Error(err))
			}
		}
	}
}

func (r *replication) handleQueuedReplication(ctx *replicationContext, qr *queuedReplication) {
	logger := ctx.logger.With(zap.Time("queued", qr.created), zap.String("type", qr.typ), zap.Int("policy", qr.policy), zap.Int("partition", qr.partition), zap.String("reason", qr.reason), zap.Int("from device", qr.fromDeviceID), zap.Int("to device", qr.toDeviceID))
	logger.Debug("trying to handle")
	ryng, _ := getRing("", qr.typ, qr.policy)
	if qr.toDeviceID < 0 {
		logger.Error("programming error; the to-device is invalid")
		return
	}
	toDev := ryng.AllDevices()[qr.toDeviceID]
	if toDev != nil && toDev.Weight < 0 {
		toDev = nil
	}
	if toDev == nil {
		logger.Debug("the to-device was removed from the ring since it had been queued")
		if err := r.aa.db.clearQueuedReplication(qr); err != nil {
			logger.Error("clearQueuedReplication", zap.Error(err))
		}
	}
	var fromDev *ring.Device
	if qr.fromDeviceID >= 0 {
		fromDev = ryng.AllDevices()[qr.fromDeviceID]
		if fromDev != nil && fromDev.Weight < 0 {
			fromDev = nil
		}
	}
	ctx.wg.Add(1)
	go func() {
		defer ctx.wg.Done()
		if fromDev != nil {
			r.runJob(ctx, logger, qr, fromDev, toDev)
		} else {
			for _, fromDev := range ryng.GetNodes(uint64(qr.partition)) {
				if fromDev.Id == toDev.Id {
					continue
				}
				if r.runJob(ctx, logger, qr, fromDev, toDev) {
					break
				}
			}
		}
	}()
}

func (r *replication) runJob(ctx *replicationContext, logger *zap.Logger, qr *queuedReplication, fromDev, toDev *ring.Device) bool {
	fromIPDevice := fmt.Sprintf("%s/%s", fromDev.Ip, fromDev.Device)
	toIPDevice := fmt.Sprintf("%s/%s", toDev.Ip, toDev.Device)
	ctx.ipDevsInUseLock.Lock()
	if ctx.ipDevsInUse[fromIPDevice] >= r.jobsPerDevice {
		ctx.ipDevsInUseLock.Unlock()
		logger.Debug("skipping as from-device is already at maximum usage")
		return false
	}
	if ctx.ipDevsInUse[toIPDevice] >= r.jobsPerDevice {
		ctx.ipDevsInUseLock.Unlock()
		logger.Debug("skipping as to-device is already at maximum usage")
		return false
	}
	ctx.ipDevsInUse[fromIPDevice]++
	ctx.ipDevsInUse[toIPDevice]++
	ctx.ipDevsInUseLock.Unlock()
	logger.Debug("starting job", zap.Int("job from device", fromDev.Id), zap.Int("job to device", toDev.Id))
	// sends this to the back of the queue in case of future retries
	if err := r.aa.db.updateQueuedReplication(qr); err != nil {
		logger.Error("updateQueuedReplication", zap.Error(err))
	}
	var msg string
	var success bool
	switch qr.typ {
	case "account":
		msg, success = accountserver.SendPriRepJob(
			&accountserver.PriorityRepJob{
				Partition:  uint64(qr.partition),
				FromDevice: fromDev,
				ToDevice:   toDev,
			},
			r.aa.client,
		)
	case "container":
		msg, success = containerserver.SendPriRepJob(
			&containerserver.PriorityRepJob{
				Partition:  uint64(qr.partition),
				FromDevice: fromDev,
				ToDevice:   toDev,
			},
			r.aa.client,
		)
	case "object":
		msg, success = objectserver.SendPriRepJob(
			&objectserver.PriorityRepJob{
				Partition:  uint64(qr.partition),
				FromDevice: fromDev,
				ToDevice:   toDev,
				Policy:     qr.policy,
			},
			r.aa.client,
		)
	}
	logger.Debug(msg)
	if success {
		if err := r.aa.db.clearQueuedReplication(qr); err != nil {
			logger.Error("clearQueuedReplication", zap.Error(err))
		}
		ctx.ipDevsInUseLock.Lock()
		ctx.ipDevsInUse[fromIPDevice]--
		ctx.ipDevsInUse[toIPDevice]--
		ctx.ipDevsInUseLock.Unlock()
		atomic.AddInt64(&ctx.countDone, 1)
	} else {
		ctx.ipDevsInUseLock.Lock()
		ctx.ipDevsInUse[fromIPDevice]--
		ctx.ipDevsInUse[toIPDevice]--
		ctx.ipDevsInUseLock.Unlock()
	}
	return success
}
