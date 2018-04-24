package tools

// In /etc/hummingbird/andrewd-server.conf:
// [unmounted-monitor]
// initial_delay = 1                # seconds to wait between requests for the first pass
// pass_time_target = 600           # seconds to try to make subsequent passes take
// report_interval = 60             # seconds between progress reports
// state_retention = 86400          # seconds to retain state entries
// server_down_limit = 14400        # seconds a server can be down before removal
// device_unmounted_limit = 3600    # seconds a device can be unmounted before removal
// ignore_duration = 14400          # seconds to ignore a device's state change after updating its state in a ring

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

type unmountedMonitor struct {
	aa *AutoAdmin
	// delay between each request; adjusted each pass to try to make passes last passTimeTarget
	delay                time.Duration
	passTimeTarget       time.Duration
	reportInterval       time.Duration
	stateRetention       time.Duration
	serverDownLimit      time.Duration
	deviceUnmountedLimit time.Duration
	ignoreDuration       time.Duration
	ignore               map[string]time.Time
}

func newUnmountedMonitor(aa *AutoAdmin) *unmountedMonitor {
	um := &unmountedMonitor{
		aa:                   aa,
		delay:                time.Duration(aa.serverconf.GetInt("unmounted-monitor", "initial_delay", 10)) * time.Second,
		passTimeTarget:       time.Duration(aa.serverconf.GetInt("unmounted-monitor", "pass_time_target", 600)) * time.Second,
		reportInterval:       time.Duration(aa.serverconf.GetInt("unmounted-monitor", "report_interval", 60)) * time.Second,
		stateRetention:       time.Duration(aa.serverconf.GetInt("unmounted-monitor", "state_retention", 86400)) * time.Second,
		serverDownLimit:      time.Duration(aa.serverconf.GetInt("unmounted-monitor", "server_down_limit", 14400)) * time.Second,
		deviceUnmountedLimit: time.Duration(aa.serverconf.GetInt("unmounted-monitor", "device_unmounted_limit", 3600)) * time.Second,
		ignoreDuration:       time.Duration(aa.serverconf.GetInt("unmounted-monitor", "ignore_duration", 14400)) * time.Second,
		ignore:               map[string]time.Time{},
	}
	if um.delay < 0 {
		um.delay = time.Second
	}
	if um.passTimeTarget < 0 {
		um.passTimeTarget = time.Second
	}
	if um.reportInterval < 0 {
		um.reportInterval = time.Second
	}
	return um
}

func (um *unmountedMonitor) runForever() {
	for {
		sleepFor := um.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

func (um *unmountedMonitor) runOnce() time.Duration {
	type reconData struct {
		Device  string
		Mounted bool
	}
	start := time.Now()
	logger := um.aa.logger.With(zap.String("process", "unmounted monitor"))
	logger.Debug("starting pass")
	if err := um.aa.db.startProcessPass("unmounted monitor", "", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	var delays int64
	var errors int64
	var serversUp int64
	var serversDown int64
	var devicesMounted int64
	var devicesUnmounted int64
	endpoints := um.reconUnmountedEndpoints()
	cancel := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(um.reportInterval):
				d := atomic.LoadInt64(&delays)
				e := atomic.LoadInt64(&errors)
				up := atomic.LoadInt64(&serversUp)
				down := atomic.LoadInt64(&serversDown)
				mounted := atomic.LoadInt64(&devicesMounted)
				unmounted := atomic.LoadInt64(&devicesUnmounted)
				var eta time.Duration
				if d > 0 {
					eta = time.Duration(int64(time.Since(start)) / d * (int64(len(endpoints)) - d))
				}
				logger.Debug("progress", zap.Int64("endpoints so far", d), zap.Int("total endpoints", len(endpoints)), zap.Int64("errors", e), zap.Int64("servers up", up), zap.Int64("servers down", down), zap.Int64("mounted devices", mounted), zap.Int64("unmounted devices", unmounted), zap.String("eta", eta.String()))
				if err := um.aa.db.progressProcessPass("unmounted monitor", "", 0, fmt.Sprintf("%d of %d endpoints, %d errors, %d/%d servers up/down, %d/%d devices mounted/unmounted, eta %s", d, len(endpoints), e, up, down, mounted, unmounted, eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	for url, endpoint := range endpoints {
		atomic.AddInt64(&delays, 1)
		time.Sleep(um.delay)
		reconLogger := logger.With(zap.String("method", "GET"), zap.String("url", url))
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			reconLogger.Error("http.NewRequest", zap.Error(err))
			atomic.AddInt64(&errors, 1)
			continue
		}
		resp, err := um.aa.client.Do(req)
		if err != nil {
			reconLogger.Error("Do", zap.Error(err))
			atomic.AddInt64(&serversDown, 1)
			um.serverDown(reconLogger, endpoint.ip, endpoint.port)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			reconLogger.Error("Body", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			atomic.AddInt64(&serversDown, 1)
			um.serverDown(reconLogger, endpoint.ip, endpoint.port)
			continue
		}
		if resp.StatusCode/100 != 2 {
			reconLogger.Error("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			atomic.AddInt64(&serversDown, 1)
			um.serverDown(reconLogger, endpoint.ip, endpoint.port)
			continue
		}
		var items []*reconData
		if err := json.Unmarshal(body, &items); err != nil {
			reconLogger.Error("JSON", zap.String("JSON", string(body)), zap.Error(err))
			atomic.AddInt64(&serversDown, 1)
			um.serverDown(reconLogger, endpoint.ip, endpoint.port)
			continue
		}
		atomic.AddInt64(&serversUp, 1)
		um.serverUp(reconLogger, endpoint.ip, endpoint.port)
		for _, item := range items {
			devLogger := reconLogger.With(zap.String("device", item.Device))
			if item.Mounted {
				atomic.AddInt64(&devicesMounted, 1)
				um.deviceMounted(devLogger, endpoint.ip, endpoint.port, item.Device)
			} else {
				atomic.AddInt64(&devicesUnmounted, 1)
				um.deviceUnmounted(devLogger, endpoint.ip, endpoint.port, item.Device)
			}
		}
	}
	close(cancel)
	<-progressDone
	um.delay = um.passTimeTarget / time.Duration(delays)
	sleepFor := time.Until(start.Add(um.passTimeTarget))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass complete", zap.Int64("errors", errors), zap.Int64("servers up", serversUp), zap.Int64("servers down", serversDown), zap.Int64("devices mounted", devicesMounted), zap.Int64("devices unmounted", devicesUnmounted), zap.String("next delay", um.delay.String()), zap.String("sleep for", sleepFor.String()))
	if err := um.aa.db.progressProcessPass("unmounted monitor", "", 0, fmt.Sprintf("%d of %d endpoints, %d errors, %d/%d servers up/down, %d/%d devices mounted/unmounted", delays, len(endpoints), errors, serversUp, serversDown, devicesMounted, devicesUnmounted)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := um.aa.db.completeProcessPass("unmounted monitor", "", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}

type endpointIPPort struct {
	ip   string
	port int
}

func (um *unmountedMonitor) reconUnmountedEndpoints() map[string]*endpointIPPort {
	endpointMap := map[string]*endpointIPPort{}
	for _, typ := range []string{"account", "container", "object"} {
		if typ == "object" {
			for _, policy := range um.aa.policies {
				ryng, _ := getRing("", typ, policy.Index)
				for _, dev := range ryng.AllDevices() {
					if dev == nil || dev.Weight < 0 {
						continue
					}
					endpointMap[fmt.Sprintf("%s://%s:%d/recon/diskusage", dev.Scheme, dev.Ip, dev.Port)] = &endpointIPPort{ip: dev.Ip, port: dev.Port}
				}
			}
		} else {
			ryng, _ := getRing("", typ, 0)
			for _, dev := range ryng.AllDevices() {
				if dev == nil || dev.Weight < 0 {
					continue
				}
				endpointMap[fmt.Sprintf("%s://%s:%d/recon/diskusage", dev.Scheme, dev.Ip, dev.Port)] = &endpointIPPort{ip: dev.Ip, port: dev.Port}
			}
		}
	}
	return endpointMap
}

func (um *unmountedMonitor) serverUp(logger *zap.Logger, ip string, port int) {
	if err := um.aa.db.addServerState(ip, port, true, time.Now().Add(-um.stateRetention)); err != nil {
		logger.Error("could not add server up state", zap.Error(err))
	}
}

func (um *unmountedMonitor) serverDown(logger *zap.Logger, ip string, port int) {
	if err := um.aa.db.addServerState(ip, port, false, time.Now().Add(-um.stateRetention)); err != nil {
		logger.Error("could not add server down state", zap.Error(err))
		return
	}
	var lastUp time.Time
	states, err := um.aa.db.serverStates(ip, port)
	if err != nil {
		logger.Error("could not retrieve server states", zap.Error(err))
		return
	}
	for _, entry := range states {
		if entry.state {
			lastUp = entry.recorded
			break
		}
	}
	// If there were no "up" entries...
	if lastUp.IsZero() {
		if len(states) > 0 {
			// Just pretend it was last "up" at the time of the oldest entry.
			lastUp = states[len(states)-1].recorded
		} else {
			// It would be weird if there are no entries, since we supposedly
			// just added one. But, in that case, just get outta here.
			return
		}
	}
	if time.Since(lastUp) < um.serverDownLimit {
		return
	}
	um.removeFromBuilders(logger, ip, port, "")
}

func (um *unmountedMonitor) deviceMounted(logger *zap.Logger, ip string, port int, device string) {
	logger.Debug("device mounted")
	if err := um.aa.db.addDeviceState(ip, port, device, true, time.Now().Add(-um.stateRetention)); err != nil {
		logger.Error("could not add device mounted state", zap.Error(err))
	}
}

func (um *unmountedMonitor) deviceUnmounted(logger *zap.Logger, ip string, port int, device string) {
	logger.Debug("device unmounted")
	if err := um.aa.db.addDeviceState(ip, port, device, false, time.Now().Add(-um.stateRetention)); err != nil {
		logger.Error("could not add device unmounted state", zap.Error(err))
	}
	var lastUp time.Time
	states, err := um.aa.db.deviceStates(ip, port, device)
	if err != nil {
		logger.Error("could not retrieve device states", zap.Error(err))
		return
	}
	for _, entry := range states {
		if entry.state {
			lastUp = entry.recorded
			break
		}
	}
	logger.Debug("device unmounted, last up initially", zap.String("last up", lastUp.String()))
	// If there were no "up" entries...
	if lastUp.IsZero() {
		if len(states) > 0 {
			// Just pretend it was last "up" at the time of the oldest entry.
			lastUp = states[len(states)-1].recorded
			logger.Debug("device unmounted, last up assumed", zap.String("last up", lastUp.String()))
		} else {
			// It would be weird if there are no entries, since we supposedly
			// just added one. But, in that case, just get outta here.
			logger.Error("device unmounted, NO ENTRIES!")
			return
		}
	}
	logger.Debug("device unmounted, duration check", zap.String("last up", lastUp.String()), zap.String("limit", um.deviceUnmountedLimit.String()), zap.String("elapsed", time.Since(lastUp).String()))
	if time.Since(lastUp) < um.deviceUnmountedLimit {
		logger.Debug("device unmounted, but not long enough yet")
		return
	}
	logger.Debug("device unmounted, it's time to remove it")
	um.removeFromBuilders(logger, ip, port, device)
	logger.Debug("device unmounted, done trying to remove it")
}

func (um *unmountedMonitor) removeFromBuilders(logger *zap.Logger, ip string, port int, device string) {
	for _, typ := range []string{"account", "container", "object"} {
		if typ == "object" {
			for _, policy := range um.aa.policies {
				um.removeFromBuilder(logger, ip, port, device, typ, policy.Index)
			}
		} else {
			um.removeFromBuilder(logger, ip, port, device, typ, 0)
		}
	}
}

func (um *unmountedMonitor) removeFromBuilder(logger *zap.Logger, ip string, port int, device, typ string, policy int) {
	ignoreKey := fmt.Sprintf("%s:%d/%s/%s/%d", ip, port, device, typ, policy)
	if time.Now().Before(um.ignore[ignoreKey]) {
		logger.Debug("ignoring", zap.String("ignore key", ignoreKey), zap.String("until", um.ignore[ignoreKey].String()))
		return
	}
	ringBuilder, ringBuilderFilePath, err := ring.GetRingBuilder(typ, policy)
	if err != nil {
		logger.Error("Could not find builder", zap.String("type", typ), zap.Int("policy", policy), zap.Error(err))
		return
	}
	ringBuilderLock, err := ring.LockBuilderPath(ringBuilderFilePath)
	if err != nil {
		logger.Error("Could not lock builder path", zap.String("type", typ), zap.Int("policy", policy), zap.String("ring builder file path", ringBuilderFilePath), zap.Error(err))
		return
	}
	defer ringBuilderLock.Close()
	ringBuilder, ringBuilderFilePath, err = ring.GetRingBuilder(typ, policy)
	if err != nil {
		logger.Error("Could not find builder after lock", zap.String("type", typ), zap.Int("policy", policy), zap.Error(err))
		return
	}
	changed := false
	for _, dev := range ringBuilder.SearchDevs(-1, -1, ip, int64(port), "", -1, device, -1, "", "") {
		if dev.Weight >= 0 {
			ringBuilder.RemoveDev(dev.Id, false)
			changed = true
			if device == "" {
				um.aa.db.addRingLog(typ, policy, fmt.Sprintf("server %s:%d down; removed device %s id:%d", ip, port, dev.Device, dev.Id))
			} else {
				um.aa.db.addRingLog(typ, policy, fmt.Sprintf("removed unmounted device %s id:%d on server %s:%d", dev.Device, dev.Id, ip, port))
			}
		}
	}
	if !changed {
		um.ignore[ignoreKey] = time.Now().Add(um.ignoreDuration)
		logger.Debug("not changed; now ignoring", zap.String("ignore key", ignoreKey), zap.String("until", um.ignore[ignoreKey].String()))
		return
	}
	err = ringBuilder.Save(ringBuilderFilePath)
	if err != nil {
		logger.Error("Error while saving builder", zap.String("type", typ), zap.Int("policy", policy), zap.String("path", ringBuilderFilePath), zap.Error(err))
	}
	_, _, _, err = ring.Rebalance(ringBuilderFilePath, false, false, true)
	if err != nil {
		logger.Error("Error while rebalancing", zap.String("type", typ), zap.Int("policy", policy), zap.String("path", ringBuilderFilePath), zap.Error(err))
	}
	if device == "" {
		um.aa.db.addRingLog(typ, policy, fmt.Sprintf("rebalanced due to downed server %s:%d", ip, port))
	} else {
		um.aa.db.addRingLog(typ, policy, fmt.Sprintf("rebalanced due to downed device %s on %s:%d", device, ip, port))
	}
	// NOTE: The ringmonitor.go will detect the above ring changes on disk and
	// initiate a fastscan for ringscan.go to push out the new rings.
	um.ignore[ignoreKey] = time.Now().Add(um.ignoreDuration)
	logger.Debug("changed; will now ignore", zap.String("ignore key", ignoreKey), zap.String("until", um.ignore[ignoreKey].String()))
}
