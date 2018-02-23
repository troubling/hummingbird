package tools

// In /etc/hummingbird/andrewd-server.conf:
// [quarantine-history]
// initial_delay = 10       # seconds to wait between requests for the first pass
// pass_time_target = 86400 # seconds to try to make subsequent passes take
// keep_history = 2592000   # seconds to keep quarantined items
// report_interval = 600    # seconds between progress reports

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type quarantineHistory struct {
	aa *AutoAdmin
	// delay between each request; adjusted each pass to try to make passes last passTimeTarget
	delay           time.Duration
	passTimeTarget  time.Duration
	keepHistoryDays int
	reportInterval  time.Duration
}

const secondsInADay = 60 * 60 * 24

func newQuarantineHistory(aa *AutoAdmin) *quarantineHistory {
	qh := &quarantineHistory{
		aa:              aa,
		delay:           time.Duration(aa.serverconf.GetInt("quarantine-history", "initial_delay", 10)) * time.Second,
		passTimeTarget:  time.Duration(aa.serverconf.GetInt("quarantine-history", "pass_time_target", secondsInADay)) * time.Second,
		keepHistoryDays: int(aa.serverconf.GetInt("quarantine-history", "keep_history", secondsInADay*30) / secondsInADay),
		reportInterval:  time.Duration(aa.serverconf.GetInt("quarantine-history", "report_interval", 600)) * time.Second,
	}
	if qh.delay < 0 {
		qh.delay = time.Second
	}
	if qh.passTimeTarget < 0 {
		qh.passTimeTarget = time.Second
	}
	if qh.keepHistoryDays < 1 {
		qh.keepHistoryDays = 1
	}
	return qh
}

func (qh *quarantineHistory) runForever() {
	for {
		sleepFor := qh.runOnce()
		if sleepFor < 0 {
			break
		}
		time.Sleep(sleepFor)
	}
}

func (qh *quarantineHistory) runOnce() time.Duration {
	responseBody := &struct {
		Message     string `json:"message"`
		Days        int    `json:"days"`
		ItemsPurged int    `json:"items_purged"`
		ItemsLeft   int    `json:"items_left"`
	}{}
	start := time.Now()
	logger := qh.aa.logger.With(zap.String("process", "quarantine history"))
	logger.Debug("starting pass")
	if err := qh.aa.db.startProcessPass("quarantine history", "", 0); err != nil {
		logger.Error("startProcessPass", zap.Error(err))
	}
	var delays int64
	var errors int64
	var itemsPurged int64
	var itemsLeft int64
	urls := qh.quarantineHistoryDeleteURLs()
	cancel := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(qh.reportInterval):
				d := atomic.LoadInt64(&delays)
				e := atomic.LoadInt64(&errors)
				p := atomic.LoadInt64(&itemsPurged)
				l := atomic.LoadInt64(&itemsLeft)
				eta := time.Duration(int64(time.Since(start)) / d * (int64(len(urls)) - d))
				logger.Debug("progress", zap.Int64("urls so far", d), zap.Int("total urls", len(urls)), zap.String("eta", eta.String()))
				if err := qh.aa.db.progressProcessPass("quarantine history", "", 0, fmt.Sprintf("%d of %d urls, %d errors, %d purged, %d left for next pass, eta %s", d, len(urls), e, p, l, eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	for _, url := range urls {
		atomic.AddInt64(&delays, 1)
		time.Sleep(qh.delay)
		deleteLogger := logger.With(zap.String("method", "DELETE"), zap.String("url", url))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			deleteLogger.Error("http.NewRequest", zap.Error(err))
			atomic.AddInt64(&errors, 1)
			continue
		}
		resp, err := qh.aa.client.Do(req)
		if err != nil {
			deleteLogger.Error("Do", zap.Error(err))
			atomic.AddInt64(&errors, 1)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			deleteLogger.Error("Body", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			atomic.AddInt64(&errors, 1)
			continue
		}
		if resp.StatusCode/100 != 2 {
			deleteLogger.Error("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			atomic.AddInt64(&errors, 1)
			continue
		}
		if err := json.Unmarshal(body, &responseBody); err != nil {
			deleteLogger.Error("JSON", zap.String("JSON", string(body)), zap.Error(err))
			atomic.AddInt64(&errors, 1)
			continue
		}
		deleteLogger.Debug("response", zap.String("Message", responseBody.Message), zap.Int("Days", responseBody.Days), zap.Int("ItemsPurged", responseBody.ItemsPurged), zap.Int("ItemsLeft", responseBody.ItemsLeft))
		atomic.AddInt64(&itemsPurged, int64(responseBody.ItemsPurged))
		atomic.AddInt64(&itemsLeft, int64(responseBody.ItemsLeft))
	}
	close(cancel)
	<-progressDone
	qh.delay = qh.passTimeTarget / time.Duration(delays)
	sleepFor := time.Until(start.Add(qh.passTimeTarget))
	if sleepFor < 0 {
		sleepFor = 0
	}
	logger.Debug("pass complete", zap.Int64("errors", errors), zap.Int64("items purged", itemsPurged), zap.Int64("items left", itemsLeft), zap.String("next delay", qh.delay.String()), zap.String("sleep for", sleepFor.String()))
	if err := qh.aa.db.progressProcessPass("quarantine history", "", 0, fmt.Sprintf("%d of %d urls, %d errors, %d purged, %d left for next pass", delays, len(urls), errors, itemsPurged, itemsLeft)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := qh.aa.db.completeProcessPass("quarantine history", "", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}

// quarantineHistoryDeleteURLs returns a list of urls used to purge older
// quarantined history items, based on all the devices on all the servers in
// all the rings for the hummingbird configuration.
func (qh *quarantineHistory) quarantineHistoryDeleteURLs() []string {
	urlMap := map[string]struct{}{}
	for _, typ := range []string{"account", "container", "object"} {
		if typ == "object" {
			for _, policy := range qh.aa.policies {
				ringg, _ := getRing("", typ, policy.Index)
				for _, dev := range ringg.AllDevices() {
					urlMap[fmt.Sprintf("%s://%s:%d/recon/%s/quarantinedhistory/%ss/%d", dev.Scheme, dev.Ip, dev.Port, dev.Device, typ, qh.keepHistoryDays)] = struct{}{}
				}
			}
		} else {
			ringg, _ := getRing("", typ, 0)
			for _, dev := range ringg.AllDevices() {
				urlMap[fmt.Sprintf("%s://%s:%d/recon/%s/quarantinedhistory/%ss/%d", dev.Scheme, dev.Ip, dev.Port, dev.Device, typ, qh.keepHistoryDays)] = struct{}{}
			}
		}
	}
	urls := make([]string, 0, len(urlMap))
	for url := range urlMap {
		urls = append(urls, url)
	}
	return urls
}
