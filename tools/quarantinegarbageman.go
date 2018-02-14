package tools

// In /etc/hummingbird/andrewd-server.conf:
// [quarantine-garbageman]
// initial-delay = 10       # seconds to wait between requests for the first pass
// pass-time-target = 86400 # seconds to try to make subsequent passes take
// keep-history = 2592000   # seconds to keep quarantined items

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type quarantineGarbageman struct {
	aa *AutoAdmin
	// delay between each request; adjusted each pass to try to make passes last passTimeTarget
	delay           time.Duration
	passTimeTarget  time.Duration
	keepHistoryDays int
}

const secondsInADay = 60 * 60 * 24

func newQuarantineGarbageman(aa *AutoAdmin) *quarantineGarbageman {
	qg := &quarantineGarbageman{
		aa:              aa,
		delay:           time.Duration(aa.serverconf.GetInt("quarantine-garbageman", "initial-delay", 10)) * time.Second,
		passTimeTarget:  time.Duration(aa.serverconf.GetInt("quarantine-garbageman", "pass-time-target", secondsInADay)) * time.Second,
		keepHistoryDays: int(aa.serverconf.GetInt("quarantine-garbageman", "keep-history", secondsInADay*30) / secondsInADay),
	}
	if qg.delay < 0 {
		qg.delay = time.Second
	}
	if qg.passTimeTarget < 0 {
		qg.passTimeTarget = time.Second
	}
	if qg.keepHistoryDays < 1 {
		qg.keepHistoryDays = 1
	}
	return qg
}

func (qg *quarantineGarbageman) runForever() {
	for {
		qg.runOnce()
	}
}

func (qg *quarantineGarbageman) runOnce() {
	responseBody := &struct {
		Message     string `json:"message"`
		Days        int    `json:"days"`
		ItemsPurged int    `json:"items_purged"`
		ItemsLeft   int    `json:"items_left"`
	}{}
	start := time.Now()
	logger := qg.aa.logger.With(zap.String("process", "quarantine garbageman"))
	logger.Debug("starting pass")
	delays := 0
	errors := 0
	itemsPurged := 0
	itemsLeft := 0
	for _, url := range qg.quarantineHistoryDeleteURLs() {
		delays++
		time.Sleep(qg.delay)
		deleteLogger := logger.With(zap.String("method", "DELETE"), zap.String("url", url))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			deleteLogger.Error("http.NewRequest", zap.Error(err))
			errors++
			continue
		}
		resp, err := qg.aa.client.Do(req)
		if err != nil {
			deleteLogger.Error("Do", zap.Error(err))
			errors++
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			deleteLogger.Error("Body", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			errors++
			continue
		}
		if resp.StatusCode/100 != 2 {
			deleteLogger.Error("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			errors++
			continue
		}
		if err := json.Unmarshal(body, &responseBody); err != nil {
			deleteLogger.Error("JSON", zap.String("JSON", string(body)), zap.Error(err))
			errors++
			continue
		}
		deleteLogger.Debug("response", zap.String("Message", responseBody.Message), zap.Int("Days", responseBody.Days), zap.Int("ItemsPurged", responseBody.ItemsPurged), zap.Int("ItemsLeft", responseBody.ItemsLeft))
		itemsPurged += responseBody.ItemsPurged
		itemsLeft += responseBody.ItemsLeft
	}
	qg.delay = qg.passTimeTarget / time.Duration(delays)
	logger.Debug("pass complete", zap.Int("errors", errors), zap.Int("itemsPurged", itemsPurged), zap.Int("itemsLeft", itemsLeft), zap.String("next-delay", qg.delay.String()))
	sleepTo := time.Until(start.Add(qg.passTimeTarget))
	if sleepTo > 0 {
		time.Sleep(sleepTo)
	}
}

// quarantineHistoryDeleteURLs returns a list of urls used to purge older
// quarantined history items, based on all the devices on all the servers in
// all the rings for the hummingbird configuration.
func (qg *quarantineGarbageman) quarantineHistoryDeleteURLs() []string {
	urlMap := map[string]struct{}{}
	for _, typ := range []string{"account", "container", "object"} {
		if typ == "object" {
			for _, policy := range qg.aa.policies {
				ringg, _ := getRing("", typ, policy.Index)
				for _, dev := range ringg.AllDevices() {
					urlMap[fmt.Sprintf("%s://%s:%d/recon/%s/quarantinedhistory/%ss/%d", dev.Scheme, dev.Ip, dev.Port, dev.Device, typ, qg.keepHistoryDays)] = struct{}{}
				}
			}
		} else {
			ringg, _ := getRing("", typ, 0)
			for _, dev := range ringg.AllDevices() {
				urlMap[fmt.Sprintf("%s://%s:%d/recon/%s/quarantinedhistory/%ss/%d", dev.Scheme, dev.Ip, dev.Port, dev.Device, typ, qg.keepHistoryDays)] = struct{}{}
			}
		}
	}
	urls := make([]string, 0, len(urlMap))
	for url := range urlMap {
		urls = append(urls, url)
	}
	return urls
}
