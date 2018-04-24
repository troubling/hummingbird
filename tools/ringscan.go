package tools

// In /etc/hummingbird/andrewd-server.conf:
// [ring-scan]
// initial_delay = 1            # seconds to wait between ring checks for the first pass
// pass_time_target = 600       # seconds to try to make subsequent passes take
// report_interval = 600        # seconds between progress reports
// fast_scan_concurrency = 25   # number of concurrent ring transfers allowed

// Example from recon:
// $ curl http://localhost:6010/recon/ringmd5
// {
//   "/etc/hummingbird/account.ring.gz": "50637e9e0eac3b5d085d21950b697f6b",
//   "/etc/hummingbird/container.ring.gz": "488602c6c8f0aa7c48c389524a8ec22b",
//   "/etc/hummingbird/object-1.ring.gz": "a16b7943bfe21cba5e0b42f811b67fba",
//   "/etc/hummingbird/object-2.ring.gz": "3529ce642ff99b724140f7af92a7f014",
//   "/etc/hummingbird/object.ring.gz": "94744009e53c8c4d1895b534285cd78c"
// }

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

type ringScan struct {
	aa *AutoAdmin
	// delay between each ring check; adjusted each pass to try to make passes last passTimeTarget
	delay               time.Duration
	passTimeTarget      time.Duration
	reportInterval      time.Duration
	fastScanConcurrency int
	fastScan            bool
	prefix              string
	suffix              string
}

func newRingScan(aa *AutoAdmin) *ringScan {
	rs := &ringScan{
		aa:                  aa,
		delay:               time.Duration(aa.serverconf.GetInt("ring-scan", "initial_delay", 1)) * time.Second,
		passTimeTarget:      time.Duration(aa.serverconf.GetInt("ring-scan", "pass_time_target", 600)) * time.Second,
		reportInterval:      time.Duration(aa.serverconf.GetInt("ring-scan", "report_interval", 600)) * time.Second,
		fastScanConcurrency: int(aa.serverconf.GetInt("ring-scan", "fast_scan_concurrency", 25)),
	}
	if rs.delay < 0 {
		rs.delay = time.Second
	}
	if rs.passTimeTarget < 0 {
		rs.passTimeTarget = time.Second
	}
	if rs.reportInterval < 0 {
		rs.reportInterval = time.Second
	}
	if rs.fastScanConcurrency < 1 {
		rs.fastScanConcurrency = 1
	}
	rs.fastScan = true
	rs.prefix, rs.suffix = getAffixes()
	return rs
}

func (rs *ringScan) runForever() {
	for {
		sleepFor := rs.runOnce()
		if sleepFor < 0 {
			break
		}
		select {
		case <-rs.aa.fastRingScan:
		CLEAR:
			for {
				select {
				case <-rs.aa.fastRingScan:
				default:
					break CLEAR
				}
			}
			rs.fastScan = true
			rs.runOnce()
			rs.fastScan = false
		case <-time.After(sleepFor):
		}
	}
}

func (rs *ringScan) runOnce() time.Duration {
	start := time.Now()
	logger := rs.aa.logger.With(zap.String("process", "ring scan"))
	if rs.fastScan {
		logger.Debug("starting fast scan")
		if err := rs.aa.db.startProcessPass("ring scan", "", 0); err != nil {
			logger.Error("startProcessPass", zap.Error(err))
		}
	} else {
		logger.Debug("starting pass")
		if err := rs.aa.db.startProcessPass("ring scan", "", 0); err != nil {
			logger.Error("startProcessPass", zap.Error(err))
		}
	}
	var delays int64
	var errors int64
	urls := rs.ringMD5URLs()
	cancel := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				close(progressDone)
				return
			case <-time.After(rs.reportInterval):
				d := atomic.LoadInt64(&delays)
				e := atomic.LoadInt64(&errors)
				var eta time.Duration
				if d > 0 {
					eta = time.Duration(int64(time.Since(start)) / d * (int64(len(urls)) - d))
				}
				logger.Debug("progress", zap.Int64("urls so far", d), zap.Int("total urls", len(urls)), zap.String("eta", eta.String()))
				if err := rs.aa.db.progressProcessPass("ring scan", "", 0, fmt.Sprintf("%d of %d urls, %d errors, eta %s", d, len(urls), e, eta)); err != nil {
					logger.Error("progressProcessPass", zap.Error(err))
				}
			}
		}
	}()
	reconURL := func(getURL string) {
		atomic.AddInt64(&delays, 1)
		getLogger := logger.With(zap.String("method", "GET"), zap.String("url", getURL))
		req, err := http.NewRequest("GET", getURL, nil)
		if err != nil {
			getLogger.Error("http.NewRequest", zap.Error(err))
			atomic.AddInt64(&errors, 1)
			return
		}
		resp, err := rs.aa.client.Do(req)
		if err != nil {
			getLogger.Error("Do", zap.Error(err))
			atomic.AddInt64(&errors, 1)
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			getLogger.Error("Body", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			atomic.AddInt64(&errors, 1)
			return
		}
		if resp.StatusCode/100 != 2 {
			getLogger.Error("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			atomic.AddInt64(&errors, 1)
			return
		}
		ringMD5s := map[string]string{}
		if err := json.Unmarshal(body, &ringMD5s); err != nil {
			getLogger.Error("JSON", zap.String("JSON", string(body)), zap.Error(err))
			atomic.AddInt64(&errors, 1)
			return
		}
		getLogger.Debug("response", zap.Any("ring md5s", ringMD5s))
		for ringPath, md5 := range ringMD5s {
			if !strings.HasSuffix(ringPath, ".ring.gz") {
				continue
			}
			parts := strings.SplitN(strings.SplitN(path.Base(ringPath), ".", 2)[0], "-", 2)
			typ := parts[0]
			policy := 0
			if len(parts) > 1 {
				policy, err = strconv.Atoi(parts[1])
				if err != nil {
					getLogger.Error("policy parsing error", zap.String("ring path", ringPath), zap.Error(err))
				}
			}
			getLogger.Debug("parsed", zap.String("type", typ), zap.Int("policy", policy), zap.String("md5", md5))
			ryng, err := ring.GetRingMD5(typ, rs.prefix, rs.suffix, policy)
			if err != nil {
				getLogger.Error("error getting ring", zap.String("type", typ), zap.Int("policy", policy), zap.String("md5", md5), zap.Error(err))
			}
			if md5 != ryng.MD5() {
				getLogger.Debug("pushing ring", zap.String("type", typ), zap.Int("policy", policy), zap.String("previous md5", md5), zap.String("new md5", ryng.MD5()))
				u, err := url.Parse(getURL)
				if err != nil {
					getLogger.Error("url.Parse", zap.String("url", getURL), zap.Error(err))
					atomic.AddInt64(&errors, 1)
					continue
				}
				putURL := fmt.Sprintf("%s://%s/ring%s", u.Scheme, u.Host, ringPath)
				putLogger := logger.With(zap.String("method", "PUT"), zap.String("url", putURL))
				fp, err := os.Open(ryng.DiskPath())
				if err != nil {
					getLogger.Error("os.Open", zap.String("disk path", ryng.DiskPath()), zap.Error(err))
					atomic.AddInt64(&errors, 1)
					continue
				}
				func() { // sub func so we can have the defer
					defer fp.Close()
					req, err := http.NewRequest("PUT", putURL, fp)
					if err != nil {
						putLogger.Error("http.NewRequest", zap.Error(err))
						atomic.AddInt64(&errors, 1)
						return
					}
					req.Header.Set("Etag", ryng.MD5())
					resp, err := rs.aa.client.Do(req)
					if err != nil {
						putLogger.Error("Do", zap.Error(err))
						atomic.AddInt64(&errors, 1)
						return
					}
					resp.Body.Close()
					if resp.StatusCode/100 != 2 {
						putLogger.Error("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
						atomic.AddInt64(&errors, 1)
						return
					}
				}()
			}
		}
	}
	if rs.fastScan {
		var wg sync.WaitGroup
		fastScanURLs := make(chan string, rs.fastScanConcurrency)
		for i := 0; i < rs.fastScanConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					url := <-fastScanURLs
					if url == "" {
						return
					}
					reconURL(url)
				}
			}()
		}
		for _, url := range urls {
			fastScanURLs <- url
		}
		close(fastScanURLs)
		wg.Wait()
	} else {
		for _, url := range urls {
			time.Sleep(rs.delay)
			reconURL(url)
		}
	}
	close(cancel)
	<-progressDone
	if !rs.fastScan {
		rs.delay = rs.passTimeTarget / time.Duration(delays)
	}
	sleepFor := time.Until(start.Add(rs.passTimeTarget))
	if sleepFor < 0 {
		sleepFor = 0
	}
	if rs.fastScan {
		logger.Debug("fast scan complete", zap.Int64("errors", errors))
	} else {
		logger.Debug("pass complete", zap.Int64("errors", errors), zap.String("next delay", rs.delay.String()), zap.String("sleep for", sleepFor.String()))
	}
	if err := rs.aa.db.progressProcessPass("ring scan", "", 0, fmt.Sprintf("%d of %d urls, %d errors", delays, len(urls), errors)); err != nil {
		logger.Error("progressProcessPass", zap.Error(err))
	}
	if err := rs.aa.db.completeProcessPass("ring scan", "", 0); err != nil {
		logger.Error("completeProcessPass", zap.Error(err))
	}
	return sleepFor
}

// ringMD5URLs returns a list of urls used to obtain the MD5s of the rings on
// that server, based on all the devices on all the servers in all the rings
// for the hummingbird configuration. Note that this distills down to one per
// IP destination. If there are account, container, and object servers running
// on the same IP, it is assumed they all share the same /etc/hummingbird or
// /etc/swift directory, and the recon/ringmd5 call will return the MD5s for
// all the ring files in that directory.
func (rs *ringScan) ringMD5URLs() []string {
	urlMap := map[string]string{}
	for _, typ := range []string{"account", "container", "object"} {
		if typ == "object" {
			for _, policy := range rs.aa.policies {
				ryng, _ := getRing("", typ, policy.Index)
				for _, dev := range ryng.AllDevices() {
					if !dev.Active() {
						continue
					}
					urlMap[dev.Ip] = fmt.Sprintf("%s://%s:%d/recon/ringmd5", dev.Scheme, dev.Ip, dev.Port)
				}
			}
		} else {
			ryng, _ := getRing("", typ, 0)
			for _, dev := range ryng.AllDevices() {
				if !dev.Active() {
					continue
				}
				urlMap[dev.Ip] = fmt.Sprintf("%s://%s:%d/recon/ringmd5", dev.Scheme, dev.Ip, dev.Port)
			}
		}
	}
	urls := make([]string, 0, len(urlMap))
	for _, url := range urlMap {
		urls = append(urls, url)
	}
	return urls
}
