//  Copyright (c) 2017 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package tools

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/objectserver"
	"golang.org/x/net/http2"
)

type ipPort struct {
	ip, scheme      string
	port            int
	replicationPort int
}

func (v *ipPort) String() string {
	return fmt.Sprintf("%s://%s:%d|%d", v.scheme, v.ip, v.port, v.replicationPort)
}

func serverId(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func deviceId(ip string, port int, device string) string {
	return fmt.Sprintf("%s:%d/%s", ip, port, device)
}

func getDistinctIPServers(errors []string) ([]*ipPort, []string) {
	serversMap := map[string]*ipPort{}
	prefix, suffix := getAffixes()
	fn := func(r ring.Ring) {
		for _, dev := range r.AllDevices() {
			if dev == nil || dev.Weight < 0 {
				continue
			}
			serversMap[dev.Ip] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
		}
	}
	if r, err := ring.GetRing("account", prefix, suffix, 0); err != nil {
		errors = append(errors, err.Error())
	} else {
		fn(r)
	}
	if r, err := ring.GetRing("container", prefix, suffix, 0); err != nil {
		errors = append(errors, err.Error())
	} else {
		fn(r)
	}
	if policies, err := conf.GetPolicies(); err != nil {
		errors = append(errors, err.Error())
	} else {
		for _, policy := range policies {
			if r, err := ring.GetRing("object", prefix, suffix, policy.Index); err != nil {
				errors = append(errors, err.Error())
			} else {
				fn(r)
			}
		}
	}
	var servers []*ipPort
	for _, server := range serversMap {
		servers = append(servers, server)
	}
	return servers, errors
}

func getDistinctObjectReplicationServers(errors []string) ([]*ipPort, []string) {
	serversMap := map[string]*ipPort{}
	prefix, suffix := getAffixes()
	fn := func(r ring.Ring) {
		for _, dev := range r.AllDevices() {
			if dev == nil || dev.Weight < 0 {
				continue
			}
			serversMap[serverId(dev.Ip, dev.ReplicationPort)] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
		}
	}
	if policies, err := conf.GetPolicies(); err != nil {
		errors = append(errors, err.Error())
	} else {
		for _, policy := range policies {
			if r, err := ring.GetRing("object", prefix, suffix, policy.Index); err != nil {
				errors = append(errors, err.Error())
			} else {
				fn(r)
			}
		}
	}
	var servers []*ipPort
	for _, server := range serversMap {
		servers = append(servers, server)
	}
	return servers, errors
}

func queryHostRecon(client common.HTTPClient, s *ipPort, endpoint string) ([]byte, error) {
	serverUrl := fmt.Sprintf("%s://%s:%d/recon/%s", s.scheme, s.ip, s.port, endpoint)
	req, err := http.NewRequest("GET", serverUrl, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func queryHostReplication(client common.HTTPClient, s *ipPort) (map[string]objectserver.DeviceStats, error) {
	serverUrl := fmt.Sprintf("http://%s:%d/progress/object-replicator", s.ip, s.replicationPort)
	req, err := http.NewRequest("GET", serverUrl, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var stats map[string]objectserver.DeviceStats
	if err = json.Unmarshal(data, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

type passable interface {
	Passed() bool
}

type ringMD5Report struct {
	Name    string
	Time    time.Time
	Pass    bool
	Servers int
	Checks  int
	Errors  []string
}

func (r *ringMD5Report) Passed() bool {
	return r.Pass
}

func (r *ringMD5Report) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += fmt.Sprintf("%d ring checks done across %d servers\n", r.Checks, r.Servers)
	return s
}

func getRingMD5Report(client common.HTTPClient, ringMap map[string]string, typeToServers map[string]map[string]*ipPort) *ringMD5Report {
	// ringMap and typeToServers parameters are for overriding for tests, leave nil normally
	report := &ringMD5Report{
		Name: "Ring MD5 Report",
		Time: time.Now().UTC(),
		Pass: true,
	}
	var err error
	if ringMap == nil {
		ringMap, err = common.GetAllRingFileMd5s()
	}
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		report.Pass = false
		return report
	}
	if typeToServers == nil {
		typeToServers = map[string]map[string]*ipPort{}
		prefix, suffix := getAffixes()
		if r, err := ring.GetRing("account", prefix, suffix, 0); err != nil {
			report.Errors = append(report.Errors, err.Error())
		} else {
			for _, dev := range r.AllDevices() {
				if dev != nil && dev.Weight >= 0 {
					m, ok := typeToServers[serverId(dev.Ip, dev.Port)]
					if !ok {
						m = map[string]*ipPort{}
						typeToServers[serverId(dev.Ip, dev.Port)] = m
					}
					m["account.ring.gz"] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
				}
			}
		}
		if r, err := ring.GetRing("container", prefix, suffix, 0); err != nil {
			report.Errors = append(report.Errors, err.Error())
		} else {
			for _, dev := range r.AllDevices() {
				if dev != nil && dev.Weight >= 0 {
					m, ok := typeToServers[serverId(dev.Ip, dev.Port)]
					if !ok {
						m = map[string]*ipPort{}
						typeToServers[serverId(dev.Ip, dev.Port)] = m
					}
					m["container.ring.gz"] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
				}
			}
		}
		if policies, err := conf.GetPolicies(); err != nil {
			report.Errors = append(report.Errors, err.Error())
		} else {
			for _, policy := range policies {
				if r, err := ring.GetRing("object", prefix, suffix, policy.Index); err != nil {
					report.Errors = append(report.Errors, err.Error())
				} else {
					for _, dev := range r.AllDevices() {
						if dev != nil && dev.Weight >= 0 {
							m, ok := typeToServers[serverId(dev.Ip, dev.Port)]
							if !ok {
								m = map[string]*ipPort{}
								typeToServers[serverId(dev.Ip, dev.Port)] = m
							}
							if policy.Index == 0 {
								m["object.ring.gz"] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
							} else {
								m[fmt.Sprintf("object-%d.ring.gz", policy.Index)] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
							}
						}
					}
				}
			}
		}
	}
	for _, serverMap := range typeToServers {
		var server *ipPort
		for _, server = range serverMap {
			break
		}
		if server == nil {
			continue
		}
		report.Servers++
		rBytes, err := queryHostRecon(client, server, "ringmd5")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(rBytes)))
			continue
		}
		for fname := range serverMap {
			a := ""
			for k, v := range rData {
				if filepath.Base(k) == fname {
					a = v
					break
				}
			}
			b := ""
			for k, v := range ringMap {
				if filepath.Base(k) == fname {
					b = v
					break
				}
			}
			if a != "" || b != "" {
				report.Checks++
			}
			if a != b {
				report.Errors = append(report.Errors, fmt.Sprintf("%s://%s:%d/recon/ringmd5 (%s => %s) doesn't match on disk md5sum %s", server.scheme, server.ip, server.port, fname, a, b))
			}
		}
	}
	report.Pass = len(report.Errors) == 0
	return report
}

type mainConfMD5Report struct {
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
}

func (r *mainConfMD5Report) Passed() bool {
	return r.Pass
}

func (r *mainConfMD5Report) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += fmt.Sprintf(
		"%d/%d hosts matched, %d error[s] while checking hosts.\n",
		r.Successes, r.Servers, len(r.Errors),
	)
	return s
}

func getMainConfMD5Report(client common.HTTPClient, servers []*ipPort) *mainConfMD5Report {
	// servers parameter is for overriding for tests, leave nil normally
	report := &mainConfMD5Report{
		Name:    "hummingbird.conf MD5 Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Pass:    true,
	}
	if servers == nil {
		servers, report.Errors = getDistinctIPServers(report.Errors)
		report.Servers = len(servers)
	}
	md5Map, err := common.FileMD5("/etc/hummingbird/hummingbird.conf")
	if err != nil {
		md5Map, err = common.FileMD5("/etc/swift/swift.conf")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("Unrecoverable error on confmd5 report: %v", err))
			report.Pass = false
			return report
		}
	}
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "hummingbirdconfmd5")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			report.Pass = false
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(rBytes)))
			report.Pass = false
			continue
		}
		allMatch := true
		for fName, md5sum := range md5Map {
			if rData[fName] != md5sum {
				report.Errors = append(report.Errors, fmt.Sprintf("%s://%s:%d/recon/hummingbirdconfmd5 (%s => %s) doesn't match on disk md5sum %s", server.scheme, server.ip, server.port, filepath.Base(fName), rData[fName], md5sum))
				report.Pass = false
				allMatch = false
			}
		}
		if allMatch {
			report.Successes++
		}
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type hummingbirdMD5Report struct {
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
}

func (r *hummingbirdMD5Report) Passed() bool {
	return r.Pass
}

func (r *hummingbirdMD5Report) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += fmt.Sprintf(
		"%d/%d hosts matched, %d error[s] while checking hosts.\n",
		r.Successes, r.Servers, len(r.Errors),
	)
	return s
}

func getHummingbirdMD5Report(client common.HTTPClient, servers []*ipPort) *hummingbirdMD5Report {
	// servers parameter is for overriding for tests, leave nil normally
	report := &hummingbirdMD5Report{
		Name:    "hummingbird MD5 Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Pass:    true,
	}
	if servers == nil {
		servers, report.Errors = getDistinctIPServers(report.Errors)
		report.Servers = len(servers)
	}
	var md5Map map[string]string
	if exePath, err := os.Executable(); err != nil {
		report.Errors = append(report.Errors, err.Error())
		report.Pass = false
		return report
	} else if md5Map, err = common.FileMD5(exePath); err != nil {
		report.Errors = append(report.Errors, err.Error())
		report.Pass = false
		return report
	}
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "hummingbirdmd5")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			report.Pass = false
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(rBytes)))
			report.Pass = false
			continue
		}
		allMatch := true
		for fName, md5sum := range md5Map {
			bName := filepath.Base(fName)
			found := false
			for rName, rmd5sum := range rData {
				if filepath.Base(rName) == bName {
					found = true
					if rmd5sum != md5sum {
						report.Errors = append(report.Errors, fmt.Sprintf("%s://%s:%d/recon/hummingbirdmd5 (%s => %s) doesn't match on disk (%s => %s)", server.scheme, server.ip, server.port, rName, rmd5sum, fName, md5sum))
						report.Pass = false
						allMatch = false
					}
				}
			}
			if !found {
				report.Errors = append(report.Errors, fmt.Sprintf("%s://%s:%d/recon/hummingbirdmd5 could not find %s md5 value", server.scheme, server.ip, server.port, bName))
				report.Pass = false
				allMatch = false
			}
		}
		if allMatch {
			report.Successes++
		}
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type timeReport struct {
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
}

func (r *timeReport) Passed() bool {
	return r.Pass
}

func (r *timeReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += fmt.Sprintf(
		"%d/%d hosts matched, %d error[s] while checking hosts.\n",
		r.Successes, r.Servers, len(r.Errors),
	)
	return s
}

func getTimeReport(client common.HTTPClient, servers []*ipPort) *timeReport {
	// servers parameter is for overriding for tests, leave nil normally
	report := &timeReport{
		Name:    "Time Sync Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
	}
	if servers == nil {
		servers, report.Errors = getDistinctIPServers(report.Errors)
		report.Servers = len(servers)
	}
	for _, server := range servers {
		preCall := time.Now().Round(time.Microsecond)
		rBytes, err := queryHostRecon(client, server, "hummingbirdtime")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		postCall := time.Now().Round(time.Microsecond)
		var rData map[string]time.Time
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(rBytes)))
			continue
		}
		if rData["time"].IsZero() {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: time was zeroed", server))
			continue
		}
		remoteTime := rData["time"].Round(time.Microsecond)
		if remoteTime.Before(preCall) || remoteTime.After(postCall) {
			report.Errors = append(report.Errors, fmt.Sprintf(
				"%s://%s:%d/recon/hummingbirdtime current time is %s but remote time is %s, differs by %.2f nsecs",
				server.scheme,
				server.ip,
				server.port,
				postCall.Format(time.StampMicro),
				remoteTime.Format(time.StampMicro),
				float64(postCall.Sub(remoteTime)),
			))
		} else {
			report.Successes++
		}
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type quarData struct {
	Accounts   int                       `json:"accounts"`
	Containers int                       `json:"containers"`
	Objects    int                       `json:"objects"`
	Policies   map[string]map[string]int `json:"policies"`
}

type quarReconStats struct {
	Accounts   map[string]int
	Containers map[string]int
	Objects    map[string]int
	Policies   map[string]map[string]int
}

func statsLine(tag string, stats map[string]int) string {
	low, high, total, reported, num_none := 0, 0, 0, 0, 0
	init := true
	for _, v := range stats {
		if init {
			low, high = v, v
			init = false
		}
		if v < low {
			low = v
		}
		if v > high {
			high = v
		}
		reported++
		if v == -1 {
			num_none++ // these might not be none- just zero. also, i dont think i care
		} else {
			total += v
		}
	}
	ave, pFail := float64(0), float64(0)
	if reported > 0 {
		ave = float64(total) / float64(reported)
		pFail = float64(num_none) / float64(reported) * 100
	}
	return fmt.Sprintf("[%s] low: %d, high: %d, avg: %.1f, total: %d, Failed: %.1f%%, no_result: %d, reported: %d",
		tag, low, high, ave, total, pFail, num_none, reported)
}

func statsLineF(tag string, stats map[string]float64) string {
	low, high, total, reported, num_none := float64(0), float64(0), float64(0), int(0), int(0)
	init := true
	for _, v := range stats {
		if init {
			low, high = v, v
			init = false
		}
		if v < low {
			low = v
		}
		if v > high {
			high = v
		}
		reported++
		if v == -1 {
			num_none++ // these might not be none- just zero. also, i dont think i care
		} else {
			total += v
		}
	}
	ave, pFail := float64(0), float64(0)
	if reported > 0 {
		ave = float64(total) / float64(reported)
		pFail = float64(num_none) / float64(reported) * 100
	}
	return fmt.Sprintf("[%s] low: %.3f, high: %.3f, avg: %.3f, Failed: %.1f%%, no_result: %d, reported: %d",
		tag, low, high, ave, pFail, num_none, reported)
}

type quarantineReport struct {
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
	Stats     *quarReconStats
}

func (r *quarantineReport) Passed() bool {
	return r.Pass
}

func (r *quarantineReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += statsLine("quarantined_account", r.Stats.Accounts) + "\n"
	s += statsLine("quarantined_container", r.Stats.Containers) + "\n"
	s += statsLine("quarantined_objects", r.Stats.Objects) + "\n"
	for pid, pmap := range r.Stats.Policies {
		s += statsLine(fmt.Sprintf("quarantined_objects_%s", pid), pmap) + "\n"
	}
	return s
}

func getQuarantineReport(client common.HTTPClient, servers []*ipPort) *quarantineReport {
	// servers parameter is for overriding for tests, leave nil normally
	report := &quarantineReport{
		Name:    "Quarantine Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Stats: &quarReconStats{
			Accounts:   map[string]int{},
			Containers: map[string]int{},
			Objects:    map[string]int{},
			Policies:   map[string]map[string]int{},
		},
	}
	if servers == nil {
		servers, report.Errors = getDistinctIPServers(report.Errors)
		report.Servers = len(servers)
	}
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "quarantined")
		report.Stats.Accounts[server.ip] = -1
		report.Stats.Containers[server.ip] = -1
		report.Stats.Objects[server.ip] = -1
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		var rData quarData
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(rBytes)))
			continue
		}
		report.Stats.Accounts[server.ip] = rData.Accounts
		report.Stats.Containers[server.ip] = rData.Containers
		report.Stats.Objects[server.ip] = rData.Objects
		for pIndex, v := range rData.Policies {
			if _, ok := report.Stats.Policies[pIndex]; !ok {
				report.Stats.Policies[pIndex] = map[string]int{}
			}
			report.Stats.Policies[pIndex][server.ip] = v["objects"]
		}
		report.Successes++
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type quarantineDetailReport struct {
	Name                        string
	Time                        time.Time
	Pass                        bool
	Servers                     int
	Successes                   int
	Errors                      []string
	TypeToServerToDeviceToItems map[string]map[string]map[string][]*quarantineDetailItem
}

func (r *quarantineDetailReport) Passed() bool {
	return r.Pass
}

func (r *quarantineDetailReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += fmt.Sprintf(
		"%d/%d hosts matched, %d error[s] while checking hosts.\n",
		r.Successes, r.Servers, len(r.Errors),
	)
	var types []string
	for typ := range r.TypeToServerToDeviceToItems {
		types = append(types, typ)
	}
	sort.Strings(types)
	for _, typ := range types {
		s += fmt.Sprintln(typ)
		var servers []string
		for server := range r.TypeToServerToDeviceToItems[typ] {
			servers = append(servers, server)
		}
		sort.Strings(servers)
		for _, server := range servers {
			s += fmt.Sprintln(" ", server)
			var devices []string
			for device := range r.TypeToServerToDeviceToItems[typ][server] {
				devices = append(devices, device)
			}
			sort.Strings(devices)
			for _, device := range devices {
				s += fmt.Sprintln("   ", device)
				items := r.TypeToServerToDeviceToItems[typ][server][device]
				sort.Slice(items, func(i, j int) bool {
					if items[i].NameOnDevice < items[j].NameOnDevice {
						return true
					}
					if items[i].NameOnDevice == items[j].NameOnDevice {
						if items[i].NameInURL < items[j].NameInURL {
							return true
						}
					}
					return false
				})
				for _, item := range items {
					s += fmt.Sprintln("     ", item.NameOnDevice, item.NameInURL)
				}
			}
		}
	}
	return s
}

type quarantineDetailItem struct {
	NameOnDevice string
	NameInURL    string
}

func getQuarantineDetailReport(client common.HTTPClient, servers []*ipPort) *quarantineDetailReport {
	// servers parameter is for overriding for tests, leave nil normally
	report := &quarantineDetailReport{
		Name:                        "Quarantine Detail Report",
		Time:                        time.Now().UTC(),
		Servers:                     len(servers),
		TypeToServerToDeviceToItems: map[string]map[string]map[string][]*quarantineDetailItem{},
	}
	if servers == nil {
		servers, report.Errors = getDistinctIPServers(report.Errors)
		report.Servers = len(servers)
	}
	for _, server := range servers {
		jsonBytes, err := queryHostRecon(client, server, "quarantineddetail")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		var serverReport map[string]map[string][]*quarantineDetailItem
		if err := json.Unmarshal(jsonBytes, &serverReport); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(jsonBytes)))
			continue
		}
		for typ, deviceToItems := range serverReport {
			for device, items := range deviceToItems {
				if len(items) > 0 {
					if report.TypeToServerToDeviceToItems[typ] == nil {
						report.TypeToServerToDeviceToItems[typ] = map[string]map[string][]*quarantineDetailItem{}
					}
					if report.TypeToServerToDeviceToItems[typ][server.ip] == nil {
						report.TypeToServerToDeviceToItems[typ][server.ip] = map[string][]*quarantineDetailItem{}
					}
					report.TypeToServerToDeviceToItems[typ][server.ip][device] = items
				}
			}
		}
		report.Successes++
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type asyncReport struct {
	Name   string
	Time   time.Time
	Pass   bool
	Errors []string
	Stats  map[int]map[string]int
}

func (r *asyncReport) Passed() bool {
	return r.Pass
}

func (r *asyncReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	var ii []int
	for i := range r.Stats {
		ii = append(ii, i)
	}
	sort.Ints(ii)
	for i := range ii {
		if i == 0 {
			s += statsLine("async_pending", r.Stats[i]) + "\n"
		} else {
			s += statsLine(fmt.Sprintf("async_pending-%d", i), r.Stats[i]) + "\n"
		}
	}
	return s
}

func getAsyncReport(client common.HTTPClient) *asyncReport {
	report := &asyncReport{
		Name:  "Async Pending Report",
		Time:  time.Now().UTC(),
		Stats: map[int]map[string]int{},
	}
	policies, err := conf.GetPolicies()
	if err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("could not get policy configurations: %s", err))
		return report
	}
	prefix, suffix := getAffixes()
	for _, policy := range policies {
		oring, err := ring.GetRing("object", prefix, suffix, policy.Index)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("could not ring for policy %d: %s", policy.Index, err))
			continue
		}
		servers := map[string]*ipPort{}
		for _, dev := range oring.AllDevices() {
			if dev == nil || dev.Weight < 0 {
				continue
			}
			sId := serverId(dev.Ip, dev.Port)
			if _, ok := servers[sId]; !ok {
				servers[sId] = &ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort}
			}
		}
		getAsyncReportHelper(client, report, servers, policy.Index)
	}
	report.Pass = len(report.Errors) == 0
	return report
}

func getAsyncReportHelper(client common.HTTPClient, report *asyncReport, servers map[string]*ipPort, policy int) {
	report.Stats[policy] = map[string]int{}
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "async")
		report.Stats[policy][serverId(server.ip, server.port)] = -1
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		var rData map[string]int
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s - %q", server, err, string(rBytes)))
			continue
		}
		report.Stats[policy][serverId(server.ip, server.port)] = rData["async_pending"]
	}
}

type devicesReport struct {
	Name            string
	Time            time.Time
	Pass            bool
	Errors          []string
	AccountReport   []*deviceReport
	ContainerReport []*deviceReport
	ObjectReports   map[int][]*deviceReport
}

func (r *devicesReport) Passed() bool {
	return r.Pass
}

func (r *devicesReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	fn := func(drs []*deviceReport) {
		up := 0
		unknown := 0
		for _, dr := range drs {
			if dr.Up {
				up++
			} else if dr.LastTime.IsZero() {
				unknown++
			} else {
				s += fmt.Sprintf("    %s:%d/%s has been down since %s (%s), last checked at %s\n", dr.IP, dr.Port, dr.Device, dr.FirstTime.Format("2006-01-02 15:04:05"), time.Since(dr.FirstTime).Round(time.Second), dr.LastTime.Format("2006-01-02 15:04:05"))
			}
		}
		if up > 0 {
			s += fmt.Sprintf("    %d devices are up\n", up)
		}
		if unknown > 0 {
			s += fmt.Sprintf("    %d devices have not been monitored yet\n", unknown)
		}
	}
	s += "Account Devices:\n"
	fn(r.AccountReport)
	s += "Container Devices:\n"
	fn(r.ContainerReport)
	policies := []int{}
	for policy := range r.ObjectReports {
		policies = append(policies, policy)
	}
	sort.Ints(policies)
	for policy := range policies {
		s += fmt.Sprintf("Object-%d Devices:\n", policy)
		fn(r.ObjectReports[policy])
	}
	return s
}

type deviceReport struct {
	FirstTime time.Time
	LastTime  time.Time
	IP        string
	Port      int
	Device    string
	Weight    float64
	Up        bool
}

func getDeviceReport(flags *flag.FlagSet) *devicesReport {
	report := &devicesReport{
		Name:          "Devices Report",
		Time:          time.Now().UTC(),
		ObjectReports: map[int][]*deviceReport{},
	}
	serverconf, err := getAndrewdConf(flags)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	db, err := newDB(serverconf, "")
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	prefix, suffix := getAffixes()
	fn := func(r ring.Ring) []*deviceReport {
		var drs []*deviceReport
		for _, dev := range r.AllDevices() {
			if dev == nil || dev.Weight < 0 {
				continue
			}
			devReport := &deviceReport{
				IP:     dev.Ip,
				Port:   dev.Port,
				Device: dev.Device,
				Weight: dev.Weight,
			}
			drs = append(drs, devReport)
			states, err := db.deviceStates(dev.Ip, dev.Port, dev.Device)
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("error getting device states for %s:%d/%s: %s", dev.Ip, dev.Port, dev.Device, err.Error()))
				continue
			}
			if len(states) > 0 {
				devReport.LastTime = states[0].recorded
				devReport.Up = states[0].state
				devReport.FirstTime = devReport.LastTime
				for i := 1; i < len(states); i++ {
					if states[i].state != devReport.Up {
						devReport.FirstTime = states[i-1].recorded
						break
					}
				}
			}
		}
		return drs
	}
	if r, err := ring.GetRing("account", prefix, suffix, 0); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		report.AccountReport = fn(r)
	}
	if r, err := ring.GetRing("container", prefix, suffix, 0); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		report.ContainerReport = fn(r)
	}
	if policies, err := conf.GetPolicies(); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		for _, policy := range policies {
			if r, err := ring.GetRing("object", prefix, suffix, policy.Index); err != nil {
				report.Errors = append(report.Errors, err.Error())
			} else {
				report.ObjectReports[policy.Index] = fn(r)
			}
		}
	}
	report.Pass = len(report.Errors) == 0
	return report
}

type replicationDurationReport struct {
	Name           string
	Time           time.Time
	Pass           bool
	Servers        int
	Successes      int
	Errors         []string
	Stats          map[string]float64
	TotalDriveZero int
}

func (r *replicationDurationReport) Passed() bool {
	return r.Pass
}

func (r *replicationDurationReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	s += statsLineF("replication_duration_secs", r.Stats) + "\n"
	s += fmt.Sprintf("Number of drives not completed a pass: %d\n", r.TotalDriveZero)
	return s
}

func getReplicationDurationReport(client common.HTTPClient, servers []*ipPort) *replicationDurationReport {
	// servers parameter is for overriding for tests, leave nil normally
	report := &replicationDurationReport{
		Name:    "Replication Duration Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Stats:   map[string]float64{},
	}
	if servers == nil {
		servers, report.Errors = getDistinctObjectReplicationServers(report.Errors)
		report.Servers = len(servers)
	}
	for _, server := range servers {
		data, err := queryHostReplication(client, server)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		totalDuration := time.Duration(0)
		totalSet := float64(0)
		for _, dStats := range data {
			if dStats.LastPassDuration > 0 {
				totalDuration += dStats.LastPassDuration
				totalSet++
			} else {
				report.TotalDriveZero++
			}
		}
		if totalSet > 0 {
			report.Stats[serverId(server.ip, server.port)] = totalDuration.Seconds() / totalSet
		} else {
			report.Stats[serverId(server.ip, server.port)] = 0
		}
		report.Successes++
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type replicationPartsSecReport struct {
	Name           string
	Time           time.Time
	Pass           bool
	Servers        int
	Successes      int
	Errors         []string
	Warnings       []string
	Stats          map[string]float64
	DriveSpeeds    map[string]float64
	OverallAverage float64
	TotalDriveZero int
}

func (r *replicationPartsSecReport) Passed() bool {
	return r.Pass
}

func (r *replicationPartsSecReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	for _, w := range r.Warnings {
		s += fmt.Sprintf("! %s\n", w)
	}
	s += statsLineF("replication_srv_parts_per_sec", r.Stats) + "\n"
	s += fmt.Sprintf("Number drives with no partitions completed: %d\n", r.TotalDriveZero)
	s += fmt.Sprintf("Cluster wide parts/sec: %.3f\n", r.OverallAverage)
	return s
}

func getReplicationPartsSecReport(client common.HTTPClient, servers []*ipPort) *replicationPartsSecReport {
	// servers parameter is for overriding for tests, leave nil normally
	report := &replicationPartsSecReport{
		Name:        "Replication Partitions Per Second Report",
		Time:        time.Now().UTC(),
		Servers:     len(servers),
		Stats:       map[string]float64{},
		DriveSpeeds: map[string]float64{},
	}
	if servers == nil {
		servers, report.Errors = getDistinctObjectReplicationServers(report.Errors)
		report.Servers = len(servers)
	}
	allDur := time.Duration(0)
	allPartsDone := int64(0)
	for _, server := range servers {
		report.Stats[serverId(server.ip, server.port)] = -1
		data, err := queryHostReplication(client, server)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		serverDuration := time.Duration(0)
		serverPartsDone := int64(0)
		for d, dStats := range data {
			driveDur := dStats.LastPassFinishDate.Sub(dStats.PassStarted)
			if dStats.LastPassFinishDate.IsZero() {
				// is in middle of run
				driveDur = time.Since(dStats.PassStarted)
			}
			serverDuration += driveDur
			serverPartsDone += dStats.PartitionsDone
			allDur += driveDur
			allPartsDone += dStats.PartitionsDone
			if dStats.PartitionsTotal == 0 {
				report.Warnings = append(report.Warnings, fmt.Sprintf("%s has no partitions\n", deviceId(server.ip, server.port, d)))
			} else {
				if dStats.PartitionsDone == 0 && driveDur > time.Hour {
					report.Warnings = append(report.Warnings, fmt.Sprintf("%s has no partitions processed\n", deviceId(server.ip, server.port, d)))
				}
			}
			report.DriveSpeeds[deviceId(server.ip, server.port, d)] = float64(dStats.PartitionsDone) / driveDur.Seconds()
		}
		if serverPartsDone > 0 {
			report.Stats[serverId(server.ip, server.port)] = float64(serverPartsDone) / serverDuration.Seconds()
		} else {
			report.Stats[serverId(server.ip, server.port)] = 0
			report.TotalDriveZero++
		}
		report.Successes++
	}
	report.OverallAverage = float64(allPartsDone) / allDur.Seconds()
	if math.IsNaN(report.OverallAverage) {
		report.OverallAverage = 0
	}
	for dId, speed := range report.DriveSpeeds {
		if speed > 0 && speed*2 < report.OverallAverage {
			report.Warnings = append(report.Warnings, fmt.Sprintf("%s @ %.3f parts/sec is %.2fx slower than cluster parts/sec: %.3f\n", dId, speed, report.OverallAverage/speed, report.OverallAverage))
		}
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type replicationCanceledReport struct {
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
	Warnings  []string
	Stats     map[string]int
}

func (r *replicationCanceledReport) Passed() bool {
	return r.Pass
}

func (r *replicationCanceledReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	for _, w := range r.Warnings {
		s += fmt.Sprintf("! %s\n", w)
	}
	s += statsLine("replication_device_cancelations", r.Stats) + "\n"
	return s
}

func getReplicationCanceledReport(client common.HTTPClient, servers []*ipPort) *replicationCanceledReport {
	// servers parameter is for overriding for tests, leave nil normally
	report := &replicationCanceledReport{
		Name:    "Stalled Replicators Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Stats:   map[string]int{},
	}
	if servers == nil {
		servers, report.Errors = getDistinctObjectReplicationServers(report.Errors)
		report.Servers = len(servers)
	}
	for _, server := range servers {
		data, err := queryHostReplication(client, server)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		for d, dStats := range data {
			report.Stats[deviceId(server.ip, server.port, d)] = int(dStats.CancelCount)
			if dStats.CancelCount > 0 {
				report.Warnings = append(report.Warnings, fmt.Sprintf("%s has had to restart its replicator %d times.\n", deviceId(server.ip, server.port, d), dStats.CancelCount))
			}
		}
		report.Successes++
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type ringActionReport struct {
	Name            string
	Time            time.Time
	Pass            bool
	Errors          []string
	Warnings        []string
	AccountReport   []*ringLogEntry
	ContainerReport []*ringLogEntry
	ObjectReports   map[int][]*ringLogEntry
}

func (r *ringActionReport) Passed() bool {
	return r.Pass
}

func (r *ringActionReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	for _, w := range r.Warnings {
		s += fmt.Sprintf("! %s\n", w)
	}
	if len(r.AccountReport) > 0 {
		s += "Account Ring:\n"
		i := 0
		if len(r.AccountReport) > 10 {
			i = len(r.AccountReport) - 10
			s += fmt.Sprintf("    %d older entries; use -json for full report\n", len(r.AccountReport)-10)
		}
		for _, entry := range r.AccountReport[i:] {
			s += fmt.Sprintf("    %s %s\n", entry.Time.Format("2006-01-02 15:04"), entry.Reason)
		}
	}
	if len(r.ContainerReport) > 0 {
		s += "Container Ring:\n"
		i := 0
		if len(r.ContainerReport) > 10 {
			i = len(r.ContainerReport) - 10
			s += fmt.Sprintf("    %d older entries; use -json for full report\n", len(r.ContainerReport)-10)
		}
		for _, entry := range r.ContainerReport[i:] {
			s += fmt.Sprintf("    %s %s\n", entry.Time.Format("2006-01-02 15:04"), entry.Reason)
		}
	}
	var policies []int
	for policy := range r.ObjectReports {
		policies = append(policies, policy)
	}
	sort.Ints(policies)
	for policy := range policies {
		if len(r.ObjectReports[policy]) > 0 {
			s += fmt.Sprintf("Object Ring %d:\n", policy)
			i := 0
			if len(r.ObjectReports[policy]) > 10 {
				i = len(r.ObjectReports[policy]) - 10
				s += fmt.Sprintf("    %d older entries; use -json for full report\n", len(r.ObjectReports[policy])-10)
			}
			for _, entry := range r.ObjectReports[policy][i:] {
				s += fmt.Sprintf("    %s %s\n", entry.Time.Format("2006-01-02 15:04"), entry.Reason)
			}
		}
	}
	return s
}

func getRingActionReport(flags *flag.FlagSet) *ringActionReport {
	report := &ringActionReport{
		Name:          "Ring Action Report",
		Time:          time.Now().UTC(),
		ObjectReports: map[int][]*ringLogEntry{},
	}
	serverconf, err := getAndrewdConf(flags)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	db, err := newDB(serverconf, "")
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	report.AccountReport, err = db.ringLogs("account", 0)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
	report.ContainerReport, err = db.ringLogs("container", 0)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
	if policies, err := conf.GetPolicies(); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		for _, policy := range policies {
			report.ObjectReports[policy.Index], err = db.ringLogs("object", policy.Index)
			if err != nil {
				report.Errors = append(report.Errors, err.Error())
			}
		}
	}
	report.Pass = len(report.Errors) == 0
	return report
}

type ringBalanceReport struct {
	Name             string
	Time             time.Time
	Pass             bool
	Errors           []string
	Warnings         []string
	AccountBalance   float64
	ContainerBalance float64
	ObjectBalances   map[int]float64
}

func (r *ringBalanceReport) Passed() bool {
	return r.Pass
}

func (r *ringBalanceReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	for _, w := range r.Warnings {
		s += fmt.Sprintf("! %s\n", w)
	}
	s += fmt.Sprintf("Account Balance: %.02f\n", r.AccountBalance)
	s += fmt.Sprintf("Container Balance: %.02f\n", r.ContainerBalance)
	var policies []int
	for policy := range r.ObjectBalances {
		policies = append(policies, policy)
	}
	sort.Ints(policies)
	for policy := range policies {
		if policy == 0 {
			s += fmt.Sprintf("Object Balance: %.02f\n", r.ObjectBalances[policy])
		} else {
			s += fmt.Sprintf("Object-%d Balance: %.02f\n", policy, r.ObjectBalances[policy])
		}
	}
	return s
}

func getRingBalanceReport(flags *flag.FlagSet) *ringBalanceReport {
	report := &ringBalanceReport{
		Name:           "Ring Balance Report",
		Time:           time.Now().UTC(),
		ObjectBalances: map[int]float64{},
	}
	builder, err := ring.NewRingBuilderFromFile("/etc/hummingbird/account.builder", false)
	if err != nil {
		builder, err = ring.NewRingBuilderFromFile("/etc/swift/account.builder", false)
	}
	if err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("unable to load account ring: %s", err))
	} else {
		report.AccountBalance = builder.GetBalance()
	}
	builder, err = ring.NewRingBuilderFromFile("/etc/hummingbird/container.builder", false)
	if err != nil {
		builder, err = ring.NewRingBuilderFromFile("/etc/swift/container.builder", false)
	}
	if err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("unable to load container ring: %s", err))
	} else {
		report.AccountBalance = builder.GetBalance()
	}
	if policies, err := conf.GetPolicies(); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		for _, policy := range policies {
			bn := "object"
			if policy.Index != 0 {
				bn = fmt.Sprintf("object-%d", policy.Index)
			}
			builder, err = ring.NewRingBuilderFromFile(fmt.Sprintf("/etc/hummingbird/%s.builder", bn), false)
			if err != nil {
				builder, err = ring.NewRingBuilderFromFile(fmt.Sprintf("/etc/swift/%s.builder", bn), false)
			}
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("unable to load %s ring: %s", bn, err))
			} else {
				report.ObjectBalances[policy.Index] = builder.GetBalance()
			}
		}
	}
	report.Pass = len(report.Errors) == 0
	return report
}

func getAndrewdConf(flags *flag.FlagSet) (*conf.Config, error) {
	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	if configs, err := conf.LoadConfigs(configFile); err != nil {
		return nil, fmt.Errorf("Error finding configs: %v", err)
	} else if len(configs) != 1 {
		return nil, fmt.Errorf("please supply single andrewd config")
	} else {
		return &configs[0], nil
	}
}

func ReconClient(flags *flag.FlagSet, cnf srv.ConfigLoader) bool {
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
	}
	certFile := flags.Lookup("certfile").Value.(flag.Getter).Get().(string)
	keyFile := flags.Lookup("keyfile").Value.(flag.Getter).Get().(string)
	if certFile != "" && keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			fmt.Printf("Error getting TLS config: %v\n", err)
			return false
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			fmt.Printf("Error setting up http2: %v\n", err)
			return false
		}
	}
	// TODO: Do we want to trace requests from this client?
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}
	var reports []passable
	if flags.Lookup("progress").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getProgressReport(flags))
	}
	if flags.Lookup("md5").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getRingMD5Report(client, nil, nil))
		reports = append(reports, getMainConfMD5Report(client, nil))
		reports = append(reports, getHummingbirdMD5Report(client, nil))
	}
	if flags.Lookup("time").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getTimeReport(client, nil))
	}
	if flags.Lookup("q").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getQuarantineReport(client, nil))
	}
	if flags.Lookup("qd").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getQuarantineDetailReport(client, nil))
	}
	if flags.Lookup("a").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getAsyncReport(client))
	}
	if flags.Lookup("rd").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getReplicationDurationReport(client, nil))
	}
	if flags.Lookup("rp").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getReplicationPartsSecReport(client, nil))
	}
	if flags.Lookup("rc").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getReplicationCanceledReport(client, nil))
	}
	if flags.Lookup("d").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getDispersionReport(flags))
	}
	if flags.Lookup("ds").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getDeviceReport(flags))
	}
	if flags.Lookup("rar").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getRingActionReport(flags))
	}
	if flags.Lookup("rbr").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getRingBalanceReport(flags))
	}
	if len(reports) == 0 {
		flags.Usage()
	}
	allPassed := true
	for _, report := range reports {
		if flags.Lookup("json").Value.(flag.Getter).Get().(bool) {
			if byts, err := json.MarshalIndent(report, "", "    "); err != nil {
				fmt.Println(err)
				return false
			} else {
				fmt.Println(string(byts))
			}
		} else {
			fmt.Print(report)
		}
		allPassed = allPassed && report.Passed()
	}
	return allPassed
}
