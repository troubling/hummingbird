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
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/objectserver"
	"golang.org/x/net/http2"
)

func queryHostRecon(client http.Client, s *ipPort, endpoint string) ([]byte, error) {
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

func queryHostReplication(client http.Client, s *ipPort) (map[string]objectserver.DeviceStats, error) {
	serverUrl := fmt.Sprintf("http://%s:%d/progress", s.ip, s.replicationPort)
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
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
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
	s += fmt.Sprintf(
		"%d/%d hosts matched, %d error[s] while checking hosts.\n",
		r.Successes, r.Servers, len(r.Errors),
	)
	return s
}

func getRingMD5Report(client http.Client, servers []*ipPort, ringMap map[string]string) *ringMD5Report {
	report := &ringMD5Report{
		Name:    "Ring MD5 Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Pass:    true,
	}
	if ringMap == nil {
		var err error
		ringMap, err = common.GetAllRingFileMd5s()
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("Unrecoverable error on ringmd5 report: %v", err))
			report.Pass = false
			return report
		}
	}
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "ringmd5")
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			report.Pass = false
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			report.Pass = false
			continue
		}
		allMatch := true
		for fName, md5sum := range ringMap {
			if rData[fName] != md5sum {
				report.Errors = append(report.Errors, fmt.Sprintf("%s://%s:%d/recon/ringmd5 (%s => %s) doesn't match on disk md5sum %s", server.scheme, server.ip, server.port, filepath.Base(fName), rData[fName], md5sum))
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

func getMainConfMD5Report(client http.Client, servers []*ipPort) *mainConfMD5Report {
	report := &mainConfMD5Report{
		Name:    "hummingbird.conf MD5 Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Pass:    true,
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
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
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

func getHummingbirdMD5Report(client http.Client, servers []*ipPort) *hummingbirdMD5Report {
	report := &hummingbirdMD5Report{
		Name:    "hummingbird MD5 Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Pass:    true,
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
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			report.Pass = false
			continue
		}
		allMatch := true
		for fName, md5sum := range md5Map {
			if rData[fName] != md5sum {
				report.Errors = append(report.Errors, fmt.Sprintf("%s://%s:%d/recon/hummingbirdmd5 (%s => %s) doesn't match on disk md5sum %s", server.scheme, server.ip, server.port, filepath.Base(fName), rData[fName], md5sum))
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

func getTimeReport(client http.Client, servers []*ipPort) *timeReport {
	report := &timeReport{
		Name:    "Time Sync Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
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
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
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

func getQuarantineReport(client http.Client, servers []*ipPort) *quarantineReport {
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
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "quarantined")
		report.Stats.Accounts[serverId(server.ip, server.port)] = -1
		report.Stats.Containers[serverId(server.ip, server.port)] = -1
		report.Stats.Objects[serverId(server.ip, server.port)] = -1
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		var rData quarData
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		report.Stats.Accounts[serverId(server.ip, server.port)] = rData.Accounts
		report.Stats.Containers[serverId(server.ip, server.port)] = rData.Containers
		report.Stats.Objects[serverId(server.ip, server.port)] = rData.Objects
		for pIndex, v := range rData.Policies {
			if _, ok := report.Stats.Policies[pIndex]; !ok {
				report.Stats.Policies[pIndex] = map[string]int{}
			}
			report.Stats.Policies[pIndex][serverId(server.ip, server.port)] = v["objects"]
		}
		report.Successes++
	}
	report.Pass = report.Successes == report.Servers
	return report
}

type asyncReport struct {
	Name      string
	Time      time.Time
	Pass      bool
	Servers   int
	Successes int
	Errors    []string
	Stats     map[string]int
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
	s += statsLine("async_pending", r.Stats) + "\n"
	return s
}

func getAsyncReport(client http.Client, servers []*ipPort) *asyncReport {
	report := &asyncReport{
		Name:    "Async Pending Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Stats:   map[string]int{},
	}
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "async")
		report.Stats[serverId(server.ip, server.port)] = -1
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		var rData map[string]int
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", server, err))
			continue
		}
		report.Stats[serverId(server.ip, server.port)] = rData["async_pending"]
		report.Successes++
	}
	report.Pass = report.Successes == report.Servers
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

func getReplicationDurationReport(client http.Client, servers []*ipPort) *replicationDurationReport {
	report := &replicationDurationReport{
		Name:    "Replication Duration Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Stats:   map[string]float64{},
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

func getReplicationPartsSecReport(client http.Client, servers []*ipPort) *replicationPartsSecReport {
	report := &replicationPartsSecReport{
		Name:        "Replication Partitions Per Second Report",
		Time:        time.Now().UTC(),
		Servers:     len(servers),
		Stats:       map[string]float64{},
		DriveSpeeds: map[string]float64{},
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

func getReplicationCanceledReport(client http.Client, servers []*ipPort) *replicationCanceledReport {
	report := &replicationCanceledReport{
		Name:    "Stalled Replicators Report",
		Time:    time.Now().UTC(),
		Servers: len(servers),
		Stats:   map[string]int{},
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

type reconOut struct {
	Ok  bool   `json:"ok"`
	Msg string `json:"msg"`
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
	prefix, suffix := getAffixes()
	oring, err := ring.GetRing("object", prefix, suffix, 0)
	if err != nil {
		fmt.Printf("Unrecoverable error on recon: %v\n", err)
		return false
	}
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
	client := http.Client{Timeout: 10 * time.Second, Transport: transport}
	_, allWeightedServers := getRingData(oring, false)
	var reports []passable
	if flags.Lookup("md5").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getRingMD5Report(client, allWeightedServers, nil))
		reports = append(reports, getMainConfMD5Report(client, allWeightedServers))
		reports = append(reports, getHummingbirdMD5Report(client, allWeightedServers))
	}
	if flags.Lookup("time").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getTimeReport(client, allWeightedServers))
	}
	if flags.Lookup("q").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getQuarantineReport(client, allWeightedServers))
	}
	if flags.Lookup("a").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getAsyncReport(client, allWeightedServers))
	}
	if flags.Lookup("rd").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getReplicationDurationReport(client, allWeightedServers))
	}
	if flags.Lookup("rp").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getReplicationPartsSecReport(client, allWeightedServers))
	}
	if flags.Lookup("rc").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getReplicationCanceledReport(client, allWeightedServers))
	}
	if flags.Lookup("d").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getDispersionReport(flags))
	}
	if flags.Lookup("ds").Value.(flag.Getter).Get().(bool) {
		reports = append(reports, getDriveReport(flags))
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
