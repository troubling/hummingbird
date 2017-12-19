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
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/objectserver"
)

func queryHostRecon(client http.Client, s ipPort, endpoint string) ([]byte, error) {
	serverUrl := fmt.Sprintf("http://%s:%d/recon/%s", s.ip, s.port, endpoint)
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

func queryHostReplication(client http.Client, s ipPort) (map[string]objectserver.DeviceStats, error) {
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

func reconReportRingMd5(client http.Client, servers []ipPort, ringMap map[string]string, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking ring md5sums\n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "ringmd5")
		if err != nil {
			errors++
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			errors++
			continue
		}
		allMatch := true
		for fName, md5sum := range ringMap {
			if rData[fName] != md5sum {
				fmt.Fprintf(w,
					"!! http://%s:%d/recon/ringmd5 (%s => %s) doesn't "+
						"match on disk md5sum\n",
					server.ip, server.port, filepath.Base(fName), md5sum)
				allMatch = false
			}
		}
		if allMatch {
			successes++
		}
	}
	fmt.Fprintf(w, "%d/%d hosts matched, %d error[s] while checking hosts.\n",
		successes, len(servers), errors)
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

func reconReportMainConfMd5(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking hummingbird.conf md5sums\n", time.Now().Format("2006-01-02 15:04:05"))
	md5Map, err := common.FileMD5("/etc/hummingbird/hummingbird.conf")
	if err != nil {
		md5Map, err = common.FileMD5("/etc/swift/swift.conf")
		if err != nil {
			fmt.Fprintf(w, "Unrecoverable error on confmd5 report: %v\n", err)
			return false
		}
	}
	errors := 0
	successes := 0
	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "hummingbirdconfmd5")
		if err != nil {
			errors++
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			errors++
			continue
		}
		allMatch := true
		for fName, md5sum := range md5Map {
			if rData[fName] != md5sum {
				fmt.Fprintf(w,
					"!! http://%s:%d/recon/hummingbirdconfmd5 (%s => %s) doesn't "+
						"match on disk md5sum\n",
					server.ip, server.port, filepath.Base(fName), md5sum)
				allMatch = false
			}
		}
		if allMatch {
			successes++
		}
	}
	fmt.Fprintf(w, "%d/%d hosts matched, %d error[s] while checking hosts.\n",
		successes, len(servers), errors)
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

func reconReportTime(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking time-sync\n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0
	for _, server := range servers {
		preCall := time.Now().Round(time.Microsecond)
		rBytes, err := queryHostRecon(client, server, "hummingbirdtime")
		if err != nil {
			errors++
			continue
		}
		postCall := time.Now().Round(time.Microsecond)
		var rData map[string]time.Time
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			errors++
			continue
		}
		if rData["time"].IsZero() {
			errors++
			continue
		}
		remoteTime := rData["time"].Round(time.Microsecond)
		if remoteTime.Before(preCall) || remoteTime.After(postCall) {
			fmt.Fprintf(w,
				"!! http://%s:%d/recon/hummingbirdtime current time is %s "+
					"but remote time is %s, differs by %.2f nsecs\n",
				server.ip, server.port, postCall.Format(time.StampMicro), remoteTime.Format(time.StampMicro), float64(postCall.Sub(remoteTime)))
		} else {
			successes++
		}
	}
	fmt.Fprintf(w, "%d/%d hosts matched, %d error[s] while checking hosts.\n",
		successes, len(servers), errors)
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

func genStats() {
}

type quarData struct {
	Accounts   int                       `json:"accounts"`
	Containers int                       `json:"containers"`
	Objects    int                       `json:"objects"`
	Policies   map[string]map[string]int `json:"policies"`
}

type quarReconStats struct {
	a        map[string]int
	c        map[string]int
	o        map[string]int
	policies map[string]map[string]int
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

func reconReportQuarantine(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking quarantine\n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0
	rStats := quarReconStats{a: map[string]int{},
		c:        map[string]int{},
		o:        map[string]int{},
		policies: map[string]map[string]int{},
	}

	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "quarantined")
		rStats.a[serverId(server.ip, server.port)] = -1
		rStats.c[serverId(server.ip, server.port)] = -1
		rStats.o[serverId(server.ip, server.port)] = -1
		if err != nil {
			errors++
			continue
		}
		var rData quarData
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			errors++
			continue
		}
		rStats.a[serverId(server.ip, server.port)] = rData.Accounts
		rStats.c[serverId(server.ip, server.port)] = rData.Containers
		rStats.o[serverId(server.ip, server.port)] = rData.Objects
		for pIndex, v := range rData.Policies {
			if _, ok := rStats.policies[pIndex]; !ok {
				rStats.policies[pIndex] = map[string]int{}
			}
			rStats.policies[pIndex][serverId(server.ip, server.port)] = v["objects"]
		}
		successes++
	}
	fmt.Fprint(w, statsLine("quarantined_account", rStats.a)+"\n")
	fmt.Fprint(w, statsLine("quarantined_container", rStats.c)+"\n")
	fmt.Fprint(w, statsLine("quarantined_objects", rStats.o)+"\n")
	for pid, pmap := range rStats.policies {
		fmt.Fprint(w, statsLine(fmt.Sprintf("quarantined_objects_%s", pid), pmap)+"\n")
	}
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

func reconReportAsync(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking async pendings\n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0
	rStats := map[string]int{}

	for _, server := range servers {
		rBytes, err := queryHostRecon(client, server, "async")
		rStats[serverId(server.ip, server.port)] = -1
		if err != nil {
			errors++
			continue
		}
		var rData map[string]int
		if err := json.Unmarshal(rBytes, &rData); err != nil {
			errors++
			continue
		}
		rStats[serverId(server.ip, server.port)] = rData["async_pending"]
		successes++
	}
	fmt.Fprint(w, statsLine("async_pending", rStats)+"\n")
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

func reconReportReplicationDuration(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking replication durations\n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0
	rStats := map[string]float64{}

	totalDriveZero := 0
	for _, server := range servers {
		data, err := queryHostReplication(client, server)
		if err != nil {
			errors += 1
			continue
		}
		totalDuration := time.Duration(0)
		totalSet := float64(0)
		for _, dStats := range data {
			if dStats.LastPassDuration > 0 {
				totalDuration += dStats.LastPassDuration
				totalSet++
			} else {
				totalDriveZero++
			}
		}
		if totalSet > 0 {
			rStats[serverId(server.ip, server.port)] = totalDuration.Seconds() / totalSet
		} else {
			rStats[serverId(server.ip, server.port)] = 0
		}
		successes++
	}
	fmt.Fprint(w, statsLineF("replication_duration_secs", rStats)+"\n")
	fmt.Fprint(w, fmt.Sprintf("Number of drives not completed a pass: %d\n", totalDriveZero))
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

func reconReportReplicationPartsSec(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking replication partitions/sec\n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0
	rStats := map[string]float64{}
	driveSpeeds := map[string]float64{}
	allDur := time.Duration(0)
	allPartsDone := int64(0)

	totalDriveZero := 0
	for _, server := range servers {
		rStats[serverId(server.ip, server.port)] = -1
		data, err := queryHostReplication(client, server)
		if err != nil {
			errors += 1
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
				fmt.Fprint(w, fmt.Sprintf("! %s has no partitions\n", deviceId(server.ip, server.port, d)))
			} else {
				if dStats.PartitionsDone == 0 && driveDur > time.Hour {
					fmt.Fprint(w, fmt.Sprintf("! %s has no partitions processed\n", deviceId(server.ip, server.port, d)))
				}
			}
			driveSpeeds[deviceId(server.ip, server.port, d)] = float64(dStats.PartitionsDone) / driveDur.Seconds()
		}
		if serverPartsDone > 0 {
			rStats[serverId(server.ip, server.port)] = float64(serverPartsDone) / serverDuration.Seconds()
		} else {
			rStats[serverId(server.ip, server.port)] = 0
			totalDriveZero++
		}
		successes++
	}
	fmt.Fprint(w, statsLineF("replication_srv_parts_per_sec", rStats)+"\n")
	overallAve := float64(allPartsDone) / allDur.Seconds()
	for dId, speed := range driveSpeeds {
		if speed > 0 && speed*2 < overallAve {
			fmt.Fprint(w, fmt.Sprintf("! %s @ %.3f parts/sec is %.2fx slower than cluster parts/sec: %.3f\n", dId, speed, overallAve/speed, overallAve))
		}
	}
	fmt.Fprint(w, fmt.Sprintf("Number drives with no partitions completed: %d\n", totalDriveZero))
	fmt.Fprint(w, fmt.Sprintf("Cluster wide parts/sec: %.3f\n", overallAve))
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}
func reconReportReplicationCancelled(client http.Client, servers []ipPort, w io.Writer) bool {
	fmt.Fprintf(w, "[%s] Checking stalled replicators \n", time.Now().Format("2006-01-02 15:04:05"))
	errors := 0
	successes := 0

	rStats := map[string]int{}
	for _, server := range servers {
		data, err := queryHostReplication(client, server)
		if err != nil {
			errors += 1
			continue
		}
		for d, dStats := range data {
			rStats[deviceId(server.ip, server.port, d)] = int(dStats.CancelCount)
			if dStats.CancelCount > 0 {
				fmt.Fprint(w, fmt.Sprintf("! %s has had to restart its replicator %d times.\n", deviceId(server.ip, server.port, d), dStats.CancelCount))
			}
		}
		successes++
	}
	fmt.Fprint(w, statsLine("replication_device_cancelations", rStats)+"\n")
	fmt.Fprintln(w, strings.Repeat("=", 79))
	return successes == len(servers)
}

type reconOut struct {
	Ok  bool   `json:"ok"`
	Msg string `json:"msg"`
}

func ReconClient(flags *flag.FlagSet, cnf srv.ConfigLoader) bool {
	prefix, suffix := getAffixes()
	oring, err := ring.GetRing("object", prefix, suffix, 0)
	if err != nil {
		fmt.Printf("Unrecoverable error on recon: %v\n", err)
		return false
	}
	jsonOut := flags.Lookup("json").Value.(flag.Getter).Get().(bool)
	var buf bytes.Buffer
	w := bufio.NewWriter(os.Stdout)
	if jsonOut {
		w = bufio.NewWriter(&buf)
	}
	client := http.Client{Timeout: 10 * time.Second}
	_, allWeightedServers := getRingData(oring, false)
	fmt.Fprintln(w, strings.Repeat("=", 79))
	fmt.Fprintf(w, "--> Starting reconnaissance on %d hosts\n", len(allWeightedServers))
	fmt.Fprintln(w, strings.Repeat("=", 79))
	pass := false
	if flags.Lookup("md5").Value.(flag.Getter).Get().(bool) {
		if ringMap, err := common.GetAllRingFileMd5s(); err != nil {
			fmt.Fprintf(w, "Unrecoverable error on ringmd5 report: %v\n", err)
		} else {
			if pass = reconReportRingMd5(client, allWeightedServers, ringMap, w); pass {
				pass = reconReportMainConfMd5(client, allWeightedServers, w)
			}
		}
	}
	if flags.Lookup("time").Value.(flag.Getter).Get().(bool) {
		pass = reconReportTime(client, allWeightedServers, w)
	}
	if flags.Lookup("q").Value.(flag.Getter).Get().(bool) {
		pass = reconReportQuarantine(client, allWeightedServers, w)
	}
	if flags.Lookup("a").Value.(flag.Getter).Get().(bool) {
		pass = reconReportAsync(client, allWeightedServers, w)
	}
	if flags.Lookup("rd").Value.(flag.Getter).Get().(bool) {
		pass = reconReportReplicationDuration(client, allWeightedServers, w)
	}
	if flags.Lookup("rp").Value.(flag.Getter).Get().(bool) {
		pass = reconReportReplicationPartsSec(client, allWeightedServers, w)
	}
	if flags.Lookup("rc").Value.(flag.Getter).Get().(bool) {
		pass = reconReportReplicationCancelled(client, allWeightedServers, w)
	}
	if flags.Lookup("d").Value.(flag.Getter).Get().(bool) {
		configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
		if configs, err := conf.LoadConfigs(configFile); err != nil {
			fmt.Fprintf(w, "Error finding configs: %v\n", err)
			pass = false
		} else if len(configs) != 1 {
			fmt.Fprintf(w, "please supply single andrewd config")
			pass = false
		} else {
			return PrintLastDispersionReport(configs[0]) == nil
		}
	}
	w.Flush()
	if jsonOut {
		if jOut, err := json.Marshal(reconOut{pass, buf.String()}); err == nil {
			os.Stdout.Write(jOut)
			os.Stdout.Write([]byte("\n"))
		} else {
			fmt.Printf("Unrecoverable error on recon: %v\n", err)
			return false
		}
	}
	return pass
}
