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
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
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

func reconReportRingMd5(client http.Client, servers []ipPort) error {
	fmt.Printf("[%s] Checking ring md5sums\n", time.Now().Format("2006-01-02 15:04:05"))
	ringMap, err := common.GetAllRingFileMd5s()
	if err != nil {
		return err
	}
	errors := 0
	successes := 0
	for _, server := range servers {
		rDataStr, err := queryHostRecon(client, server, "ringmd5")
		if err != nil {
			errors++
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rDataStr, &rData); err != nil {
			errors++
			continue
		}
		allMatch := true
		for fName, md5sum := range ringMap {
			if rData[fName] != md5sum {
				fmt.Printf(
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
	fmt.Printf("%d/%d hosts matched, %d error[s] while checking hosts.\n",
		successes, len(servers), errors)
	fmt.Println(strings.Repeat("=", 79))
	return nil
}

func reconReportMainConfMd5(client http.Client, servers []ipPort) error {
	fmt.Printf("[%s] Checking ring md5sums\n", time.Now().Format("2006-01-02 15:04:05"))
	md5Map, err := common.FileMD5("/etc/hummingbird/hummingbird.conf")
	if err != nil {
		md5Map, err = common.FileMD5("/etc/swift/swift.conf")
		if err != nil {
			return err
		}
	}
	errors := 0
	successes := 0
	for _, server := range servers {
		rDataStr, err := queryHostRecon(client, server, "hummingbirdconfmd5")
		if err != nil {
			errors++
			continue
		}
		var rData map[string]string
		if err := json.Unmarshal(rDataStr, &rData); err != nil {
			errors++
			continue
		}
		allMatch := true
		for fName, md5sum := range md5Map {
			if rData[fName] != md5sum {
				fmt.Printf(
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
	fmt.Printf("%d/%d hosts matched, %d error[s] while checking hosts.\n",
		successes, len(servers), errors)
	fmt.Println(strings.Repeat("=", 79))
	return nil
}
func ReconClient(flags *flag.FlagSet, cnf srv.ConfigLoader) {
	prefix, suffix := getAffixes()
	oring, err := ring.GetRing("object", prefix, suffix, 0)
	if err != nil {
		fmt.Printf("Unrecoverable error on recon: %v\n", err)
		return
	}
	client := http.Client{Timeout: 10 * time.Second}
	_, allServers := getRingData(oring, false)
	fmt.Println(strings.Repeat("=", 79))
	fmt.Printf("--> Starting reconnaissance on %d hosts\n", len(allServers))
	fmt.Println(strings.Repeat("=", 79))
	md5 := flags.Lookup("md5").Value.(flag.Getter).Get().(bool)
	if md5 {
		if err = reconReportRingMd5(client, allServers); err != nil {
			fmt.Printf("Unrecoverable error on ringmd5 report: %v\n", err)
		}
		if err = reconReportMainConfMd5(client, allServers); err != nil {
			fmt.Printf("Unrecoverable error on confmd5 report: %v\n", err)
		}
	}
}
