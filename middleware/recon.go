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

package middleware

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/srv"
)

func DumpReconCache(reconCachePath string, source string, cacheData map[string]interface{}) error {
	reconFile := filepath.Join(reconCachePath, source+".recon")

	if lock, err := fs.LockPath(filepath.Dir(reconFile), 5*time.Second); err != nil {
		return err
	} else {
		defer lock.Close()
	}

	filedata, _ := ioutil.ReadFile(reconFile)
	var reconData = make(map[string]interface{})
	if filedata != nil && len(filedata) > 0 {
		var data interface{}
		if json.Unmarshal(filedata, &data) == nil {
			if _, ok := data.(map[string]interface{}); ok {
				reconData = data.(map[string]interface{})
			}
		}
	}
	for key, item := range cacheData {
		switch item := item.(type) {
		case map[string]interface{}:
			if len(item) == 0 {
				delete(reconData, key)
				continue
			}
			if _, ok := reconData[key].(map[string]interface{}); !ok {
				reconData[key] = make(map[string]interface{})
			}
			for itemk, itemv := range item {
				if itemvmap, ok := itemv.(map[string]interface{}); ok && len(itemvmap) == 0 {
					delete(reconData[key].(map[string]interface{}), itemk)
				} else if itemv == nil {
					delete(reconData[key].(map[string]interface{}), itemk)
				} else {
					reconData[key].(map[string]interface{})[itemk] = itemv
				}
			}
		case nil:
			delete(reconData, key)
		default:
			reconData[key] = item
		}
	}
	newdata, err := json.Marshal(reconData)
	if err != nil {
		return err
	}
	f, err := fs.NewAtomicFileWriter(reconCachePath, reconCachePath)
	if err != nil {
		return err
	}
	defer f.Abandon()
	f.Write(newdata)
	return f.Save(reconFile)
}

// getMem dumps the contents of /proc/meminfo if it's available, otherwise it pulls what it can from gopsutil/mem
func getMem() interface{} {
	if fp, err := os.Open("/proc/meminfo"); err == nil {
		defer fp.Close()
		results := make(map[string]string)
		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			vals := strings.Split(scanner.Text(), ":")
			results[strings.TrimSpace(vals[0])] = strings.TrimSpace(vals[1])
		}
		return results
	} else {
		vmem, err := mem.VirtualMemory()
		if err != nil {
			return nil
		}
		swap, err := mem.SwapMemory()
		if err != nil {
			return nil
		}
		return map[string]string{
			"MemTotal":  strconv.FormatUint(vmem.Total, 10),
			"MemFree":   strconv.FormatUint(vmem.Available, 10),
			"Buffers":   strconv.FormatUint(vmem.Buffers, 10),
			"Cached":    strconv.FormatUint(vmem.Cached, 10),
			"SwapTotal": strconv.FormatUint(swap.Total, 10),
			"SwapFree":  strconv.FormatUint(swap.Free, 10),
		}
	}
}

func getSockstats() interface{} {
	results := make(map[string]int64)

	fp, err := os.Open("/proc/net/sockstat")
	if err != nil {
		return nil
	}
	defer fp.Close()
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "TCP: inuse") {
			parts := strings.Split(line, " ")
			results["tcp_in_use"], _ = strconv.ParseInt(parts[2], 10, 64)
			results["orphan"], _ = strconv.ParseInt(parts[4], 10, 64)
			results["time_wait"], _ = strconv.ParseInt(parts[6], 10, 64)
			results["tcp_mem_allocated_bytes"], _ = strconv.ParseInt(parts[10], 10, 64)
			results["tcp_mem_allocated_bytes"] *= int64(os.Getpagesize())
		}
	}

	fp, err = os.Open("/proc/net/sockstat6")
	if err != nil {
		return nil
	}
	defer fp.Close()
	scanner = bufio.NewScanner(fp)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "TCP6: inuse") {
			parts := strings.Split(line, " ")
			results["tcp6_in_use"], _ = strconv.ParseInt(parts[2], 10, 64)
		}
	}

	return results
}

func getLoad() interface{} {
	results := make(map[string]interface{})
	avg, err := load.Avg()
	if err != nil {
		return nil
	}
	misc, err := load.Misc()
	if err != nil {
		return nil
	}
	pids, err := process.Pids()
	if err != nil {
		return nil
	}
	results["1m"] = avg.Load1
	results["5m"] = avg.Load5
	results["15m"] = avg.Load15
	results["tasks"] = fmt.Sprintf("%d/%d", misc.ProcsRunning, len(pids))
	// swift's recon puts the pid of the last created process in this field, which seems kind of useless.
	// I'm making it the number of processes, which seems like what it was meant to be.
	results["processes"] = len(pids)
	// also adding these two fields, since they might be useful.
	results["running"] = misc.ProcsRunning
	results["blocked"] = misc.ProcsBlocked
	return results
}

func getMounts() interface{} {
	results := make([]map[string]string, 0)
	partitions, err := disk.Partitions(true)
	if err != nil {
		return nil
	}
	for _, part := range partitions {
		results = append(results, map[string]string{"device": part.Device, "path": part.Mountpoint})
	}
	return results
}

func fromReconCache(reconCachePath string, source string, keys ...string) (interface{}, error) {
	results := make(map[string]interface{})
	for _, key := range keys {
		results[key] = nil
	}
	filedata, err := ioutil.ReadFile(filepath.Join(reconCachePath, fmt.Sprintf("%s.recon", source)))
	if err != nil {
		results["recon_error"] = fmt.Sprintf("Error: %s", err)
		return results, nil
	}
	var data interface{}
	json.Unmarshal(filedata, &data)
	switch data := data.(type) {
	case map[string]interface{}:
		for _, key := range keys {
			results[key] = data[key]
		}
	default:
		return nil, fmt.Errorf("Unexpected data type %T in recon file.", data)
	}
	return results, nil
}

func getUnmounted(driveRoot string) (interface{}, error) {
	unmounted := make([]map[string]interface{}, 0)
	dirInfo, err := os.Stat(driveRoot)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range fileInfo {
		if info.Sys().(*syscall.Stat_t).Dev == dirInfo.Sys().(*syscall.Stat_t).Dev {
			unmounted = append(unmounted, map[string]interface{}{"device": info.Name(), "mounted": false})
		}
	}
	return unmounted, nil
}

func getTotalAsyncs(driveRoot, reconCachePath string) (interface{}, error) {
	asyncs := map[string]int64{}
	_, err := os.Stat(driveRoot)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	total := int64(0)
	for _, info := range fileInfo {
		rKey := fmt.Sprintf("async_pending_%s", info.Name())
		content, err := fromReconCache(reconCachePath, "object", rKey)
		if err != nil {
			return nil, err
		}
		amap, ok := content.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid recon map data: %v", content)
		}
		cnt, ok := amap[rKey].(float64)
		if !ok {
			cnt = 0
		}
		asyncs[rKey] = int64(cnt)
		total += int64(cnt)
	}
	asyncs["async_pending"] = total
	return asyncs, nil
}

func ListDevices(driveRoot string) (map[string][]string, error) {
	fileInfo, err := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	fileList := make([]string, 0)
	for _, info := range fileInfo {
		fileList = append(fileList, info.Name())
	}
	return map[string][]string{driveRoot: fileList}, nil
}

func quarantineCounts(driveRoot string) (map[string]interface{}, error) {
	qcounts := map[string]interface{}{"objects": 0, "containers": 0, "accounts": 0, "policies": map[string]map[string]uint64{}}
	deviceList, err := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range deviceList {
		qTypeList, err := ioutil.ReadDir(filepath.Join(driveRoot, info.Name(), "quarantined"))
		if err != nil {
			if os.IsNotExist(err) {
				return qcounts, nil
			}
			return nil, err
		}
		policyDict := map[string]map[string]uint64{}
		oCnt := uint64(0)
		for _, qType := range qTypeList {
			key := qType.Name()
			if strings.HasPrefix(key, "objects") {
				if stat, err := os.Stat(filepath.Join(driveRoot, info.Name(), "quarantined", key)); err == nil {
					pIndex := "0"
					if qarr := strings.SplitN(key, "-", 2); len(qarr) > 1 {
						pIndex = qarr[1]
					}
					if _, err := strconv.ParseInt(pIndex, 10, 64); err == nil {
						cnt := stat.Sys().(*syscall.Stat_t).Nlink - 2
						policyDict[pIndex] = map[string]uint64{
							"objects": cnt}
						oCnt += cnt

					} else {
						return nil, err
					}
				} else {
					return nil, err
				}
			} else {
				if _, ok := qcounts[key]; ok {
					if stat, err := os.Stat(filepath.Join(driveRoot, info.Name(), "quarantined", key)); err == nil {
						qcounts[key] = stat.Sys().(*syscall.Stat_t).Nlink - 2
					} else {
						return nil, err
					}
				}

			}
		}
		if len(policyDict) > 0 {
			qcounts["policies"] = policyDict
		}
		qcounts["objects"] = oCnt
	}
	return qcounts, nil
}

func quarantineDelete(driveRoot, deviceName, reconType, itemPath string) (map[string]interface{}, error) {
	cleanedDeviceName := path.Clean(deviceName)
	// don't allow full paths, empty paths ".", nor up paths ".."
	if cleanedDeviceName[0] == '/' || cleanedDeviceName[0] == '.' {
		return nil, fmt.Errorf("invalid device name given: %q", deviceName)
	}
	deviceName = cleanedDeviceName
	if reconType != "accounts" && reconType != "containers" && reconType != "objects" && !strings.HasPrefix(reconType, "objects-") {
		return nil, fmt.Errorf("invalid recon type: %q", reconType)
	}
	cleanedItemPath := path.Clean(itemPath)
	// don't allow full paths, empty paths ".", nor up paths ".."
	if cleanedItemPath[0] == '/' || cleanedItemPath[0] == '.' {
		return nil, fmt.Errorf("invalid item path given: %q", itemPath)
	}
	fromPath := path.Join(driveRoot, deviceName, "quarantined", reconType, cleanedItemPath)
	toDir := path.Join(driveRoot, deviceName, "quarantined-history", reconType)
	toPath := path.Join(toDir, cleanedItemPath)
	if err := os.MkdirAll(toDir, 0755); err != nil {
		return nil, err
	}
	if err := os.Rename(fromPath, toPath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if err := os.Chtimes(toPath, time.Now(), time.Now()); err != nil {
		return nil, err
	}
	return nil, nil
}

func quarantineHistoryDelete(driveRoot, deviceName, reconType, trailingPath string) (map[string]interface{}, error) {
	cleanedDeviceName := path.Clean(deviceName)
	// don't allow full paths, empty paths ".", nor up paths ".."
	if cleanedDeviceName[0] == '/' || cleanedDeviceName[0] == '.' {
		return nil, fmt.Errorf("invalid device name given: %q", deviceName)
	}
	deviceName = cleanedDeviceName
	if reconType != "accounts" && reconType != "containers" && reconType != "objects" && !strings.HasPrefix(reconType, "objects-") {
		return nil, fmt.Errorf("invalid recon type: %q", reconType)
	}
	days, err := strconv.Atoi(trailingPath)
	if err != nil {
		return nil, fmt.Errorf("could not parse days value: %q", trailingPath)
	}
	historyPath := path.Join(driveRoot, deviceName, "quarantined-history", reconType)
	items, err := ioutil.ReadDir(historyPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	checkTime := time.Now().Add(-time.Hour * time.Duration(24*days))
	itemsPurged := 0
	itemsLeft := 0
	for _, item := range items {
		if item.ModTime().Before(checkTime) {
			os.RemoveAll(path.Join(historyPath, item.Name())) // ignoring any error
			itemsPurged++
		} else {
			itemsLeft++
		}
	}
	return map[string]interface{}{
		"message":      "history deletion complete",
		"days":         days,
		"items_purged": itemsPurged,
		"items_left":   itemsLeft,
	}, nil
}

func quarantineDetail(driveRoot string) (interface{}, error) {
	type entry struct {
		NameOnDevice string
		NameInURL    string
	}
	// Map of type to device to entries; type is accounts, containers, objects,
	// objects-1, etc.
	typeToDeviceToEntries := map[string]map[string][]*entry{}
	deviceList, err := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, device := range deviceList {
		qTypeList, err := ioutil.ReadDir(filepath.Join(driveRoot, device.Name(), "quarantined"))
		if err != nil {
			if os.IsNotExist(err) {
				return typeToDeviceToEntries, nil
			}
			return nil, err
		}
		for _, qType := range qTypeList {
			key := qType.Name()
			if key == "accounts" || key == "containers" || strings.HasPrefix(key, "objects") {
				listing, err := ioutil.ReadDir(filepath.Join(driveRoot, device.Name(), "quarantined", key))
				if err != nil {
					continue
				}
				for count, listingItem := range listing {
					if count > 100 {
						// We only show detail for the first 100. As they get
						// repaired, the remaining items will be shown.
						break
					}
					ent := &entry{NameOnDevice: listingItem.Name()}
					if typeToDeviceToEntries[key] == nil {
						typeToDeviceToEntries[key] = map[string][]*entry{}
					}
					typeToDeviceToEntries[key][device.Name()] = append(typeToDeviceToEntries[key][device.Name()], ent)
					if key == "accounts" || key == "containers" {
						parts := strings.SplitN(listingItem.Name(), "-", 2)
						if len(parts) != 2 {
							continue
						}
						db, err := sql.Open("sqlite3", filepath.Join(driveRoot, device.Name(), "quarantined", key, listingItem.Name(), parts[0]+".db"))
						if err != nil {
							continue
						}
						var a, c string
						if key == "accounts" {
							if db.QueryRow("select account from account_stat").Scan(&a) == nil {
								ent.NameInURL = "/" + a
							}
						} else {
							if db.QueryRow("select account, container from container_info").Scan(&a, &c) == nil {
								ent.NameInURL = "/" + path.Join(a, c)
							}
						}
						db.Close()
					} else { // strings.HasPrefix(key, "objects")
						listing2, err := ioutil.ReadDir(filepath.Join(driveRoot, device.Name(), "quarantined", key, listingItem.Name()))
						if err != nil {
							// Assume regular file (EC quarantine)
							file, err := os.Open(filepath.Join(driveRoot, device.Name(), "quarantined", key, listingItem.Name()))
							if err != nil {
								continue
							}
							data := make([]byte, 4096)
							count, err := file.Read(data)
							file.Close()
							if err == nil && count < 4096 {
								ent.NameInURL = string(data[:count])
								break
							}
							continue
						}
						for _, listing2Item := range listing2 {
							metadata, err := common.SwiftObjectReadMetadata(filepath.Join(driveRoot, device.Name(), "quarantined", key, listingItem.Name(), listing2Item.Name()))
							if err != nil {
								continue
							}
							ent.NameInURL = metadata["name"]
							if ent.NameInURL != "" {
								break
							}
						}
					}
				}
			}
		}
	}
	return typeToDeviceToEntries, nil
}

func diskUsage(driveRoot string) ([]map[string]interface{}, error) {
	devices := make([]map[string]interface{}, 0)
	dirInfo, err := os.Stat(driveRoot)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range fileInfo {
		if info.Sys().(*syscall.Stat_t).Dev == dirInfo.Sys().(*syscall.Stat_t).Dev {
			devices = append(devices, map[string]interface{}{"device": info.Name(), "mounted": false,
				"size": "", "used": "", "avail": ""})
		} else {
			var fsinfo syscall.Statfs_t
			err := syscall.Statfs(filepath.Join(driveRoot, info.Name()), &fsinfo)
			if err == nil {
				capacity := int64(fsinfo.Bsize) * int64(fsinfo.Blocks)
				used := int64(fsinfo.Bsize) * (int64(fsinfo.Blocks) - int64(fsinfo.Bavail))
				available := int64(fsinfo.Bsize) * int64(fsinfo.Bavail)
				devices = append(devices, map[string]interface{}{"device": info.Name(), "mounted": true,
					"size": capacity, "used": used, "avail": available})
			}
		}
	}
	return devices, nil
}

func ReconHandler(driveRoot string, reconCachePath string, mountCheck bool, writer http.ResponseWriter, request *http.Request) {
	var content interface{} = nil

	vars := srv.GetVars(request)
	var err error

	switch vars["method"] {
	case "mem":
		content = getMem()
	case "load":
		content = getLoad()
	case "async":
		content, err = getTotalAsyncs(driveRoot, reconCachePath)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "replication":
		if vars["recon_type"] == "account" {
			content, err = fromReconCache(reconCachePath, "account", "replication_time", "replication_stats", "replication_last")
		} else if vars["recon_type"] == "container" {
			content, err = fromReconCache(reconCachePath, "container", "replication_time", "replication_stats", "replication_last")
		} else if vars["recon_type"] == "object" {
			content, err = fromReconCache(reconCachePath, "object", "object_replication_time", "object_replication_last")
		} else if vars["recon_type"] == "" {
			// handle old style object replication requests
			content, err = fromReconCache(reconCachePath, "object", "object_replication_time", "object_replication_last")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "devices":
		content, err = ListDevices(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "updater":
		if vars["recon_type"] == "container" {
			content, err = fromReconCache(reconCachePath, "container", "container_updater_sweep")
		} else if vars["recon_type"] == "object" {
			content, err = fromReconCache(reconCachePath, "object", "object_updater_sweep")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "auditor":
		if vars["recon_type"] == "account" {
			content, err = fromReconCache(reconCachePath, "account", "account_audits_passed", "account_auditor_pass_completed", "account_audits_since", "account_audits_failed")
		} else if vars["recon_type"] == "container" {
			content, err = fromReconCache(reconCachePath, "container", "container_audits_passed", "container_auditor_pass_completed", "container_audits_since", "container_audits_failed")
		} else if vars["recon_type"] == "object" {
			content, err = fromReconCache(reconCachePath, "object", "object_auditor_stats_ALL", "object_auditor_stats_ZBF")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "expirer":
		content, err = fromReconCache(reconCachePath, "object", "object_expiration_pass", "expired_last_pass")
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "mounted":
		content = getMounts()
	case "unmounted":
		if !mountCheck {
			content = make([]map[string]interface{}, 0)
		} else {
			content, err = getUnmounted(driveRoot)
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "ringmd5":
		if content, err = common.GetAllRingFileMd5s(); err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "swiftconfmd5":
		content, err = common.FileMD5("/etc/hummingbird/hummingbird.conf")
		if err != nil {
			content, err = common.FileMD5("/etc/swift/swift.conf")
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "hummingbirdconfmd5":
		content, err = common.FileMD5("/etc/hummingbird/hummingbird.conf")
		if err != nil {
			content, err = common.FileMD5("/etc/swift/swift.conf")
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "hummingbirdmd5":
		if exe, err := os.Executable(); err == nil {
			content, err = common.FileMD5(exe)
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		} else {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "quarantined":
		if request.Method == "DELETE" {
			content, err = quarantineDelete(driveRoot, vars["device"], vars["recon_type"], vars["item_path"])
		} else {
			content, err = quarantineCounts(driveRoot)
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "quarantineddetail":
		content, err = quarantineDetail(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "quarantinedhistory":
		if request.Method == "DELETE" {
			content, err = quarantineHistoryDelete(driveRoot, vars["device"], vars["recon_type"], vars["item_path"])
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "sockstat":
		content = getSockstats()
	case "version":
		content = map[string]string{"version": "idunno"}
	case "diskusage":
		content, err = diskUsage(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "time":
		//Similar to python time.time()
		content = float64(time.Now().UnixNano()) / float64(time.Second)
	case "hummingbirdtime":
		content = map[string]time.Time{"time": time.Now()}
	case "driveaudit":
		content, err = fromReconCache(reconCachePath, "drive", "drive_audit_errors")
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	if content == nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(200)
	serialized, _ := json.MarshalIndent(content, "", "  ")
	writer.Write(serialized)
}
