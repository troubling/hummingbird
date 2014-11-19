package hummingbird

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func getMem() interface{} {
	results := make(map[string]string)
	fp, _ := os.Open("/proc/meminfo")
	defer fp.Close()
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		vals := strings.Split(scanner.Text(), ":")
		results[strings.TrimSpace(vals[0])] = strings.TrimSpace(vals[1])
	}
	return results
}

func getSockstats() interface{} {
	results := make(map[string]int64)

	fp, _ := os.Open("/proc/net/sockstat")
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

	fp, _ = os.Open("/proc/net/sockstat6")
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
	fp, _ := os.Open("/proc/loadavg")
	defer fp.Close()
	data, _ := ioutil.ReadAll(fp)
	parts := strings.Split(strings.TrimSpace(string(data)), " ")
	results["1m"], _ = strconv.ParseFloat(parts[0], 64)
	results["5m"], _ = strconv.ParseFloat(parts[1], 64)
	results["15m"], _ = strconv.ParseFloat(parts[2], 64)
	results["tasks"] = parts[3]
	results["processes"], _ = strconv.ParseInt(parts[4], 10, 64)
	return results
}

func getMounts() interface{} {
	results := make([]map[string]string, 0)
	fp, _ := os.Open("/proc/mounts")
	defer fp.Close()
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		vals := strings.Split(scanner.Text(), " ")
		results = append(results, map[string]string{vals[0]: vals[1]})
	}
	return results
}

func fromReconCache(source string, keys ...string) (interface{}, error) {
	filedata, err := ioutil.ReadFile(fmt.Sprintf("/var/cache/swift/%s.recon", source))
	if err != nil {
		return nil, err
	}
	var data interface{}
	json.Unmarshal(filedata, &data)
	results := make(map[string]interface{})
	switch data := data.(type) {
	case map[string]interface{}:
		for _, key := range keys {
			results[key] = data[key]
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unexpected data type %T in recon file.", data))
	}
	return results, nil
}

func getUnmounted(driveRoot string) (interface{}, error) {
	unmounted := make([]map[string]bool, 0)
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
			unmounted = append(unmounted, map[string]bool{info.Name(): false})
		}
	}
	return unmounted, nil
}

func fileMD5(files ...string) (map[string]string, error) {
	response := make(map[string]string)
	for _, file := range files {
		fp, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer fp.Close()
		hash := md5.New()
		io.Copy(hash, fp)
		response[file] = fmt.Sprintf("%x", hash.Sum(nil))
	}
	return response, nil
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
	qcounts := map[string]interface{}{"objects": 0, "containers": 0, "accounts": 0}
	deviceList, err := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range deviceList {
		for key, _ := range qcounts {
			stat, err := os.Stat(filepath.Join(driveRoot, info.Name(), "quarantined", key))
			if err == nil {
				qcounts[key] = stat.Sys().(*syscall.Stat_t).Nlink
			}
		}
	}
	return qcounts, nil
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

func ReconHandler(driveRoot string, writer http.ResponseWriter, request *http.Request) {
	var content interface{} = nil

	parts := strings.Split(request.URL.Path[1:], "/")

	switch parts[1] {
	case "mem":
		content = getMem()
	case "load":
		content = getLoad()
	case "async":
		var err error
		content, err = fromReconCache("object", "async_pending")
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "replication":
		var err error
		if parts[2] == "account" {
			content, err = fromReconCache("account", "replication_time", "replication_stats", "replication_last")
		} else if parts[2] == "container" {
			content, err = fromReconCache("container", "replication_time", "replication_stats", "replication_last")
		} else if parts[2] == "object" {
			content, err = fromReconCache("object", "replication_time", "replication_last")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "devices":
		var err error
		content, err = ListDevices(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "updater":
		var err error
		if parts[2] == "container" {
			content, err = fromReconCache("container", "container_updater_sweep")
		} else if parts[2] == "object" {
			content, err = fromReconCache("object", "object_updater_sweep")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "auditor":
		var err error
		if parts[2] == "account" {
			content, err = fromReconCache("account", "account_audits_passed", "account_auditor_pass_completed", "account_audits_since", "account_audits_failed")
		} else if parts[2] == "container" {
			content, err = fromReconCache("container", "container_audits_passed", "container_auditor_pass_completed", "container_audits_since", "container_audits_failed")
		} else if parts[2] == "object" {
			content, err = fromReconCache("object", "object_auditor_stats_ALL", "object_auditor_stats_ZBF")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "expirer":
		var err error
		content, err = fromReconCache("object", "object_expiration_pass", "expired_last_pass")
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "mounted":
		content = getMounts()
	case "unmounted":
		var err error
		content, err = getUnmounted(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "ringmd5":
		var err error
		content, err = fileMD5("/etc/hummingbird/object.ring.gz", "/etc/hummingbird/container.ring.gz", "/etc/hummingbird/account.ring.gz")
		if err != nil {
			content, err = fileMD5("/etc/swift/object.ring.gz", "/etc/swift/container.ring.gz", "/etc/swift/account.ring.gz")
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "swiftconfmd5":
		var err error
		content, err = fileMD5("/etc/hummingbird/hummingbird.conf")
		if err != nil {
			content, err = fileMD5("/etc/swift/swift.conf")
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "quarantined":
		var err error
		content, err = quarantineCounts(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "sockstat":
		content = getSockstats()
	case "version":
		content = map[string]string{"version": "idunno"}
	case "diskusage":
		var err error
		content, err = diskUsage(driveRoot)
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
