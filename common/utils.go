//  Copyright (c) 2015 Rackspace
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

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

const ONE_WEEK = 604800

type HttpRange struct {
	Start, End int64
}

var GMT *time.Location

var urlSafeMap = [256]bool{'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true,
	'G': true, 'H': true, 'I': true, 'J': true, 'K': true, 'L': true, 'M': true, 'N': true,
	'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true, 'U': true, 'V': true,
	'W': true, 'X': true, 'Y': true, 'Z': true, 'a': true, 'b': true, 'c': true, 'd': true,
	'e': true, 'f': true, 'g': true, 'h': true, 'i': true, 'j': true, 'k': true, 'l': true,
	'm': true, 'n': true, 'o': true, 'p': true, 'q': true, 'r': true, 's': true, 't': true,
	'u': true, 'v': true, 'w': true, 'x': true, 'y': true, 'z': true, '0': true, '1': true,
	'2': true, '3': true, '4': true, '5': true, '6': true, '7': true, '8': true, '9': true,
	'_': true, '.': true, '-': true, '/': true,
}

func Urlencode(str string) string {
	// output matches python's urllib.quote()

	finalSize := len(str)
	for i := 0; i < len(str); i++ {
		if !urlSafeMap[str[i]] {
			finalSize += 2
		}
	}
	if finalSize == len(str) {
		return str
	}
	buf := make([]byte, finalSize)
	j := 0
	for i := 0; i < len(str); i++ {
		if urlSafeMap[str[i]] {
			buf[j] = str[i]
			j++
		} else {
			buf[j] = '%'
			buf[j+1] = "0123456789ABCDEF"[str[i]>>4]
			buf[j+2] = "0123456789ABCDEF"[str[i]&15]
			j += 3
		}
	}
	return string(buf)
}

func ParseDate(date string) (time.Time, error) {
	if date == "" {
		return time.Now(), errors.New("invalid time")
	}
	if GMT == nil {
		var err error
		GMT, err = time.LoadLocation("GMT")
		if err != nil {
			return time.Now(), err
		}
	}
	if ius, err := time.ParseInLocation(time.RFC1123, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.RFC1123Z, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.ANSIC, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.RFC850, date, GMT); err == nil {
		return ius, nil
	}
	if strings.Contains(date, "_") {
		all_date_parts := strings.Split(date, "_")
		date = all_date_parts[0]
	}
	if timestamp, err := strconv.ParseFloat(date, 64); err == nil {
		nans := int64((timestamp - float64(int64(timestamp))) * 1.0e9)
		return time.Unix(int64(timestamp), nans).In(GMT), nil
	}
	return time.Now(), errors.New("invalid time")
}

func FormatTimestamp(timestamp string) (string, error) {
	parsed, err := ParseDate(timestamp)
	if err != nil {
		return "", err
	}
	return parsed.Format("2006-01-02T15:04:05.999999"), nil
}

func CanonicalTimestamp(t float64) string {
	ret := strconv.FormatFloat(t, 'f', 5, 64)
	for len(ret) < 16 {
		ret = "0" + ret
	}
	return ret
}

func LooksTrue(check string) bool {
	check = strings.TrimSpace(strings.ToLower(check))
	return check == "true" || check == "yes" || check == "1" || check == "on" || check == "t" || check == "y"
}

func UUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", rand.Int63n(0xffffffff), rand.Int63n(0xffff), rand.Int63n(0xffff), rand.Int63n(0xffff), rand.Int63n(0xffffffffffff))
}

func GetTimestamp() string {
	return CanonicalTimestamp(float64(time.Now().UnixNano()) / 1000000000.0)
}

func GetTransactionId() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func HeaderGetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

func ParseRange(rangeHeader string, fileSize int64) (reqRanges []HttpRange, err error) {
	rangeHeader = strings.Replace(strings.ToLower(rangeHeader), " ", "", -1)
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, nil
	}
	rangeHeader = rangeHeader[6:]
	rangeStrings := strings.Split(rangeHeader, ",")
	if len(rangeStrings) > 100 {
		return nil, errors.New("Too many ranges")
	}
	if len(rangeStrings) == 0 {
		return nil, nil
	}
	for _, rng := range rangeStrings {
		var start, end int64
		var err error
		startend := strings.Split(rng, "-")
		if len(startend) != 2 || (startend[0] == "" && startend[1] == "") {
			return nil, nil
		}
		if start, err = strconv.ParseInt(startend[0], 0, 64); err != nil && startend[0] != "" {
			return nil, nil
		}
		if end, err = strconv.ParseInt(startend[1], 0, 64); err != nil && startend[1] != "" {
			return nil, nil
		} else if startend[1] != "" && end < start {
			return nil, nil
		}

		if startend[0] == "" {
			if end == 0 {
				continue
			} else if end > fileSize {
				reqRanges = append(reqRanges, HttpRange{0, fileSize})
			} else {
				reqRanges = append(reqRanges, HttpRange{fileSize - end, fileSize})
			}
		} else if startend[1] == "" {
			if start < fileSize {
				reqRanges = append(reqRanges, HttpRange{start, fileSize})
			} else {
				continue
			}
		} else if start < fileSize {
			if end+1 < fileSize {
				reqRanges = append(reqRanges, HttpRange{start, end + 1})
			} else {
				reqRanges = append(reqRanges, HttpRange{start, fileSize})
			}
		}
	}
	if len(reqRanges) == 0 {
		return nil, errors.New("Unsatisfiable range")
	}
	return reqRanges, nil
}

func SetRlimits() {
	syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Max: 65536, Cur: 65536})
}

func GetEpochFromTimestamp(timestamp string) (string, error) {
	split_timestamp := strings.Split(timestamp, "_")
	floatTimestamp, err := strconv.ParseFloat(split_timestamp[0], 64)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", split_timestamp[0]))
	}
	return CanonicalTimestamp(floatTimestamp), nil
}

func StandardizeTimestamp(timestamp string) (string, error) {
	offset := strings.Contains(timestamp, "_")
	if offset {
		split_timestamp := strings.Split(timestamp, "_")
		floatTimestamp, err := strconv.ParseFloat(split_timestamp[0], 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", split_timestamp[0]))
		}
		intOffset, err := strconv.ParseInt(split_timestamp[1], 16, 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse int from '%s'.", split_timestamp[1]))
		}

		split_timestamp[0] = CanonicalTimestamp(floatTimestamp)
		split_timestamp[1] = fmt.Sprintf("%016x", intOffset)
		timestamp = strings.Join(split_timestamp, "_")
	} else {
		floatTimestamp, err := strconv.ParseFloat(timestamp, 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", timestamp))
		}
		timestamp = CanonicalTimestamp(floatTimestamp)
	}
	return timestamp, nil
}

// will split out url path the proxy would receive and return map
// with keys: "vrs", "account", "container", "object"
func ParseProxyPath(path string) (pathMap map[string]string, err error) {
	pathParts := []string{"", "vrs", "account", "container", "object"}
	pathSplit := strings.SplitN(path, "/", 5)
	if pathSplit[0] != "" {
		return nil, errors.New(fmt.Sprintf("Invalid path: %s", path))
	}
	pathMap = map[string]string{}
	for i := 1; i < len(pathParts); i++ {
		if len(pathSplit) <= i {
			pathMap[pathParts[i]] = ""
		} else {
			if pathSplit[i] == "" && len(pathSplit)-1 != i {
				return nil, errors.New(fmt.Sprintf("Invalid path: %s", path))
			}
			pathMap[pathParts[i]] = pathSplit[i]
		}
	}
	return pathMap, err
}

var buf64kpool = NewFreePool(128)

func Copy(src io.Reader, dsts ...io.Writer) (written int64, err error) {
	var buf []byte
	var ok bool
	if buf, ok = buf64kpool.Get().([]byte); !ok {
		buf = make([]byte, 64*1024)
	}
	written, err = io.CopyBuffer(io.MultiWriter(dsts...), src, buf)
	buf64kpool.Put(buf)
	return
}

func CopyN(src io.Reader, n int64, dsts ...io.Writer) (written int64, err error) {
	written, err = Copy(io.LimitReader(src, n), dsts...)
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		err = io.EOF
	}
	return
}

func GetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

// More like a map of semaphores.  I don't know what to call it.
type KeyedLimit struct {
	limitPerKey int64
	totalLimit  int64
	lock        sync.Mutex
	locked      map[string]bool
	inUse       map[string]int64
	totalUse    int64
}

func (k *KeyedLimit) Acquire(key string, force bool) int64 {
	// returns 0 if Acquire is successful, otherwise the number of requests inUse by disk or -1 if disk is locked
	k.lock.Lock()
	if k.locked[key] {
		k.lock.Unlock()
		return -1
	} else if v := k.inUse[key]; !force && ((k.limitPerKey > 0 && v >= k.limitPerKey) || (k.totalLimit > 0 && k.totalUse > k.totalLimit)) {
		k.lock.Unlock()
		return v
	} else {
		k.inUse[key] += 1
		k.totalUse += 1
		k.lock.Unlock()
		return 0
	}
}

func (k *KeyedLimit) Release(key string) {
	k.lock.Lock()
	k.inUse[key] -= 1
	k.totalUse -= 1
	k.lock.Unlock()
}

func (k *KeyedLimit) Lock(key string) {
	k.lock.Lock()
	k.locked[key] = true
	k.lock.Unlock()
}

func (k *KeyedLimit) Unlock(key string) {
	k.lock.Lock()
	k.locked[key] = false
	k.lock.Unlock()
}

func (k *KeyedLimit) Keys() []string {
	k.lock.Lock()
	keys := make([]string, len(k.inUse))
	i := 0
	for key := range k.inUse {
		keys[i] = key
		i += 1
	}
	k.lock.Unlock()
	return keys
}

func (k *KeyedLimit) MarshalJSON() ([]byte, error) {
	k.lock.Lock()
	data, err := json.Marshal(k.inUse)
	k.lock.Unlock()
	return data, err
}

func NewKeyedLimit(limitPerKey int64, totalLimit int64) *KeyedLimit {
	return &KeyedLimit{limitPerKey: limitPerKey, totalLimit: totalLimit, locked: make(map[string]bool), inUse: make(map[string]int64)}
}

func CollectRuntimeMetrics(statsdHost string, statsdPort, statsdPause int64, prefix string) {
	address := fmt.Sprintf("%s:%d", statsdHost, statsdPort)
	client, err := statsd.NewClient(address, prefix)
	if err != nil {
		panic(fmt.Sprintf("Unable to connect to Statsd"))
	}

	defer client.Close()

	for {

		err = client.Gauge("cpu.goroutines", int64(runtime.NumGoroutine()), 1.0)
		if err != nil {
			panic(fmt.Sprintf("unable to send data"))
		}
		// CGo calls
		client.Gauge("cpu.cgo_calls", int64(runtime.NumCgoCall()), 1.0)

		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)

		client.Gauge("mem.alloc", int64(m.Alloc), 1.0)
		client.Gauge("mem.total", int64(m.TotalAlloc), 1.0)
		client.Gauge("mem.sys", int64(m.Sys), 1.0)
		client.Gauge("mem.lookups", int64(m.Lookups), 1.0)
		client.Gauge("mem.malloc", int64(m.Mallocs), 1.0)
		client.Gauge("mem.frees", int64(m.Frees), 1.0)
		client.Gauge("mem.stack.inuse", int64(m.StackInuse), 1.0)
		client.Gauge("mem.stack.sys", int64(m.StackSys), 1.0)
		client.Gauge("mem.stack.mspan_inuse", int64(m.MSpanInuse), 1.0)
		client.Gauge("mem.stack.mspan_sys", int64(m.MSpanSys), 1.0)
		client.Gauge("mem.stack.mcache_inuse", int64(m.MCacheInuse), 1.0)
		client.Gauge("mem.stack.mcache_sys", int64(m.MCacheSys), 1.0)
		client.Gauge("mem.heap.alloc", int64(m.HeapAlloc), 1.0)
		client.Gauge("mem.heap.sys", int64(m.HeapSys), 1.0)
		client.Gauge("mem.heap.idle", int64(m.HeapIdle), 1.0)
		client.Gauge("mem.heap.inuse", int64(m.HeapInuse), 1.0)
		client.Gauge("mem.heap.released", int64(m.HeapReleased), 1.0)
		client.Gauge("mem.heap.objects", int64(m.HeapObjects), 1.0)
		client.Gauge("mem.othersys", int64(m.OtherSys), 1.0)
		client.Gauge("mem.gc.sys", int64(m.GCSys), 1.0)
		client.Gauge("mem.gc.next", int64(m.NextGC), 1.0)
		client.Gauge("mem.gc.last", int64(m.LastGC), 1.0)
		client.Gauge("mem.gc.pause_total", int64(m.PauseTotalNs), 1.0)
		client.Gauge("mem.gc.pause", int64(m.PauseNs[(m.NumGC+255)%256]), 1.0)
		client.Gauge("mem.gc.count", int64(m.NumGC), 1.0)

		time.Sleep(time.Duration(statsdPause) * time.Second)
	}

}

func Map2Headers(m map[string]string) http.Header {
	if m == nil {
		return nil
	}
	headers := make(http.Header, len(m))
	for k, v := range m {
		headers.Set(k, v)
	}
	return headers
}

func Headers2Map(headers http.Header) map[string]string {
	if headers == nil {
		return nil
	}
	m := make(map[string]string, len(headers))
	for k := range headers {
		m[k] = headers.Get(k)
	}
	return m
}

func CheckNameFormat(req *http.Request, name string, target string) (string, error) {
	if name == "" {
		return "", errors.New(fmt.Sprintf("%s name cannot be empty", target))
	}
	if strings.Contains(name, "/") {
		return "", errors.New(fmt.Sprintf("%s name cannot contain slashes", target))
	}
	return name, nil
}
