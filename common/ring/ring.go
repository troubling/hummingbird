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

package ring

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const reloadTime = 15 * time.Second

type Ring interface {
	GetNodes(partition uint64) (response []*Device)
	GetJobNodes(partition uint64, localDevice int) (response []*Device, handoff bool)
	GetPartition(account string, container string, object string) uint64
	LocalDevices(localPort int) (devs []*Device, err error)
	AllDevices() (devs []*Device)
	GetMoreNodes(partition uint64) MoreNodes
	ReplicaCount() (cnt uint64)
	PartitionCount() (cnt uint64)
	PartitionForHash(string) (uint64, error)
}

type MoreNodes interface {
	Next() *Device
}

type Device struct {
	Id              int     `json:"id"`
	Device          string  `json:"device"`
	Scheme          string  `json:"scheme"`
	Ip              string  `json:"ip"`
	Meta            string  `json:"meta"`
	Port            int     `json:"port"`
	Region          int     `json:"region"`
	ReplicationIp   string  `json:"replication_ip"`
	ReplicationPort int     `json:"replication_port"`
	Weight          float64 `json:"weight"`
	Zone            int     `json:"zone"`
}

type RingMD5 interface {
	Ring
	MD5() string
	DiskPath() string
	RingMatching(md5 string) RingMD5
	Reload() error
	AssignmentCount(devId int) int
}

type ringData struct {
	Devs                                []*Device `json:"devs"`
	ReplicaCount                        int       `json:"replica_count"`
	PartShift                           uint64    `json:"part_shift"`
	replica2part2devId                  [][]uint16
	regionCount, zoneCount, ipPortCount int
	md5                                 string
}

type hashRing struct {
	data    atomic.Value
	path    string
	prefix  string
	suffix  string
	mtime   time.Time
	calcMD5 bool
}

type regionZone struct {
	region, zone int
}

type ipPort struct {
	region, zone, port int
	ip                 string
}

type hashMoreNodes struct {
	r                 *hashRing
	used, sameRegions map[int]bool
	sameZones         map[regionZone]bool
	sameIpPorts       map[ipPort]bool
	parts, start, inc int
	partition         uint64
	m                 sync.Mutex
}

func (d *Device) String() string {
	return fmt.Sprintf("Device{Id: %d, Device: %s, Ip: %s, Port: %d}", d.Id, d.Device, d.Ip, d.Port)
}

func (d *Device) Active() bool {
	return d != nil && d.Weight >= 0
}

func (r *hashRing) getData() *ringData {
	return r.data.Load().(*ringData)
}

func (r *hashRing) GetNodes(partition uint64) (response []*Device) {
	d := r.getData()
	if partition >= uint64(len(d.replica2part2devId[0])) {
		return nil
	}
	for i := 0; i < d.ReplicaCount; i++ {
		response = append(response, d.Devs[d.replica2part2devId[i][partition]])
	}
	return response
}

func (r *hashRing) GetJobNodes(partition uint64, localDevice int) (response []*Device, handoff bool) {
	d := r.getData()
	handoff = true
	if partition >= uint64(len(d.replica2part2devId[0])) {
		return nil, false
	}
	for i := 0; i < d.ReplicaCount; i++ {
		dev := d.Devs[d.replica2part2devId[i][partition]]
		if dev.Id == localDevice {
			handoff = false
		} else {
			response = append(response, dev)
		}
	}
	return response, handoff
}

func (r *hashRing) GetPartition(account string, container string, object string) uint64 {
	d := r.getData()
	hash := md5.New()
	hash.Write([]byte(r.prefix + "/" + account))
	if container != "" {
		hash.Write([]byte("/" + container))
		if object != "" {
			hash.Write([]byte("/" + object))
		}
	}
	hash.Write([]byte(r.suffix))
	digest := hash.Sum(nil)
	// treat as big endian unsigned int
	val := uint64(digest[0])<<24 | uint64(digest[1])<<16 | uint64(digest[2])<<8 | uint64(digest[3])
	return val >> d.PartShift
}

// uses first 8 bytes of hsh to figure out partition
func (r *hashRing) PartitionForHash(hsh string) (uint64, error) {
	hshi, err := strconv.ParseUint(hsh[:8], 16, 64)
	if err != nil {
		return 0, err
	}
	return hshi >> r.getData().PartShift, nil
}

func (r *hashRing) LocalDevices(localPort int) (devs []*Device, err error) {
	d := r.getData()
	var localIPs = make(map[string]bool)

	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range localAddrs {
		localIPs[strings.Split(addr.String(), "/")[0]] = true
	}

	for i, dev := range d.Devs {
		if !dev.Active() {
			continue
		}
		if localIPs[dev.ReplicationIp] && dev.ReplicationPort == localPort {
			devs = append(devs, d.Devs[i])
		}
	}
	return devs, nil
}

func (r *hashRing) AllDevices() (devs []*Device) {
	d := r.getData()
	return d.Devs
}

func (r *hashRing) GetMoreNodes(partition uint64) MoreNodes {
	return &hashMoreNodes{r: r, partition: partition, used: nil}
}

func (r *hashRing) ReplicaCount() (cnt uint64) {
	d := r.getData()
	return uint64(len(d.replica2part2devId))
}

func (r *hashRing) PartitionCount() (cnt uint64) {
	d := r.getData()
	return uint64(len(d.replica2part2devId[0]))
}

func (r *hashRing) MD5() string {
	d := r.getData()
	return d.md5
}

func (r *hashRing) DiskPath() string {
	return r.path
}

func (r *hashRing) RingMatching(md5 string) RingMD5 {
	d := r.getData()
	if d.md5 == md5 {
		if r2, err := LoadRingMD5(r.path, r.prefix, r.suffix); err == nil {
			return r2
		}
	}
	backupsDir := filepath.Join(filepath.Dir(r.path), "backups")
	f, err := os.Open(backupsDir)
	if err != nil {
		return nil
	}
	fis, err := f.Readdir(-1)
	if err != nil {
		return nil
	}
	// object.ring.gz => ["object"]
	// object-1.ring.gz => ["object", "1"]
	parts := strings.SplitN(strings.SplitN(filepath.Base(r.path), ".", 2)[0], "-", 2)
	typ := parts[0]
	policyString := ""
	if len(parts) > 1 {
		policyString = parts[1]
	}
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		if !strings.HasSuffix(fi.Name(), ".ring.gz") {
			continue
		}
		// 1520549739234856985.object.ring.gz => ["1520549739234856985", "object.ring.gz"]
		// 1520549739289612841.object-1.ring.gz => ["1520549739289612841", "object-1.ring.gz"]
		parts = strings.SplitN(fi.Name(), ".", 2)
		if len(parts) < 2 {
			continue
		}
		// object.ring.gz => ["object"]
		// object-1.ring.gz => ["object", "1"]
		parts = strings.SplitN(strings.SplitN(parts[1], ".", 2)[0], "-", 2)
		if parts[0] != typ {
			continue
		}
		if len(parts) < 2 && policyString != "" {
			continue
		}
		if len(parts) > 1 && parts[1] != policyString {
			continue
		}
		r2, err := LoadRingMD5(filepath.Join(backupsDir, fi.Name()), r.prefix, r.suffix)
		if err != nil {
			continue
		}
		if r2.MD5() == md5 {
			return r2
		}
	}
	return nil
}

func (r *hashRing) Reload() error {
	fi, err := os.Stat(r.path)
	if err != nil {
		return err
	}
	if fi.ModTime() == r.mtime {
		return nil
	}
	data := &ringData{}
	fp, err := os.Open(r.path)
	if err != nil {
		return err
	}
	defer fp.Close()
	if r.calcMD5 {
		h := md5.New()
		if _, err := io.Copy(h, fp); err != nil {
			return err
		}
		data.md5 = fmt.Sprintf("%x", h.Sum(nil))
		fp.Seek(0, 0)
	}
	gz, err := gzip.NewReader(fp)
	if err != nil {
		return err
	}
	magicBuf := make([]byte, 4)
	io.ReadFull(gz, magicBuf)
	if string(magicBuf) != "R1NG" {
		return errors.New("Bad magic string")
	}
	var ringVersion uint16
	binary.Read(gz, binary.BigEndian, &ringVersion)
	if ringVersion != 1 {
		return fmt.Errorf("Unknown ring version %d", ringVersion)
	}
	var json_len uint32
	binary.Read(gz, binary.BigEndian, &json_len)
	jsonBuf := make([]byte, json_len)
	io.ReadFull(gz, jsonBuf)
	if err := json.Unmarshal(jsonBuf, data); err != nil {
		return err
	}
	partitionCount := 1 << (32 - data.PartShift)
	for i := 0; i < data.ReplicaCount; i++ {
		part2dev := make([]uint16, partitionCount)
		binary.Read(gz, binary.LittleEndian, &part2dev)
		data.replica2part2devId = append(data.replica2part2devId, part2dev)
	}
	regionCount := make(map[int]bool)
	zoneCount := make(map[regionZone]bool)
	ipPortCount := make(map[ipPort]bool)
	for _, d := range data.Devs {
		if !d.Active() {
			continue
		}
		if d.ReplicationIp == "" {
			d.ReplicationIp = d.Ip
		}
		if d.ReplicationPort == 0 {
			d.ReplicationPort = d.Port + 500
		}
		if d.Scheme == "" {
			d.Scheme = "http"
		}
		regionCount[d.Region] = true
		zoneCount[regionZone{d.Region, d.Zone}] = true
		ipPortCount[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
	}
	data.regionCount = len(regionCount)
	data.zoneCount = len(zoneCount)
	data.ipPortCount = len(ipPortCount)
	r.mtime = fi.ModTime()
	r.data.Store(data)
	return nil
}

func (r *hashRing) reloader() error {
	for {
		time.Sleep(reloadTime)
		r.Reload()
	}
}

func (r *hashRing) AssignmentCount(devId int) int {
	count := 0
	for _, part2devId := range r.getData().replica2part2devId {
		for _, rDevId := range part2devId {
			if int(rDevId) == devId {
				count++
			}
		}
	}
	return count
}

func (m *hashMoreNodes) addDevice(d *Device) {
	m.used[d.Id] = true
	m.sameRegions[d.Region] = true
	m.sameZones[regionZone{d.Region, d.Zone}] = true
	m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
}

func (m *hashMoreNodes) initialize() {
	d := m.r.getData()
	m.parts = len(d.replica2part2devId[0])
	m.used = make(map[int]bool)
	m.sameRegions = make(map[int]bool)
	m.sameZones = make(map[regionZone]bool)
	m.sameIpPorts = make(map[ipPort]bool)
	for _, mp := range d.replica2part2devId {
		m.addDevice(d.Devs[mp[m.partition]])
	}
	hash := md5.New()
	hash.Write([]byte(strconv.FormatUint(m.partition, 10)))
	digest := hash.Sum(nil)
	m.start = int((uint64(digest[0])<<24 | uint64(digest[1])<<16 | uint64(digest[2])<<8 | uint64(digest[3])) >> d.PartShift)
	m.inc = m.parts / 65536
	if m.inc == 0 {
		m.inc = 1
	}
}

func (m *hashMoreNodes) Next() *Device {
	m.m.Lock()
	defer m.m.Unlock()
	d := m.r.getData()
	if m.used == nil {
		m.initialize()
	}
	var check func(d *Device) bool
	if len(m.sameRegions) < d.regionCount {
		check = func(d *Device) bool { return !m.sameRegions[d.Region] }
	} else if len(m.sameZones) < d.zoneCount {
		check = func(d *Device) bool { return !m.sameZones[regionZone{d.Region, d.Zone}] }
	} else if len(m.sameIpPorts) < d.ipPortCount {
		check = func(d *Device) bool { return !m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] }
	} else {
		check = func(d *Device) bool { return !m.used[d.Id] }
	}
	for i := 0; i < m.parts; i += m.inc {
		handoffPart := (i + m.start) % m.parts
		for _, part2devId := range d.replica2part2devId {
			if handoffPart < len(part2devId) {
				if check(d.Devs[part2devId[handoffPart]]) {
					m.addDevice(d.Devs[part2devId[handoffPart]])
					return d.Devs[part2devId[handoffPart]]
				}
			}
		}
	}
	return nil
}

var loadedRingsLock sync.Mutex
var loadedRings map[string]*hashRing = make(map[string]*hashRing)

func LoadRing(path string, prefix string, suffix string) (Ring, error) {
	loadedRingsLock.Lock()
	defer loadedRingsLock.Unlock()
	ring := loadedRings[path]
	if ring == nil {
		ring = &hashRing{prefix: prefix, suffix: suffix, path: path, mtime: time.Unix(0, 0)}
		if err := ring.Reload(); err != nil {
			return nil, err
		}
		go ring.reloader()
		loadedRings[path] = ring
	}
	return ring, nil
}

func LoadRingMD5(path string, prefix string, suffix string) (RingMD5, error) {
	ring := &hashRing{prefix: prefix, suffix: suffix, path: path, mtime: time.Unix(0, 0), calcMD5: true}
	if err := ring.Reload(); err != nil {
		return nil, err
	}
	return ring, nil
}

// save serializes the hashRing to disk
func (r *hashRing) Save(filename string) error {
	// NOTE: the swift ringbuilder puts a time in, do we really need this?
	fp, err := os.Create(filename)
	defer fp.Close()
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(fp)
	defer gz.Close()
	// Write out the magic string
	_, err = gz.Write([]byte("R1NG"))
	// Write out the version (1)
	ringVersion := uint16(1)
	binary.Write(gz, binary.BigEndian, &ringVersion)
	// Generate the json data
	data := r.getData()
	dataBuf, err := json.Marshal(data)
	if err != nil {
		return err
	}
	json_len := uint32(len(dataBuf))
	// Write out the json length
	binary.Write(gz, binary.BigEndian, &json_len)
	// Write the json
	gz.Write(dataBuf)
	// Write replica2part2devId
	d := r.getData()
	for i := range d.replica2part2devId {
		if err := binary.Write(gz, binary.LittleEndian, d.replica2part2devId[i]); err != nil {
			return err
		}
	}
	return nil
}

// GetRing returns the current ring given the ring_type ("account", "container", "object"),
// hash path prefix, and hash path suffix. An error is returned if the requested ring does
// not exist.
func GetRing(ringType, prefix, suffix string, policy int) (Ring, error) {
	ring, err := getRingLogic(ringType, prefix, suffix, policy, LoadRing)
	if err != nil {
		return nil, err
	}
	return ring, nil
}

// GetRingMD5 returns the current ring given the ring_type ("account",
// "container", "object"), hash path prefix, and hash path suffix. An error is
// returned if the requested ring does not exist. This differs from GetRing in
// that it returns a ring satisfying the RingMD5 interface and that it will
// compute the MD5 hash of the ring's persisted contents. Also, it will not
// automatically reload itself -- an explicit Reload is required.
func GetRingMD5(ringType, prefix, suffix string, policy int) (RingMD5, error) {
	ring, err := getRingLogic(
		ringType, prefix, suffix, policy,
		func(path string, prefix string, suffix string) (Ring, error) {
			ring, err := LoadRingMD5(path, prefix, suffix)
			return ring, err
		},
	)
	if err != nil {
		return nil, err
	}
	return ring.(RingMD5), nil
}

func getRingLogic(ringType, prefix, suffix string, policy int, loadRing func(path string, prefix string, suffix string) (Ring, error)) (Ring, error) {
	var ring Ring
	var err error
	var err2 error
	ringFile := fmt.Sprintf("%s.ring.gz", ringType)
	if policy != 0 {
		ringFile = fmt.Sprintf("%s-%d.ring.gz", ringType, policy)
	}
	if ring, err = loadRing(fmt.Sprintf("/etc/hummingbird/%s", ringFile), prefix, suffix); err != nil {
		if ring, err2 = loadRing(fmt.Sprintf("/etc/swift/%s", ringFile), prefix, suffix); err2 != nil {
			return nil, fmt.Errorf("Error loading %s:%d ring: %s: %s", ringType, policy, err, err2)
		}
	}
	return ring, nil
}
