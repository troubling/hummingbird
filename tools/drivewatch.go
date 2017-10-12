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
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var DB_NAME = "andrewd.db"

type ipPort struct {
	ip   string
	port int
}

type reconData struct {
	Device  string
	Mounted bool
}
type ringData struct {
	r           ring.Ring
	p           *conf.Policy
	builderPath string
}

type ReportDevice struct {
	Ip         string
	Port       int
	Device     string
	Weight     float64
	Mounted    bool
	Reachable  bool
	LastUpdate time.Time
}

type ReportData struct {
	TotalDevices      int
	TotalWeight       float64
	LastSuccessfulRun time.Time
	UnmountedDevices  []ReportDevice
	LastRingZeroes    []ReportDevice
}

type driveWatch struct {
	policyToRing map[int]ringData
	logger       srv.LowLevelLogger
	db           *sql.DB
	dbl          sync.Mutex

	maxBadDevAge    time.Duration
	ringUpdateFreq  time.Duration
	runFreq         time.Duration
	maxWeightChange float64
	doNotRebalance  bool
	sqlDir          string
}

func (dw *driveWatch) deviceId(ip string, port int, device string) string {
	return fmt.Sprintf("%s:%d/%s", ip, port, device)
}

func (dw *driveWatch) serverId(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func (dw *driveWatch) getDbAndLock() (*sql.DB, error) {
	dw.dbl.Lock()
	if dw.db != nil {
		if err := dw.db.Ping(); err == nil {
			return dw.db, nil
		}
		dw.db.Close()
	}
	db, err := sql.Open("sqlite3", filepath.Join(dw.sqlDir, DB_NAME))
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	sqlCreate := `
		CREATE TABLE IF NOT EXISTS device (
			id INTEGER PRIMARY KEY,
			policy INTEGER NOT NULL,
			ip VARCHAR(40) NOT NULL,
			port INTEGER NOT NULL,
			device VARCHAR(40) NOT NULL,
			in_ring INTEGER NOT NULL,
			weight FLOAT NOT NULL,
			mounted INTEGER NOT NULL,
			reachable INTEGER NOT NULL,
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS device_log (
			deviceId INTEGER,
			mounted INTEGER,
			reachable INTEGER,
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			notes VARCHAR(255),
			FOREIGN KEY (deviceId) REFERENCES device(id)
		); 

		CREATE TABLE IF NOT EXISTS ring_action (
			id INTEGER PRIMARY KEY,
			policy INTEGER NOT NULL,
			ip VARCHAR(40),
			port INTEGER,
			device VARCHAR(40),
			action VARCHAR(20) NOT NULL,
			create_date TIMESTAMP NOT NULL
		); 

		CREATE TABLE IF NOT EXISTS run_log (
			id INTEGER PRIMARY KEY,
			policy INTEGER NOT NULL,
			success INTEGER, notes TEXT,
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		); 

		CREATE TRIGGER IF NOT EXISTS device_last_update
			AFTER UPDATE ON device FOR EACH ROW
			BEGIN
				UPDATE device SET last_update = CURRENT_TIMESTAMP
				WHERE id = OLD.id AND (mounted != OLD.mounted OR reachable != OLD.reachable);
			END;

		CREATE TRIGGER IF NOT EXISTS device_logger
			AFTER UPDATE ON device FOR EACH ROW
			BEGIN
				INSERT INTO device_log (deviceId, mounted, reachable)
				VALUES (OLD.id, OLD.mounted, OLD.reachable);
			END;
		`
	_, err = tx.Exec(sqlCreate)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	dw.db = db
	return dw.db, nil
}

func (dw *driveWatch) gatherReconData(servers []ipPort) (unmountedDevices map[string]ring.Device, downServers map[string]ipPort) {
	downServers = make(map[string]ipPort)
	unmountedDevices = make(map[string]ring.Device)
	client := http.Client{Timeout: 10 * time.Second}

	for _, s := range servers {
		serverUrl := fmt.Sprintf("http://%s:%d/recon/unmounted", s.ip, s.port)
		req, err := http.NewRequest("GET", serverUrl, nil)
		if err != nil {
			dw.logger.Error("Could not create recon request", zap.String("url", serverUrl), zap.Error(err))
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			dw.logger.Error("Could not do recon request", zap.String("url", serverUrl), zap.Error(err))
			downServers[dw.serverId(s.ip, s.port)] = ipPort{ip: s.ip, port: s.port}
			continue
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			dw.logger.Error("Could not read recon resp", zap.String("url", serverUrl), zap.Error(err))
			continue
		}
		var serverReconData []*reconData
		if err := json.Unmarshal(data, &serverReconData); err == nil {
			for _, rd := range serverReconData {
				if !rd.Mounted {
					unmountedDevices[dw.deviceId(s.ip,
						s.port, rd.Device)] = ring.Device{
						Device: rd.Device, Ip: s.ip, Port: s.port}
				}
			}
		} else {
			dw.logger.Error("Could not parse recon resp", zap.String("url", serverUrl), zap.Error(err))
		}
	}
	return unmountedDevices, downServers
}

func (dw *driveWatch) getRingData(rd ringData) (map[string]*ring.Device, []ipPort) {
	oring := rd.r
	allRingDevices := make(map[string]*ring.Device)
	var allWeightedServers []ipPort
	weightedServers := make(map[string]bool)
	for _, dev := range oring.AllDevices() {
		if dev == nil {
			continue
		}
		allRingDevices[dw.deviceId(dev.Ip, dev.Port, dev.Device)] = dev

		if dev.Weight > 0 {
			if _, ok := weightedServers[dw.serverId(dev.Ip, dev.Port)]; !ok {
				allWeightedServers =
					append(allWeightedServers, ipPort{ip: dev.Ip, port: dev.Port})
				weightedServers[dw.serverId(dev.Ip, dev.Port)] = true
			}
		}
	}
	return allRingDevices, allWeightedServers
}

func (dw *driveWatch) needRingUpdate(rd ringData) bool {
	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		dw.logger.Error("getDbAndLock for needRingUpdate", zap.Error(err))
		return false
	}
	rows, err := db.Query("SELECT create_date FROM ring_action WHERE policy = ? ORDER BY create_date DESC LIMIT 1", rd.p.Index)
	defer rows.Close()
	if err != nil {
		dw.logger.Error("SELECT for needRingUpdate", zap.Error(err))
		return false
	}
	if rows.Next() {
		var createDate time.Time
		if err := rows.Scan(&createDate); err == nil {
			return time.Since(createDate) > dw.ringUpdateFreq
		}
	} else {
		return true
	}
	return false
}

func (dw *driveWatch) updateDb(rd ringData) error {
	policy := rd.p.Index
	allRingDevices, allWeightedServers := dw.getRingData(rd)
	unmountedDevices, downServers := dw.gatherReconData(allWeightedServers)

	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(
		"SELECT id, ip, port, device, weight, mounted FROM device WHERE policy = ?", policy)
	if err != nil {
		return err
	}
	defer rows.Close()
	var qryErrors []error
	changesMade := 0
	for rows.Next() {
		var ip, device string
		var dID, port int
		var mounted bool
		var weight float64

		if err := rows.Scan(&dID, &ip, &port, &device, &weight, &mounted); err != nil {
			qryErrors = append(qryErrors, err)
		} else {

			dKey := dw.deviceId(ip, port, device)
			rDev, inRing := allRingDevices[dKey]
			_, inUnmounted := unmountedDevices[dKey]
			_, notReachable := downServers[dw.serverId(ip, port)]
			if !inRing {
				changesMade++
				if _, err = tx.Exec("UPDATE device SET in_ring=0 WHERE id = ?", dID); err != nil {
					return err
				}
			} else {
				if !notReachable {
					if rDev.Weight != weight || mounted == inUnmounted {
						changesMade++
						if _, err = tx.Exec("UPDATE device SET weight=?, mounted=? "+
							"WHERE id=?", rDev.Weight, !inUnmounted, dID); err != nil {
							return err
						}
					}
				}
			}
			delete(allRingDevices, dKey)
		}
	}
	for _, ipp := range downServers {
		changesMade++
		if _, err = tx.Exec("UPDATE device SET reachable=0 "+
			"WHERE policy = ? AND ip=? AND port=?", policy, ipp.ip, ipp.port); err != nil {
			return err
		}
	}
	for _, rDev := range allRingDevices {
		dKey := dw.deviceId(rDev.Ip, rDev.Port, rDev.Device)
		_, isUnmounted := unmountedDevices[dKey]
		_, notReachable := downServers[dw.serverId(rDev.Ip, rDev.Port)]
		changesMade++
		if _, err = tx.Exec("INSERT INTO device (policy, ip, port, device, in_ring, weight, mounted, reachable) "+
			"VALUES (?,?,?,?,?,?,?,?)",
			policy, rDev.Ip, rDev.Port, rDev.Device, true, rDev.Weight, !isUnmounted, !notReachable); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	dw.logger.Info("Updated andrewd DB", zap.Int("policy", policy),
		zap.Int("unmounted", len(unmountedDevices)),
		zap.Int("unreachable", len(downServers)),
		zap.Int("newDevs", len(allRingDevices)),
		zap.Int("changesMade", changesMade))

	return nil
}

func (dw *driveWatch) logRun(rd ringData, success bool, errText string) error {
	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec("INSERT INTO run_log (policy, success, notes) VALUES (?, ?,?)", rd.p.Index, success, errText)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (dw *driveWatch) getReportData(rd ringData) (*ReportData, error) {
	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		return nil, err
	}
	policy := rd.p.Index
	rows, err := db.Query("SELECT count(*), SUM(weight) FROM device WHERE policy = ? AND in_ring=1", policy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var numDevices int
	var totalWeight float64
	rows.Next()
	if err := rows.Scan(&numDevices, &totalWeight); err != nil {
		return nil, err
	}
	rData := ReportData{
		TotalDevices: numDevices,
		TotalWeight:  totalWeight}

	if rows, err = db.Query("SELECT ip, port, device, weight, mounted, reachable, last_update "+
		"FROM device WHERE policy = ? AND (mounted=0 OR reachable=0) AND in_ring=1 ORDER BY last_update", policy); err != nil {
		return nil, err
	}
	for rows.Next() {
		var ip, device string
		var port int
		var weight float64
		var lastUpdate time.Time
		var mounted, reachable bool

		if err := rows.Scan(&ip, &port, &device,
			&weight, &mounted, &reachable, &lastUpdate); err != nil {
			return nil, err
		} else {
			rData.UnmountedDevices = append(
				rData.UnmountedDevices,
				ReportDevice{
					Ip: ip, Port: port, Device: device, Weight: weight,
					Mounted: mounted, Reachable: reachable, LastUpdate: lastUpdate})
		}
	}
	if rows, err = db.Query("SELECT ip, port, device, action, create_date FROM ring_action WHERE policy = ? AND action = ? AND "+
		"create_date = (SELECT MAX(create_date) FROM ring_action) ORDER BY create_date", policy, "ZEROED"); err != nil {
		return nil, err
	}
	for rows.Next() {
		var action string
		var ip, device sql.NullString
		var port sql.NullInt64
		var createDate time.Time

		if err := rows.Scan(&ip, &port, &device, &action, &createDate); err != nil {
			return nil, err
		} else {
			rData.LastRingZeroes = append(
				rData.LastRingZeroes,
				ReportDevice{
					Ip: ip.String, Port: int(port.Int64), Device: device.String, LastUpdate: createDate})
		}
	}
	rows.Close()
	rows, err = db.Query("SELECT create_date FROM run_log WHERE policy = ? ORDER BY create_date DESC LIMIT 1", policy)
	if err != nil {
		dw.logger.Error("SELECT for run_log report", zap.Error(err))
		return nil, err
	}
	if rows.Next() {
		var createDate time.Time
		if err := rows.Scan(&createDate); err == nil {
			rData.LastSuccessfulRun = createDate
		}
	}
	rows.Close()
	return &rData, nil
}

func (dw *driveWatch) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data := make(map[int]*ReportData)

	for p, rd := range dw.policyToRing {
		rData, err := dw.getReportData(rd)
		if err != nil {
			dw.logger.Error("getReportData error", zap.Error(err))
			srv.SimpleErrorResponse(w, 500, "Error getting data.")
			return
		}
		data[p] = rData
	}
	d, err := json.Marshal(data)
	if err != nil {
		dw.logger.Error("getReportData json error", zap.Error(err))
		srv.SimpleErrorResponse(w, 500, "Error jsonifying.")
		return
	}
	w.Header().Set("Content-Type", "app/json")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(d)), 10))
	w.WriteHeader(200)
	w.Write(d)
	return
}

func (dw *driveWatch) getOverdueDevices(rd ringData) (toZeroDevs []ring.Device, overweightToZeroDevs []ring.Device, err error) {
	allRingDevices, _ := dw.getRingData(rd)
	policy := rd.p.Index
	totalWeight := float64(0)
	ringZeroDevices := make(map[string]bool)
	for _, dev := range allRingDevices {
		totalWeight += dev.Weight
		if dev.Weight == 0 {
			ringZeroDevices[dw.deviceId(dev.Ip, dev.Port, dev.Device)] = true
		}
	}
	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	rows, err := db.Query("SELECT ip, port, device, weight, mounted, reachable "+
		"FROM device WHERE policy = ? AND "+
		"(mounted=0 OR reachable=0) AND last_update < ? ORDER BY last_update",
		policy, now.Add(-dw.maxBadDevAge))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	weightToZero := float64(0)
	for rows.Next() {
		var ip, device string
		var port int
		var weight float64
		var mounted, reachable bool

		if err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable); err != nil {
			return nil, nil, err
		} else {
			if _, alreadyZero := ringZeroDevices[dw.deviceId(ip, port, device)]; !alreadyZero {
				weightToZero += weight
				if len(toZeroDevs) == 0 || weightToZero < totalWeight*dw.maxWeightChange {
					// always allow removal of 1 device
					toZeroDevs = append(toZeroDevs, ring.Device{
						Ip: ip, Port: port, Device: device, Weight: weight})
				} else {
					overweightToZeroDevs = append(overweightToZeroDevs, ring.Device{
						Ip: ip, Port: port, Device: device, Weight: weight})
				}
			}
		}
	}
	return toZeroDevs, overweightToZeroDevs, nil
}

func (dw *driveWatch) updateRing(rd ringData) (outputStr string, err error) {
	toZeroDevs, _, err := dw.getOverdueDevices(rd)
	if err != nil {
		return "", err
	}
	if len(toZeroDevs) == 0 {
		return "", nil
	}
	policy := rd.p.Index
	var output []string
	if err != nil {
		return "", err
	}
	b, e := ring.NewRingBuilderFromFile(rd.builderPath, false)
	if e != nil {
		return "", e
	}
	for _, dev := range toZeroDevs {
		for _, rbd := range b.SearchDevs(-1, -1, dev.Ip, int64(dev.Port), "", -1,
			dev.Device, -1, "") {
			deviceLoc := dw.deviceId(rbd.Ip, int(rbd.Port), rbd.Device)
			if e := b.SetDevWeight(rbd.Id, 0); e != nil {
				dw.logger.Error("erroring setting 0 weight",
					zap.String("deviceLoc", deviceLoc), zap.Error(e))
			} else {
				output = append(output,
					fmt.Sprintf("setting device %s weight to 0", deviceLoc))
			}
			if e := b.UpdateDevInfo(rbd.Id, rbd.Ip, rbd.Port, rbd.ReplicationIp,
				rbd.ReplicationPort, rbd.Device,
				fmt.Sprintf("andrewd zeroed weight on: %s",
					time.Now().UTC().Format(time.UnixDate))); e != nil {
				dw.logger.Error("erroring setting metadata",
					zap.String("deviceLoc", deviceLoc), zap.Error(e))
			}
		}
	}
	b.Save(rd.builderPath)

	if dw.doNotRebalance {
		dw.logger.Info("NOT Rebalancing ring",
			zap.Int("policy", policy))
	} else {
		if e := ring.Rebalance(rd.builderPath, false); e != nil {
			dw.logger.Error("error rebalancing ring",
				zap.String("builderPath", rd.builderPath), zap.Error(e))
			return "error rebalancing ring", e
		}
	}
	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		return "", err
	}
	tx, err := db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	now := time.Now()
	for _, dev := range toZeroDevs {
		_, err = tx.Exec("INSERT INTO ring_action "+
			"(policy, ip, port, device, action, create_date) "+
			"VALUES (?,?,?,?,?,?)", policy, dev.Ip, dev.Port, dev.Device,
			"ZEROED", now)
		if err != nil {
			return "", err
		}
	}
	if !dw.doNotRebalance {
		_, err = tx.Exec("INSERT INTO ring_action "+
			"(policy, action, create_date) "+
			"VALUES (?,?,?)", policy, "REBALANCED", now)
		if err != nil {
			return "", err
		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	dw.logger.Info("devices zeroed in ring",
		zap.Int("zeroedDevices", len(toZeroDevs)),
		zap.Int("policy", policy), zap.String("builderPath", rd.builderPath))
	return strings.Join(output, "\n"), nil
}

func (dw *driveWatch) Run() {
	for p, rd := range dw.policyToRing {
		dw.logger.Info("Recon Starting Run", zap.Int("policyIndex", p))
		err := dw.updateDb(rd)
		if err != nil {
			dw.logger.Error("error updating db", zap.Error(err))
		}
		var msg string
		if dw.needRingUpdate(rd) {
			msg, err = dw.updateRing(rd)
			if err != nil {
				msg = fmt.Sprintf("update ring Error: %v", err)
			}
		}
		if e := dw.logRun(rd, err == nil, msg); e != nil {
			dw.logger.Error("error logging run", zap.Error(e))
		}
	}
	time.Sleep(dw.runFreq)
}

func NewDriveWatch(logger srv.LowLevelLogger,
	metricsScope tally.Scope, serverconf conf.Config, cnf srv.ConfigLoader) *driveWatch {
	pl, err := cnf.GetPolicies()
	if err != nil {
		panic(fmt.Sprintf("Unable to load policies: %v", err))
	}
	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		panic(fmt.Sprintf("Unable to load hash path prefix and suffix: %v", err))
	}
	pMap := map[int]ringData{}
	for _, p := range pl {
		objectRing, err := cnf.GetRing("object", hashPathPrefix, hashPathSuffix, p.Index)
		if err != nil {
			panic(fmt.Sprintf("Could not load ring with policy: %d", p.Index))
		}
		rName := "object.builder"
		if p.Index > 0 {
			rName = fmt.Sprintf("object-%d.builder", p.Index)
		}
		bPath := fmt.Sprintf("/etc/hummingbird/%s", rName)
		if _, e := ring.NewRingBuilderFromFile(bPath, false); e != nil {
			bPath = fmt.Sprintf("/etc/swift/%s", rName)
			if _, e := ring.NewRingBuilderFromFile(bPath, false); e != nil {
				panic(fmt.Sprintf("Could not find ring builder with policy: %d", p.Index))
			}
		}
		pMap[p.Index] = ringData{objectRing, p, bPath}
	}
	sqlDir, ok := serverconf.Get("drive_watch", "sql_dir")
	if !ok {
		panic("Invalid Config, no drive_watch sql_dir")
	}
	return &driveWatch{
		policyToRing: pMap,
		logger:       logger,
		//metricsScope: metricsScope,
		runFreq:         time.Duration(serverconf.GetInt("drive_watch", "run_frequency_sec", 3600) * int64(time.Second)),
		maxBadDevAge:    time.Duration(serverconf.GetInt("drive_watch", "max_bad_drive_age_sec", common.ONE_WEEK) * int64(time.Second)),
		ringUpdateFreq:  time.Duration(serverconf.GetInt("drive_watch", "ring_update_frequency_sec", common.ONE_WEEK) * int64(time.Second)),
		maxWeightChange: serverconf.GetFloat("drive_watch", "max_weight_change", 0.01),
		doNotRebalance:  serverconf.GetBool("drive_watch", "do_not_rebalance", false),
		sqlDir:          sqlDir,
	}
}
