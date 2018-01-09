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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"github.com/gholt/brimtext"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var DB_NAME = "andrewd.db"
var RING_LOCK_DIR = "ringUpdateLock"

type ipPort struct {
	ip, scheme      string
	port            int
	replicationPort int
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
	policyToRing    map[int]ringData
	logger          srv.LowLevelLogger
	db              *sql.DB
	dbl             sync.Mutex
	client          *http.Client
	maxBadDevAge    time.Duration
	ringUpdateFreq  time.Duration
	runFreq         time.Duration
	maxWeightChange float64
	updateRing      bool
	sqlDir          string
}

func deviceId(ip string, port int, device string) string {
	return fmt.Sprintf("%s:%d/%s", ip, port, device)
}

func serverId(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func getRingData(oring ring.Ring, onlyWeighted bool) (map[string]*ring.Device, []ipPort) {
	allRingDevices := make(map[string]*ring.Device)
	var servers []ipPort
	weightedServers := make(map[string]bool)
	for _, dev := range oring.AllDevices() {
		if dev == nil {
			continue
		}
		allRingDevices[deviceId(dev.Ip, dev.Port, dev.Device)] = dev

		if !onlyWeighted || dev.Weight > 0 {
			if _, ok := weightedServers[serverId(dev.Ip, dev.Port)]; !ok {
				servers =
					append(servers, ipPort{ip: dev.Ip, port: dev.Port, scheme: dev.Scheme, replicationPort: dev.ReplicationPort})
				weightedServers[serverId(dev.Ip, dev.Port)] = true
			}
		}
	}
	return allRingDevices, servers
}

func PrintDriveReport(serverconf conf.Config) error {
	sqlDir := serverconf.GetDefault("drive_watch", "sql_dir", "/var/local/hummingbird")
	if _, err := os.Open(sqlDir); err != nil {
		return fmt.Errorf("Invalid Config, no sql_dir at %s", sqlDir)
	}
	db, err := sql.Open("sqlite3", filepath.Join(sqlDir, DB_NAME))
	if err != nil {
		return err
	}
	defer db.Close()
	maxBadDevAge := time.Duration(serverconf.GetInt("drive_watch", "max_bad_drive_age_sec", common.ONE_WEEK) * int64(time.Second))
	pList, err := conf.GetPolicies()
	if err != nil {
		return err
	}
	for _, p := range pList {
		fmt.Printf("Weighted / Unmounted / Unreachable devices report for Policy %d\n", p.Index)
		rows, err := db.Query("SELECT ip, port, device, weight, mounted, "+
			"reachable, last_update FROM device WHERE policy = ? AND "+
			"(mounted=0 OR reachable=0) ORDER BY last_update",
			p.Index)
		if err != nil {
			return err
		}
		defer rows.Close()
		data := make([][]string, 0)
		data = append(data, []string{"POLICY", "IP ADDRESS", "PORT", "DEVICE", "WEIGHT", "MOUNTED", "REACHABLE", "LAST STATE CHANGE", "SHOULD BE ZEROED"})
		for rows.Next() {
			var ip, device string
			var port int
			var weight float64
			var mounted, reachable bool
			var lastUpdate time.Time
			if err := rows.Scan(&ip, &port, &device,
				&weight, &mounted, &reachable, &lastUpdate); err != nil {
				return err
			} else {
				data = append(data, []string{strconv.Itoa(p.Index), ip,
					strconv.Itoa(port), device, fmt.Sprintf("%.2f", weight),
					fmt.Sprintf("%v", mounted), fmt.Sprintf("%v", reachable),
					lastUpdate.UTC().Format(time.UnixDate),
					fmt.Sprintf("%v", time.Since(lastUpdate) > maxBadDevAge)})
			}
		}
		if len(data) == 1 {
			fmt.Println("No weighted drives are currently reported as unmounted or unreachable.")
		} else {
			fmt.Println(brimtext.Align(data, brimtext.NewSimpleAlignOptions()))
		}
	}
	return nil
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

        CREATE TABLE IF NOT EXISTS dispersion_report (
            id INTEGER PRIMARY KEY,
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			rtype VARCHAR(20) NOT NULL,
            policy INTEGER NOT NULL,
            items INTEGER NOT NULL,
            items_found INTEGER NOT NULL,
            report_text TEXT
            );

        CREATE TABLE IF NOT EXISTS dispersion_report_detail (
            id INTEGER PRIMARY KEY,
            dispersion_report_id INTEGER NOT NULL,
			rtype VARCHAR(20) NOT NULL,
            policy INTEGER NOT NULL,
            partition INTEGER NOT NULL,
            partition_item_path VARCHAR(100) NOT NULL,
            items_found INTEGER NOT NULL,
            items_need INTEGER NOT NULL,
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
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
	for _, s := range servers {
		serverUrl := fmt.Sprintf("%s://%s:%d/recon/unmounted", s.scheme, s.ip, s.port)
		req, err := http.NewRequest("GET", serverUrl, nil)
		if err != nil {
			dw.logger.Error("Could not create recon request", zap.String("url", serverUrl), zap.Error(err))
			continue
		}
		resp, err := dw.client.Do(req)
		if err != nil {
			dw.logger.Error("Could not do recon request", zap.String("url", serverUrl), zap.Error(err))
			downServers[serverId(s.ip, s.port)] = ipPort{scheme: s.scheme, ip: s.ip, port: s.port}
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
					unmountedDevices[deviceId(s.ip,
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

func (dw *driveWatch) needRingUpdate(rd ringData) bool {
	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()
	if err != nil {
		dw.logger.Error("getDbAndLock for needRingUpdate", zap.Error(err))
		return false
	}
	rows, err := db.Query("SELECT create_date FROM run_log WHERE policy = ? ORDER BY create_date DESC LIMIT 1", rd.p.Index)
	if err != nil {
		dw.logger.Error("SELECT for needUpdate run_log report", zap.Error(err))
		return false
	}
	defer rows.Close()
	if rows.Next() {
		var createDate time.Time
		if err := rows.Scan(&createDate); err == nil {
			if time.Since(createDate) > 2*dw.runFreq {
				return false
			}
		}
	}
	rows, err = db.Query("SELECT create_date FROM ring_action WHERE policy = ? ORDER BY create_date DESC LIMIT 1", rd.p.Index)
	if err != nil {
		dw.logger.Error("SELECT for needRingUpdate", zap.Error(err))
		return false
	}
	defer rows.Close()
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
	allRingDevices, allWeightedServers := getRingData(rd.r, true)
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
			dKey := deviceId(ip, port, device)
			rDev, inRing := allRingDevices[dKey]
			_, inUnmounted := unmountedDevices[dKey]
			_, notReachable := downServers[serverId(ip, port)]
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
	rows, err = tx.Query(
		"SELECT id, ip, port, device FROM device WHERE policy = ? AND reachable = 0", policy)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ip, device string
		var dID, port int

		if err := rows.Scan(&dID, &ip, &port, &device); err != nil {
			qryErrors = append(qryErrors, err)
		} else {

			_, notReachable := downServers[serverId(ip, port)]
			if !notReachable {
				changesMade++
				if _, err = tx.Exec("UPDATE device SET reachable=? WHERE id=?", true, dID); err != nil {
					return err
				}
			}
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
		dKey := deviceId(rDev.Ip, rDev.Port, rDev.Device)
		_, isUnmounted := unmountedDevices[dKey]
		_, notReachable := downServers[serverId(rDev.Ip, rDev.Port)]
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

	rows, err = db.Query("SELECT ip, port, device, weight, mounted, reachable, last_update "+
		"FROM device WHERE policy = ? AND (mounted=0 OR reachable=0) AND in_ring=1 ORDER BY last_update", policy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	rows, err = db.Query("SELECT ip, port, device, action, create_date FROM ring_action WHERE policy = ? AND action = ? AND "+
		"create_date = (SELECT MAX(create_date) FROM ring_action) ORDER BY create_date", policy, "ZEROED")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	rows, err = db.Query("SELECT create_date FROM run_log WHERE policy = ? ORDER BY create_date DESC LIMIT 1", policy)
	if err != nil {
		dw.logger.Error("SELECT for run_log report", zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var createDate time.Time
		if err := rows.Scan(&createDate); err == nil {
			rData.LastSuccessfulRun = createDate
		}
	}
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
	allRingDevices, _ := getRingData(rd.r, true)
	policy := rd.p.Index
	totalWeight := float64(0)
	ringZeroDevices := make(map[string]bool)
	for _, dev := range allRingDevices {
		totalWeight += dev.Weight
		if dev.Weight == 0 {
			ringZeroDevices[deviceId(dev.Ip, dev.Port, dev.Device)] = true
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
			if _, alreadyZero := ringZeroDevices[deviceId(ip, port, device)]; !alreadyZero {
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

func (dw *driveWatch) doUpdateRing(rd ringData) (outputStr string, err error) {
	lock, err := fs.LockPath(filepath.Join(dw.sqlDir, RING_LOCK_DIR), 10*time.Second)
	if err != nil {
		return "", err
	}
	defer lock.Close()
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
			dev.Device, -1, "", "") {
			deviceLoc := deviceId(rbd.Ip, int(rbd.Port), rbd.Device)
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
					time.Now().UTC().Format(time.UnixDate)), ""); e != nil {
				dw.logger.Error("erroring setting metadata",
					zap.String("deviceLoc", deviceLoc), zap.Error(e))
			}
		}
	}
	b.Save(rd.builderPath)

	if e := ring.Rebalance(rd.builderPath, false, false); e != nil {
		dw.logger.Error("error rebalancing ring",
			zap.String("builderPath", rd.builderPath), zap.Error(e))
		return "error rebalancing ring", e
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
	_, err = tx.Exec("INSERT INTO ring_action "+
		"(policy, action, create_date) "+
		"VALUES (?,?,?)", policy, "REBALANCED", now)
	if err != nil {
		return "", err
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
		if dw.updateRing && dw.needRingUpdate(rd) {
			msg, err = dw.doUpdateRing(rd)
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
	metricsScope tally.Scope, serverconf conf.Config, cnf srv.ConfigLoader, certFile, keyFile string) *driveWatch {
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
	sqlDir := serverconf.GetDefault("drive_watch", "sql_dir", "/var/local/hummingbird")
	sqlDirOk := false
	if _, err := os.Open(sqlDir); err == nil {
		sqlDirOk = true
	} else {
		if os.IsNotExist(err) && os.MkdirAll(sqlDir, 0755) == nil {
			sqlDirOk = true
		}
	}
	if !sqlDirOk {
		panic(fmt.Sprintf("Invalid drive_watch Config, could not open sql_dir: %s. Please create that directory.", sqlDir))
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
	}
	if certFile != "" && keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			panic(fmt.Sprintf("Error getting TLS config: %v", err))
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			panic(fmt.Sprintf("Error setting up http2: %v", err))
		}
	}
	return &driveWatch{
		policyToRing: pMap,
		logger:       logger,
		client: &http.Client{Timeout: 10 * time.Second,
			Transport: transport},
		//metricsScope: metricsScope,
		runFreq:         time.Duration(serverconf.GetInt("drive_watch", "run_frequency_sec", 3600) * int64(time.Second)),
		maxBadDevAge:    time.Duration(serverconf.GetInt("drive_watch", "max_bad_drive_age_sec", common.ONE_WEEK) * int64(time.Second)),
		ringUpdateFreq:  time.Duration(serverconf.GetInt("drive_watch", "ring_update_frequency_sec", common.ONE_WEEK) * int64(time.Second)),
		maxWeightChange: serverconf.GetFloat("drive_watch", "max_weight_change", 0.01),
		updateRing:      serverconf.GetBool("drive_watch", "update_ring", false),
		sqlDir:          sqlDir,
	}
}
