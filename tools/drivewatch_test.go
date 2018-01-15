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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

func getDw(testRing ring.Ring, settings ...string) (*driveWatch, error) {
	confLoader := srv.NewTestConfigLoader(testRing)

	sqlDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	configString := fmt.Sprintf(
		"[drive_watch]\nsql_dir=%s\n", sqlDir)
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	confStr, err := conf.StringConfig(configString)
	if err != nil {
		return nil, err
	}

	metricsScope := tally.NewTestScope("hb_andrewd", map[string]string{})
	dw := NewDriveWatch(&FakeLowLevelLogger{}, metricsScope, confStr, confLoader, "", "")
	return dw, nil

}

func closeDw(dw *driveWatch) {
	os.RemoveAll(dw.sqlDir)
}

func TestGatherReconData(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": false}]")

	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr := &FakeRing{}
	dw, _ := getDw(fr)
	defer closeDw(dw)

	allWeightedServers := []*ipPort{{ip: host, port: port, scheme: "http"}}
	unmountedDevices, downServers := dw.gatherReconData(allWeightedServers)
	require.Equal(t, len(downServers), 0)
	require.Equal(t, len(unmountedDevices), 1)
	require.Equal(t, unmountedDevices[fmt.Sprintf("%s:%d/sdb1", host, port)].Device, "sdb1")
}

func TestGetRingData(t *testing.T) {
	t.Parallel()
	fr := &FakeRing{}

	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 1, Device: "sdb1", Ip: "1.2", Port: 1, Weight: 1})
	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 2, Device: "sdb2", Ip: "1.2", Port: 1, Weight: 1})
	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 2, Device: "sdb1", Ip: "1.2", Port: 2, Weight: 1})
	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 2, Device: "sdb1", Ip: "1.2", Port: 3, Weight: 0})

	dw, _ := getDw(fr)
	defer closeDw(dw)

	allRingDevices, allWeightedServers := getRingData(fr, true)

	require.Equal(t, len(allRingDevices), 4)
	require.Equal(t, len(allWeightedServers), 2)

	devMap := make(map[string]bool)
	for _, dev := range allRingDevices {
		devMap[deviceId(dev.Ip, dev.Port, dev.Device)] = true
	}
	require.Equal(t, len(devMap), 4)
}

func TestPopulateDbWithRing(t *testing.T) {
	t.Parallel()
	handlerRan := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": true}]")

	}))

	defer ts.Close()
	fr := &FakeRing{}

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 1, Device: "sdb1", Ip: "1.2", Port: 1, Weight: 1.1, Scheme: "http"})
	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 2, Device: "sdb2", Ip: "1.2", Port: 1, Weight: 3.5, Scheme: "http"})
	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 2, Device: "sdb3", Ip: host, Port: port, Weight: 5.5, Scheme: "http"})

	dw, _ := getDw(fr)
	defer closeDw(dw)

	p := conf.Policy{Name: "hat"}

	rd := ringData{r: fr, p: &p, builderPath: "hey"}
	dw.updateDb(rd)

	db, err := dw.getDbAndLock()
	defer dw.dbl.Unlock()

	require.Equal(t, err, nil)
	rows, _ := db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device")
	devMap := make(map[string]float64)
	cnt := 0
	for rows.Next() {
		var ip, device string
		var port int
		var mounted, reachable bool
		var weight float64

		err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable)
		require.Equal(t, err, nil)
		devMap[deviceId(ip, int(port), device)] = weight
		require.Equal(t, mounted, true)
		if device == "sdb3" {
			require.Equal(t, reachable, true)
		} else {
			require.Equal(t, reachable, false)
		}
		cnt += 1

	}
	require.Equal(t, cnt, 3)
	require.Equal(t, devMap["1.2:1/sdb1"], 1.1)
	require.Equal(t, devMap["1.2:1/sdb2"], 3.5)
	require.Equal(t, devMap[fmt.Sprintf("%s:%d/sdb3", host, port)], 5.5)
}

func TestUpdateDb(t *testing.T) {
	t.Parallel()
	handlerRan := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": false}]")

	}))

	defer ts.Close()
	fr := &FakeRing{}

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 1, Device: "sdb1", Ip: host, Port: port, Weight: 1.1, Scheme: "http"})
	p := conf.Policy{Name: "hat"}
	rd := ringData{r: fr, p: &p, builderPath: "hey"}

	dw, _ := getDw(fr)
	defer closeDw(dw)

	db, _ := dw.getDbAndLock()
	defer db.Close()

	tx, err := db.Begin()
	require.Equal(t, err, nil)

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, policy) VALUES"+
		"(?,?,?,?,?,?,?,?)", host, port, "sdb1", true, 2.3, true, true, 0)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	dw.dbl.Unlock()

	dw.updateDb(rd)
	rows, _ := db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device")
	cnt := 0
	for rows.Next() {
		var ip, device string
		var port int
		var mounted, reachable bool
		var weight float64

		err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable)
		require.Equal(t, err, nil)
		require.Equal(t, mounted, false)
		require.Equal(t, reachable, true)
		require.Equal(t, weight, 1.1)
		cnt += 1
	}
	rows.Close()
	require.Equal(t, cnt, 1)
}

func TestUpdateDbUnreachable(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {
		return
	}))
	fr := &FakeRing{}

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 1, Device: "sdb1", Ip: host, Port: port, Weight: 1.1, Scheme: "http"})
	p := conf.Policy{Name: "hat"}
	rd := ringData{r: fr, p: &p, builderPath: "hey"}

	dw, _ := getDw(fr)
	defer closeDw(dw)
	db, _ := dw.getDbAndLock()
	defer db.Close()

	tx, err := db.Begin()
	require.Equal(t, err, nil)

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, policy) VALUES"+
		"(?,?,?,?,?,?,?,?)", host, port, "sdb1", true, 2.3, true, true, 0)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	dw.dbl.Unlock()

	ts.Close()
	dw.updateDb(rd)
	rows, _ := db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device")
	cnt := 0
	for rows.Next() {
		var ip, device string
		var port int
		var mounted, reachable bool
		var weight float64

		err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable)
		require.Equal(t, err, nil)
		require.Equal(t, mounted, true)
		require.Equal(t, reachable, false)
		require.Equal(t, weight, 2.3)
		cnt += 1
	}
	rows.Close()
	require.Equal(t, cnt, 1)
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": false}]")
	}))
	defer ts.Close()
	u, _ = url.Parse(ts.URL)
	host, ports, _ = net.SplitHostPort(u.Host)
	port, _ = strconv.Atoi(ports)
	fr.Devs[0].Port = port

	db, _ = dw.getDbAndLock()
	defer db.Close()
	tx, err = db.Begin()
	require.Equal(t, err, nil)
	_, err = tx.Exec("UPDATE device set port = ?", port)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	dw.dbl.Unlock()
	dw.updateDb(rd)
	rows, _ = db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device")
	cnt = 0
	for rows.Next() {
		var ip, device string
		var port int
		var mounted, reachable bool
		var weight float64

		err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable)
		require.Equal(t, err, nil)
		require.Equal(t, mounted, false)
		require.Equal(t, reachable, true)
		require.Equal(t, weight, 1.1)
		cnt += 1
	}
	rows.Close()
	require.Equal(t, cnt, 1)
}

func TestUpdateRing(t *testing.T) {
	t.Parallel()

	handlerRan := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": false}]")

	}))

	defer ts.Close()
	fr := &FakeRing{}
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)
	dw, _ := getDw(fr)

	db, _ := dw.getDbAndLock()
	defer closeDw(dw)
	defer db.Close()

	tx, err := db.Begin()
	require.Equal(t, err, nil)

	now := time.Now()
	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, last_update, policy) VALUES"+
		"(?,?,?,?,?,?,?,?,?)", host, port, "sdb1", true, 2.3, false, true, now.AddDate(0, 0, -8), 0)
	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, last_update, policy) VALUES"+
		"(?,?,?,?,?,?,?,?,?)", host, port, "sdb2", true, 2.3, false, true, now.AddDate(0, 0, -3), 0)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	dw.dbl.Unlock()

	fr.Devs = append(fr.Devs,
		&ring.Device{Id: 1, Device: "sdb1", Ip: host, Port: port, Weight: 1.1})
	p := conf.Policy{Name: "hat"}

	ringBuilderPath := fmt.Sprintf("%s/object.builder", dw.sqlDir)
	rd := ringData{r: fr, p: &p, builderPath: ringBuilderPath}
	err = ring.CreateRing(ringBuilderPath, 4, 1, 1, false)
	require.Equal(t, err, nil)

	_, err = ring.AddDevice(ringBuilderPath, -1, 1, 1, "http", host, int64(port), host, 6500, "sdb1", 1.0, false)
	require.Equal(t, err, nil)

	_, err = ring.AddDevice(ringBuilderPath, -1, 1, 1, "http", host, int64(port), host, 6500, "sdb2", 1.0, false)
	require.Equal(t, err, nil)

	_, err = os.Stat(ringBuilderPath)
	require.Equal(t, err, nil)

	b, err := ring.NewRingBuilderFromFile(ringBuilderPath, false)
	require.Equal(t, err, nil)
	for _, rbd := range b.SearchDevs(-1, -1, host, int64(port), "", -1,
		"", -1, "", "http") {
		require.Equal(t, 1.0, rbd.Weight)
	}
	_, err = dw.doUpdateRing(rd)
	require.Equal(t, err, nil)
	b, err = ring.NewRingBuilderFromFile(ringBuilderPath, false)
	require.Equal(t, err, nil)
	for _, rbd := range b.SearchDevs(-1, -1, host, int64(port), "", -1,
		"", -1, "", "http") {
		if rbd.Device == "sdb1" {
			require.Equal(t, 0.0, rbd.Weight)
		} else {
			require.Equal(t, 1.0, rbd.Weight)
		}
	}

	rows, err := db.Query("SELECT count(*) FROM ring_action WHERE device=? and action=?", "sdb1", "ZEROED")
	var cnt int
	if rows != nil {
		rows.Next()
		rows.Scan(&cnt)
		require.Equal(t, cnt, 1)
	}

	rows, err = db.Query("SELECT count(*) FROM ring_action WHERE device=? and action=?", "sdb2", "ZEROED")
	if rows != nil {
		rows.Next()
		rows.Scan(&cnt)
		require.Equal(t, cnt, 0)
	}
	rData, err := dw.getReportData(rd)
	require.Equal(t, err, nil)
	require.Equal(t, rData.TotalDevices, 2)
	require.Equal(t, len(rData.LastRingZeroes), 1)
	require.Equal(t, rData.LastRingZeroes[0].Device, "sdb1")
}

func TestProduceReport(t *testing.T) {
	t.Parallel()

	p := conf.Policy{Name: "hat"}
	fr := &FakeRing{}
	dw, _ := getDw(fr)
	defer closeDw(dw)

	rd := ringData{r: fr, p: &p, builderPath: "hey"}
	dw.updateDb(rd)

	db, err := dw.getDbAndLock()
	tx, err := db.Begin()

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, policy) VALUES"+
		"(?,?,?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", true, 2.3, true, true, 0)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)

	dw.dbl.Unlock()
	db.Close()
	rData, err := dw.getReportData(rd)
	require.Equal(t, err, nil)
	require.Equal(t, rData.TotalDevices, 1)
	require.Equal(t, rData.TotalWeight, 2.3)
	require.Equal(t, len(rData.UnmountedDevices), 0)
	require.Equal(t, len(rData.LastRingZeroes), 0)

	db, err = dw.getDbAndLock()
	tx, err = db.Begin()
	require.Equal(t, err, nil)

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, policy) VALUES"+
		"(?,?,?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb2", true, 0, false, true, 0)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	dw.dbl.Unlock()
	db.Close()

	rData, err = dw.getReportData(rd)
	require.Equal(t, err, nil)
	require.Equal(t, rData.TotalDevices, 2)
	require.Equal(t, rData.TotalWeight, 2.3)
	require.Equal(t, len(rData.UnmountedDevices), 1)
	require.Equal(t, len(rData.LastRingZeroes), 0)
}

func TestNeedRingUpdate(t *testing.T) {
	t.Parallel()

	p := conf.Policy{Name: "hat"}

	fr := &FakeRing{}
	rd := ringData{r: fr, p: &p, builderPath: "hey"}
	dw, _ := getDw(fr)
	defer closeDw(dw)

	require.True(t, dw.needRingUpdate(rd)) // will update ring on init run

	db, err := dw.getDbAndLock()
	tx, err := db.Begin()
	require.Equal(t, err, nil)
	now := time.Now()
	_, err = tx.Exec("INSERT INTO ring_action "+
		"(ip, port, device, action, create_date, policy) VALUES"+
		"(?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", "ZEROED", now, 0)
	_, err = tx.Exec("INSERT INTO ring_action "+
		"(ip, port, device, action, create_date, policy) VALUES"+
		"(?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", "ZEROED", now, 0)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	time.Sleep(10000)

	dw.dbl.Unlock()
	dw.ringUpdateFreq = 10
	require.True(t, dw.needRingUpdate(rd))
	dw.ringUpdateFreq = 1000000000
	require.False(t, dw.needRingUpdate(rd))
}
