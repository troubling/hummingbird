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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/containerserver"
	"github.com/troubling/nectar/nectarutil"
	"github.com/uber-go/tally"
)

type FakeLowLevelLogger struct{}

func (f *FakeLowLevelLogger) Error(msg string, fields ...zapcore.Field) {
	return
}
func (f *FakeLowLevelLogger) Info(msg string, fields ...zapcore.Field) {
	return
}
func (f *FakeLowLevelLogger) Debug(msg string, fields ...zapcore.Field) {
	return
}
func (f *FakeLowLevelLogger) With(fields ...zapcore.Field) *zap.Logger {
	return nil
}

type FakeRing struct {
	Devs      []*ring.Device
	Ip        string
	Port      int
	nodeCalls int
	replicas  uint64
}

func (r *FakeRing) GetNodes(partition uint64) (response []*ring.Device) {
	if r.nodeCalls <= 0 {
		return nil
	}
	r.nodeCalls--
	for i := range r.Devs {
		response = append(response, r.Devs[i])
	}
	return response
}

func (r *FakeRing) GetNodesInOrder(partition uint64) (response []*ring.Device) {
	if partition > 3 {
		return nil
	}
	for i := range r.Devs {
		response = append(response, r.Devs[i])
	}
	return response
}

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*ring.Device, handoff bool) {
	return nil, false
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	objParts := strings.Split(object, "-")
	if p, err := strconv.ParseUint(objParts[0], 10, 64); err == nil {
		return p
	}
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*ring.Device, err error) {
	return nil, nil
}

func (r *FakeRing) AllDevices() (devs []*ring.Device) {
	return r.Devs
}

func (r *FakeRing) GetMoreNodes(partition uint64) ring.MoreNodes {
	return nil
}

func (r *FakeRing) PartitionCount() uint64 {
	return 1
}

func (r *FakeRing) ReplicaCount() uint64 {
	if r.replicas > 0 {
		return r.replicas
	}
	return 2
}

func TestGetDispersionObjects(t *testing.T) {
	fakeDevs := []*ring.Device{
		{Ip: "127.0.0.1", Port: 80, Device: "sda", Scheme: "http"},
		{Ip: "127.0.0.1", Port: 80, Device: "sdb", Scheme: "http"}}

	oring := &FakeRing{Devs: fakeDevs, nodeCalls: 3}
	dObjs := make(chan string)
	container := "objs"
	cancel := make(chan struct{})
	go getDispersionNames(container, "", oring, dObjs, cancel)
	for val := range dObjs {
		part := oring.GetPartition(AdminAccount, container, val)
		require.Equal(t, strings.Index(val, fmt.Sprintf("%d-", part)), 0)
	}
	close(cancel)
}

func TestPutDispersionObjects(t *testing.T) {
	p := conf.Policy{Name: "hat"}
	c := &testDispersionClient{objRing: &FakeRing{Devs: []*ring.Device{{Device: "sda"}, {Device: "sdb"}, {Device: "sdc"}}, nodeCalls: 3}}
	require.True(t, putDispersionObjects(c, &p, &FakeLowLevelLogger{}))
	require.Equal(t, 4, c.objPuts)
}

func TestScanDispersionObjects(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Index(r.URL.Path, "sda") >= 0 {
			srv.SimpleErrorResponse(w, 404, "")
			return
		}
		srv.SimpleErrorResponse(w, 200, "")
	}))

	defer ts.Close()
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)
	c := &testDispersionClient{objRing: &FakeRing{
		Devs: []*ring.Device{
			{Device: "sda", Ip: host, Port: port, Scheme: "http"},
			{Device: "sdb", Ip: host, Port: port, Scheme: "http"},
			{Device: "sdc", Ip: host, Port: port, Scheme: "http"}}, nodeCalls: 3},
		contCalls: 1, objCalls: 1}

	metricsScope := tally.NewTestScope("hb_andrewd", map[string]string{})

	fr := &FakeRing{}
	dw, _ := getDw(fr)
	d, _ := NewDispersion(&FakeLowLevelLogger{}, c, metricsScope, dw, "", "")
	d.onceFullDispersion = true

	dummyCanceler := make(chan struct{})
	defer close(dummyCanceler)
	d.scanDispersionObjs(dummyCanceler)
	for k, v := range metricsScope.Snapshot().Gauges() {
		if strings.Index(k, "hb_andrewd.dispersion_object_p0_missing_0") >= 0 {
			require.Equal(t, 1, int(v.Value()))
		} else {
			require.Equal(t, 0, int(v.Value()))
		}
	}
}

func TestScanDispersionObjectsReport(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Index(r.URL.Path, "sda") >= 0 {
			srv.SimpleErrorResponse(w, 404, "")
			return
		}
		srv.SimpleErrorResponse(w, 200, "")
	}))

	defer ts.Close()
	u, err := url.Parse(ts.URL)
	require.Nil(t, err)
	host, ports, err := net.SplitHostPort(u.Host)
	require.Nil(t, err)
	port, err := strconv.Atoi(ports)
	require.Nil(t, err)
	c := &testDispersionClient{objRing: &FakeRing{
		Devs: []*ring.Device{
			{Device: "sda", Ip: host, Port: port, Scheme: "http"},
			{Device: "sdb", Ip: host, Port: port, Scheme: "http"},
			{Device: "sdc", Ip: host, Port: port, Scheme: "http"}}, nodeCalls: 3, replicas: 3},
		contCalls: 1, objCalls: 1}

	metricsScope := tally.NewTestScope("hb_andrewd", map[string]string{})

	fr := &FakeRing{replicas: 3}
	dw, _ := getDw(fr)
	d, _ := NewDispersion(&FakeLowLevelLogger{}, c, metricsScope, dw, "", "")
	d.onceFullDispersion = true

	dummyCanceler := make(chan struct{})
	defer close(dummyCanceler)
	d.scanDispersionObjs(dummyCanceler)

	db, err := d.dw.getDbAndLock()
	require.Nil(t, err)
	defer d.dw.dbl.Unlock()
	rows, err := db.Query(`SELECT policy, report_text FROM dispersion_report`)
	require.Nil(t, err)
	defer rows.Close()
	for rows.Next() {
		var pol, rep string
		err = rows.Scan(&pol, &rep)
		require.Nil(t, err)
		require.True(t, strings.Index(rep, "There were 1 partitions missing 1 copies") > 0)
	}
}

type testDispersionClient struct {
	objRing   ring.Ring
	contRing  ring.Ring
	objPuts   int
	contCalls int
	objCalls  int
}

func (c *testDispersionClient) PutAccount(account string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) PostAccount(account string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) GetAccount(account string, options map[string]string, headers http.Header) *http.Response {
	if c.contCalls > 0 {
		c.contCalls--
		olrs := []accountserver.ContainerListingRecord{{Name: "disp-objs-0"}}
		out, _ := json.Marshal(olrs)
		return nectarutil.ResponseStub(200, string(out))
	}
	olrs := []accountserver.ContainerListingRecord{}
	out, _ := json.Marshal(olrs)
	return nectarutil.ResponseStub(200, string(out))
}

func (c *testDispersionClient) HeadAccount(account string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) DeleteAccount(account string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) PutContainer(account string, container string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) PostContainer(account string, container string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) GetContainer(account string, container string, options map[string]string, headers http.Header) *http.Response {
	if c.objCalls > 0 {
		c.objCalls--
		olrs := []containerserver.ObjectListingRecord{{Name: "0-12345"}}
		out, _ := json.Marshal(olrs)
		return nectarutil.ResponseStub(200, string(out))
	}
	olrs := []containerserver.ObjectListingRecord{}
	out, _ := json.Marshal(olrs)
	return nectarutil.ResponseStub(200, string(out))
}

func (c *testDispersionClient) GetContainerInfo(account string, container string) (*client.ContainerInfo, error) {
	return nil, nil
}

func (c *testDispersionClient) HeadContainer(account string, container string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) DeleteContainer(account string, container string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader) *http.Response {
	c.objPuts++
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) PostObject(account string, container string, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) GetObject(account string, container string, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) HeadObject(account string, container string, obj string, headers http.Header) *http.Response {
	if obj == "object-init" {
		return nectarutil.ResponseStub(404, "")
	}
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) DeleteObject(account string, container string, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) ObjectRingFor(account string, container string) (ring.Ring, *http.Response) {
	return c.objRing, nil //nectarutil.ResponseStub(200, "")
}

func (c *testDispersionClient) ContainerRing() ring.Ring {
	return c.contRing
}
