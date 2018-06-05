package proxyserver

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"
	"github.com/troubling/hummingbird/proxyserver/middleware"
	"go.uber.org/zap"
)

var endpointsPolicyList = conf.PolicyList(map[int]*conf.Policy{
	0: {Index: 0, Type: "hec", Name: "gold", Aliases: []string{}, Default: true, Deprecated: false, Config: map[string]string{}},
	1: {Index: 0, Type: "rep", Name: "silver", Aliases: []string{}, Default: true, Deprecated: false, Config: map[string]string{}},
})

func TestEndpointsObjectGetHandler(t *testing.T) {
	p := ProxyServer{}
	headers := make(http.Header, 1)
	statuses := map[string]int{"S": 1}
	var body bytes.Buffer
	devices := []*ring.Device{
		{Scheme: "http", Device: "sda1", Ip: "127.0.0.1"},
		{Scheme: "http", Device: "sdb1", Ip: "127.0.0.2"},
		{Scheme: "http", Device: "sdc1", Ip: "127.0.0.3"},
		{Scheme: "http", Device: "sdd1", Ip: "127.0.0.4"},
	}
	fakeWriter := test.MockResponseWriter{SaveHeader: &headers, StatusMap: statuses, Body: &body}
	f, err := client.NewProxyClient(endpointsPolicyList, srv.NewTestConfigLoader(&test.FakeRing{MockDevices: devices}), nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	r := httptest.NewRequest("GET", "/endpoints/a/c/o", nil)
	ctx := &middleware.ProxyContext{C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{"container/a/c": {}}, zap.NewNop())}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c", "obj": "o"})
	p.EndpointsObjectGetHandler(fakeWriter, r)
	if statuses["S"] != 200 {
		t.Fatal(statuses)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal(headers.Get("Content-Type"))
	}
	bodyBytes, err := ioutil.ReadAll(&body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyBytes, []byte(`["http://127.0.0.1:0/sda1/0/a/c/o","http://127.0.0.2:0/sdb1/0/a/c/o","http://127.0.0.3:0/sdc1/0/a/c/o"]`)) {
		t.Fatal(string(bodyBytes))
	}
}

func TestEndpointsContainerGetHandler(t *testing.T) {
	p := ProxyServer{}
	headers := make(http.Header, 1)
	statuses := map[string]int{"S": 1}
	var body bytes.Buffer
	devices := []*ring.Device{
		{Scheme: "http", Device: "sda1", Ip: "127.0.0.1"},
		{Scheme: "http", Device: "sdb1", Ip: "127.0.0.2"},
		{Scheme: "http", Device: "sdc1", Ip: "127.0.0.3"},
		{Scheme: "http", Device: "sdd1", Ip: "127.0.0.4"},
	}
	fakeWriter := test.MockResponseWriter{SaveHeader: &headers, StatusMap: statuses, Body: &body}
	f, err := client.NewProxyClient(endpointsPolicyList, srv.NewTestConfigLoader(&test.FakeRing{MockDevices: devices}), nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	r := httptest.NewRequest("GET", "/endpoints/a/c", nil)
	ctx := &middleware.ProxyContext{C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{"container/a/c": {}}, zap.NewNop())}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c"})
	p.EndpointsContainerGetHandler(fakeWriter, r)
	if statuses["S"] != 200 {
		t.Fatal(statuses)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal(headers.Get("Content-Type"))
	}
	bodyBytes, err := ioutil.ReadAll(&body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyBytes, []byte(`["http://127.0.0.1:0/sda1/0/a/c","http://127.0.0.2:0/sdb1/0/a/c","http://127.0.0.3:0/sdc1/0/a/c"]`)) {
		t.Fatal(string(bodyBytes))
	}
}

func TestEndpointsAccountGetHandler(t *testing.T) {
	p := ProxyServer{}
	headers := make(http.Header, 1)
	statuses := map[string]int{"S": 1}
	var body bytes.Buffer
	devices := []*ring.Device{
		{Scheme: "http", Device: "sda1", Ip: "127.0.0.1"},
		{Scheme: "http", Device: "sdb1", Ip: "127.0.0.2"},
		{Scheme: "http", Device: "sdc1", Ip: "127.0.0.3"},
		{Scheme: "http", Device: "sdd1", Ip: "127.0.0.4"},
	}
	fakeWriter := test.MockResponseWriter{SaveHeader: &headers, StatusMap: statuses, Body: &body}
	f, err := client.NewProxyClient(endpointsPolicyList, srv.NewTestConfigLoader(&test.FakeRing{MockDevices: devices}), nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	r := httptest.NewRequest("GET", "/endpoints/a", nil)
	ctx := &middleware.ProxyContext{C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{"container/a/c": {}}, zap.NewNop())}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a"})
	p.EndpointsAccountGetHandler(fakeWriter, r)
	if statuses["S"] != 200 {
		t.Fatal(statuses)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal(headers.Get("Content-Type"))
	}
	bodyBytes, err := ioutil.ReadAll(&body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyBytes, []byte(`["http://127.0.0.1:0/sda1/0/a","http://127.0.0.2:0/sdb1/0/a","http://127.0.0.3:0/sdc1/0/a"]`)) {
		t.Fatal(string(bodyBytes))
	}
}

func TestEndpointsObjectGetHandler2(t *testing.T) {
	p := ProxyServer{}
	headers := make(http.Header, 1)
	statuses := map[string]int{"S": 1}
	var body bytes.Buffer
	devices := []*ring.Device{
		{Scheme: "http", Device: "sda1", Ip: "127.0.0.1"},
		{Scheme: "http", Device: "sdb1", Ip: "127.0.0.2"},
		{Scheme: "http", Device: "sdc1", Ip: "127.0.0.3"},
		{Scheme: "http", Device: "sdd1", Ip: "127.0.0.4"},
	}
	fakeWriter := test.MockResponseWriter{SaveHeader: &headers, StatusMap: statuses, Body: &body}
	f, err := client.NewProxyClient(endpointsPolicyList, srv.NewTestConfigLoader(&test.FakeRing{MockDevices: devices}), nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	r := httptest.NewRequest("GET", "/endpoints/v2/a/c/o", nil)
	ctx := &middleware.ProxyContext{C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{"container/a/c": {}}, zap.NewNop())}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c", "obj": "o"})
	p.EndpointsObjectGetHandler2(fakeWriter, r)
	if statuses["S"] != 200 {
		t.Fatal(statuses)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal(headers.Get("Content-Type"))
	}
	bodyBytes, err := ioutil.ReadAll(&body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyBytes, []byte(`{"endpoints":["http://127.0.0.1:0/sda1/0/a/c/o","http://127.0.0.2:0/sdb1/0/a/c/o","http://127.0.0.3:0/sdc1/0/a/c/o"],"headers":{"X-Backend-Storage-Policy-Index":"0"}}`)) {
		t.Fatal(string(bodyBytes))
	}
}

func TestEndpointsContainerGetHandler2(t *testing.T) {
	p := ProxyServer{}
	headers := make(http.Header, 1)
	statuses := map[string]int{"S": 1}
	var body bytes.Buffer
	devices := []*ring.Device{
		{Scheme: "http", Device: "sda1", Ip: "127.0.0.1"},
		{Scheme: "http", Device: "sdb1", Ip: "127.0.0.2"},
		{Scheme: "http", Device: "sdc1", Ip: "127.0.0.3"},
		{Scheme: "http", Device: "sdd1", Ip: "127.0.0.4"},
	}
	fakeWriter := test.MockResponseWriter{SaveHeader: &headers, StatusMap: statuses, Body: &body}
	f, err := client.NewProxyClient(endpointsPolicyList, srv.NewTestConfigLoader(&test.FakeRing{MockDevices: devices}), nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	r := httptest.NewRequest("GET", "/endpoints/v2/a/c", nil)
	ctx := &middleware.ProxyContext{C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{"container/a/c": {}}, zap.NewNop())}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a", "container": "c"})
	p.EndpointsContainerGetHandler2(fakeWriter, r)
	if statuses["S"] != 200 {
		t.Fatal(statuses)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal(headers.Get("Content-Type"))
	}
	bodyBytes, err := ioutil.ReadAll(&body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyBytes, []byte(`{"endpoints":["http://127.0.0.1:0/sda1/0/a/c","http://127.0.0.2:0/sdb1/0/a/c","http://127.0.0.3:0/sdc1/0/a/c"],"headers":{"X-Backend-Storage-Policy-Index":"0"}}`)) {
		t.Fatal(string(bodyBytes))
	}
}

func TestEndpointsAccountGetHandler2(t *testing.T) {
	p := ProxyServer{}
	headers := make(http.Header, 1)
	statuses := map[string]int{"S": 1}
	var body bytes.Buffer
	devices := []*ring.Device{
		{Scheme: "http", Device: "sda1", Ip: "127.0.0.1"},
		{Scheme: "http", Device: "sdb1", Ip: "127.0.0.2"},
		{Scheme: "http", Device: "sdc1", Ip: "127.0.0.3"},
		{Scheme: "http", Device: "sdd1", Ip: "127.0.0.4"},
	}
	fakeWriter := test.MockResponseWriter{SaveHeader: &headers, StatusMap: statuses, Body: &body}
	f, err := client.NewProxyClient(endpointsPolicyList, srv.NewTestConfigLoader(&test.FakeRing{MockDevices: devices}), nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	r := httptest.NewRequest("GET", "/endpoints/v2/a", nil)
	ctx := &middleware.ProxyContext{C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{"container/a/c": {}}, zap.NewNop())}
	r = r.WithContext(context.WithValue(r.Context(), "proxycontext", ctx))
	r = srv.SetVars(r, map[string]string{"account": "a"})
	p.EndpointsAccountGetHandler2(fakeWriter, r)
	if statuses["S"] != 200 {
		t.Fatal(statuses)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal(headers.Get("Content-Type"))
	}
	bodyBytes, err := ioutil.ReadAll(&body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyBytes, []byte(`{"endpoints":["http://127.0.0.1:0/sda1/0/a","http://127.0.0.2:0/sdb1/0/a","http://127.0.0.3:0/sdc1/0/a"],"headers":{}}`)) {
		t.Fatal(string(bodyBytes))
	}
}
