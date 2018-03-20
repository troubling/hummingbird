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

package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/srv"
)

func TestDumpReconCache(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value", "somethingelse": "othervalue"})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "value", mapdata["something"])
}

func TestDumpReconCacheEmptyDeletes(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value"})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	_, ok := mapdata["something"]
	assert.False(t, ok)
}

func TestDumpReconCacheNilDeletes(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value"})
	DumpReconCache(dir, "object", map[string]interface{}{"something": nil})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	_, ok := mapdata["something"]
	assert.False(t, ok)
}

func TestDumpReconCacheOverwrite(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value", "somethingelse": "othervalue"})
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value2"})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "othervalue", mapdata["somethingelse"])
	assert.Equal(t, "value2", mapdata["something"])
}

func TestDumpReconCacheUpdateSubMap(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "subvalue1", "subkey2": "subvalue2"}})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "newvalue"}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "subvalue2", mapdata["something"].(map[string]interface{})["subkey2"])
	assert.Equal(t, "newvalue", mapdata["something"].(map[string]interface{})["subkey1"])
}

func TestDumpReconCacheNilDeleteSubKey(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "subvalue1", "subkey2": "subvalue2"}})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": nil}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "subvalue2", mapdata["something"].(map[string]interface{})["subkey2"])
	_, ok := mapdata["something"].(map[string]interface{})["subkey1"]
	assert.False(t, ok)
}

func TestDumpReconCacheEmptyDeleteSubKey(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "subvalue1", "subkey2": "subvalue2"}})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": map[string]interface{}{}}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "subvalue2", mapdata["something"].(map[string]interface{})["subkey2"])
	_, ok := mapdata["something"].(map[string]interface{})["subkey1"]
	assert.False(t, ok)
}

func TestGetMounts(t *testing.T) {
	m := getMounts()
	mounts, ok := m.([]map[string]string)
	assert.True(t, ok)
	for _, mountPoint := range mounts {
		_, ok = mountPoint["device"]
		assert.True(t, ok)
		_, ok = mountPoint["path"]
		assert.True(t, ok)
	}
}

type testWriter struct {
	h http.Header
	f *bytes.Buffer
	s int
}

func (w *testWriter) Header() http.Header {
	return w.h
}

func (w *testWriter) Write(d []byte) (int, error) {
	return w.f.Write(d)
}

func (w *testWriter) WriteHeader(s int) {
	w.s = s
}

func TestGetMem(t *testing.T) {
	r, _ := http.NewRequest("GET", "/I/dont/think/this/matters", nil)
	vars := map[string]string{"method": "mem"}
	r = srv.SetVars(r, vars)
	w := &testWriter{make(http.Header), bytes.NewBuffer(nil), 0}
	ReconHandler("", "", false, w, r)
	output := w.f.Bytes()
	var v map[string]string
	err := json.Unmarshal(output, &v)
	require.Nil(t, err)
	_, ok := v["MemTotal"]
	require.True(t, ok)
}

func TestGetLoad(t *testing.T) {
	r, _ := http.NewRequest("GET", "/I/dont/think/this/matters", nil)
	vars := map[string]string{"method": "load"}
	r = srv.SetVars(r, vars)
	w := &testWriter{make(http.Header), bytes.NewBuffer(nil), 0}
	ReconHandler("", "", false, w, r)
	output := w.f.Bytes()
	var v map[string]interface{}
	err := json.Unmarshal(output, &v)
	require.Nil(t, err)
	m5, ok := v["5m"]
	require.True(t, ok)
	m5f, ok := m5.(float64)
	require.True(t, ok)
	require.True(t, m5f > 0.0)
}

func TestQuarantineDetail(t *testing.T) {
	jsonStruct, err := quarantineDetail("testdata/quarantineDetail")
	if err != nil {
		t.Fatal(err)
	}
	jsonBytes, err := json.MarshalIndent(jsonStruct, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var jsonIface interface{}
	err = json.Unmarshal(jsonBytes, &jsonIface)
	if err != nil {
		t.Fatal(err)
	}
	jsonMap := jsonIface.(map[string]interface{})
	if len(jsonMap) != 3 {
		t.Fatal(len(jsonMap))
	}

	accounts := jsonMap["accounts"].(map[string]interface{})
	if len(accounts) != 1 {
		t.Fatal(len(accounts))
	}
	devAccounts := accounts["sdb4"].([]interface{})
	if len(devAccounts) != 2 {
		t.Fatal(len(devAccounts))
	}
	pathToItem := map[string]string{}
	for _, account := range devAccounts {
		accountMap := account.(map[string]interface{})
		pathToItem[accountMap["NameOnDevice"].(string)] = accountMap["NameInURL"].(string)
	}
	if v, ok := pathToItem["52f9146296db1c31308103a83a7667ed-8848222"]; !ok || v != "" {
		t.Fatal(ok, v)
	}
	if v, ok := pathToItem["a2d288042a86975c5e000e0e4b8d5a2b-12343444"]; !ok || v != "/.admin" {
		t.Fatal(ok, v)
	}

	containers := jsonMap["containers"].(map[string]interface{})
	if len(containers) != 1 {
		t.Fatal(len(containers))
	}
	devContainers := containers["sdb4"].([]interface{})
	if len(devContainers) != 2 {
		t.Fatal(len(devContainers))
	}
	pathToItem = map[string]string{}
	for _, container := range devContainers {
		containerMap := container.(map[string]interface{})
		pathToItem[containerMap["NameOnDevice"].(string)] = containerMap["NameInURL"].(string)
	}
	if v, ok := pathToItem["ff2d04f90fe4099ce8ecc514bbf514b2-413332114"]; !ok || v != "" {
		t.Fatal(ok, v)
	}
	if v, ok := pathToItem["330db13d1978d2eaca43612c433bb1be-234234234"]; !ok || v != "/.admin/disp-conts-204-270" {
		t.Fatal(ok, v)
	}

	objects := jsonMap["objects"].(map[string]interface{})
	if len(objects) != 1 {
		t.Fatal(len(objects))
	}
	devObjects := objects["sdb4"].([]interface{})
	if len(devObjects) != 2 {
		t.Fatal(len(devObjects))
	}
	pathToItem = map[string]string{}
	for _, object := range devObjects {
		objectMap := object.(map[string]interface{})
		pathToItem[objectMap["NameOnDevice"].(string)] = objectMap["NameInURL"].(string)
	}
	if v, ok := pathToItem["197ce7d697904ffaada1a16ee3f7a8c0-8585858"]; !ok || v != "" {
		t.Fatal(ok, v)
	}
	// Have to accept "" as a valid v since not all systems will get the xattrs
	// from the testdata files.
	if v, ok := pathToItem["a4f4d624d9a18c20addf439bcb7192e8-2399494"]; !ok || (v != "/AUTH_test/test-container/.git/objects/ea/0192ee16fc8ee99f594c42c6804012732d9153" && v != "") {
		t.Fatal(ok, v)
	}
}

func TestQuarantineDeleteAndQuarantineHistoryDelete(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(driveRoot)
	if err = os.MkdirAll(path.Join(driveRoot, "sda1/quarantined/accounts/item"), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(path.Join(driveRoot, "sda1/quarantined/containers/item"), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(path.Join(driveRoot, "sda1/quarantined/objects/item"), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(path.Join(driveRoot, "sda1/quarantined/objects-2/item"), 0755); err != nil {
		t.Fatal(err)
	}

	var contents map[string]interface{}
	if contents, err = quarantineDelete(driveRoot, "sda1", "accounts", "item"); err != nil {
		t.Fatal(err)
	}
	if contents != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/accounts/item")); err != nil {
		t.Fatal(err)
	}
	if contents, err = quarantineDelete(driveRoot, "sda1", "containers", "item"); err != nil {
		t.Fatal(err)
	}
	if contents != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/containers/item")); err != nil {
		t.Fatal(err)
	}
	if contents, err = quarantineDelete(driveRoot, "sda1", "objects", "item"); err != nil {
		t.Fatal(err)
	}
	if contents != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/objects/item")); err != nil {
		t.Fatal(err)
	}
	if contents, err = quarantineDelete(driveRoot, "sda1", "objects-2", "item"); err != nil {
		t.Fatal(err)
	}
	if contents != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/objects-2/item")); err != nil {
		t.Fatal(err)
	}

	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "accounts", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "1" {
		t.Fatal(v)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "containers", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "1" {
		t.Fatal(v)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "objects", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "1" {
		t.Fatal(v)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "objects-2", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "1" {
		t.Fatal(v)
	}

	newTime := time.Now().Add(-time.Hour * 24 * 31)
	if err = os.Chtimes(path.Join(driveRoot, "sda1/quarantined-history/accounts/item"), newTime, newTime); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path.Join(driveRoot, "sda1/quarantined-history/containers/item"), newTime, newTime); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path.Join(driveRoot, "sda1/quarantined-history/objects/item"), newTime, newTime); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path.Join(driveRoot, "sda1/quarantined-history/objects-2/item"), newTime, newTime); err != nil {
		t.Fatal(err)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "accounts", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "0" {
		t.Fatal(v)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/accounts/item")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "containers", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "0" {
		t.Fatal(v)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/containers/item")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "objects", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "0" {
		t.Fatal(v)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/objects/item")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if contents, err = quarantineHistoryDelete(driveRoot, "sda1", "objects-2", "30"); err != nil {
		t.Fatal(err)
	}
	if v := fmt.Sprintf("%v", contents["items_left"]); v != "0" {
		t.Fatal(v)
	}
	if _, err = os.Stat(path.Join(driveRoot, "sda1/quarantined-history/objects-2/item")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}
