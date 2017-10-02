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

package conf

import "strings"

type SyncRealm struct {
	Name     string
	Key1     string
	Key2     string
	Clusters map[string]string
}

type SyncRealmList map[string]SyncRealm

func (l SyncRealmList) ValidateSyncTo(syncHeader string) bool {
	if !strings.HasPrefix(syncHeader, "//") {
		return false
	}
	parts := strings.Split(syncHeader[2:], "/")
	if len(parts) < 4 {
		return false
	}
	realm := parts[0]
	cluster := parts[1]
	account := parts[2]
	container := parts[3]
	if account == "" || container == "" {
		return false
	}
	if l[realm].Key1 == "" {
		return false
	}
	if l[realm].Clusters[cluster] == "" {
		return false
	}
	return true
}

var syncRealmConfigLocations = []string{"/etc/hummingbird/container-sync-realms.conf", "/etc/swift/container-sync-realms.conf"}

func GetSyncRealms() (SyncRealmList, error) {
	resp := make(map[string]SyncRealm)
	for _, loc := range syncRealmConfigLocations {
		if conf, err := LoadConfig(loc); err == nil {
			for realm, config := range conf.File {
				realm := SyncRealm{Name: realm, Clusters: make(map[string]string)}
				for k, v := range config {
					if k == "key" {
						realm.Key1 = v
					} else if k == "key2" {
						realm.Key2 = v
					} else if strings.HasPrefix(k, "cluster_") {
						realm.Clusters[k[8:]] = v
					}
				}
				resp[realm.Name] = realm
			}
		}
	}
	return SyncRealmList(resp), nil
}
