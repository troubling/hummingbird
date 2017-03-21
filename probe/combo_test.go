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

package probe

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/troubling/hummingbird/common"
)

func TestAuditReplicate(t *testing.T) {
	e := NewEnvironment()
	defer e.Close()

	// put a file
	timestamp := common.GetTimestamp()
	for i := 0; i < 3; i++ {
		e.PutObject(i, timestamp, "X", 0)
	}

	// simulate bit-rot of the file contents
	locations := e.FileLocations("a", "c", "o", 0)
	path := filepath.Join(locations[0], timestamp+".data")
	f, _ := os.OpenFile(path, os.O_RDWR, 0777)
	f.Write([]byte("!"))
	f.Close()

	// make sure the file is gone after an audit pass
	e.auditors[0].Run()
	assert.False(t, e.ObjExists(0, timestamp, 0))

	// make sure the file is replaced after another server's replicator runs
	e.replicatorServers[1].replicator.Run()
	assert.True(t, e.ObjExists(0, timestamp, 0))
}
