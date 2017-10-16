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

package objectserver

import (
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/fs"
	"github.com/troubling/hummingbird/common/ring"
	"golang.org/x/net/http2"
)

// walk through the nursery and check every object (and tombstone) if it is
// fully replicated.  that means HEAD other primaries for the object. if the
// ALL other primaries return 2xx/matching x-timestamp then mv local copy to
// stable directory. the other servers will mv their own copies as they find
// them. That means the remote copies can be either in nursery or stable as
// HEAD requests always check both

// if the object is not on one of the other primaries or has mismatched
// timestamps then leave the object alone and wait for existing replicator
// daemon to fully replicate it.

// the existing replicator will not really be changed. but it only walks the
// nursery. the stable objects dir will only be affected by priority
// replication calls sent to it by andrewd (or ops). these calls will happen
// within the existing framework except that they will reference the stable
// dirs.  these priority rep calls will be triggered by ring changes,
// manually by ops, in response to dispersion scan missing objects.  since
// long unmounted drives are zeroed out in ring by the andrewd/drivewatch
// these are also handled once the ring is changed

// PUTs / POST / DELETE will only write to nursery GET / HEAD will check
// nursery and stable on every request. i think it has to check both. it
// seems possible that a more recent copy could be in stable with all the
// handoffs / replication going on in the nursery

// once tombstones get put into stable they can set there indefinitely- i
// guess auditor can clean them up?

//TODO: dfg the prob where a stable obejct gets quarantined and replication doesnt
// know to fix it
const nurseryObjectSleep = 10 * time.Millisecond

type nurseryDevice struct {
	r         *Replicator
	passStart time.Time
	dev       *ring.Device
	policy    int
	oring     ring.Ring
	canchan   chan struct{}
	client    http.Client
	objEngine NurseryObjectEngine
}

func (nrd *nurseryDevice) updateStat(stat string, amount int64) {
	key := deviceKeyId(nrd.dev.Device, nrd.policy)
	nrd.r.updateStat <- statUpdate{"object-nursery", key, stat, amount}
}

func (nrd *nurseryDevice) stabilizeDevice() {
	nrd.updateStat("startRun", 1)
	if mounted, err := fs.IsMount(filepath.Join(nrd.r.deviceRoot, nrd.dev.Device)); nrd.r.checkMounts && (err != nil || mounted != true) {
		nrd.r.logger.Error("[stabilizeDevice] Drive not mounted", zap.String("Device", nrd.dev.Device), zap.Error(err))
		return
	}
	c := make(chan ObjectStabilizer, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go nrd.objEngine.GetNurseryObjects(nrd.dev.Device, c, cancel)
	for o := range c {
		nrd.updateStat("checkin", 1)
		func() {
			nrd.r.nurseryConcurrencySem <- struct{}{}
			defer func() {
				<-nrd.r.nurseryConcurrencySem
			}()
			if err := o.Stabilize(nrd.oring, nrd.dev, nrd.policy); err == nil {
				nrd.updateStat("objectsStabilized", 1)
			} else {
				nrd.r.logger.Error("[stabilizeDevice] error Stabilize obj", zap.String("Object", o.Repr()), zap.Error(err))
			}
		}()
		select {
		case <-time.After(nurseryObjectSleep):
		case <-nrd.canchan:
			return
		}
	}
	nrd.updateStat("PassComplete", 1)
}

func (nrd *nurseryDevice) stabilizeLoop() {
	for {
		select {
		case <-nrd.canchan:
			return
		default:
			nrd.stabilizeDevice()
			time.Sleep(10 * time.Second)
		}
	}
}

func (nrd *nurseryDevice) cancel() {
	close(nrd.canchan)
}

var newNurseryDevice = func(dev *ring.Device, oring ring.Ring, policy int, r *Replicator, objEngine ObjectEngine) (*nurseryDevice, error) {
	noe, ok := objEngine.(NurseryObjectEngine)
	if !ok {
		return nil, fmt.Errorf("Not a nursery object engine")
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
	}
	if r.CertFile != "" && r.KeyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(r.CertFile, r.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("Error getting TLS config: %v", err)
		}
		transport.TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(transport); err != nil {
			return nil, fmt.Errorf("Error setting up http2: %v", err)
		}
	}
	return &nurseryDevice{
		r:         r,
		dev:       dev,
		policy:    policy,
		oring:     oring,
		passStart: time.Now(),
		canchan:   make(chan struct{}),
		client:    http.Client{Timeout: 10 * time.Second, Transport: transport},
		objEngine: noe,
	}, nil
}
