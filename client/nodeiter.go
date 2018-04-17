package client

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/troubling/hummingbird/common/ring"
)

type ringFilter interface {
	ReplicaCount() (cnt uint64)
	GetPartition(account string, container string, object string) uint64
	GetNodes(partition uint64) []*ring.Device
	getReadNodes(partition uint64) ([]*ring.Device, ring.MoreNodes)
	getWriteNodes(partition uint64, deviceLimit int) ([]*ring.Device, ring.MoreNodes)
	ring() ring.Ring
}

type writeNodeIter struct {
	mutex      sync.Mutex
	devs       []*ring.Device
	more       ring.MoreNodes
	waffRegion int
	waffCount  int
	limit      int
}

func (wni *writeNodeIter) next() *ring.Device {
	var dev *ring.Device
	for {
		if len(wni.devs) > 0 {
			dev = wni.devs[0]
			wni.devs = wni.devs[1:]
		} else {
			dev = wni.more.Next()
		}
		if dev == nil {
			return nil
		}
		if wni.waffCount <= 0 || wni.waffRegion == -1 || dev.Region == wni.waffRegion {
			wni.waffCount--
			return dev
		}
	}
}

func (wni *writeNodeIter) Next() *ring.Device {
	wni.mutex.Lock()
	defer wni.mutex.Unlock()

	if wni.limit <= 0 {
		return nil
	}
	wni.limit--
	return wni.next()
}

type readAffSection struct {
	zone   int
	region int
	weight float64
}

type clientRingFilter struct {
	ring.Ring
	raffs      []readAffSection
	waffRegion int
	waffCount  int
}

func (a *clientRingFilter) ring() ring.Ring {
	return a.Ring
}

func (a *clientRingFilter) getReadNodes(partition uint64) ([]*ring.Device, ring.MoreNodes) {
	devs := a.GetNodes(partition)
	d2a := make(map[*ring.Device]int, len(devs))
	for i := len(a.raffs) - 1; i >= 0; i-- {
		af := a.raffs[i]
		for _, dev := range devs {
			if (af.region == -1 || af.region == dev.Region) && (af.zone == -1 || af.zone == dev.Zone) {
				d2a[dev] = i
			}
		}
	}
	rand.Shuffle(len(devs), func(i, j int) { devs[i], devs[j] = devs[j], devs[i] })
	sort.SliceStable(devs, func(i, j int) bool { return d2a[devs[i]] < d2a[devs[j]] })
	return devs, a.Ring.GetMoreNodes(partition)
}

func (a *clientRingFilter) getWriteNodes(partition uint64, deviceLimit int) ([]*ring.Device, ring.MoreNodes) {
	var ndevs []*ring.Device
	devs := a.GetNodes(partition)
	if deviceLimit == 0 {
		deviceLimit = len(devs)
	}
	more := &writeNodeIter{
		devs:       devs,
		more:       a.GetMoreNodes(partition),
		waffRegion: a.waffRegion,
		waffCount:  a.waffCount,
		limit:      deviceLimit,
	}
	if deviceLimit < len(devs) {
		ndevs = make([]*ring.Device, deviceLimit)
	} else {
		ndevs = make([]*ring.Device, len(devs))
	}
	for i := range ndevs {
		ndevs[i] = more.next()
	}
	return ndevs, more
}

func newClientRingFilter(r ring.Ring, readAff, writeAff, waffCount string) *clientRingFilter {
	waffRegion := -1
	fmt.Sscanf(writeAff, "r%d", &waffRegion)

	wc := 0
	var f float64
	if v, err := strconv.ParseInt(waffCount, 0, 64); err == nil {
		wc = int(v)
	} else if n, err := fmt.Sscanf(waffCount, "%f * replicas", &f); err == nil && n == 1 {
		wc = int(math.Ceil(f * float64(r.ReplicaCount())))
	} else {
		wc = int(2 * r.ReplicaCount())
	}

	sections := strings.Split(readAff, ",")
	raffs := make([]readAffSection, 0, len(sections))
	for i := range sections {
		var weight float64
		var zone, region int
		if n, err := fmt.Sscanf(strings.TrimSpace(sections[i]), "r%dz%d=%f", &region, &zone, &weight); err == nil && n == 3 {
			raffs = append(raffs, readAffSection{zone: zone, region: region, weight: weight})
		} else if n, err := fmt.Sscanf(strings.TrimSpace(sections[i]), "r%d=%f", &region, &weight); err == nil && n == 2 {
			raffs = append(raffs, readAffSection{zone: -1, region: region, weight: weight})
		}
	}
	sort.Slice(raffs, func(i, j int) bool { return raffs[i].weight < raffs[j].weight })
	raffs = append(raffs, readAffSection{zone: -1, region: -1, weight: -1})

	return &clientRingFilter{
		Ring:       r,
		raffs:      raffs,
		waffRegion: waffRegion,
		waffCount:  wc,
	}
}
