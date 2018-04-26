package client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
)

func TestAffinityReadOrder(t *testing.T) {
	r := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 0, Region: 1, Zone: 2, Device: "sda"},
			{Id: 1, Region: 2, Zone: 2, Device: "sdc"},
			{Id: 2, Region: 1, Zone: 1, Device: "sdb"},
		},
	}

	a := newClientRingFilter(r, "r1z1=100, r1=200", "", "")
	devs, _ := a.getReadNodes(1)
	require.Equal(t, "sdb", devs[0].Device)
	require.Equal(t, "sda", devs[1].Device)
	require.Equal(t, "sdc", devs[2].Device)

	a = newClientRingFilter(r, "r1=200, r1z1=100", "", "")
	devs, _ = a.getReadNodes(1)
	require.Equal(t, "sdb", devs[0].Device)
	require.Equal(t, "sda", devs[1].Device)
	require.Equal(t, "sdc", devs[2].Device)
}

type fakeRing struct {
	*test.FakeRing
	nodes []*ring.Device
}

func (fr *fakeRing) GetNodes(partition uint64) []*ring.Device {
	return fr.nodes
}

func TestWriteAffinityFiltering(t *testing.T) {
	r := &fakeRing{
		FakeRing: &test.FakeRing{
			MockMoreNodes: &ring.Device{Id: 6, Region: 1, Zone: 1, Device: "sdg"},
		},
		nodes: []*ring.Device{
			{Id: 0, Region: 1, Zone: 1, Device: "sda"},
			{Id: 1, Region: 2, Zone: 1, Device: "sdb"},
			{Id: 2, Region: 1, Zone: 1, Device: "sdc"},
			{Id: 3, Region: 2, Zone: 1, Device: "sdd"},
			{Id: 4, Region: 1, Zone: 1, Device: "sde"},
			{Id: 5, Region: 2, Zone: 1, Device: "sdf"},
		},
	}

	a := newClientRingFilter(r, "", "r1", "")
	devs, _ := a.getWriteNodes(1, 4)
	require.Equal(t, 4, len(devs))
	require.Equal(t, 0, devs[0].Id)
	require.Equal(t, 2, devs[1].Id)
	require.Equal(t, 4, devs[2].Id)
	require.Equal(t, 6, devs[3].Id)
}

func TestParseWaffCount(t *testing.T) {
	a := newClientRingFilter(&test.FakeRing{}, "", "r1", "")
	require.Equal(t, 6, a.waffCount)
	a = newClientRingFilter(&test.FakeRing{}, "", "r1", "7")
	require.Equal(t, 7, a.waffCount)
	a = newClientRingFilter(&test.FakeRing{}, "", "r1", "3 * replicas")
	require.Equal(t, 9, a.waffCount)
	a = newClientRingFilter(&test.FakeRing{}, "", "r1", "2.5 * replicas")
	require.Equal(t, 8, a.waffCount)
	a = newClientRingFilter(&test.FakeRing{}, "", "r1", "7 * monkeys")
	require.Equal(t, 6, a.waffCount)
}

func TestDeviceLimit(t *testing.T) {
	r := &fakeRing{
		FakeRing: &test.FakeRing{
			MockMoreNodes: &ring.Device{Id: 6, Region: 1, Zone: 1, Device: "sdg"},
		},
		nodes: []*ring.Device{
			{Id: 0, Region: 1, Zone: 1, Device: "sda"},
			{Id: 1, Region: 2, Zone: 1, Device: "sdb"},
			{Id: 2, Region: 1, Zone: 1, Device: "sdc"},
			{Id: 3, Region: 2, Zone: 1, Device: "sdd"},
			{Id: 4, Region: 1, Zone: 1, Device: "sde"},
			{Id: 5, Region: 2, Zone: 1, Device: "sdf"},
		},
	}
	a := newClientRingFilter(r, "", "", "")

	devs, _ := a.getWriteNodes(1, 0)
	require.Equal(t, 6, len(devs))

	devs, more := a.getWriteNodes(1, 4)
	require.Equal(t, 4, len(devs))
	i := 0
	for more.Next() != nil {
		i++
	}
	require.Equal(t, 4, i)

	devs, more = a.getWriteNodes(1, 2)
	require.Equal(t, 2, len(devs))

	i = 0
	for more.Next() != nil {
		i++
	}
	require.Equal(t, 2, i)
}

func TestWriteAffinityNonPreferred(t *testing.T) {
	r := &fakeRing{
		FakeRing: &test.FakeRing{
			MockMoreNodes: &ring.Device{Id: 6, Region: 1, Zone: 1, Device: "sdg"},
		},
		nodes: []*ring.Device{
			{Id: 0, Region: 1, Zone: 1, Device: "sda"},
			{Id: 1, Region: 2, Zone: 1, Device: "sdb"},
			{Id: 2, Region: 1, Zone: 1, Device: "sdc"},
			{Id: 3, Region: 2, Zone: 1, Device: "sdd"},
			{Id: 4, Region: 1, Zone: 1, Device: "sde"},
			{Id: 5, Region: 2, Zone: 1, Device: "sdf"},
		},
	}

	a := newClientRingFilter(r, "", "r1", "4")
	devs, more := a.getWriteNodes(1, 3)
	require.Equal(t, 3, len(devs))
	require.Equal(t, 0, devs[0].Id)
	require.Equal(t, 2, devs[1].Id)
	require.Equal(t, 4, devs[2].Id)
	require.Equal(t, 6, more.Next().Id)
	require.Equal(t, 1, more.Next().Id)
	require.Equal(t, 3, more.Next().Id)
	require.Equal(t, (*ring.Device)(nil), more.Next())

	a = newClientRingFilter(r, "", "r1", "2")
	devs, more = a.getWriteNodes(1, 3)
	require.Equal(t, 3, len(devs))
	require.Equal(t, 0, devs[0].Id)
	require.Equal(t, 2, devs[1].Id)
	require.Equal(t, 3, devs[2].Id)
	require.Equal(t, 4, more.Next().Id)
	require.Equal(t, 5, more.Next().Id)
}
