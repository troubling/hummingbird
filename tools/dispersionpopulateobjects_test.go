package tools

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

func TestPutDispersionObjects(t *testing.T) {
	p := &conf.Policy{Name: "hat"}
	c := &testDispersionClient{objRing: &FakeRing{Devs: []*ring.Device{{Device: "sda"}, {Device: "sdb"}, {Device: "sdc"}}, nodeCalls: 3}}
	db, err := newDB(nil, "TestPutDispersionObjects")
	if err != nil {
		t.Fatal(err)
	}
	require.True(t, newDispersionPopulateObjects(&AutoAdmin{logger: zap.NewNop(), hClient: c, policies: conf.PolicyList{p.Index: p}, db: db}).runOnce() < 0)
	require.Equal(t, 4, c.objPuts)
}
