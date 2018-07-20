package tools

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

func TestPutDispersionObjects(t *testing.T) {
	p := &conf.Policy{Name: "hat"}
	// nodeCalls = 4 because we have 4 partitions.
	c := &testDispersionClient{objRing: &FakeRing{Devs: []*ring.Device{{Device: "sda"}, {Device: "sdb"}, {Device: "sdc"}}, nodeCalls: 4}}
	db, err := newDB(nil, dbTestName("TestPutDispersionObjects"))
	if err != nil {
		t.Fatal(err)
	}
	require.True(t, newDispersionPopulateObjects(&AutoAdmin{logger: zap.NewNop(), hClient: c, policies: conf.PolicyList{p.Index: p}, db: db, metricsScope: common.NewTestScope()}).runOnce() < 0)
	// 5 because we put one object per partition and there are 4 partitions, and then we put the object-init marker file.
	require.Equal(t, 5, c.objPuts)
}
