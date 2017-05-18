package client

import (
	"github.com/troubling/hummingbird/common/conf"
	"testing"
)

func TestClientInfoSourceNil(t *testing.T) {
	c, err := NewProxyDirectClient(nil)
	if c != nil {
		t.Fatal("should have resulted in an error")
	}
	if err == nil {
		t.Fatal("should have resulted in an error")
	}
}

type testClientInfoSource int

func (t *testClientInfoSource) GetContainerInfo(account, container string) *ContainerInfo {
	return nil
}

func (t *testClientInfoSource) DefaultPolicyIndex() int {
	return 0
}

func (t *testClientInfoSource) PolicyByName(name string) *conf.Policy {
	return nil
}

func TestContainerInfoNil(t *testing.T) {
	c, err := NewProxyDirectClient(new(testClientInfoSource))
	if err != nil {
		t.Fatal(err)
	}
	oc, s := c.(*ProxyDirectClient).objectClient("a", "c")
	if oc != nil {
		t.Fatal("should have resulted in an error status")
	}
	if s != 500 {
		t.Fatal("should have resulted in an error status")
	}
}
