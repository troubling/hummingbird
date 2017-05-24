package client

import (
	"github.com/troubling/hummingbird/common/conf"
	"net/http"
	"testing"
)

func TestClientInfoSourceNil(t *testing.T) {
	c, err := NewProxyDirectClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := c.(*ProxyDirectClient)
	if !ok {
		t.Fatal("")
	}
	if p.clientInfoSource == nil {
		t.Fatal("")
	}
	if _, ok := p.clientInfoSource.(*standaloneClientInfoSource); !ok {
		t.Fatal("")
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

func (t *testClientInfoSource) HTTPClient() *http.Client {
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
