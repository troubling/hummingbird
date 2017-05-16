package client

import "testing"

func TestContainerInfoSourceNil(t *testing.T) {
	c, err := NewProxyDirectClient(nil)
	if c != nil {
		t.Fatal("should have resulted in an error")
	}
	if err == nil {
		t.Fatal("should have resulted in an error")
	}
}

type testContainerInfoSource int

func (t *testContainerInfoSource) GetContainerInfo(account, container string) ContainerInfo {
	return nil
}

func TestContainerInfoNil(t *testing.T) {
	c, err := NewProxyDirectClient(new(testContainerInfoSource))
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
