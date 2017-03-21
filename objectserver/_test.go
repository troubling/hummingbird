//  Copyright (c) 2016 Red Hat, Inc.

package objectserver

import (
	"os"
	"testing"

	"github.com/troubling/hummingbird/common/conf"
)

func testGetHashPrefixAndSuffix() (pfx string, sfx string, err error) {
	return "", "983abc1de3ff4258", nil
}

func TestMain(m *testing.M) {
	conf.GetHashPrefixAndSuffix = testGetHashPrefixAndSuffix
	os.Exit(m.Run())
}
