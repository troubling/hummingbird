package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRing(t *testing.T) {
	tests := []struct {
		prefix    string
		suffix    string
		ring_type string
	}{
		{"t", "t", "test"},
		{"t", "t", "object"},
		{"t", "t", "account"},
		{"t", "t", "container"},
	}

	for _, test := range tests {
		ring, err := GetRing(test.ring_type, test.prefix, test.suffix)
		if test.ring_type == "test" {
			assert.Equal(t, "Error loading test ring", err.Error())
		} else {
			assert.NotNil(t, ring)
		}
	}

}
