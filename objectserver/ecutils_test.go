package objectserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardLength(t *testing.T) {
	length := ecShardLength(1000, 4)
	assert.Equal(t, int64(250), length)
	length = ecShardLength(0, 4)
	assert.Equal(t, int64(0), length)
	length = ecShardLength(-12340, 4)
	assert.Equal(t, int64(0), length)
	length = ecShardLength(1001, 4)
	assert.Equal(t, int64(251), length)
	length = ecShardLength(1001, 5)
	assert.Equal(t, int64(201), length)
	length = ecShardLength(1007, 10)
	assert.Equal(t, int64(101), length)
}
