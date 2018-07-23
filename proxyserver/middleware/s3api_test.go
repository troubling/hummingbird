package middleware

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidBucketName(t *testing.T) {
	assert.False(t, validBucketName("a"))
	assert.False(t, validBucketName("12345678901234567890123456789012345678901234567890123456789012345"))
	assert.False(t, validBucketName(")kjdsflk"))
	assert.False(t, validBucketName("kjsdflsjf."))
	assert.False(t, validBucketName("8293.-k3j"))
	assert.False(t, validBucketName("389-.2898"))
	assert.False(t, validBucketName("ksjlkf.28lsj..8298"))
	assert.False(t, validBucketName("123.123.123.123"))
	assert.False(t, validBucketName("upperCaseIsBad"))
	assert.False(t, validBucketName("bucket+invalid"))
	assert.True(t, validBucketName("boring.bucket.name"))
}
