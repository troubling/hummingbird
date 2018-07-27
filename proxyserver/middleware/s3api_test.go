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

func TestS3DateString(t *testing.T) {
	// Removes 3 of the last 6 digits
	assert.Equal(t, "2018-07-05T18:16:09.295Z", s3DateString("2018-07-05T18:16:09.295890Z"))
	// Always adds a Z
	assert.Equal(t, "123456Z", s3DateString("123456789"))
	// Doesn't index out of range.
	assert.Equal(t, "no", s3DateString("no"))
}
