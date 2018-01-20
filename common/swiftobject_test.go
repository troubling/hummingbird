package common

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteReadMetadata(t *testing.T) {

	data := map[string]string{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	testFile, err := ioutil.TempFile("/tmp", "backend_test")
	defer testFile.Close()
	defer os.Remove(testFile.Name())
	assert.Equal(t, err, nil)
	SwiftObjectWriteMetadata(testFile.Fd(), data)
	checkData := map[string]string{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	readData, err := SwiftObjectReadMetadata(testFile.Name())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))

	readData, err = SwiftObjectReadMetadata(testFile.Fd())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))
}
