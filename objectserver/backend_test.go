package objectserver

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteReadMetadata(t *testing.T) {

	data := map[string]interface{}{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	testFile, err := ioutil.TempFile("/tmp", "backend_test")
	defer testFile.Close()
	defer os.Remove(testFile.Name())
	assert.Equal(t, err, nil)
	WriteMetadata(testFile.Fd(), data)
	checkData := map[interface{}]interface{}{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	readData, err := ReadMetadata(testFile.Name())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))

	readData, err = ReadMetadata(testFile.Fd())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))
}
