package functional

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/dsnet/compress/bzip2"
	"github.com/troubling/hummingbird/common"
)

func TestBulkPutTar(t *testing.T) {
	bulkPutTester(t, "tar")
}

func TestBulkPutTarGz(t *testing.T) {
	bulkPutTester(t, "tar.gz")
}

func TestBulkPutTarBz2(t *testing.T) {
	bulkPutTester(t, "tar.bz2")
}

func bulkPutTester(t *testing.T, format string) {
	if !run {
		t.Skip("HUMMINGBIRD_FUNCTIONAL_TESTS not enabled")
	}
	var archiveBuffer bytes.Buffer
	var tarWriter *tar.Writer
	var nestedCloser io.Closer
	switch format {
	case "tar":
		tarWriter = tar.NewWriter(&archiveBuffer)
	case "tar.gz":
		gzipWriter := gzip.NewWriter(&archiveBuffer)
		nestedCloser = gzipWriter
		tarWriter = tar.NewWriter(gzipWriter)
	case "tar.bz2":
		bzip2Writer, err := bzip2.NewWriter(&archiveBuffer, &bzip2.WriterConfig{Level: bzip2.BestSpeed})
		if err != nil {
			t.Fatal(err)
		}
		nestedCloser = bzip2Writer
		tarWriter = tar.NewWriter(bzip2Writer)
	default:
		t.Fatal(format)
	}
	fileCount := 10
	fileSize := int64(1)
	for i := 0; i < fileCount; i++ {
		if err := tarWriter.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("file%d", i),
			Size: fileSize,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write([]byte(" ")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.WriteHeader(&tar.Header{
		Name: strings.Repeat("f", common.MAX_OBJECT_NAME_LENGTH+1),
		Size: fileSize,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(" ")); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if nestedCloser != nil {
		if err := nestedCloser.Close(); err != nil {
			t.Fatal(err)
		}
	}
	c := getDefaultClient(t)
	if c == nil {
		t.Fatal(c)
	}
	container := getRandomContainerName()
	if resp := c.PutContainer(container, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	parts := strings.SplitN(c.GetURL(), "/", -1)
	account := parts[len(parts)-1]
	defer emptyAndDeleteContainer(c, container)
	if resp := c.Raw("PUT", "/"+container+"?extract-archive="+format, nil, &archiveBuffer); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	} else {
		expecting := strings.Replace(strings.Replace("Response Status: 400 Bad Request\nResponse Body: \nNumber Files Created: 10\nErrors:\n/v1/AUTH_test/hummingbird-functional-test-4090e6130e37b41d/fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff, 400 Bad Request\n", "AUTH_test", account, -1), "hummingbird-functional-test-4090e6130e37b41d", container, -1)
		if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(bodyBytes, []byte(expecting)) {
			t.Fatalf("\n%q\n%q", string(bodyBytes), expecting)
		}
	}
	if listing, resp := c.GetContainer(container, "", "", 0, "", "", false, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	} else {
		if len(listing) != fileCount {
			t.Fatal(len(listing), fileCount)
		}
		for i, item := range listing {
			if item.Name != fmt.Sprintf("file%d", i) {
				t.Fatal(item)
			}
			if item.Bytes != 1 {
				t.Fatal(item)
			}
			if resp := c.GetObject(container, item.Name, nil); resp.StatusCode/100 != 2 {
				t.Fatal(item, resp)
			} else {
				if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
					t.Fatal(item, err)
				} else if int64(len(bodyBytes)) != fileSize {
					t.Fatal(item, len(bodyBytes), fileSize)
				} else {
					resp.Body.Close()
				}
			}
		}
	}
}

func TestBulkPutTarJSON(t *testing.T) {
	if !run {
		t.Skip("HUMMINGBIRD_FUNCTIONAL_TESTS not enabled")
	}
	var tarBuffer bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuffer)
	fileCount := 10
	fileSize := int64(1)
	for i := 0; i < fileCount; i++ {
		if err := tarWriter.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("file%d", i),
			Size: fileSize,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write([]byte(" ")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.WriteHeader(&tar.Header{
		Name: strings.Repeat("f", common.MAX_OBJECT_NAME_LENGTH+1),
		Size: fileSize,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(" ")); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	c := getDefaultClient(t)
	if c == nil {
		t.Fatal(c)
	}
	container := getRandomContainerName()
	if resp := c.PutContainer(container, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	parts := strings.SplitN(c.GetURL(), "/", -1)
	account := parts[len(parts)-1]
	defer emptyAndDeleteContainer(c, container)
	if resp := c.Raw("PUT", "/"+container+"?extract-archive=tar", map[string]string{"Accept": "application/json"}, &tarBuffer); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	} else {
		expecting := strings.Replace(strings.Replace("{\"Response Status\":\"400 Bad Request\",\"Response Body\":\"\",\"Number Files Created\":10,\"Errors\":[[\"/v1/AUTH_test/hummingbird-functional-test-5df08d16884dc626/fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\",\"400 Bad Request\"]]}\n", "AUTH_test", account, -1), "hummingbird-functional-test-5df08d16884dc626", container, -1)
		if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(bodyBytes, []byte(expecting)) {
			t.Fatalf("\n%q\n%q", string(bodyBytes), expecting)
		}
	}
}

func TestBulkPutTarXML(t *testing.T) {
	if !run {
		t.Skip("HUMMINGBIRD_FUNCTIONAL_TESTS not enabled")
	}
	var tarBuffer bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuffer)
	fileCount := 10
	fileSize := int64(1)
	for i := 0; i < fileCount; i++ {
		if err := tarWriter.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("file%d", i),
			Size: fileSize,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write([]byte(" ")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.WriteHeader(&tar.Header{
		Name: strings.Repeat("f", common.MAX_OBJECT_NAME_LENGTH+1),
		Size: fileSize,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(" ")); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	c := getDefaultClient(t)
	if c == nil {
		t.Fatal(c)
	}
	container := getRandomContainerName()
	if resp := c.PutContainer(container, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	parts := strings.SplitN(c.GetURL(), "/", -1)
	account := parts[len(parts)-1]
	defer emptyAndDeleteContainer(c, container)
	if resp := c.Raw("PUT", "/"+container+"?extract-archive=tar", map[string]string{"Accept": "application/xml"}, &tarBuffer); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	} else {
		expecting := strings.Replace(strings.Replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<delete><response_status>400 Bad Request</response_status><response_body></response_body><number_files_created>10</number_files_created><errors><object><name>/v1/AUTH_test/hummingbird-functional-test-38b559f824766c29/fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff</name><status>400 Bad Request</status></object></errors></delete>\n", "AUTH_test", account, -1), "hummingbird-functional-test-38b559f824766c29", container, -1)
		if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(bodyBytes, []byte(expecting)) {
			t.Fatalf("\n%q\n%q", string(bodyBytes), expecting)
		}
	}
}
