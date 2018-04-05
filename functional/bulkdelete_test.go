package functional

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestBulkDeletePlain(t *testing.T) {
	bulkDeleteTester(t, "text/plain", "Response Status: 400 Bad Request\nResponse Body: \nNumber Deleted: 12\nNumber Not Found: 2\nErrors:\n/hummingbird-functional-test-4938dd9a65eaa780, 409 Conflict\n")
}

func TestBulkDeleteJSON(t *testing.T) {
	bulkDeleteTester(t, "application/json", "{\"Response Status\":\"400 Bad Request\",\"Response Body\":\"\",\"Number Deleted\":12,\"Number Not Found\":2,\"Errors\":[[\"/hummingbird-functional-test-4938dd9a65eaa780\",\"409 Conflict\"]]}\n")
}

func TestBulkDeleteXML(t *testing.T) {
	bulkDeleteTester(t, "application/xml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<delete><response_status>400 Bad Request</response_status><response_body></response_body><number_deleted>12</number_deleted><number_not_found>2</number_not_found><errors><object><name>/hummingbird-functional-test-4938dd9a65eaa780</name><status>409 Conflict</status></object></errors></delete>\n")
}

func bulkDeleteTester(t *testing.T, accept, expect string) {
	if !run {
		t.Skip("HUMMINGBIRD_FUNCTIONAL_TESTS not enabled")
	}
	c := getDefaultClient(t)
	if c == nil {
		t.Fatal(c)
	}
	container := getRandomContainerName()
	if resp := c.PutContainer(container, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	defer emptyAndDeleteContainer(c, container)
	objectCount := 10
	for i := 0; i <= objectCount; i++ {
		object := fmt.Sprintf("object%d", i)
		if resp := c.PutObject(container, object, nil, bytes.NewBuffer([]byte(" "))); resp.StatusCode/100 != 2 {
			t.Fatal(resp, object)
		}
	}
	container2 := getRandomContainerName()
	if resp := c.PutContainer(container2, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	defer emptyAndDeleteContainer(c, container2)
	if resp := c.PutObject(container2, "object", nil, bytes.NewBuffer([]byte(" "))); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	container3 := getRandomContainerName()
	if resp := c.PutContainer(container3, nil); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	}
	defer emptyAndDeleteContainer(c, container3)
	bulkDeleteLines := "/something/that/does/not/exist\n"
	bulkDeleteLines += "/" + container2 + "\n" // Should fail due to 409 Conflict with that single object in there
	bulkDeleteLines += "/" + container3 + "\n"
	bulkDeleteLines += "just junk which is okay\n"
	for i := 0; i <= objectCount; i++ {
		object := fmt.Sprintf("object%d", i)
		bulkDeleteLines += "/" + container + "/" + object + "\n"
	}
	// just a double check
	for i := 0; i <= objectCount; i++ {
		object := fmt.Sprintf("object%d", i)
		if resp := c.HeadObject(container, object, nil); resp.StatusCode/100 != 2 {
			t.Fatal(resp, object)
		}
	}
	if resp := c.Raw("DELETE", "?bulk-delete", map[string]string{"Accept": accept}, bytes.NewBuffer([]byte(bulkDeleteLines))); resp.StatusCode/100 != 2 {
		t.Fatal(resp)
	} else {
		expecting := strings.Replace(expect, "hummingbird-functional-test-4938dd9a65eaa780", container2, -1)
		if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(bodyBytes, []byte(expecting)) {
			t.Fatalf("\n%q\n%q", string(bodyBytes), expecting)
		}
	}
	for i := 0; i <= objectCount; i++ {
		object := fmt.Sprintf("object%d", i)
		if resp := c.HeadObject(container, object, nil); resp.StatusCode != 404 {
			t.Fatal(resp, object)
		}
	}
}
