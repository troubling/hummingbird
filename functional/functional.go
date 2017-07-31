package functional

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
)

var run = common.LooksTrue(os.Getenv("HUMMINGBIRD_FUNCTIONAL_TESTS"))

var defaultClient client.Client
var defaultClientOnce sync.Once

func getDefaultClient(t *testing.T) client.Client {
	defaultClientOnce.Do(func() {
		internal, _ := strconv.ParseBool(os.Getenv("STORAGE_INTERNAL"))
		defaultClient = getClient(t, os.Getenv("AUTH_TENANT"), os.Getenv("AUTH_USER"), os.Getenv("AUTH_PASSWORD"), os.Getenv("AUTH_KEY"), os.Getenv("STORAGE_REGION"), os.Getenv("AUTH_URL"), internal)
	})
	return defaultClient
}

func getClient(t *testing.T, tenant, user, password, key, region, authURL string, internal bool) client.Client {
	t.Log("CREATING CLIENT!")
	c, resp := client.NewClient(tenant, user, password, key, region, authURL, internal)
	if resp != nil {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Auth responded with %d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	return c
}

var rander = rand.New(rand.NewSource(time.Now().UnixNano()))
var randerLock sync.Mutex

func getRandomContainerName() string {
	randerLock.Lock()
	defer randerLock.Unlock()
	return fmt.Sprintf("hummingbird-functional-test-%016x", rander.Uint64())
}

func emptyAndDeleteContainer(c client.Client, container string) *http.Response {
	if resp := c.DeleteContainer(container, nil); resp.StatusCode != http.StatusConflict {
		return resp
	}
	listing, resp := c.GetContainer(container, "", "", 0, "", "", false, nil)
	if resp.StatusCode/100 != 2 {
		return resp
	}
	for _, item := range listing {
		// Don't care about errors; we're just attempting to empty the container.
		c.DeleteObject(container, item.Name, nil)
	}
	return c.DeleteContainer(container, nil)
}
