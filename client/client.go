package client

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/troubling/hummingbird/common/ring"
)

// ResponseStub returns a fake response with the given info.
//
// Note: The Request field of the returned response will be nil; you may want
// to set the Request field if you have a specific request to reference.
func ResponseStub(statusCode int, body string) *http.Response {
	bodyBytes := []byte(body)
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		StatusCode:    statusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBuffer(bodyBytes)),
		ContentLength: int64(len(bodyBytes)),
		Header:        http.Header{"Content-Length": {fmt.Sprintf("%d", len(bodyBytes))}, "Content-Type": {"text/plain"}},
	}
}

// StubResponse returns a standalone response with the detail from the original
// response; the full body will be read into memory and the original response's
// Body closed. This is used to allow the response to complete and close so the
// transport can be used for another request/response.
//
// Note: Any error reading the original response's body will be ignored.
//
// Note: The Request field of the returned response will be nil; you may want
// to set the Request field if you have a specific request to reference.
func StubResponse(resp *http.Response) *http.Response {
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	header := make(http.Header, len(resp.Header))
	for headerName, headerValues := range resp.Header {
		copiedHeaderValues := make([]string, len(headerValues))
		for headerValueIndex, headerValue := range headerValues {
			copiedHeaderValues[headerValueIndex] = headerValue
		}
		header[headerName] = copiedHeaderValues
	}
	return &http.Response{
		Status:        resp.Status,
		StatusCode:    resp.StatusCode,
		Proto:         resp.Proto,
		ProtoMajor:    resp.ProtoMajor,
		ProtoMinor:    resp.ProtoMinor,
		Body:          ioutil.NopCloser(bytes.NewBuffer(bodyBytes)),
		ContentLength: int64(len(bodyBytes)),
		Header:        header,
	}
}

// ContainerRecord is an entry in an account listing.
type ContainerRecord struct {
	Count int64  `json:"count"`
	Bytes int64  `json:"bytes"`
	Name  string `json:"name"`
}

// ObjectRecord is an entry in a container listing.
type ObjectRecord struct {
	Hash         string `json:"hash"`
	LastModified string `json:"last_modified"`
	Bytes        int    `json:"bytes"`
	Name         string `json:"name"`
	ContentType  string `json:"content_type"`
}

// Client is an API interface to CloudFiles.
type Client interface {
	PutAccount(headers map[string]string) *http.Response
	PostAccount(headers map[string]string) *http.Response
	// GetAccount reads the body of the response and converts it into a
	// []ContainerRecord while also returning the response instance itself.
	GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse string, headers map[string]string) ([]ContainerRecord, *http.Response)
	HeadAccount(headers map[string]string) *http.Response
	DeleteAccount(headers map[string]string) *http.Response
	PutContainer(container string, headers map[string]string) *http.Response
	PostContainer(container string, headers map[string]string) *http.Response
	// GetContainer reads the body of the response and converts it into an
	// []ObjectRecord while also returning the response instance itself.
	GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse string, headers map[string]string) ([]ObjectRecord, *http.Response)
	HeadContainer(container string, headers map[string]string) *http.Response
	DeleteContainer(container string, headers map[string]string) *http.Response
	PutObject(container string, obj string, headers map[string]string, src io.Reader) *http.Response
	PostObject(container string, obj string, headers map[string]string) *http.Response
	GetObject(container string, obj string, headers map[string]string) *http.Response
	HeadObject(container string, obj string, headers map[string]string) *http.Response
	DeleteObject(container string, obj string, headers map[string]string) *http.Response
}

// ProxyClient is similar to Client except it also accepts an account parameter to its operations.  This is meant to be used by the proxy server.
type ProxyClient interface {
	PutAccount(account string, headers http.Header) *http.Response
	PostAccount(account string, headers http.Header) *http.Response
	GetAccount(account string, options map[string]string, headers http.Header) *http.Response
	HeadAccount(account string, headers http.Header) *http.Response
	DeleteAccount(account string, headers http.Header) *http.Response
	PutContainer(account string, container string, headers http.Header) *http.Response
	PostContainer(account string, container string, headers http.Header) *http.Response
	GetContainer(account string, container string, options map[string]string, headers http.Header) *http.Response
	GetContainerInfo(account string, container string) (*ContainerInfo, error)
	HeadContainer(account string, container string, headers http.Header) *http.Response
	DeleteContainer(account string, container string, headers http.Header) *http.Response
	PutObject(account string, container string, obj string, headers http.Header, src io.Reader) *http.Response
	PostObject(account string, container string, obj string, headers http.Header) *http.Response
	GetObject(account string, container string, obj string, headers http.Header) *http.Response
	HeadObject(account string, container string, obj string, headers http.Header) *http.Response
	DeleteObject(account string, container string, obj string, headers http.Header) *http.Response
	// ObjectRingFor returns the object ring for the given account/container or
	// a response as to why the ring could not be returned.
	ObjectRingFor(account string, container string) (ring.Ring, *http.Response)
}

// ContainerInfo is persisted in memcache via JSON; so this needs to continue to have public fields.
type ContainerInfo struct {
	ReadACL            string
	WriteACL           string
	SyncKey            string
	ObjectCount        int64
	ObjectBytes        int64
	Metadata           map[string]string
	SysMetadata        map[string]string
	StoragePolicyIndex int
}
