package client

import (
	"io"
	"net/http"

	"github.com/troubling/hummingbird/common/ring"
)

// ProxyClient is similar to github.com/troubling/nectar.Client except it also accepts an account parameter to its operations.  This is meant to be used by the proxy server.
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
	ContainerRing() ring.Ring
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
