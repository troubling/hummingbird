package client

import (
	"io"
	"net/http"

	"context"

	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
)

// RequestClient is similar to github.com/troubling/nectar.Client, but its calls accept a context and it is scoped to a specific API request.
type RequestClient interface {
	PutAccount(ctx context.Context, account string, headers http.Header) *http.Response
	PostAccount(ctx context.Context, account string, headers http.Header) *http.Response
	GetAccountRaw(ctx context.Context, account string, options map[string]string, headers http.Header) *http.Response
	HeadAccount(ctx context.Context, account string, headers http.Header) *http.Response
	DeleteAccount(ctx context.Context, account string, headers http.Header) *http.Response
	PutContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response
	PostContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response
	GetContainerRaw(ctx context.Context, account string, container string, options map[string]string, headers http.Header) *http.Response
	GetContainerInfo(ctx context.Context, account string, container string) (*ContainerInfo, error)
	SetContainerInfo(ctx context.Context, account string, container string, resp *http.Response) (*ContainerInfo, error)
	HeadContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response
	DeleteContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response
	PutObject(ctx context.Context, account string, container string, obj string, headers http.Header, src io.Reader) *http.Response
	PostObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response
	GetObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response
	HeadObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response
	DeleteObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response
	// ObjectRingFor returns the object ring for the given account/container or
	// a response as to why the ring could not be returned.
	ObjectRingFor(ctx context.Context, account string, container string) (ring.Ring, *http.Response)
	ContainerRing() ring.Ring
}

// ProxyClient is the factory for RequestClients, and manages any persistent/shared client resources.
type ProxyClient interface {
	NewRequestClient(mc ring.MemcacheRing, lc map[string]*ContainerInfo, logger srv.LowLevelLogger) RequestClient
	Close() error
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
