package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
)

const PostQuorumTimeoutMs = 100

func mkquery(options map[string]string) string {
	query := ""
	for k, v := range options {
		query += url.QueryEscape(k) + "=" + url.QueryEscape(v) + "&"
	}
	if query != "" {
		return "?" + strings.TrimRight(query, "&")
	}
	return ""
}

func addUpdateHeaders(prefix string, headers http.Header, devices []*ring.Device, i, replicas int) {
	if i < len(devices) {
		host := ""
		device := ""
		for ; i < len(devices); i += replicas {
			host += fmt.Sprintf("%s:%d,", devices[i].Ip, devices[i].Port)
			device += devices[i].Device + ","
		}
		headers.Set(prefix+"-Host", strings.TrimRight(host, ","))
		headers.Set(prefix+"-Device", strings.TrimRight(device, ","))
	}
}

type ProxyDirectClient struct {
	policyList    conf.PolicyList
	client        *http.Client
	AccountRing   ring.Ring
	ContainerRing ring.Ring
}

func NewProxyDirectClient(policyList conf.PolicyList) (*ProxyDirectClient, error) {
	c := &ProxyDirectClient{
		policyList: policyList,
		client: &http.Client{
			Transport: &http.Transport{
				DisableCompression: true,
				Dial: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 5 * time.Second,
				}).Dial,
			},
			Timeout: 120 * time.Minute,
		},
	}
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}
	c.ContainerRing, err = ring.GetRing("container", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return nil, err
	}
	c.AccountRing, err = ring.GetRing("account", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ProxyDirectClient) quorumResponse(reqs ...*http.Request) *http.Response {
	// this is based on swift's best_response function.
	responses := make(chan *http.Response)
	cancel := make(chan struct{})
	defer close(cancel)
	for _, req := range reqs {
		go func(req *http.Request) {
			var entry *http.Response
			if resp, err := c.client.Do(req); err != nil {
				entry = ResponseStub(http.StatusInternalServerError, err.Error())
			} else {
				entry = StubResponse(resp)
			}
			select {
			case responses <- entry:
			case <-cancel:
			}
		}(req)
	}
	quorum := int(math.Ceil(float64(len(reqs)) / 2.0))
	responseClasses := []int{0, 0, 0, 0, 0, 0}
	responseCount := 0
	var chosenResponse *http.Response
	for response := range responses {
		responseCount++
		class := response.StatusCode / 100
		if class <= 5 {
			responseClasses[class]++
			if responseClasses[class] >= quorum {
				chosenResponse = response
				break
			}
		}
	}
	// Give any pending requests *some* chance to finish. This will increase
	// the likelihood that a read immediately after a write will get the latest
	// information, as we'll force the caller to wait until all backends have
	// responded, with a timeout limit.
	timeout := time.After(PostQuorumTimeoutMs * time.Millisecond)
waiting:
	for responseCount < len(reqs) {
		select {
		case <-responses:
			responseCount++
		case <-timeout:
			break waiting
		}
	}
	return chosenResponse
}

func (c *ProxyDirectClient) firstResponse(reqs ...*http.Request) (resp *http.Response) {
	success := make(chan *http.Response)
	returned := make(chan struct{})
	defer close(returned)

	for _, req := range reqs {
		go func(r *http.Request) {
			response, err := c.client.Do(r)
			if err != nil {
				response = nil
			}
			select {
			case success <- response:
			case <-returned:
				if response != nil {
					response.Body.Close()
				}
			}
		}(req)

		select {
		case resp = <-success:
			if resp != nil && (resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusPreconditionFailed || resp.StatusCode == http.StatusNotModified || resp.StatusCode == http.StatusRequestedRangeNotSatisfiable) {
				resp = StubResponse(resp)
				resp.Header.Set("Accept-Ranges", "bytes")
				if etag := resp.Header.Get("Etag"); etag != "" {
					resp.Header.Set("Etag", strings.Trim(etag, "\""))
				}
				return resp
			}
		case <-time.After(time.Second):
		}
	}
	return ResponseStub(http.StatusNotFound, "")
}

type proxyClient struct {
	pdc *ProxyDirectClient
	mc  ring.MemcacheRing
	lc  map[string]*ContainerInfo
}

var _ ProxyClient = &proxyClient{}

func NewProxyClient(pdc *ProxyDirectClient, mc ring.MemcacheRing, lc map[string]*ContainerInfo) ProxyClient {
	return &proxyClient{pdc: pdc, mc: mc, lc: lc}
}

func (c *proxyClient) invalidateContainerInfo(account string, container string) {
	key := fmt.Sprintf("container/%s/%s", account, container)
	if c.lc != nil {
		delete(c.lc, key)
	}
	if c.mc != nil {
		c.mc.Delete(key)
	}
}

func (c *proxyClient) PutAccount(account string, headers http.Header) *http.Response {
	return c.pdc.PutAccount(account, headers)
}
func (c *proxyClient) PostAccount(account string, headers http.Header) *http.Response {
	return c.pdc.PostAccount(account, headers)
}
func (c *proxyClient) GetAccount(account string, options map[string]string, headers http.Header) *http.Response {
	return c.pdc.GetAccount(account, options, headers)
}
func (c *proxyClient) HeadAccount(account string, headers http.Header) *http.Response {
	return c.pdc.HeadAccount(account, headers)
}
func (c *proxyClient) DeleteAccount(account string, headers http.Header) *http.Response {
	return c.pdc.DeleteAccount(account, headers)
}
func (c *proxyClient) PutContainer(account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.PutContainer(account, container, headers)
}
func (c *proxyClient) PostContainer(account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.PostContainer(account, container, headers)
}
func (c *proxyClient) GetContainer(account string, container string, options map[string]string, headers http.Header) *http.Response {
	return c.pdc.GetContainer(account, container, options, headers)
}
func (c *proxyClient) GetContainerInfo(account string, container string) (*ContainerInfo, error) {
	return c.pdc.GetContainerInfo(account, container, c.mc, c.lc)
}
func (c *proxyClient) HeadContainer(account string, container string, headers http.Header) *http.Response {
	return c.pdc.HeadContainer(account, container, headers)
}
func (c *proxyClient) DeleteContainer(account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.DeleteContainer(account, container, headers)
}
func (c *proxyClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader) *http.Response {
	return c.pdc.PutObject(account, container, obj, headers, src, c.mc, c.lc)
}
func (c *proxyClient) PostObject(account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.PostObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) GetObject(account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.GetObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) HeadObject(account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.HeadObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) DeleteObject(account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.DeleteObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) ObjectRingFor(account string, container string) (ring.Ring, *http.Response) {
	return c.pdc.ObjectRingFor(account, container, c.mc, c.lc)
}

func (c *ProxyDirectClient) PutAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, common.Urlencode(account))
		req, _ := http.NewRequest("PUT", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PostAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, common.Urlencode(account))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) GetAccount(account string, options map[string]string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	query := mkquery(options)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), query)
		req, _ := http.NewRequest("GET", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.firstResponse(reqs...)
}

func (c *ProxyDirectClient) HeadAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.firstResponse(reqs...)
}

func (c *ProxyDirectClient) DeleteAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, common.Urlencode(account))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PutContainer(account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	policyIndex := -1
	if c.policyList == nil {
		c.policyList = conf.LoadPolicies()
	}
	policyDefault := c.policyList.Default()
	policyName := headers.Get("X-Storage-Policy")
	if policyName != "" {
		var policy *conf.Policy
		for _, v := range c.policyList {
			if v.Name == policyName {
				policy = v
				break
			}
		}
		if policy == nil {
			return ResponseStub(http.StatusBadRequest, fmt.Sprintf("Invalid X-Storage-Policy %q", policyName))
		}
		if policy.Deprecated {
			return ResponseStub(http.StatusBadRequest, fmt.Sprintf("Storage Policy %q is deprecated", policyName))
		}
		policyIndex = policy.Index
	}
	reqs := make([]*http.Request, 0)
	containerReplicaCount := int(c.ContainerRing.ReplicaCount())
	for i, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, _ := http.NewRequest("PUT", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policyIndex))
		req.Header.Set("X-Backend-Storage-Policy-Default", strconv.Itoa(policyDefault))
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PostContainer(account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) GetContainer(account string, container string, options map[string]string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	reqs := make([]*http.Request, 0)
	query := mkquery(options)
	for _, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container), query)
		req, _ := http.NewRequest("GET", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.firstResponse(reqs...)
}

// NilContainerInfo is useful for testing.
var NilContainerInfo = &ContainerInfo{}

func (c *ProxyDirectClient) GetContainerInfo(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (*ContainerInfo, error) {
	key := fmt.Sprintf("container/%s/%s", account, container)
	var ci *ContainerInfo
	if lc != nil {
		ci = lc[key]
	}
	if ci == nil && mc != nil {
		if err := mc.GetStructured(key, &ci); err != nil {
			ci = nil
		}
	}
	if ci == nil {
		resp := c.HeadContainer(account, container, nil)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("%d error retrieving info for container %s/%s", resp.StatusCode, account, container)
		}
		ci = &ContainerInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		var err error
		if ci.ObjectCount, err = strconv.ParseInt(resp.Header.Get("X-Container-Object-Count"), 10, 64); err != nil {
			return nil, fmt.Errorf("Error retrieving info for container %s/%s : %s", account, container, err)
		}
		if ci.ObjectBytes, err = strconv.ParseInt(resp.Header.Get("X-Container-Bytes-Used"), 10, 64); err != nil {
			return nil, fmt.Errorf("Error retrieving info for container %s/%s : %s", account, container, err)
		}
		if ci.StoragePolicyIndex, err = strconv.Atoi(resp.Header.Get("X-Backend-Storage-Policy-Index")); err != nil {
			return nil, fmt.Errorf("Error retrieving info for container %s/%s : %s", account, container, err)
		}
		for k := range resp.Header {
			if strings.HasPrefix(k, "X-Container-Meta-") {
				ci.Metadata[k[17:]] = resp.Header.Get(k)
			} else if strings.HasPrefix(k, "X-Container-Sysmeta-") {
				ci.SysMetadata[k[20:]] = resp.Header.Get(k)
			} else if k == "X-Container-Read" {
				ci.ReadACL = resp.Header.Get(k)
			} else if k == "X-Container-Write" {
				ci.WriteACL = resp.Header.Get(k)
			} else if k == "X-Container-Sync-Key" {
				ci.SyncKey = resp.Header.Get(k)
			}
		}
		if mc != nil {
			mc.Set(key, ci, 30)
		}
	}
	if ci == NilContainerInfo {
		return nil, errors.New("No container info for testing")
	}
	return ci, nil
}

func (c *ProxyDirectClient) HeadContainer(account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.firstResponse(reqs...)
}

func (c *ProxyDirectClient) DeleteContainer(account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	reqs := make([]*http.Request, 0)
	containerReplicaCount := int(c.ContainerRing.ReplicaCount())
	for i, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return newObjectClient(c, account, container, mc, lc).putObject(obj, headers, src)
}

func (c *ProxyDirectClient) PostObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return newObjectClient(c, account, container, mc, lc).postObject(obj, headers)
}

func (c *ProxyDirectClient) GetObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return newObjectClient(c, account, container, mc, lc).getObject(obj, headers)
}

func (c *ProxyDirectClient) GrepObject(account string, container string, obj string, search string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return newObjectClient(c, account, container, mc, lc).grepObject(obj, search)
}

func (c *ProxyDirectClient) HeadObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return newObjectClient(c, account, container, mc, lc).headObject(obj, headers)
}

func (c *ProxyDirectClient) DeleteObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return newObjectClient(c, account, container, mc, lc).deleteObject(obj, headers)
}

func (c *ProxyDirectClient) ObjectRingFor(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (ring.Ring, *http.Response) {
	return newObjectClient(c, account, container, mc, lc).ring()
}

type proxyObjectClient interface {
	putObject(obj string, headers http.Header, src io.Reader) *http.Response
	postObject(obj string, headers http.Header) *http.Response
	getObject(obj string, headers http.Header) *http.Response
	grepObject(obj string, search string) *http.Response
	headObject(obj string, headers http.Header) *http.Response
	deleteObject(obj string, headers http.Header) *http.Response
	ring() (ring.Ring, *http.Response)
}

type erroringObjectClient struct {
	body string
}

func (oc *erroringObjectClient) putObject(obj string, headers http.Header, src io.Reader) *http.Response {
	return ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) postObject(obj string, headers http.Header) *http.Response {
	return ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) getObject(obj string, headers http.Header) *http.Response {
	return ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) grepObject(obj string, search string) *http.Response {
	return ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) headObject(obj string, headers http.Header) *http.Response {
	return ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) deleteObject(obj string, headers http.Header) *http.Response {
	return ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) ring() (ring.Ring, *http.Response) {
	return nil, ResponseStub(http.StatusInternalServerError, oc.body)
}

type standardObjectClient struct {
	proxyDirectClient *ProxyDirectClient
	account           string
	container         string
	policy            int
	objectRing        ring.Ring
}

func newObjectClient(proxyDirectClient *ProxyDirectClient, account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) proxyObjectClient {
	ci, err := proxyDirectClient.GetContainerInfo(account, container, mc, lc)
	if err != nil {
		return &erroringObjectClient{body: "Could not retrieve container information."}
	}
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return &erroringObjectClient{body: "Could not retrieve hash path prefix and suffix."}
	}
	objectRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, ci.StoragePolicyIndex)
	if err != nil {
		return &erroringObjectClient{body: fmt.Sprintf("Could not load object ring for policy %d.", ci.StoragePolicyIndex)}
	}
	return &standardObjectClient{proxyDirectClient: proxyDirectClient, account: account, container: container, policy: ci.StoragePolicyIndex, objectRing: objectRing}
}

func (oc *standardObjectClient) putObject(obj string, headers http.Header, src io.Reader) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	var writers []*io.PipeWriter
	reqs := make([]*http.Request, 0)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	for i, device := range oc.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		rp, wp := io.Pipe()
		defer wp.Close()
		defer rp.Close()
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			continue
		}
		writers = append(writers, wp)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		// req.ContentLength = request.ContentLength // TODO
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, i, objectReplicaCount)
		req.Header.Set("Expect", "100-Continue")
		reqs = append(reqs, req)
	}
	go func() {
		// TODO: Need to change up this code because MultiWriter will stop everything on any error.
		ws := make([]io.Writer, len(writers))
		for i, w := range writers {
			ws[i] = w
		}
		mw := io.MultiWriter(ws...)
		io.Copy(mw, src)
		for _, writer := range writers {
			writer.Close()
		}
	}()
	return oc.proxyDirectClient.quorumResponse(reqs...)
}

func (oc *standardObjectClient) postObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	reqs := make([]*http.Request, 0)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	for i, device := range oc.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, i, objectReplicaCount)
		reqs = append(reqs, req)
	}
	return oc.proxyDirectClient.quorumResponse(reqs...)
}

func (oc *standardObjectClient) getObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	nodes := oc.objectRing.GetNodes(partition)
	reqs := make([]*http.Request, 0, len(nodes))
	for _, device := range nodes {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		reqs = append(reqs, req)
	}
	return oc.proxyDirectClient.firstResponse(reqs...)
}

func (oc *standardObjectClient) grepObject(obj string, search string) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	nodes := oc.objectRing.GetNodes(partition)
	reqs := make([]*http.Request, 0, len(nodes))
	for _, device := range nodes {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s?e=%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj), common.Urlencode(search))
		req, err := http.NewRequest("GREP", url, nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		reqs = append(reqs, req)
	}
	return oc.proxyDirectClient.firstResponse(reqs...)
}

func (oc *standardObjectClient) headObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	nodes := oc.objectRing.GetNodes(partition)
	reqs := make([]*http.Request, 0, len(nodes))
	for _, device := range nodes {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		reqs = append(reqs, req)
	}
	return oc.proxyDirectClient.firstResponse(reqs...)
}

func (oc *standardObjectClient) deleteObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	reqs := make([]*http.Request, 0)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	for i, device := range oc.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, i, objectReplicaCount)
		reqs = append(reqs, req)
	}
	return oc.proxyDirectClient.quorumResponse(reqs...)
}

func (oc *standardObjectClient) ring() (ring.Ring, *http.Response) {
	return oc.objectRing, nil
}

type directClient struct {
	pc      ProxyClient
	account string
}

var _ Client = &directClient{}

func (c *directClient) PutAccount(headers map[string]string) *http.Response {
	return c.pc.PutAccount(c.account, common.Map2Headers(headers))
}

func (c *directClient) PostAccount(headers map[string]string) *http.Response {
	return c.pc.PostAccount(c.account, common.Map2Headers(headers))
}

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse string, headers map[string]string) ([]ContainerRecord, *http.Response) {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
		"reverse":    reverse,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	resp := c.pc.GetAccount(c.account, options, common.Map2Headers(headers))
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var accountListing []ContainerRecord
	if err := json.NewDecoder(resp.Body).Decode(&accountListing); err != nil {
		resp.Body.Close()
		return nil, ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	return accountListing, resp
}

func (c *directClient) HeadAccount(headers map[string]string) *http.Response {
	return c.pc.HeadAccount(c.account, common.Map2Headers(headers))
}

func (c *directClient) DeleteAccount(headers map[string]string) *http.Response {
	return c.pc.DeleteAccount(c.account, common.Map2Headers(headers))
}

func (c *directClient) PutContainer(container string, headers map[string]string) *http.Response {
	return c.pc.PutContainer(c.account, container, common.Map2Headers(headers))
}

func (c *directClient) PostContainer(container string, headers map[string]string) *http.Response {
	return c.pc.PostContainer(c.account, container, common.Map2Headers(headers))
}

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse string, headers map[string]string) ([]ObjectRecord, *http.Response) {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
		"reverse":    reverse,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	resp := c.pc.GetContainer(c.account, container, options, common.Map2Headers(headers))
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var containerListing []ObjectRecord
	if err := json.NewDecoder(resp.Body).Decode(&containerListing); err != nil {
		resp.Body.Close()
		return nil, ResponseStub(http.StatusInternalServerError, err.Error())
	}
	resp.Body.Close()
	return containerListing, resp
}

func (c *directClient) HeadContainer(container string, headers map[string]string) *http.Response {
	return c.pc.HeadContainer(c.account, container, common.Map2Headers(headers))
}

func (c *directClient) DeleteContainer(container string, headers map[string]string) *http.Response {
	return c.pc.DeleteContainer(c.account, container, common.Map2Headers(headers))
}

func (c *directClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) *http.Response {
	return c.pc.PutObject(c.account, container, obj, common.Map2Headers(headers), src)
}

func (c *directClient) PostObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.PostObject(c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) GetObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.GetObject(c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) HeadObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.HeadObject(c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) DeleteObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.DeleteObject(c.account, container, obj, common.Map2Headers(headers))
}

// NewDirectClient creates a new direct client with the given account name.
func NewDirectClient(account string) (Client, error) {
	pdc, err := NewProxyDirectClient(nil)
	if err != nil {
		return nil, err
	}
	return &directClient{account: account, pc: NewProxyClient(pdc, nil, nil)}, nil
}
