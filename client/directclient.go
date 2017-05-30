package client

import (
	"encoding/json"
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

const PostQuorumTimeoutMs = 50

func mkquery(options map[string]string) string {
	query := ""
	for k, v := range options {
		if v != "" {
			query += url.QueryEscape(k) + "=" + url.QueryEscape(v) + "&"
		}
	}
	if query != "" {
		return "?" + strings.TrimRight(query, "&")
	}
	return ""
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

type QuorumResponseEntry struct {
	StatusCode int
	Headers    http.Header
}

func (c *ProxyDirectClient) quorumResponse(reqs ...*http.Request) (http.Header, int) {
	// this is based on swift's best_response function.
	responses := make(chan *QuorumResponseEntry)
	cancel := make(chan struct{})
	defer close(cancel)
	for _, req := range reqs {
		go func(req *http.Request) {
			entry := &QuorumResponseEntry{StatusCode: 500}
			if resp, err := c.client.Do(req); err == nil {
				entry.StatusCode = resp.StatusCode
				entry.Headers = resp.Header
				resp.Body.Close()
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
	var chosenResponse *QuorumResponseEntry
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
	// give any pending requests *some* chance to finish
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
	if chosenResponse != nil {
		return chosenResponse.Headers, chosenResponse.StatusCode
	}
	return nil, 503
}

func (c *ProxyDirectClient) firstResponse(reqs ...*http.Request) (resp *http.Response) {
	success := make(chan *http.Response)
	returned := make(chan struct{})
	defer close(returned)

	for _, req := range reqs {
		go func(r *http.Request) {
			cancel := make(chan struct{})
			r.Cancel = cancel
			response, err := c.client.Do(r)
			if err != nil {
				response = nil
			}
			select {
			case success <- response:
			case <-returned:
				close(cancel)
			}
		}(req)

		select {
		case resp = <-success:
			if resp != nil && (resp.StatusCode/100 == 2 || resp.StatusCode == 304 || resp.StatusCode == 416) {
				return resp
			}
		case <-time.After(time.Second):
		}
	}
	return nil
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

func (c *proxyClient) PutAccount(account string, headers http.Header) int {
	return c.pdc.PutAccount(account, headers)
}
func (c *proxyClient) PostAccount(account string, headers http.Header) int {
	return c.pdc.PostAccount(account, headers)
}
func (c *proxyClient) GetAccount(account string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int) {
	return c.pdc.GetAccount(account, options, headers)
}
func (c *proxyClient) HeadAccount(account string, headers http.Header) (http.Header, int) {
	return c.pdc.HeadAccount(account, headers)
}
func (c *proxyClient) DeleteAccount(account string, headers http.Header) int {
	return c.pdc.DeleteAccount(account, headers)
}
func (c *proxyClient) PutContainer(account string, container string, headers http.Header) int {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.PutContainer(account, container, headers)
}
func (c *proxyClient) PostContainer(account string, container string, headers http.Header) int {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.PostContainer(account, container, headers)
}
func (c *proxyClient) GetContainer(account string, container string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int) {
	return c.pdc.GetContainer(account, container, options, headers)
}
func (c *proxyClient) GetContainerInfo(account string, container string) *ContainerInfo {
	return c.pdc.GetContainerInfo(account, container, c.mc, c.lc)
}
func (c *proxyClient) HeadContainer(account string, container string, headers http.Header) (http.Header, int) {
	return c.pdc.HeadContainer(account, container, headers)
}
func (c *proxyClient) DeleteContainer(account string, container string, headers http.Header) int {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.DeleteContainer(account, container, headers)
}
func (c *proxyClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader) (http.Header, int) {
	return c.pdc.PutObject(account, container, obj, headers, src, c.mc, c.lc)
}
func (c *proxyClient) PostObject(account string, container string, obj string, headers http.Header) int {
	return c.pdc.PostObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) GetObject(account string, container string, obj string, headers http.Header) (io.ReadCloser, http.Header, int) {
	return c.pdc.GetObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) HeadObject(account string, container string, obj string, headers http.Header) (http.Header, int) {
	return c.pdc.HeadObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) DeleteObject(account string, container string, obj string, headers http.Header) int {
	return c.pdc.DeleteObject(account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) ObjectRingFor(account string, container string) (ring.Ring, int) {
	return c.pdc.ObjectRingFor(account, container, c.mc, c.lc)
}

func (c *ProxyDirectClient) PutAccount(account string, headers http.Header) int {
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
	headers, statusCode := c.quorumResponse(reqs...)
	return statusCode
}

func (c *ProxyDirectClient) PostAccount(account string, headers http.Header) int {
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
	headers, statusCode := c.quorumResponse(reqs...)
	return statusCode
}

func (c *ProxyDirectClient) GetAccount(account string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int) {
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
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) HeadAccount(account string, headers http.Header) (http.Header, int) {
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
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, 404
	}
	resp.Body.Close()
	return resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) DeleteAccount(account string, headers http.Header) int {
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
	headers, statusCode := c.quorumResponse(reqs...)
	return statusCode
}

func (c *ProxyDirectClient) PutContainer(account string, container string, headers http.Header) int {
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
		if policy == nil || policy.Deprecated {
			return 400
		}
		policyIndex = policy.Index
	}
	if policyIndex < 0 {
		policyIndex = policyDefault
	}
	reqs := make([]*http.Request, 0)
	for i, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, _ := http.NewRequest("PUT", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		req.Header.Set("X-Account-Host", fmt.Sprintf("%s:%d", accountDevices[i].Ip, accountDevices[i].Port))
		req.Header.Set("X-Account-Device", accountDevices[i].Device)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policyIndex))
		req.Header.Set("X-Backend-Storage-Policy-Default", strconv.Itoa(policyDefault))
		reqs = append(reqs, req)
	}
	headers, statusCode := c.quorumResponse(reqs...)
	return statusCode
}

func (c *ProxyDirectClient) PostContainer(account string, container string, headers http.Header) int {
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
	headers, statusCode := c.quorumResponse(reqs...)
	return statusCode
}

func (c *ProxyDirectClient) GetContainer(account string, container string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int) {
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
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) GetContainerInfo(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *ContainerInfo {
	var err error
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
		headers, code := c.HeadContainer(account, container, nil)
		if code/100 != 2 {
			return nil
		}
		ci = &ContainerInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		if ci.ObjectCount, err = strconv.ParseInt(headers.Get("X-Container-Object-Count"), 10, 64); err != nil {
			return nil
		}
		if ci.ObjectBytes, err = strconv.ParseInt(headers.Get("X-Container-Bytes-Used"), 10, 64); err != nil {
			return nil
		}
		if ci.StoragePolicyIndex, err = strconv.Atoi(headers.Get("X-Backend-Storage-Policy-Index")); err != nil {
			return nil
		}
		for k := range headers {
			if strings.HasPrefix(k, "X-Container-Meta-") {
				ci.Metadata[k[17:]] = headers.Get(k)
			} else if strings.HasPrefix(k, "X-Container-Sysmeta-") {
				ci.SysMetadata[k[20:]] = headers.Get(k)
			}
		}
		if mc != nil {
			mc.Set(key, ci, 30)
		}
	}
	return ci
}

func (c *ProxyDirectClient) HeadContainer(account string, container string, headers http.Header) (http.Header, int) {
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
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, 404
	}
	resp.Body.Close()
	return resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) DeleteContainer(account string, container string, headers http.Header) int {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	reqs := make([]*http.Request, 0)
	for i, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		req.Header.Set("X-Account-Host", fmt.Sprintf("%s:%d", accountDevices[i].Ip, accountDevices[i].Port))
		req.Header.Set("X-Account-Device", accountDevices[i].Device)
		reqs = append(reqs, req)
	}
	headers, statusCode := c.quorumResponse(reqs...)
	return statusCode
}

func (c *ProxyDirectClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (http.Header, int) {
	oc, status := c.objectClient(account, container, mc, lc)
	if status != 200 {
		return nil, status
	}
	return oc.putObject(obj, headers, src)
}

func (c *ProxyDirectClient) PostObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) int {
	oc, status := c.objectClient(account, container, mc, lc)
	if status != 200 {
		return status
	}
	return oc.postObject(obj, headers)
}

func (c *ProxyDirectClient) GetObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (io.ReadCloser, http.Header, int) {
	oc, status := c.objectClient(account, container, mc, lc)
	if status != 200 {
		return nil, nil, status
	}
	return oc.getObject(obj, headers)
}

func (c *ProxyDirectClient) GrepObject(account string, container string, obj string, search string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (io.ReadCloser, http.Header, int) {
	oc, status := c.objectClient(account, container, mc, lc)
	if status != 200 {
		return nil, nil, status
	}
	return oc.grepObject(obj, search)
}

func (c *ProxyDirectClient) HeadObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (http.Header, int) {
	oc, status := c.objectClient(account, container, mc, lc)
	if status != 200 {
		return nil, status
	}
	return oc.headObject(obj, headers)
}

func (c *ProxyDirectClient) DeleteObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) int {
	oc, status := c.objectClient(account, container, mc, lc)
	if status != 200 {
		return status
	}
	return oc.deleteObject(obj, headers)
}

func (c *ProxyDirectClient) objectClient(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (proxyObjectClient, int) {
	ci := c.GetContainerInfo(account, container, mc, lc)
	if ci == nil {
		return nil, 500
	}
	return newStandardObjectClient(c, account, container, ci.StoragePolicyIndex)
}

func (c *ProxyDirectClient) ObjectRingFor(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (ring.Ring, int) {
	oc, status := c.objectClient(account, container, mc, lc)
	if oc != nil && status == 200 {
		return oc.ring(), status
	}
	return nil, status
}

type proxyObjectClient interface {
	putObject(obj string, headers http.Header, src io.Reader) (http.Header, int)
	postObject(obj string, headers http.Header) int
	getObject(obj string, headers http.Header) (io.ReadCloser, http.Header, int)
	grepObject(obj string, search string) (io.ReadCloser, http.Header, int)
	headObject(obj string, headers http.Header) (http.Header, int)
	deleteObject(obj string, headers http.Header) int
	ring() ring.Ring
}

type standardObjectClient struct {
	proxyDirectClient *ProxyDirectClient
	account           string
	container         string
	policy            int
	objectRing        ring.Ring
}

func newStandardObjectClient(proxyDirectClient *ProxyDirectClient, account string, container string, policy int) (*standardObjectClient, int) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, 500
	}
	objectRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, policy)
	if err != nil {
		return nil, 500
	}
	return &standardObjectClient{proxyDirectClient: proxyDirectClient, account: account, container: container, policy: policy, objectRing: objectRing}, 200
}

func (oc *standardObjectClient) putObject(obj string, headers http.Header, src io.Reader) (http.Header, int) {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	var writers []*io.PipeWriter
	reqs := make([]*http.Request, 0)
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
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
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

func (oc *standardObjectClient) postObject(obj string, headers http.Header) int {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	reqs := make([]*http.Request, 0)
	for i, device := range oc.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		reqs = append(reqs, req)
	}
	headers, statusCode := oc.proxyDirectClient.quorumResponse(reqs...)
	return statusCode
}

func (oc *standardObjectClient) getObject(obj string, headers http.Header) (io.ReadCloser, http.Header, int) {
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
	resp := oc.proxyDirectClient.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (oc *standardObjectClient) grepObject(obj string, search string) (io.ReadCloser, http.Header, int) {
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
	resp := oc.proxyDirectClient.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (oc *standardObjectClient) headObject(obj string, headers http.Header) (http.Header, int) {
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
	resp := oc.proxyDirectClient.firstResponse(reqs...)
	if resp == nil {
		return nil, 404
	}
	resp.Body.Close()
	return resp.Header, resp.StatusCode
}

func (oc *standardObjectClient) deleteObject(obj string, headers http.Header) int {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	reqs := make([]*http.Request, 0)
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
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		reqs = append(reqs, req)
	}
	headers, statusCode := oc.proxyDirectClient.quorumResponse(reqs...)
	return statusCode
}

func (oc *standardObjectClient) ring() ring.Ring {
	return oc.objectRing
}

type directClient struct {
	pc      ProxyClient
	account string
}

var _ Client = &directClient{}

func (c *directClient) PutAccount(headers map[string]string) (err error) {
	if code := c.pc.PutAccount(c.account, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostAccount(headers map[string]string) (err error) {
	if code := c.pc.PostAccount(c.account, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse string, headers map[string]string) ([]ContainerRecord, map[string]string, error) {
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
	r, h, code := c.pc.GetAccount(c.account, options, common.Map2Headers(headers))
	if code != 200 {
		return nil, nil, HTTPError(code)
	}
	var accountListing []ContainerRecord
	decoder := json.NewDecoder(r)
	decoder.Decode(&accountListing)
	return accountListing, common.Headers2Map(h), nil
}

func (c *directClient) HeadAccount(headers map[string]string) (map[string]string, error) {
	h, code := c.pc.HeadAccount(c.account, common.Map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) DeleteAccount(headers map[string]string) (err error) {
	if code := c.pc.DeleteAccount(c.account, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PutContainer(container string, headers map[string]string) (err error) {
	if code := c.pc.PutContainer(c.account, container, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostContainer(container string, headers map[string]string) (err error) {
	if code := c.pc.PostContainer(c.account, container, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse string, headers map[string]string) ([]ObjectRecord, map[string]string, error) {
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
	r, h, code := c.pc.GetContainer(c.account, container, options, common.Map2Headers(headers))
	if code != 200 {
		return nil, nil, HTTPError(code)
	}
	defer r.Close()
	var containerListing []ObjectRecord
	decoder := json.NewDecoder(r)
	decoder.Decode(&containerListing)
	return containerListing, common.Headers2Map(h), nil
}

func (c *directClient) HeadContainer(container string, headers map[string]string) (map[string]string, error) {
	h, code := c.pc.HeadContainer(c.account, container, common.Map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) DeleteContainer(container string, headers map[string]string) (err error) {
	if code := c.pc.DeleteContainer(c.account, container, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) (map[string]string, error) {
	h, code := c.pc.PutObject(c.account, container, obj, common.Map2Headers(headers), src)
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) PostObject(container string, obj string, headers map[string]string) (err error) {
	if code := c.pc.PostObject(c.account, container, obj, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetObject(container string, obj string, headers map[string]string) (io.ReadCloser, map[string]string, error) {
	r, h, code := c.pc.GetObject(c.account, container, obj, common.Map2Headers(headers))
	if code/100 != 2 {
		if r != nil {
			r.Close()
		}
		return nil, nil, HTTPError(code)
	}
	return r, common.Headers2Map(h), nil
}

func (c *directClient) HeadObject(container string, obj string, headers map[string]string) (map[string]string, error) {
	h, code := c.pc.HeadObject(c.account, container, obj, common.Map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) DeleteObject(container string, obj string, headers map[string]string) (err error) {
	if code := c.pc.DeleteObject(c.account, container, obj, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

// NewDirectClient creates a new direct client with the given account name.
func NewDirectClient(account string) (Client, error) {
	pdc, err := NewProxyDirectClient(nil)
	if err != nil {
		return nil, err
	}
	return &directClient{account: account, pc: NewProxyClient(pdc, nil, nil)}, nil
}
