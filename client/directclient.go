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
	client              *http.Client
	containerInfoSource ContainerInfoSource
	AccountRing         ring.Ring
	ContainerRing       ring.Ring
}

// ContainerInfo is persisted in memcache via JSON; so this needs to continue to have public fields.
type ContainerInfo struct {
	ObjectCount        int64
	ObjectBytes        int64
	Metadata           map[string]string
	SysMetadata        map[string]string
	StoragePolicyIndex int
}

type ContainerInfoSource interface {
	GetContainerInfo(account, container string) *ContainerInfo
}

func NewProxyDirectClient(cis ContainerInfoSource) (ProxyClient, error) {
	if cis == nil {
		return nil, errors.New("nil ContainerInfoSource")
	}
	c := &ProxyDirectClient{containerInfoSource: cis}
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
	c.client = &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 5 * time.Second,
			}).Dial,
		},
		Timeout: 120 * time.Minute,
	}
	return c, nil
}

func (c *ProxyDirectClient) quorumResponse(reqs ...*http.Request) int {
	// this is based on swift's best_response function.
	statusCodes := make(chan int)
	cancel := make(chan struct{})
	defer close(cancel)
	for _, req := range reqs {
		go func(req *http.Request) {
			status := 500
			if resp, err := c.client.Do(req); err == nil {
				status = resp.StatusCode
				resp.Body.Close()
			}
			select {
			case statusCodes <- status:
			case <-cancel:
			}
		}(req)
	}
	quorum := int(math.Ceil(float64(len(reqs)) / 2.0))
	responseClasses := []int{0, 0, 0, 0, 0, 0}
	for status := range statusCodes {
		class := status / 100
		if class <= 5 {
			responseClasses[class]++
			if responseClasses[class] >= quorum {
				return status
			}
		}
	}
	return 503
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
			if resp != nil && resp.StatusCode/100 == 2 {
				return resp
			}
		case <-time.After(time.Second):
		}
	}
	return nil
}

var _ ProxyClient = &ProxyDirectClient{}

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
	return c.quorumResponse(reqs...)
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
	return c.quorumResponse(reqs...)
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
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PutContainer(account string, container string, headers http.Header) int {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	policyList := conf.LoadPolicies()
	policyIndex := -1
	policyDefault := policyList.Default()
	policyName := headers.Get("X-Storage-Policy")
	if policyName != "" {
		policy := policyList.ByName(policyName)
		if policy == nil || policy.Deprecated {
			return 400
		}
		policyIndex = policy.Index
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
	return c.quorumResponse(reqs...)
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
	return c.quorumResponse(reqs...)
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
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader) int {
	oc, status := c.objectClient(account, container)
	if status != 0 {
		return status
	}
	return oc.putObject(obj, headers, src)
}

func (c *ProxyDirectClient) PostObject(account string, container string, obj string, headers http.Header) int {
	oc, status := c.objectClient(account, container)
	if status != 0 {
		return status
	}
	return oc.postObject(obj, headers)
}

func (c *ProxyDirectClient) GrepObject(account string, container string, obj string, search string) (io.ReadCloser, http.Header, int) {
	oc, status := c.objectClient(account, container)
	if status != 0 {
		return nil, nil, status
	}
	return oc.grepObject(obj, search)
}

func (c *ProxyDirectClient) GetObject(account string, container string, obj string, headers http.Header) (io.ReadCloser, http.Header, int) {
	oc, status := c.objectClient(account, container)
	if status != 0 {
		return nil, nil, status
	}
	return oc.getObject(obj, headers)
}

func (c *ProxyDirectClient) HeadObject(account string, container string, obj string, headers http.Header) (http.Header, int) {
	oc, status := c.objectClient(account, container)
	if status != 0 {
		return nil, status
	}
	return oc.headObject(obj, headers)
}

func (c *ProxyDirectClient) DeleteObject(account string, container string, obj string, headers http.Header) int {
	oc, status := c.objectClient(account, container)
	if status != 0 {
		return status
	}
	return oc.deleteObject(obj, headers)
}

func (c *ProxyDirectClient) objectClient(account string, container string) (proxyObjectClient, int) {
	ci := c.containerInfoSource.GetContainerInfo(account, container)
	if ci == nil {
		return nil, 500
	}
	return newStandardObjectClient(c, account, container, ci.StoragePolicyIndex)
}

type proxyObjectClient interface {
	putObject(obj string, headers http.Header, src io.Reader) int
	postObject(obj string, headers http.Header) int
	getObject(obj string, headers http.Header) (io.ReadCloser, http.Header, int)
	grepObject(obj string, search string) (io.ReadCloser, http.Header, int)
	headObject(obj string, headers http.Header) (http.Header, int)
	deleteObject(obj string, headers http.Header) int
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
	return &standardObjectClient{proxyDirectClient: proxyDirectClient, account: account, container: container, policy: policy, objectRing: objectRing}, 0
}

func (oc *standardObjectClient) putObject(obj string, headers http.Header, src io.Reader) int {
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
		mw := io.MultiWriter(writers[0], writers[1], writers[2])
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
	return oc.proxyDirectClient.quorumResponse(reqs...)
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
	return oc.proxyDirectClient.quorumResponse(reqs...)
}

type directClient struct {
	*ProxyDirectClient
	account string
}

var _ Client = &directClient{}

func (c *directClient) PutAccount(headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PutAccount(c.account, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostAccount(headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PostAccount(c.account, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ContainerRecord, map[string]string, error) {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	r, h, code := c.ProxyDirectClient.GetAccount(c.account, options, common.Map2Headers(headers))
	if code != 200 {
		return nil, nil, HTTPError(code)
	}
	var accountListing []ContainerRecord
	decoder := json.NewDecoder(r)
	decoder.Decode(&accountListing)
	return accountListing, common.Headers2Map(h), nil
}

func (c *directClient) HeadAccount(headers map[string]string) (map[string]string, error) {
	h, code := c.ProxyDirectClient.HeadAccount(c.account, common.Map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) DeleteAccount(headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.DeleteAccount(c.account, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PutContainer(container string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PutContainer(c.account, container, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostContainer(container string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PostContainer(c.account, container, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ObjectRecord, map[string]string, error) {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	r, h, code := c.ProxyDirectClient.GetContainer(c.account, container, options, common.Map2Headers(headers))
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
	h, code := c.ProxyDirectClient.HeadContainer(c.account, container, common.Map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) DeleteContainer(container string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.DeleteContainer(c.account, container, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) (err error) {
	if code := c.ProxyDirectClient.PutObject(c.account, container, obj, common.Map2Headers(headers), src); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostObject(container string, obj string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PostObject(c.account, container, obj, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetObject(container string, obj string, headers map[string]string) (io.ReadCloser, map[string]string, error) {
	r, h, code := c.ProxyDirectClient.GetObject(c.account, container, obj, common.Map2Headers(headers))
	if code/100 != 2 {
		if r != nil {
			r.Close()
		}
		return nil, nil, HTTPError(code)
	}
	return r, common.Headers2Map(h), nil
}

func (c *directClient) HeadObject(container string, obj string, headers map[string]string) (map[string]string, error) {
	h, code := c.ProxyDirectClient.HeadObject(c.account, container, obj, common.Map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return common.Headers2Map(h), nil
}

func (c *directClient) DeleteObject(container string, obj string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.DeleteObject(c.account, container, obj, common.Map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

// NewDirectClient creates a new direct client with the given account name.
func NewDirectClient(cis ContainerInfoSource, account string) (Client, error) {
	rdc, err := NewProxyDirectClient(cis)
	if err != nil {
		return nil, err
	}
	return &directClient{account: account, ProxyDirectClient: rdc.(*ProxyDirectClient)}, nil
}
