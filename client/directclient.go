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
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

const PostQuorumTimeoutMs = 100
const postPutTimeout = time.Second * 30

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

func NewProxyDirectClient(config *conf.Config, policyList conf.PolicyList, logger srv.LowLevelLogger) (*ProxyDirectClient, error) {
	var xport http.RoundTripper = &http.Transport{
		DisableCompression: true,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
		ExpectContinueTimeout: 10 * time.Minute, // TODO: this should probably be like infinity.
	}
	if config != nil {
		autoCloseSeconds := config.GetInt("debug", "debug_http_close", 0)
		if autoCloseSeconds != 0 {
			xport = &autoCloseResponses{RoundTripper: xport, autoCloseAfter: time.Duration(autoCloseSeconds) * time.Second}
			if logger != nil {
				logger.Info("AUTO CLOSING HTTP RESPONSES IS TURNED ON", zap.Int64("debug_http_close", autoCloseSeconds))
			}
		}
	}
	c := &ProxyDirectClient{
		policyList: policyList,
		client: &http.Client{
			Transport: xport,
			Timeout:   120 * time.Minute,
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

type respRec struct {
	i int
	r *http.Response
	d *ring.Device
}

// quorumer launches requests and does bookkeeping to target quorum on writes.
//
// I'm hoping that it's a good hook for implementing write affinity and if we end
// up having to log when an object can't be written to multiple regions or whatever.
type quorumer interface {
	start()
	addResponse(int, *http.Response)
	quorumMet(int) bool
	launchUntilQuorumPossible() bool
	getResponse(time.Duration) *http.Response
}

type stdQuorumer struct {
	q                   int
	makeRequest         func(int, *ring.Device) *http.Response
	devs                []*ring.Device
	more                ring.MoreNodes
	responses           []*http.Response
	failures            []int
	responseClassCounts []int
	requestCount        int
	replicaCount        int
	responsec           chan *respRec
	cancel              chan struct{}
}

func (q *stdQuorumer) launchRequest() bool {
	// step through primaries, then go to handoffs.
	launch := func(index int, dev *ring.Device) {
		resp := q.makeRequest(index, dev)
		select {
		case q.responsec <- &respRec{i: index, r: resp, d: dev}:
		case <-q.cancel:
		}
	}
	if q.requestCount < len(q.devs) {
		go launch(q.requestCount, q.devs[q.requestCount])
	} else {
		var index int // assign handoffs the index of a failed primary, so we send update headers.
		index, q.failures = q.failures[len(q.failures)-1], q.failures[:len(q.failures)-1]
		dev := q.more.Next()
		if dev == nil {
			return false
		}
		go launch(index, dev)
	}
	q.requestCount++
	return true
}

func (q *stdQuorumer) start() {
	for q.requestCount < q.replicaCount {
		q.launchRequest()
	}
}

func (q *stdQuorumer) addResponse(index int, resp *http.Response) {
	q.responses = append(q.responses, resp)
	if resp.StatusCode >= 500 || resp.StatusCode < 0 {
		q.failures = append(q.failures, index)
		return
	}
	q.responseClassCounts[resp.StatusCode/100]++
}

func (q *stdQuorumer) launchUntilQuorumPossible() bool {
	// if quorum has become impossible, launch requests until it's possible again.
	for q.requestCount < q.replicaCount*2 {
		for responseClass := 2; responseClass < 5; responseClass++ {
			if (q.responseClassCounts[responseClass]+q.requestCount)-len(q.responses) >= q.q {
				return true
			}
		}
		if !q.launchRequest() {
			break
		}
	}
	return false
}

func (q *stdQuorumer) quorumMet(writers int) bool {
	if writers >= q.replicaCount {
		return true
	}
	if writers >= q.q && (len(q.responses)+writers) >= q.replicaCount*2 {
		return true
	}
	for responseClass := 2; responseClass < 5; responseClass++ {
		if q.responseClassCounts[responseClass] >= q.q {
			return true
		}
	}
	return false
}

func (q *stdQuorumer) getResponse(timeout time.Duration) *http.Response {
	getResponseTimeout := time.After(timeout)
	for {
		// see if quorum has already been met
		for _, r := range q.responses {
			if q.responseClassCounts[r.StatusCode/100] >= q.q {
				// Give pending requests a chance to finish, to improve consistency of read-after-write.
				finalizeTimeout := time.After(PostQuorumTimeoutMs * time.Millisecond)
				for q.requestCount > len(q.responses) {
					select {
					case resp := <-q.responsec:
						q.addResponse(resp.i, resp.r)
					case <-finalizeTimeout:
						return r
					}
				}
				return r
			}
		}
		// bail out if quorum isn't possible
		quorumPossible := false
		for _, c := range q.responseClassCounts {
			if (c+q.requestCount)-len(q.responses) >= q.q {
				quorumPossible = true
			}
		}
		if !quorumPossible {
			return ResponseStub(http.StatusServiceUnavailable, "The service is currently unavailable.")
		}
		// if we haven't made quorum, but it's still possible, then there
		// are outstanding requests we need to wait on.
		select {
		case response := <-q.responsec:
			q.addResponse(response.i, response.r)
		case <-getResponseTimeout:
			return ResponseStub(http.StatusServiceUnavailable, "The service is currently unavailable.")
		}
	}
}

func newQuorumer(r ring.Ring, partition uint64, responsec chan *respRec, cancel chan struct{}, makeRequest func(index int, dev *ring.Device) *http.Response) quorumer {
	return &stdQuorumer{
		makeRequest:         makeRequest,
		q:                   int(math.Ceil(float64(r.ReplicaCount()) / 2.0)),
		replicaCount:        int(r.ReplicaCount()),
		devs:                r.GetNodes(partition),
		more:                r.GetMoreNodes(partition),
		responseClassCounts: make([]int, 6),
		cancel:              cancel,
		responsec:           responsec,
	}
}

// quorumResponse returns with a response representative of a quorum of nodes.
//
// This is analogous to swift's best_response function.
func (c *ProxyDirectClient) quorumResponse(r ring.Ring, partition uint64, devToRequest func(int, *ring.Device) (*http.Request, error)) *http.Response {
	responses := make(chan *respRec)
	cancel := make(chan struct{})
	defer close(cancel)
	q := newQuorumer(r, partition, responses, cancel, func(index int, dev *ring.Device) *http.Response {
		if req, err := devToRequest(index, dev); err != nil {
			return ResponseStub(http.StatusInternalServerError, err.Error())
		} else if r, err := c.client.Do(req); err != nil {
			return ResponseStub(http.StatusInternalServerError, err.Error())
		} else {
			return StubResponse(r)
		}
	})
	q.start()
	return q.getResponse(postPutTimeout)
}

func (c *ProxyDirectClient) firstResponse(r ring.Ring, partition uint64, devToRequest func(*ring.Device) (*http.Request, error)) (resp *http.Response) {
	success := make(chan *http.Response)
	returned := make(chan struct{})
	defer close(returned)
	devs := r.GetNodes(partition)
	more := r.GetMoreNodes(partition)

	internalErrors := 0
	for requestCount := 0; requestCount < int(r.ReplicaCount()+2); requestCount++ {
		var dev *ring.Device
		if requestCount < len(devs) {
			dev = devs[requestCount]
		} else {
			dev = more.Next()
			if dev == nil {
				break
			}
		}
		req, err := devToRequest(dev)
		if err != nil {
			internalErrors++
			continue
		}

		go func(r *http.Request) {
			response, err := c.client.Do(r)
			if err != nil {
				if response != nil {
					response.Body.Close()
				}
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
			if resp != nil && (resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusPreconditionFailed ||
				resp.StatusCode == http.StatusNotModified || resp.StatusCode == http.StatusRequestedRangeNotSatisfiable) {
				resp.Header.Set("Accept-Ranges", "bytes")
				if etag := resp.Header.Get("Etag"); etag != "" {
					resp.Header.Set("Etag", strings.Trim(etag, "\""))
				}
				return resp
			}
			if resp == nil || resp.StatusCode/100 == 5 {
				internalErrors++
			}
			if resp != nil {
				resp.Body.Close()
			}
		case <-time.After(time.Second):
		}
	}
	if internalErrors >= int(r.ReplicaCount()) {
		return ResponseStub(http.StatusServiceUnavailable, "")
	} else {
		return ResponseStub(http.StatusNotFound, "")
	}
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
	return c.quorumResponse(c.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) PostAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.quorumResponse(c.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) GetAccount(account string, options map[string]string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	query := mkquery(options)
	return c.firstResponse(c.AccountRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), query)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) HeadAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.firstResponse(c.AccountRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) DeleteAccount(account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.quorumResponse(c.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
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
	containerReplicaCount := int(c.ContainerRing.ReplicaCount())
	return c.quorumResponse(c.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policyIndex))
		req.Header.Set("X-Backend-Storage-Policy-Default", strconv.Itoa(policyDefault))
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		return req, nil
	})
}

func (c *ProxyDirectClient) PostContainer(account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	return c.quorumResponse(c.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) GetContainer(account string, container string, options map[string]string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	query := mkquery(options)
	return c.firstResponse(c.ContainerRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), query)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
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
	return c.firstResponse(c.ContainerRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) DeleteContainer(account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	containerReplicaCount := int(c.ContainerRing.ReplicaCount())
	return c.quorumResponse(c.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		return req, nil
	})
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

// putReader is a Reader proxy that sends its reader over the ready channel the first time Read is called.
// This is important because "Expect: 100-continue" requests don't call Read unless/until they get a 100 response.
type putReader struct {
	io.Reader
	cancel chan struct{}
	ready  chan io.WriteCloser
	w      io.WriteCloser
}

func (p *putReader) Read(b []byte) (int, error) {
	// if Read() is called, it means we've received a 100-continue.
	// So we notify the ready channel that we're good to go.
	if p.ready != nil {
		select {
		case <-p.cancel:
			return 0, errors.New("Request was cancelled")
		case p.ready <- p.w:
			p.ready = nil
		}
	}
	if i, err := p.Reader.Read(b); err == nil {
		return i, err
	} else {
		select {
		case <-p.cancel:
			return 0, errors.New("Request was cancelled")
		default:
			return i, err
		}
	}
}

func (oc *standardObjectClient) putObject(obj string, headers http.Header, src io.Reader) *http.Response {
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	objectPartition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	ready := make(chan io.WriteCloser)
	cancel := make(chan struct{})
	defer close(cancel)
	responses := make(chan *respRec)
	writers := make([]io.Writer, 0)
	cWriters := make([]io.WriteCloser, 0)
	q := newQuorumer(oc.objectRing, objectPartition, responses, cancel, func(index int, dev *ring.Device) *http.Response {
		trp, wp := io.Pipe()
		rp := &putReader{Reader: trp, cancel: cancel, w: wp, ready: ready}
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, objectPartition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			return ResponseStub(http.StatusInternalServerError, err.Error())
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, index, objectReplicaCount)
		req.Header.Set("Expect", "100-Continue")
		// requests that get a 100-continue will wait inside Do() until we have a quorum of writers
		if r, err := oc.proxyDirectClient.client.Do(req); err != nil {
			return ResponseStub(http.StatusInternalServerError, err.Error())
		} else {
			return StubResponse(r)
		}
	})
	q.start()
	for !q.quorumMet(len(writers)) {
		if !q.launchUntilQuorumPossible() {
			return ResponseStub(http.StatusServiceUnavailable, "The service is currently unavailable.")
		}
		select {
		case response := <-responses:
			q.addResponse(response.i, response.r)
		case w := <-ready:
			defer w.Close()
			writers = append(writers, w)
			cWriters = append(cWriters, w)
		}
	}
	// TODO: get a Copy function that only errors if we can't complete a quorum of writers.  Or something.
	if _, err := common.Copy(src, writers...); err != nil {
		return ResponseStub(http.StatusServiceUnavailable, "The service is currently unavailable.")
	}
	for _, w := range cWriters {
		w.Close()
	}
	// TODO: verify that response etags match
	return q.getResponse(postPutTimeout)
}

func (oc *standardObjectClient) postObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	return oc.proxyDirectClient.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, i, objectReplicaCount)
		return req, nil
	})
}

func (oc *standardObjectClient) getObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) grepObject(obj string, search string) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s?e=%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj), common.Urlencode(search))
		req, err := http.NewRequest("GREP", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) headObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) deleteObject(obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(oc.account, oc.container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(oc.account, oc.container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	return oc.proxyDirectClient.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(oc.account), common.Urlencode(oc.container), common.Urlencode(obj))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, i, objectReplicaCount)
		return req, nil
	})
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

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]ContainerRecord, *http.Response) {
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
	if reverse {
		options["reverse"] = "true"
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

func (c *directClient) GetAccountRaw(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response {
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
	if reverse {
		options["reverse"] = "true"
	}
	return c.pc.GetAccount(c.account, options, common.Map2Headers(headers))
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

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]ObjectRecord, *http.Response) {
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
	if reverse {
		options["reverse"] = "true"
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

func (c *directClient) GetContainerRaw(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response {
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
	if reverse {
		options["reverse"] = "true"
	}
	return c.pc.GetContainer(c.account, container, options, common.Map2Headers(headers))
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
	pdc, err := NewProxyDirectClient(nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &directClient{account: account, pc: NewProxyClient(pdc, nil, nil)}, nil
}
