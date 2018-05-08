package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/tracing"
	"github.com/troubling/nectar"
	"github.com/troubling/nectar/nectarutil"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

const PostQuorumTimeoutMs = 100
const postPutTimeout = time.Second * 30
const firstResponseFinalTimeout = time.Second * 30

func addUpdateHeaders(prefix string, headers http.Header, devices []*ring.Device, i, replicas int) {
	if i < len(devices) {
		host := ""
		device := ""
		scheme := ""
		for ; i < len(devices); i += replicas {
			host += fmt.Sprintf("%s:%d,", devices[i].Ip, devices[i].Port)
			device += devices[i].Device + ","
			scheme += devices[i].Scheme + ","
		}
		headers.Set(prefix+"-Scheme", strings.TrimRight(scheme, ","))
		headers.Set(prefix+"-Host", strings.TrimRight(host, ","))
		headers.Set(prefix+"-Device", strings.TrimRight(device, ","))
	}
}

type ProxyDirectClient struct {
	policyList        conf.PolicyList
	client            common.HTTPClient
	AccountRing       ringFilter
	ContainerRing     ringFilter
	objectClients     map[int]proxyObjectClient
	lcm               sync.RWMutex
	Logger            srv.LowLevelLogger
	ClientTraceCloser io.Closer
}

func NewProxyDirectClient(policyList conf.PolicyList, cnf srv.ConfigLoader, logger srv.LowLevelLogger, certFile, keyFile, readAffinity, writeAffinity, writeAffinityCount string, serverconf conf.Config) (*ProxyDirectClient, error) {
	var xport http.RoundTripper = &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        0,
		IdleConnTimeout:     5 * time.Second,
		DisableCompression:  true,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
		ExpectContinueTimeout: 10 * time.Minute, // TODO: this should probably be like infinity.
	}
	if certFile != "" && keyFile != "" {
		tlsConf, err := common.NewClientTLSConfig(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		xport.(*http.Transport).TLSClientConfig = tlsConf
		if err = http2.ConfigureTransport(xport.(*http.Transport)); err != nil {
			return nil, err
		}
	}
	httpClient := &http.Client{
		Transport: xport,
		Timeout:   120 * time.Minute,
	}
	// Debug hook to auto-close responses and report on it. See debug.go
	// xport = &autoCloseResponses{transport: xport}
	c := &ProxyDirectClient{
		policyList: policyList,
		client:     httpClient,
		Logger:     logger,
	}
	if serverconf.HasSection("tracing") {
		clientTracer, clientTraceCloser, err := tracing.Init("proxydirect-client", logger, serverconf.GetSection("tracing"))
		if err != nil {
			return nil, fmt.Errorf("Error setting up tracer: %v", err)
		}
		c.ClientTraceCloser = clientTraceCloser
		enableHTTPTrace := serverconf.GetBool("tracing", "enable_httptrace", true)
		c.client, err = NewTracingClient(clientTracer, httpClient, enableHTTPTrace)
		if err != nil {
			return nil, fmt.Errorf("Error setting up tracing client: %v", err)
		}
	}

	if c.policyList == nil {
		policyList, err := cnf.GetPolicies()
		if err != nil {
			return nil, err
		}
		c.policyList = policyList
	}
	hashPathPrefix, hashPathSuffix, err := cnf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}
	containerRing, err := cnf.GetRing("container", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return nil, err
	}
	c.ContainerRing = newClientRingFilter(containerRing, readAffinity, "", "")
	accountRing, err := cnf.GetRing("account", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return nil, err
	}
	c.AccountRing = newClientRingFilter(accountRing, readAffinity, "", "")
	c.objectClients = make(map[int]proxyObjectClient)
	for _, policy := range policyList {
		// TODO: the intention is to (if it becomes necessary) have a policy type to object client
		// constructor mapping here, similar to how object engines are loaded by policy type.
		ring, err := cnf.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index)
		if err != nil {
			return nil, err
		}
		policyReadAffinity := policy.Config["read_affinity"]
		if policyReadAffinity == "" {
			policyReadAffinity = readAffinity
		}
		policyWriteAffinity := policy.Config["write_affinity"]
		if policyWriteAffinity == "" {
			policyWriteAffinity = writeAffinity
		}
		policyWriteAffinityCount := policy.Config["write_affinity_node_count"]
		if policyWriteAffinityCount == "" {
			policyWriteAffinityCount = writeAffinityCount
		}
		client := &standardObjectClient{
			proxyDirectClient: c,
			policy:            policy.Index,
			objectRing:        newClientRingFilter(ring, policyReadAffinity, policyWriteAffinity, policyWriteAffinityCount),
			Logger:            logger,
		}
		if policy.Type == "hec" {
			if replicas, err := strconv.Atoi(policy.Config["nursery_replicas"]); err == nil && replicas > 0 {
				client.deviceLimit = replicas
			} else {
				client.deviceLimit = 3
			}
		}
		c.objectClients[policy.Index] = client
	}
	return c, nil
}

// quorumResponse returns with a response representative of a quorum of nodes.
//
// This is analogous to swift's best_response function.
func (c *ProxyDirectClient) quorumResponse(r ringFilter, partition uint64, devToRequest func(int, *ring.Device) (*http.Request, error)) *http.Response {
	cancel := make(chan struct{})
	defer close(cancel)
	responsec := make(chan *http.Response)
	devs, more := r.getWriteNodes(partition, 0)
	for i := 0; i < int(r.ReplicaCount()); i++ {
		go func(index int) {
			var resp *http.Response
			var firstResp *http.Response
			for dev := devs[index]; dev != nil; dev = more.Next() {
				if req, err := devToRequest(index, dev); err != nil {
					c.Logger.Error("unable to get response", zap.Error(err))
					resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
				} else if r, err := c.client.Do(req); err != nil {
					c.Logger.Error("unable to get response", zap.Error(err))
					resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
				} else {
					resp = nectarutil.StubResponse(r)
				}
				if firstResp == nil {
					firstResp = resp
				}
				if resp.StatusCode >= 200 && resp.StatusCode < 500 {
					break
				}
			}
			// In the case where we're about to respond with Not Found, ensure
			// it's a response from the primary node. This corrects for the
			// case where the primary node 5xx errored and subsequent nodes
			// don't know about the item requested.
			if resp.StatusCode == 404 {
				resp = firstResp
			}
			select {
			case responsec <- resp:
			case <-cancel:
				return
			}
		}(i)
	}
	responseClassCounts := make([]int, 6)
	quorum := int(math.Ceil(float64(r.ReplicaCount()) / 2.0))
	for i := 0; i < int(r.ReplicaCount()); i++ {
		if resp := <-responsec; resp != nil {
			responseClassCounts[resp.StatusCode/100]++
			if responseClassCounts[resp.StatusCode/100] >= quorum {
				timeout := time.After(time.Duration(PostQuorumTimeoutMs) * time.Millisecond)
				for i < int(r.ReplicaCount()-1) {
					select {
					case <-responsec:
						i++
					case <-timeout:
						return resp
					}
				}
				return resp
			}
		}
	}
	return nectarutil.ResponseStub(http.StatusServiceUnavailable, "Unknown State")
}

func (c *ProxyDirectClient) firstResponse(r ringFilter, partition uint64, devToRequest func(*ring.Device) (*http.Request, error)) (resp *http.Response) {
	success := make(chan *http.Response)
	returned := make(chan struct{})
	defer close(returned)
	devs, more := r.getReadNodes(partition)
	internalErrors := 0
	notFounds := 0
	interpretResponse := func(resp *http.Response) *http.Response {
		if resp != nil && (resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusPreconditionFailed ||
			resp.StatusCode == http.StatusNotModified || resp.StatusCode == http.StatusRequestedRangeNotSatisfiable) {
			resp.Header.Set("Accept-Ranges", "bytes")
			if etag := resp.Header.Get("Etag"); etag != "" {
				resp.Header.Set("Etag", strings.Trim(etag, "\""))
			}
			return resp
		}
		if resp != nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNotFound {
				notFounds++
			} else {
				internalErrors++
			}
		} else {
			internalErrors++
		}
		return nil
	}
	maxRequests := int(r.ReplicaCount()) * 2
	requestsPending := 0
	for requestCount := 0; requestCount < maxRequests; requestCount++ {
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
			c.Logger.Error("firstResponse devToRequest error", zap.Error(err))
			internalErrors++
			continue
		}

		requestsPending++
		go func(r *http.Request) {
			response, err := c.client.Do(r)
			if err != nil {
				c.Logger.Error("firstResponse response", zap.Error(err))
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
			requestsPending--
			resp = interpretResponse(resp)
			if resp != nil {
				return resp
			}
		case <-time.After(time.Second):
		}
	}
	giveUp := time.After(firstResponseFinalTimeout)
	for requestsPending > 0 {
		select {
		case resp = <-success:
			requestsPending--
			resp = interpretResponse(resp)
			if resp != nil {
				return resp
			}
		case <-giveUp:
			internalErrors += requestsPending
			requestsPending = 0
		}
	}
	if notFounds > internalErrors {
		return nectarutil.ResponseStub(http.StatusNotFound, "")
	}
	return nectarutil.ResponseStub(http.StatusServiceUnavailable, "")
}

type proxyClient struct {
	pdc    *ProxyDirectClient
	mc     ring.MemcacheRing
	lc     map[string]*ContainerInfo
	Logger srv.LowLevelLogger
}

var _ ProxyClient = &proxyClient{}

func NewProxyClient(pdc *ProxyDirectClient, mc ring.MemcacheRing, lc map[string]*ContainerInfo, logger srv.LowLevelLogger) ProxyClient {
	return &proxyClient{pdc: pdc, mc: mc, lc: lc, Logger: logger}
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

func (c *proxyClient) PutAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	return c.pdc.PutAccount(ctx, account, headers)
}
func (c *proxyClient) PostAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	return c.pdc.PostAccount(ctx, account, headers)
}
func (c *proxyClient) GetAccount(ctx context.Context, account string, options map[string]string, headers http.Header) *http.Response {
	return c.pdc.GetAccount(ctx, account, options, headers)
}
func (c *proxyClient) HeadAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	return c.pdc.HeadAccount(ctx, account, headers)
}
func (c *proxyClient) DeleteAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	return c.pdc.DeleteAccount(ctx, account, headers)
}
func (c *proxyClient) PutContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.PutContainer(ctx, account, container, headers)
}
func (c *proxyClient) PostContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.PostContainer(ctx, account, container, headers)
}
func (c *proxyClient) GetContainer(ctx context.Context, account string, container string, options map[string]string, headers http.Header) *http.Response {
	return c.pdc.GetContainer(ctx, account, container, options, headers)
}
func (c *proxyClient) GetContainerInfo(ctx context.Context, account string, container string) (*ContainerInfo, error) {
	return c.pdc.GetContainerInfo(ctx, account, container, c.mc, c.lc)
}
func (c *proxyClient) SetContainerInfo(ctx context.Context, account string, container string, resp *http.Response) (*ContainerInfo, error) {
	return c.pdc.SetContainerInfo(ctx, account, container, c.mc, c.lc, resp)
}
func (c *proxyClient) HeadContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	return c.pdc.HeadContainer(ctx, account, container, headers)
}
func (c *proxyClient) DeleteContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(account, container)
	return c.pdc.DeleteContainer(ctx, account, container, headers)
}
func (c *proxyClient) PutObject(ctx context.Context, account string, container string, obj string, headers http.Header, src io.Reader) *http.Response {
	return c.pdc.PutObject(ctx, account, container, obj, headers, src, c.mc, c.lc)
}
func (c *proxyClient) PostObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.PostObject(ctx, account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) GetObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.GetObject(ctx, account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) HeadObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.HeadObject(ctx, account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) DeleteObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.pdc.DeleteObject(ctx, account, container, obj, headers, c.mc, c.lc)
}
func (c *proxyClient) ObjectRingFor(ctx context.Context, account string, container string) (ring.Ring, *http.Response) {
	return c.pdc.ObjectRingFor(ctx, account, container, c.mc, c.lc)
}
func (c *proxyClient) ContainerRing() ring.Ring {
	return c.pdc.ContainerRing.ring()
}

func (c *ProxyDirectClient) PutAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.quorumResponse(c.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) PostAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.quorumResponse(c.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) GetAccount(ctx context.Context, account string, options map[string]string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	query := nectarutil.Mkquery(options)
	return c.firstResponse(c.AccountRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), query)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) HeadAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.firstResponse(c.AccountRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) DeleteAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.AccountRing.GetPartition(account, "", "")
	return c.quorumResponse(c.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) PutContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	policyIndex := -1
	policyDefault := c.policyList.Default()
	if policyName := strings.TrimSpace(headers.Get("X-Storage-Policy")); policyName != "" {
		policy := c.policyList.NameLookup(policyName)
		if policy == nil {
			return nectarutil.ResponseStub(http.StatusBadRequest, fmt.Sprintf("Invalid X-Storage-Policy %q", policyName))
		} else if policy.Deprecated {
			return nectarutil.ResponseStub(http.StatusBadRequest, fmt.Sprintf("Storage Policy %q is deprecated", policyName))
		}
		policyIndex = policy.Index
	}
	containerReplicaCount := int(c.ContainerRing.ReplicaCount())
	return c.quorumResponse(c.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		if policyIndex != -1 {
			req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policyIndex))
		}
		req.Header.Set("X-Backend-Storage-Policy-Default", strconv.Itoa(policyDefault))
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		return req, nil
	})
}

func (c *ProxyDirectClient) PostContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	return c.quorumResponse(c.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) GetContainer(ctx context.Context, account string, container string, options map[string]string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	query := nectarutil.Mkquery(options)
	return c.firstResponse(c.ContainerRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), query)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) SetContainerInfo(ctx context.Context, account, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo, resp *http.Response) (*ContainerInfo, error) {
	key := fmt.Sprintf("container/%s/%s", account, container)
	ci := &ContainerInfo{
		Metadata:    make(map[string]string),
		SysMetadata: make(map[string]string),
	}
	var err error
	if ci.ObjectCount, err = strconv.ParseInt(resp.Header.Get("X-Container-Object-Count"), 10, 64); err != nil {
		return nil, fmt.Errorf("Error retrieving X-Container-Object-Count for container %s/%s : %s", account, container, resp.Header.Get("X-Container-Object-Count"))
	}
	if ci.ObjectBytes, err = strconv.ParseInt(resp.Header.Get("X-Container-Bytes-Used"), 10, 64); err != nil {
		return nil, fmt.Errorf("Error retrieving X-Container-Bytes-Used for container %s/%s : %s", account, container, resp.Header.Get("X-Container-Bytes-Used"))
	}
	if ci.StoragePolicyIndex, err = strconv.Atoi(resp.Header.Get("X-Backend-Storage-Policy-Index")); err != nil {
		return nil, fmt.Errorf("Error retrieving X-Backend-Storage-Policy-Index for container %s/%s : %s", account, container, resp.Header.Get("X-Backend-Storage-Policy-Index"))
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
	if lc != nil && ci != nil {
		c.lcm.Lock()
		lc[key] = ci
		c.lcm.Unlock()
	}
	if mc != nil {
		mc.Set(key, ci, 10) // throwing away error here..
	}
	return ci, nil
}

// NilContainerInfo is useful for testing.
var NilContainerInfo = &ContainerInfo{}

var ContainerNotFound = errors.New("Container Not Found")

// if finds container info returns: *ci, nil
// if gets 404 on HeadContainer returns: nil, ContainerNotFound
// if errors on getting container retuns nil, err
func (c *ProxyDirectClient) GetContainerInfo(ctx context.Context, account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (*ContainerInfo, error) {
	key := fmt.Sprintf("container/%s/%s", account, container)
	var ci *ContainerInfo
	contInCache := false
	if lc != nil {
		c.lcm.RLock()
		ci, contInCache = lc[key]
		c.lcm.RUnlock()
	}
	if ci == nil && contInCache {
		return nil, ContainerNotFound
	}
	if !contInCache && mc != nil {
		if err := mc.GetStructured(key, &ci); err == nil {
			c.lcm.RLock()
			lc[key] = ci
			c.lcm.RUnlock()
			contInCache = true
		} else {
			ci = nil
		}
	}
	if !contInCache {
		resp := c.HeadContainer(ctx, account, container, nil)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			if resp.StatusCode == 404 {
				c.lcm.RLock()
				lc[key] = nil
				c.lcm.RUnlock()
				return nil, ContainerNotFound
			}
			return nil, fmt.Errorf("%d error retrieving info for container %s/%s", resp.StatusCode, account, container)
		}
		var err error
		if ci, err = c.SetContainerInfo(ctx, account, container, mc, lc, resp); err != nil {
			return nil, err
		}
	}
	if ci == NilContainerInfo {
		return nil, errors.New("No container info for testing")
	}
	return ci, nil
}

func (c *ProxyDirectClient) HeadContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	return c.firstResponse(c.ContainerRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *ProxyDirectClient) DeleteContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	containerReplicaCount := int(c.ContainerRing.ReplicaCount())
	return c.quorumResponse(c.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		return req, nil
	})
}

func (c *ProxyDirectClient) getObjectClient(ctx context.Context, account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) proxyObjectClient {
	ci, err := c.GetContainerInfo(ctx, account, container, mc, lc)
	if err != nil {
		st := http.StatusInternalServerError
		if err == ContainerNotFound {
			st = http.StatusNotFound
		}
		return &erroringObjectClient{st, err.Error()}
	}
	return c.objectClients[ci.StoragePolicyIndex]
}

func (c *ProxyDirectClient) PutObject(ctx context.Context, account string, container string, obj string, headers http.Header, src io.Reader, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(ctx, account, container, mc, lc).putObject(ctx, account, container, obj, headers, src)
}

func (c *ProxyDirectClient) PostObject(ctx context.Context, account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(ctx, account, container, mc, lc).postObject(ctx, account, container, obj, headers)
}

func (c *ProxyDirectClient) GetObject(ctx context.Context, account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(ctx, account, container, mc, lc).getObject(ctx, account, container, obj, headers)
}

func (c *ProxyDirectClient) GrepObject(ctx context.Context, account string, container string, obj string, search string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(ctx, account, container, mc, lc).grepObject(ctx, account, container, obj, search)
}

func (c *ProxyDirectClient) HeadObject(ctx context.Context, account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(ctx, account, container, mc, lc).headObject(ctx, account, container, obj, headers)
}

func (c *ProxyDirectClient) DeleteObject(ctx context.Context, account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(ctx, account, container, mc, lc).deleteObject(ctx, account, container, obj, headers)
}

func (c *ProxyDirectClient) ObjectRingFor(ctx context.Context, account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (ring.Ring, *http.Response) {
	return c.getObjectClient(ctx, account, container, mc, lc).ring()
}

type proxyObjectClient interface {
	putObject(ctx context.Context, account, container, obj string, headers http.Header, src io.Reader) *http.Response
	postObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response
	getObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response
	grepObject(ctx context.Context, account, container, obj string, search string) *http.Response
	headObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response
	deleteObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response
	ring() (ring.Ring, *http.Response)
}

type erroringObjectClient struct {
	status int
	body   string
}

func (oc *erroringObjectClient) putObject(ctx context.Context, account, container, obj string, headers http.Header, src io.Reader) *http.Response {
	return nectarutil.ResponseStub(oc.status, oc.body)
}
func (oc *erroringObjectClient) postObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(oc.status, oc.body)
}
func (oc *erroringObjectClient) getObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(oc.status, oc.body)
}
func (oc *erroringObjectClient) grepObject(ctx context.Context, account, container, obj string, search string) *http.Response {
	return nectarutil.ResponseStub(oc.status, oc.body)
}
func (oc *erroringObjectClient) headObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(oc.status, oc.body)
}
func (oc *erroringObjectClient) deleteObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(oc.status, oc.body)
}
func (oc *erroringObjectClient) ring() (ring.Ring, *http.Response) {
	return nil, nectarutil.ResponseStub(oc.status, oc.body)
}

type standardObjectClient struct {
	proxyDirectClient *ProxyDirectClient
	policy            int
	objectRing        ringFilter
	deviceLimit       int
	Logger            srv.LowLevelLogger
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

func (oc *standardObjectClient) putObject(ctx context.Context, account, container, obj string, headers http.Header, src io.Reader) *http.Response {
	objectPartition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	ready := make(chan io.WriteCloser)
	cancel := make(chan struct{})
	defer close(cancel)
	responsec := make(chan *http.Response)
	devs, more := oc.objectRing.getWriteNodes(objectPartition, oc.deviceLimit)
	objectReplicaCount := len(devs)

	devToRequest := func(index int, dev *ring.Device) (*http.Request, error) {
		trp, wp := io.Pipe()
		rp := &putReader{Reader: trp, cancel: cancel, w: wp, ready: ready}
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, objectPartition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/octet-stream")
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, index, objectReplicaCount)
		req.Header.Set("Expect", "100-continue")
		return req, nil
	}

	for i := 0; i < objectReplicaCount; i++ {
		go func(index int) {
			var resp *http.Response
			for dev := devs[index]; dev != nil; dev = more.Next() {
				if req, err := devToRequest(index, dev); err != nil {
					oc.Logger.Error("unable create PUT request", zap.Error(err))
					resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
				} else if r, err := oc.proxyDirectClient.client.Do(req); err != nil {
					oc.Logger.Error("unable to PUT object", zap.Error(err))
					resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
				} else {
					resp = nectarutil.StubResponse(r)
					if r.StatusCode >= 200 && r.StatusCode < 500 {
						break
					}
				}
				select {
				case <-cancel:
					return
				default:
				}
			}
			if resp == nil {
				err := fmt.Errorf("no more nodes to try")
				oc.Logger.Error("unable to PUT object", zap.Error(err))
				resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
			}
			select {
			case responsec <- resp:
			case <-cancel:
				return
			}
		}(i)
	}
	responseClassCounts := make([]int, 6)
	quorum := int(math.Ceil(float64(objectReplicaCount) / 2.0))
	writers := make([]io.Writer, 0)
	cWriters := make([]io.WriteCloser, 0)
	responseCount := 0
	written := false
	for {
		select {
		case resp := <-responsec:
			responseCount++
			if resp != nil {
				responseClassCounts[resp.StatusCode/100]++
				if responseClassCounts[resp.StatusCode/100] >= quorum {
					timeout := time.After(time.Duration(PostQuorumTimeoutMs) * time.Millisecond)
					for responseCount < objectReplicaCount {
						select {
						case <-responsec:
							responseCount++
						case <-timeout:
							return resp
						}
					}
					return resp
				} else if responseCount == objectReplicaCount {
					return nectarutil.ResponseStub(http.StatusServiceUnavailable, "The service is currently unavailable.")
				}
			}
		case w := <-ready:
			defer w.Close()
			writers = append(writers, w)
			cWriters = append(cWriters, w)
		}
		if !written && len(writers) >= quorum && len(writers)+responseCount == objectReplicaCount {
			written = true
			if _, err := common.CopyQuorum(src, quorum, writers...); err != nil {
				return nectarutil.ResponseStub(http.StatusServiceUnavailable, "The service is currently unavailable.")
			}
			for _, w := range cWriters {
				w.Close()
			}
		}
	}
}

func (oc *standardObjectClient) postObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	return oc.proxyDirectClient.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, i, objectReplicaCount)
		return req, nil
	})
}

func (oc *standardObjectClient) getObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) grepObject(ctx context.Context, account, container, obj string, search string) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s?e=%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj), common.Urlencode(search))
		req, err := http.NewRequest("GREP", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) headObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) deleteObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	return oc.proxyDirectClient.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
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
	return oc.objectRing.ring(), nil
}

type directClient struct {
	pc      ProxyClient
	account string
}

var _ nectar.Client = &directClient{}

func (c *directClient) GetURL() string {
	return "<direct>/" + c.account
}

func (c *directClient) PutAccount(headers map[string]string) *http.Response {
	return c.pc.PutAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) PostAccount(headers map[string]string) *http.Response {
	return c.pc.PostAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]*nectar.ContainerRecord, *http.Response) {
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
	resp := c.pc.GetAccount(context.Background(), c.account, options, common.Map2Headers(headers))
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var accountListing []*nectar.ContainerRecord
	if err := json.NewDecoder(resp.Body).Decode(&accountListing); err != nil {
		resp.Body.Close()
		// FIXME. Log something.
		return nil, nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
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
	return c.pc.GetAccount(context.Background(), c.account, options, common.Map2Headers(headers))
}

func (c *directClient) HeadAccount(headers map[string]string) *http.Response {
	return c.pc.HeadAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) DeleteAccount(headers map[string]string) *http.Response {
	return c.pc.DeleteAccount(context.Background(), c.account, common.Map2Headers(headers))
}

func (c *directClient) PutContainer(container string, headers map[string]string) *http.Response {
	return c.pc.PutContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) PostContainer(container string, headers map[string]string) *http.Response {
	return c.pc.PostContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]*nectar.ObjectRecord, *http.Response) {
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
	resp := c.pc.GetContainer(context.Background(), c.account, container, options, common.Map2Headers(headers))
	if resp.StatusCode/100 != 2 {
		return nil, resp
	}
	var containerListing []*nectar.ObjectRecord
	if err := json.NewDecoder(resp.Body).Decode(&containerListing); err != nil {
		resp.Body.Close()
		// FIXME. Log something.
		return nil, nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
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
	return c.pc.GetContainer(context.Background(), c.account, container, options, common.Map2Headers(headers))
}

func (c *directClient) HeadContainer(container string, headers map[string]string) *http.Response {
	return c.pc.HeadContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) DeleteContainer(container string, headers map[string]string) *http.Response {
	return c.pc.DeleteContainer(context.Background(), c.account, container, common.Map2Headers(headers))
}

func (c *directClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) *http.Response {
	return c.pc.PutObject(context.Background(), c.account, container, obj, common.Map2Headers(headers), src)
}

func (c *directClient) PostObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.PostObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) GetObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.GetObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) HeadObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.HeadObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) DeleteObject(container string, obj string, headers map[string]string) *http.Response {
	return c.pc.DeleteObject(context.Background(), c.account, container, obj, common.Map2Headers(headers))
}

func (c *directClient) Raw(method, urlAfterAccount string, headers map[string]string, body io.Reader) *http.Response {
	return nectarutil.ResponseStub(http.StatusNotImplemented, "Raw requests not implemented for direct clients")
}
