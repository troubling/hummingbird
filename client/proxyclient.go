package client

import (
	"context"
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

type proxyClient struct {
	policyList        conf.PolicyList
	client            common.HTTPClient
	AccountRing       ringFilter
	ContainerRing     ringFilter
	objectClients     map[int]proxyObjectClient
	Logger            srv.LowLevelLogger
	ClientTraceCloser io.Closer
}

var _ ProxyClient = &proxyClient{}

func NewProxyClient(policyList conf.PolicyList, cnf srv.ConfigLoader, logger srv.LowLevelLogger, certFile, keyFile, readAffinity, writeAffinity, writeAffinityCount string, serverconf conf.Config) (ProxyClient, error) {
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
	c := &proxyClient{
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
	c.ContainerRing = newClientRingFilter(containerRing, readAffinity, "", "", 0)
	accountRing, err := cnf.GetRing("account", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		return nil, err
	}
	c.AccountRing = newClientRingFilter(accountRing, readAffinity, "", "", 0)
	c.objectClients = make(map[int]proxyObjectClient)
	for _, policy := range c.policyList {
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
		var deviceLimit int
		if policy.Type == "hec" {
			if replicas, err := strconv.Atoi(policy.Config["nursery_replicas"]); err == nil && replicas > 0 {
				deviceLimit = replicas
			} else {
				deviceLimit = 3
			}
		}
		client := &standardObjectClient{
			pdc:        c,
			policy:     policy.Index,
			objectRing: newClientRingFilter(ring, policyReadAffinity, policyWriteAffinity, policyWriteAffinityCount, deviceLimit),
			Logger:     logger,
		}
		c.objectClients[policy.Index] = client
	}
	return c, nil
}

// quorumResponse returns with a response representative of a quorum of nodes.
//
// This is analogous to swift's best_response function.
func (c *proxyClient) quorumResponse(r ringFilter, partition uint64, devToRequest func(int, *ring.Device) (*http.Request, error)) *http.Response {
	cancel := make(chan struct{})
	defer close(cancel)
	responsec := make(chan *http.Response)
	devs, more := r.getWriteNodes(partition)
	for i := 0; i < int(len(devs)); i++ {
		go func(index int) {
			var resp *http.Response
			var firstResp *http.Response
			for dev := devs[index]; dev != nil; dev = more.Next() {
				if req, err := devToRequest(index, dev); err != nil {
					c.Logger.Error("unable to create request", zap.Error(err))
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
	quorum := int(math.Ceil(float64(len(devs)) / 2.0))
	for i := 0; i < len(devs); i++ {
		if resp := <-responsec; resp != nil {
			responseClassCounts[resp.StatusCode/100]++
			if responseClassCounts[resp.StatusCode/100] >= quorum {
				timeout := time.After(time.Duration(PostQuorumTimeoutMs) * time.Millisecond)
				for i < int(len(devs)-1) {
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

func (c *proxyClient) firstResponse(r ringFilter, partition uint64, devToRequest func(*ring.Device) (*http.Request, error)) (resp *http.Response) {
	receivedResponses := make(chan *http.Response)
	alreadyFoundGoodResponse := make(chan struct{})
	defer close(alreadyFoundGoodResponse)
	devs, more := r.getReadNodes(partition)
	internalErrors := 0
	notFounds := 0
	backendHeaders := map[string]string{}
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
			for k := range resp.Header {
				if strings.HasPrefix(k, "X-Backend") {
					backendHeaders[k] = resp.Header.Get(k)
				}
			}
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
			case receivedResponses <- response:
			case <-alreadyFoundGoodResponse:
				if response != nil {
					response.Body.Close()
				}
			}
		}(req)

		select {
		case resp = <-receivedResponses:
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
		case resp = <-receivedResponses:
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
		r := nectarutil.ResponseStub(http.StatusNotFound, "")
		for k, v := range backendHeaders {
			r.Header.Set(k, v)
		}
		return r
	}
	return nectarutil.ResponseStub(http.StatusServiceUnavailable, "")
}

func (c *proxyClient) Close() error {
	if c.ClientTraceCloser != nil {
		return c.ClientTraceCloser.Close()
	}
	return nil
}

// NilContainerInfo is used for testing.
var NilContainerInfo = &ContainerInfo{}

var ContainerNotFound = errors.New("Container Not Found")

func (c *proxyClient) NewRequestClient(mc ring.MemcacheRing, lc map[string]*ContainerInfo, logger srv.LowLevelLogger) RequestClient {
	return &requestClient{pdc: c, mc: mc, lc: lc, Logger: logger}
}

type requestClient struct {
	pdc    *proxyClient
	mc     ring.MemcacheRing
	lc     map[string]*ContainerInfo
	lcm    sync.RWMutex
	Logger srv.LowLevelLogger
}

var _ RequestClient = &requestClient{}

func (c *requestClient) getObjectClient(ctx context.Context, account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) proxyObjectClient {
	ci, err := c.GetContainerInfo(ctx, account, container)
	if err != nil {
		st := http.StatusInternalServerError
		if err == ContainerNotFound {
			st = http.StatusNotFound
		}
		return &erroringObjectClient{st, err.Error()}
	}
	return c.pdc.objectClients[ci.StoragePolicyIndex]
}

func (c *requestClient) invalidateContainerInfo(ctx context.Context, account string, container string) {
	key := fmt.Sprintf("container/%s/%s", account, container)
	if c.lc != nil {
		delete(c.lc, key)
	}
	if c.mc != nil {
		c.mc.Delete(ctx, key)
	}
}

func (c *requestClient) PutAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.pdc.AccountRing.GetPartition(account, "", "")
	return c.pdc.quorumResponse(c.pdc.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) PostAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.pdc.AccountRing.GetPartition(account, "", "")
	return c.pdc.quorumResponse(c.pdc.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) GetAccountRaw(ctx context.Context, account string, options map[string]string, headers http.Header) *http.Response {
	partition := c.pdc.AccountRing.GetPartition(account, "", "")
	query := nectarutil.Mkquery(options)
	return c.pdc.firstResponse(c.pdc.AccountRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), query)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) HeadAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.pdc.AccountRing.GetPartition(account, "", "")
	return c.pdc.firstResponse(c.pdc.AccountRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) DeleteAccount(ctx context.Context, account string, headers http.Header) *http.Response {
	partition := c.pdc.AccountRing.GetPartition(account, "", "")
	return c.pdc.quorumResponse(c.pdc.AccountRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition, common.Urlencode(account))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) PutContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(ctx, account, container)
	partition := c.pdc.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.pdc.AccountRing.GetPartition(account, "", "")
	accountDevices := c.pdc.AccountRing.GetNodes(accountPartition)
	policyIndex := -1
	policyDefault := c.pdc.policyList.Default()
	if policyName := strings.TrimSpace(headers.Get("X-Storage-Policy")); policyName != "" {
		policy := c.pdc.policyList.NameLookup(policyName)
		if policy == nil {
			return nectarutil.ResponseStub(http.StatusBadRequest, fmt.Sprintf("Invalid X-Storage-Policy %q", policyName))
		} else if policy.Deprecated {
			return nectarutil.ResponseStub(http.StatusBadRequest, fmt.Sprintf("Storage Policy %q is deprecated", policyName))
		}
		policyIndex = policy.Index
	}
	containerReplicaCount := int(c.pdc.ContainerRing.ReplicaCount())
	return c.pdc.quorumResponse(c.pdc.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
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

func (c *requestClient) PostContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(ctx, account, container)
	partition := c.pdc.ContainerRing.GetPartition(account, container, "")
	return c.pdc.quorumResponse(c.pdc.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) GetContainerRaw(ctx context.Context, account string, container string, options map[string]string, headers http.Header) *http.Response {
	partition := c.pdc.ContainerRing.GetPartition(account, container, "")
	query := nectarutil.Mkquery(options)
	return c.pdc.firstResponse(c.pdc.ContainerRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), query)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) GetContainerInfo(ctx context.Context, account string, container string) (*ContainerInfo, error) {
	// if finds container info returns: *ci, nil
	// if gets 404 on HeadContainer returns: nil, ContainerNotFound
	// if errors on getting container retuns nil, err
	key := fmt.Sprintf("container/%s/%s", account, container)
	var ci *ContainerInfo
	contInCache := false
	if c.lc != nil {
		c.lcm.RLock()
		ci, contInCache = c.lc[key]
		c.lcm.RUnlock()
	}
	if ci == nil && contInCache {
		return nil, ContainerNotFound
	}
	if !contInCache && c.mc != nil {
		if err := c.mc.GetStructured(ctx, key, &ci); err == nil {
			if c.lc != nil {
				c.lcm.RLock()
				c.lc[key] = ci
				c.lcm.RUnlock()
			}
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
				if c.lc != nil {
					c.lcm.RLock()
					c.lc[key] = nil
					c.lcm.RUnlock()
				}
				return nil, ContainerNotFound
			}
			return nil, fmt.Errorf("%d error retrieving info for container %s/%s", resp.StatusCode, account, container)
		}
		var err error
		if ci, err = c.SetContainerInfo(ctx, account, container, resp); err != nil {
			return nil, err
		}
	}
	if ci == NilContainerInfo {
		return nil, errors.New("No container info for testing")
	}
	return ci, nil
}

func (c *requestClient) SetContainerInfo(ctx context.Context, account string, container string, resp *http.Response) (*ContainerInfo, error) {
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
	if c.lc != nil && ci != nil {
		c.lcm.Lock()
		c.lc[key] = ci
		c.lcm.Unlock()
	}
	if c.mc != nil {
		c.mc.Set(ctx, key, ci, 10) // throwing away error here..
	}
	return ci, nil
}

func (c *requestClient) HeadContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	partition := c.pdc.ContainerRing.GetPartition(account, container, "")
	return c.pdc.firstResponse(c.pdc.ContainerRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		return req, nil
	})
}

func (c *requestClient) DeleteContainer(ctx context.Context, account string, container string, headers http.Header) *http.Response {
	defer c.invalidateContainerInfo(ctx, account, container)
	partition := c.pdc.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.pdc.AccountRing.GetPartition(account, "", "")
	accountDevices := c.pdc.AccountRing.GetNodes(accountPartition)
	containerReplicaCount := int(c.pdc.ContainerRing.ReplicaCount())
	return c.pdc.quorumResponse(c.pdc.ContainerRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		addUpdateHeaders("X-Account", req.Header, accountDevices, i, containerReplicaCount)
		return req, nil
	})
}

func (c *requestClient) PutObject(ctx context.Context, account string, container string, obj string, headers http.Header, src io.Reader) *http.Response {
	return c.getObjectClient(ctx, account, container, c.mc, c.lc).putObject(ctx, account, container, obj, headers, src)
}

func (c *requestClient) PostObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.getObjectClient(ctx, account, container, c.mc, c.lc).postObject(ctx, account, container, obj, headers)
}

func (c *requestClient) GetObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.getObjectClient(ctx, account, container, c.mc, c.lc).getObject(ctx, account, container, obj, headers)
}

func (c *requestClient) HeadObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.getObjectClient(ctx, account, container, c.mc, c.lc).headObject(ctx, account, container, obj, headers)
}

func (c *requestClient) DeleteObject(ctx context.Context, account string, container string, obj string, headers http.Header) *http.Response {
	return c.getObjectClient(ctx, account, container, c.mc, c.lc).deleteObject(ctx, account, container, obj, headers)
}

func (c *requestClient) ObjectRingFor(ctx context.Context, account string, container string) (ring.Ring, *http.Response) {
	return c.getObjectClient(ctx, account, container, c.mc, c.lc).ring()
}

func (c *requestClient) ContainerRing() ring.Ring {
	return c.pdc.ContainerRing.ring()
}

func (c *requestClient) AccountRing() ring.Ring {
	return c.pdc.AccountRing.ring()
}
