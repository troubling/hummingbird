package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
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
	"github.com/troubling/nectar"
	"github.com/troubling/nectar/nectarutil"
	"go.uber.org/zap"
)

const PostQuorumTimeoutMs = 100
const postPutTimeout = time.Second * 30

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
	objectClients map[int]proxyObjectClient
	lcm           sync.RWMutex
	Logger        srv.LowLevelLogger
}

func NewProxyDirectClient(policyList conf.PolicyList, logger srv.LowLevelLogger) (*ProxyDirectClient, error) {
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
	// Debug hook to auto-close responses and report on it. See debug.go
	// xport = &autoCloseResponses{transport: xport}
	c := &ProxyDirectClient{
		policyList: policyList,
		client: &http.Client{
			Transport: xport,
			Timeout:   120 * time.Minute,
		},
		Logger: logger,
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
	c.objectClients = make(map[int]proxyObjectClient)
	for _, policy := range policyList {
		// TODO: the intention is to (if it becomes necessary) have a policy type to object client
		// constructor mapping here, similar to how object engines are loaded by policy type.
		ring, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index)
		if err != nil {
			return nil, err
		}
		client := &standardObjectClient{proxyDirectClient: c, policy: policy.Index, objectRing: ring, Logger: logger}
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

func (c *ProxyDirectClient) writeNodes(r ring.Ring, partition uint64) ([]*ring.Device, ring.MoreNodes) {
	// TODO: if the client has been configured for write affinity, devices will be filtered here.
	return r.GetNodes(partition), r.GetMoreNodes(partition)
}

// quorumResponse returns with a response representative of a quorum of nodes.
//
// This is analogous to swift's best_response function.
func (c *ProxyDirectClient) quorumResponse(r ring.Ring, partition uint64, devToRequest func(int, *ring.Device) (*http.Request, error)) *http.Response {
	cancel := make(chan struct{})
	defer close(cancel)
	responsec := make(chan *http.Response)
	devs, more := c.writeNodes(r, partition)
	for i := 0; i < int(r.ReplicaCount()); i++ {
		go func(index int) {
			var resp *http.Response
			for dev := devs[index]; dev != nil; dev = more.Next() {
				if req, err := devToRequest(index, dev); err != nil {
					c.Logger.Error("unable to get response", zap.Error(err))
					resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
				} else if r, err := c.client.Do(req); err != nil {
					c.Logger.Error("unable to get response", zap.Error(err))
					resp = nectarutil.ResponseStub(http.StatusInternalServerError, err.Error())
				} else {
					resp = nectarutil.StubResponse(r)
					if r.StatusCode >= 200 && r.StatusCode < 500 {
						break
					}
				}
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

func (c *ProxyDirectClient) firstResponse(r ring.Ring, partition uint64, devToRequest func(*ring.Device) (*http.Request, error)) (resp *http.Response) {
	success := make(chan *http.Response)
	returned := make(chan struct{})
	defer close(returned)
	devs := r.GetNodes(partition)
	for i := range devs {
		j := rand.Intn(i + 1)
		devs[i], devs[j] = devs[j], devs[i]
	}
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
			c.Logger.Error("firstResponse devToRequest error", zap.Error(err))
			internalErrors++
			continue
		}

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
		return nectarutil.ResponseStub(http.StatusServiceUnavailable, "")
	} else {
		return nectarutil.ResponseStub(http.StatusNotFound, "")
	}
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
func (c *proxyClient) ContainerRing() ring.Ring {
	return c.pdc.ContainerRing
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
	query := nectarutil.Mkquery(options)
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
			return nectarutil.ResponseStub(http.StatusBadRequest, fmt.Sprintf("Invalid X-Storage-Policy %q", policyName))
		}
		if policy.Deprecated {
			return nectarutil.ResponseStub(http.StatusBadRequest, fmt.Sprintf("Storage Policy %q is deprecated", policyName))
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
	query := nectarutil.Mkquery(options)
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
		c.lcm.RLock()
		ci = lc[key]
		c.lcm.RUnlock()
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
	if lc != nil && ci != nil {
		c.lcm.Lock()
		lc[key] = ci
		c.lcm.Unlock()
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

func (c *ProxyDirectClient) getObjectClient(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) proxyObjectClient {
	ci, err := c.GetContainerInfo(account, container, mc, lc)
	if err != nil {
		return &erroringObjectClient{err.Error()}
	}
	return c.objectClients[ci.StoragePolicyIndex]
}

func (c *ProxyDirectClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(account, container, mc, lc).putObject(account, container, obj, headers, src)
}

func (c *ProxyDirectClient) PostObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(account, container, mc, lc).postObject(account, container, obj, headers)
}

func (c *ProxyDirectClient) GetObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(account, container, mc, lc).getObject(account, container, obj, headers)
}

func (c *ProxyDirectClient) GrepObject(account string, container string, obj string, search string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(account, container, mc, lc).grepObject(account, container, obj, search)
}

func (c *ProxyDirectClient) HeadObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(account, container, mc, lc).headObject(account, container, obj, headers)
}

func (c *ProxyDirectClient) DeleteObject(account string, container string, obj string, headers http.Header, mc ring.MemcacheRing, lc map[string]*ContainerInfo) *http.Response {
	return c.getObjectClient(account, container, mc, lc).deleteObject(account, container, obj, headers)
}

func (c *ProxyDirectClient) ObjectRingFor(account string, container string, mc ring.MemcacheRing, lc map[string]*ContainerInfo) (ring.Ring, *http.Response) {
	return c.getObjectClient(account, container, mc, lc).ring()
}

type proxyObjectClient interface {
	putObject(account, container, obj string, headers http.Header, src io.Reader) *http.Response
	postObject(account, container, obj string, headers http.Header) *http.Response
	getObject(account, container, obj string, headers http.Header) *http.Response
	grepObject(account, container, obj string, search string) *http.Response
	headObject(account, container, obj string, headers http.Header) *http.Response
	deleteObject(account, container, obj string, headers http.Header) *http.Response
	ring() (ring.Ring, *http.Response)
}

type erroringObjectClient struct {
	body string
}

func (oc *erroringObjectClient) putObject(account, container, obj string, headers http.Header, src io.Reader) *http.Response {
	return nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) postObject(account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) getObject(account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) grepObject(account, container, obj string, search string) *http.Response {
	return nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) headObject(account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) deleteObject(account, container, obj string, headers http.Header) *http.Response {
	return nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}
func (oc *erroringObjectClient) ring() (ring.Ring, *http.Response) {
	return nil, nectarutil.ResponseStub(http.StatusInternalServerError, oc.body)
}

type standardObjectClient struct {
	proxyDirectClient *ProxyDirectClient
	policy            int
	objectRing        ring.Ring
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

func (oc *standardObjectClient) writeNodes(r ring.Ring, partition uint64) ([]*ring.Device, ring.MoreNodes) {
	devs, more := oc.proxyDirectClient.writeNodes(r, partition)
	if oc.deviceLimit > 0 && len(devs) > oc.deviceLimit {
		return devs[0:oc.deviceLimit], more
	}
	return devs, more
}

func (oc *standardObjectClient) putObject(account, container, obj string, headers http.Header, src io.Reader) *http.Response {
	objectPartition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	ready := make(chan io.WriteCloser)
	cancel := make(chan struct{})
	defer close(cancel)
	responsec := make(chan *http.Response)
	devs, more := oc.writeNodes(oc.objectRing, objectPartition)
	objectReplicaCount := len(devs)

	devToRequest := func(index int, dev *ring.Device) (*http.Request, error) {
		trp, wp := io.Pipe()
		rp := &putReader{Reader: trp, cancel: cancel, w: wp, ready: ready}
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, objectPartition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		addUpdateHeaders("X-Container", req.Header, containerDevices, index, objectReplicaCount)
		req.Header.Set("Expect", "100-Continue")
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

func (oc *standardObjectClient) postObject(account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	return oc.proxyDirectClient.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
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

func (oc *standardObjectClient) getObject(account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
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

func (oc *standardObjectClient) grepObject(account, container, obj string, search string) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s?e=%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj), common.Urlencode(search))
		req, err := http.NewRequest("GREP", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) headObject(account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.proxyDirectClient.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
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

func (oc *standardObjectClient) deleteObject(account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.proxyDirectClient.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.proxyDirectClient.ContainerRing.GetNodes(containerPartition)
	objectReplicaCount := int(oc.objectRing.ReplicaCount())
	return oc.proxyDirectClient.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
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

var _ nectar.Client = &directClient{}

func (c *directClient) GetURL() string {
	return "<direct>/" + c.account
}

func (c *directClient) PutAccount(headers map[string]string) *http.Response {
	return c.pc.PutAccount(c.account, common.Map2Headers(headers))
}

func (c *directClient) PostAccount(headers map[string]string) *http.Response {
	return c.pc.PostAccount(c.account, common.Map2Headers(headers))
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
	resp := c.pc.GetAccount(c.account, options, common.Map2Headers(headers))
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
	resp := c.pc.GetContainer(c.account, container, options, common.Map2Headers(headers))
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

func (c *directClient) Raw(method, urlAfterAccount string, headers map[string]string, body io.Reader) *http.Response {
	return nectarutil.ResponseStub(http.StatusNotImplemented, "Raw requests not implemented for direct clients")
}

// NewDirectClient creates a new direct client with the given account name.
func NewDirectClient(account string) (nectar.Client, error) {
	pdc, err := NewProxyDirectClient(nil, zap.NewNop())
	if err != nil {
		return nil, err
	}
	return &directClient{account: account, pc: NewProxyClient(pdc, nil, nil, zap.NewNop())}, nil
}
