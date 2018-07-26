package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/tracing"
	"github.com/troubling/nectar/nectarutil"
	"go.uber.org/zap"
)

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
	pdc         *proxyClient
	policy      int
	objectRing  ringFilter
	deviceLimit int
	Logger      srv.LowLevelLogger
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
	containerPartition := oc.pdc.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.pdc.ContainerRing.GetNodes(containerPartition)
	ready := make(chan io.WriteCloser)
	cancel := make(chan struct{})
	defer close(cancel)
	responsec := make(chan *http.Response)
	devs, more := oc.objectRing.getWriteNodes(objectPartition)
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
		req.Header.Set("User-Agent", oc.pdc.userAgent)
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
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
				} else if r, err := oc.pdc.client.Do(req); err != nil {
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
	containerPartition := oc.pdc.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.pdc.ContainerRing.GetNodes(containerPartition)
	devs, _ := oc.objectRing.getWriteNodes(partition)
	objectReplicaCount := len(devs)
	return oc.pdc.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", oc.pdc.userAgent)
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
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
	return oc.pdc.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", oc.pdc.userAgent)
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) grepObject(ctx context.Context, account, container, obj string, search string) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.pdc.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s?e=%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj), common.Urlencode(search))
		req, err := http.NewRequest("GREP", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", oc.pdc.userAgent)
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) headObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	return oc.pdc.firstResponse(oc.objectRing, partition, func(dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", oc.pdc.userAgent)
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(oc.policy))
		return req, nil
	})
}

func (oc *standardObjectClient) deleteObject(ctx context.Context, account, container, obj string, headers http.Header) *http.Response {
	partition := oc.objectRing.GetPartition(account, container, obj)
	containerPartition := oc.pdc.ContainerRing.GetPartition(account, container, "")
	containerDevices := oc.pdc.ContainerRing.GetNodes(containerPartition)
	devs, _ := oc.objectRing.getWriteNodes(partition)
	objectReplicaCount := len(devs)
	return oc.pdc.quorumResponse(oc.objectRing, partition, func(i int, dev *ring.Device) (*http.Request, error) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", dev.Scheme, dev.Ip, dev.Port, dev.Device, partition,
			common.Urlencode(account), common.Urlencode(container), common.Urlencode(obj))
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", oc.pdc.userAgent)
		req = req.WithContext(tracing.CopySpanFromContext(ctx))
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
