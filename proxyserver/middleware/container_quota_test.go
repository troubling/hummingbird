package middleware

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/common/test"

	"go.uber.org/zap"
)

func passthroughQuotaHandler() http.Handler {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
	})
	section := conf.Section{}
	testScope := common.NewTestScope()
	quota, _ := NewContainerQuota(section, testScope)
	return quota(next)
}

func TestQuotaBytes(t *testing.T) {
	h := passthroughQuotaHandler()
	f, err := client.NewProxyClient(staticPolicyList, srv.NewTestConfigLoader(&test.FakeRing{}),
		nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	ctx := &ProxyContext{
		Logger: zap.NewNop(),
		C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{
			"container/a/c": {
				Metadata: map[string]string{"Quota-Bytes": "3"},
			},
		}, zap.NewNop()),
	}

	req, err := http.NewRequest("PUT", "/v1/a/c/o", strings.NewReader("MORETHAN3"))
	require.Nil(t, err)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 413, resp.StatusCode)
	require.Equal(t, "Upload exceeds quota.", string(body))
}

func TestQuotaCount(t *testing.T) {
	h := passthroughQuotaHandler()
	f, err := client.NewProxyClient(staticPolicyList, srv.NewTestConfigLoader(&test.FakeRing{}),
		nil, "", "", "", "", "", conf.Config{})
	require.Nil(t, err)
	ctx := &ProxyContext{
		Logger: zap.NewNop(),
		C: f.NewRequestClient(nil, map[string]*client.ContainerInfo{
			"container/a/c": {
				Metadata:    map[string]string{"Quota-Count": "3"},
				ObjectCount: 42,
			},
		}, zap.NewNop()),
	}

	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	require.Nil(t, err)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 413, resp.StatusCode)
	require.Equal(t, "Upload exceeds quota.", string(body))
}

func TestBadQuotaBytes(t *testing.T) {
	h := passthroughQuotaHandler()

	req, err := http.NewRequest("POST", "/v1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Container-Meta-Quota-Bytes", "wharrgarbl")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "Invalid bytes quota.", string(body))
}

func TestBadQuotaCount(t *testing.T) {
	h := passthroughQuotaHandler()

	req, err := http.NewRequest("POST", "/v1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Container-Meta-Quota-Count", "wharrgarbl")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "Invalid count quota.", string(body))
}
