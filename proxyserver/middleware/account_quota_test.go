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

	"go.uber.org/zap"
)

func passthroughAccountQuotaHandler() http.Handler {
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
	})
	section := conf.Section{}
	testScope := common.NewTestScope()
	quota, _ := NewAccountQuota(section, testScope)
	return quota(next)
}

func TestAccountQuotaBytes(t *testing.T) {
	h := passthroughAccountQuotaHandler()
	ctx := &ProxyContext{
		Logger: zap.NewNop(),
		C:      client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{}, zap.NewNop()),
		accountInfoCache: map[string]*AccountInfo{
			"account/a": {
				Metadata: map[string]string{"Quota-Bytes": "3"},
			},
		},
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

func TestBadAccountQuotaBytes(t *testing.T) {
	h := passthroughAccountQuotaHandler()
	ctx := NewFakeProxyContext(h)
	ctx.ResellerRequest = true

	req, err := http.NewRequest("POST", "/v1/a", nil)
	require.Nil(t, err)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))
	req.Header.Set("X-Account-Meta-Quota-Bytes", "wharrgarbl")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "Invalid bytes quota.", string(body))
}
