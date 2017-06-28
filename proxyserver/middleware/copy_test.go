package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"go.uber.org/zap"
)

func NewFakeProxyContext(next http.Handler) *ProxyContext {
	return &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{
			next: next,
		},
		Logger: zap.NewNop(),
	}
}

func TestGetSourceObject(t *testing.T) {
	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.Header().Set("X-Hi", "There")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("stuff"))
	})
	fakeContext := NewFakeProxyContext(passthrough)
	dummy, err := http.NewRequest("GET", "/someurl", nil)
	dummy = dummy.WithContext(context.WithValue(dummy.Context(), "proxycontext", fakeContext))
	require.Nil(t, err)

	c := &copyMiddleware{
		next: passthrough,
	}

	body, header, code := c.getSourceObject("/ver/a/c/o", dummy, false)
	require.Equal(t, 200, code)
	buf := make([]byte, 1024)
	num, err := body.Read(buf)
	require.Nil(t, err)
	require.Equal(t, 5, num)
	require.Equal(t, "stuff", string(buf[:5]))
	require.NotNil(t, header)
	require.NotNil(t, body)
}

func TestPut(t *testing.T) {
	section := conf.Section{}
	c, err := NewCopyMiddleware(section)
	require.Nil(t, err)

	passthrough := NewPassthroughFunc(t, Simple200GetResponseFunc, Simple200PutResponseFunc)
	handler := c(passthrough)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/a/c/o2", nil)
	req.Header.Set("X-Copy-From", "c/o")

	ctx := NewFakeProxyContext(handler)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	handler.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)
	require.Equal(t, "stuff", rr.Body.String())
	require.Equal(t, "c/o", rr.Header().Get("X-Copied-From"))
	require.Equal(t, "a", rr.Header().Get("X-Copied-From-Account"))
}

func TestPutAllNewMeta(t *testing.T) {
	section := conf.Section{}
	c, err := NewCopyMiddleware(section)
	require.Nil(t, err)

	passthrough := NewPassthroughFunc(t, Simple200GetResponseFunc, AllNewMetaPutResponseFunc)
	handler := c(passthrough)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/a/c/o2", nil)
	req.Header.Set("X-Copy-From", "c/o")
	req.Header.Set("X-Object-Sysmeta-Foo", "NewObjectSysmetaFoo")
	req.Header.Set("Etag", "NewObjectEtag")
	req.Header.Set("Content-Type", "NewObjectContentType")
	req.Header.Set("X-Object-Metadata-Foo", "NewObjectMetadataFoo")

	ctx := NewFakeProxyContext(handler)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	handler.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)
	require.Equal(t, "stuff", rr.Body.String())
	require.Equal(t, "c/o", rr.Header().Get("X-Copied-From"))
	require.Equal(t, "a", rr.Header().Get("X-Copied-From-Account"))
}

func TestCopy(t *testing.T) {
	section := conf.Section{}
	c, err := NewCopyMiddleware(section)
	require.Nil(t, err)

	passthrough := NewPassthroughFunc(t, Simple200GetResponseFunc, Simple200PutResponseFunc)
	handler := c(passthrough)

	// 412, no Destination
	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("COPY", "/v1/a/c/o", nil)
	ctx := NewFakeProxyContext(handler)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))
	handler.ServeHTTP(rr, req)

	require.Equal(t, 412, rr.Code)

	passthrough = NewPassthroughFunc(t, Simple200GetResponseFunc, Simple200PutResponseFunc)
	handler = c(passthrough)

	// 412, bad Destination
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("COPY", "/v1/a/c/o", nil)
	ctx = NewFakeProxyContext(handler)
	req.Header.Set("Destination", common.Urlencode("//o2"))
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	handler.ServeHTTP(rr, req)

	require.Equal(t, 412, rr.Code)

	// 200, specifying Destination
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("COPY", "/v1/a/c/o", nil)
	ctx = NewFakeProxyContext(handler)
	req.Header.Set("Destination", common.Urlencode("c/o2"))
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	handler.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)
	require.Equal(t, "stuff", rr.Body.String())
	reqUrl := common.Urlencode("/v1/a/c/o2")
	require.Equal(t, reqUrl, req.URL.Path)
	co, _ := url.QueryUnescape(rr.Header().Get("X-Copied-From"))
	require.Equal(t, "c/o", co)
	a, _ := url.QueryUnescape(rr.Header().Get("X-Copied-From-Account"))
	require.Equal(t, "a", a)

	passthrough = NewPassthroughFunc(t, Simple200GetResponseFunc, NewAccountPutResponseFunc)
	handler = c(passthrough)

	// 200, specifying Destination and Destination-Account
	rr = httptest.NewRecorder()
	req, _ = http.NewRequest("COPY", "/v1/a/c/o", nil)
	ctx = NewFakeProxyContext(handler)
	req.Header.Set("Destination", common.Urlencode("c/o2"))
	req.Header.Set("Destination-Account", common.Urlencode("a2"))
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	handler.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)
	require.Equal(t, "stuff", rr.Body.String())
	reqUrl = common.Urlencode("/v1/a2/c/o2")
	require.Equal(t, reqUrl, req.URL.Path)
	co, _ = url.QueryUnescape(rr.Header().Get("X-Copied-From"))
	require.Equal(t, "c/o", co)
	a, _ = url.QueryUnescape(rr.Header().Get("X-Copied-From-Account"))
	require.Equal(t, "a", a)
}

type testHandlerFunc func(*testing.T, http.ResponseWriter, *http.Request)

func NewPassthroughFunc(t *testing.T, funcList ...testHandlerFunc) http.Handler {
	var funcs []testHandlerFunc
	for _, f := range funcList {
		funcs = append(funcs, f)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := funcs[0]
		funcs = funcs[1:]
		if cur == nil {
			t.Error("No http handlers left!")
		}
		cur(t, w, r)
	})
}

func Simple200GetResponseFunc(t *testing.T, w http.ResponseWriter, r *http.Request) {
	require.Equal(t, "GET", r.Method)
	require.Equal(t, "/v1/a/c/o", r.URL.Path)
	require.Nil(t, r.Body)
	body := "stuff"
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("X-Object-Sysmeta-Foo", "SourceObjectSysmetaFoo")
	w.Header().Set("Etag", "SourceObjectEtag")
	w.Header().Set("Content-Type", "SourceObjectContentType")
	w.Header().Set("X-Timestamp", "SourceXTimestamp")
	w.Header().Set("X-Backend-Timestamp", "SourceXBackendTimestamp")
	w.Header().Set("X-Object-Metadata-Foo", "SourceObjectMetadataFoo")
	w.WriteHeader(200)
	w.Write([]byte(body))
}

func Simple200PutResponseFunc(t *testing.T, w http.ResponseWriter, r *http.Request) {
	require.Equal(t, "PUT", r.Method)
	require.Equal(t, "/v1/a/c/o2", r.URL.Path)
	require.Equal(t, "SourceObjectSysmetaFoo", r.Header.Get("X-Object-Sysmeta-Foo"))
	require.Equal(t, "SourceObjectEtag", r.Header.Get("Etag"))
	require.Equal(t, "SourceObjectContentType", r.Header.Get("Content-Type"))
	require.Equal(t, "", r.Header.Get("X-Timestamp"))
	require.Equal(t, "", r.Header.Get("X-Backend-Timestamp"))
	require.Equal(t, "SourceObjectMetadataFoo", r.Header.Get("X-Object-Metadata-Foo"))
	if r.Body != nil {
		buf := make([]byte, 1024)
		_, _ = r.Body.Read(buf)
		// Hangs until pipe reader is closed.
		_, _ = r.Body.Read(buf)
	}
	body := "stuff"
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(200)
	w.Write([]byte(body))
}

func AllNewMetaPutResponseFunc(t *testing.T, w http.ResponseWriter, r *http.Request) {
	require.Equal(t, "PUT", r.Method)
	require.Equal(t, "/v1/a/c/o2", r.URL.Path)
	require.Equal(t, "NewObjectSysmetaFoo", r.Header.Get("X-Object-Sysmeta-Foo"))
	require.Equal(t, "SourceObjectEtag", r.Header.Get("Etag"))
	require.Equal(t, "NewObjectContentType", r.Header.Get("Content-Type"))
	require.Equal(t, "NewObjectMetadataFoo", r.Header.Get("X-Object-Metadata-Foo"))
	if r.Body != nil {
		buf := make([]byte, 1024)
		_, _ = r.Body.Read(buf)
		// Hangs until pipe reader is closed.
		_, _ = r.Body.Read(buf)
	}
	body := "stuff"
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(200)
	w.Write([]byte(body))
}

func NewAccountPutResponseFunc(t *testing.T, w http.ResponseWriter, r *http.Request) {
	require.Equal(t, "PUT", r.Method)
	require.Equal(t, "/v1/a2/c/o2", r.URL.Path)
	if r.Body != nil {
		buf := make([]byte, 1024)
		_, _ = r.Body.Read(buf)
		// Hangs until pipe reader is closed.
		_, _ = r.Body.Read(buf)
	}
	body := "stuff"
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(200)
	w.Write([]byte(body))
}

func PostAsCopyPostResponseFunc(t *testing.T, w http.ResponseWriter, r *http.Request) {
	require.Equal(t, "POST", r.Method)
	require.Equal(t, "/v1/a/c/o", r.URL.Path)
	w.WriteHeader(409)
}

func PostAsCopyPutResponseFunc(t *testing.T, w http.ResponseWriter, r *http.Request) {
	require.Equal(t, "PUT", r.Method)
	require.Equal(t, "/v1/a/c/o", r.URL.Path)
	if r.Body != nil {
		buf := make([]byte, 1024)
		_, _ = r.Body.Read(buf)
		// Hangs until pipe reader is closed.
		_, _ = r.Body.Read(buf)
	}
	body := "stuff"
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(201)
	w.Write([]byte(body))
}

func TestPostAsCopy(t *testing.T) {

	configString := "[filter:copy]\nobject_post_as_copy = true"
	config, err := conf.StringConfig(configString)
	require.Nil(t, err)
	section := config.GetSection("filter:copy")
	c, err := NewCopyMiddleware(section)
	require.Nil(t, err)

	passthrough := NewPassthroughFunc(t, PostAsCopyPostResponseFunc, Simple200GetResponseFunc, PostAsCopyPutResponseFunc)
	handler := c(passthrough)

	rr := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/a/c/o", nil)
	req.Header.Set("Content-Type", "something")

	ctx := NewFakeProxyContext(handler)
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", ctx))

	handler.ServeHTTP(rr, req)

	require.Equal(t, 202, rr.Code)
	require.Equal(t, "stuff", rr.Body.String())
}
