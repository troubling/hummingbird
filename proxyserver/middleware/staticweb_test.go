package middleware

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"go.uber.org/zap"
)

type testNext struct {
	requests []*http.Request
}

func newTestStaticWebHandler(next http.Handler) (*staticWebHandler, *common.TestCounter) {
	requestsMetric := common.NewTestScope().Counter("test_staticweb")
	return &staticWebHandler{
		next:           next,
		requestsMetric: requestsMetric,
	}, requestsMetric.(*common.TestCounter)
}

func (t *testNext) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Status Code 999 is used in places because that shouldn't be in actual use and therefore should break tests if returned.
	t.requests = append(t.requests, request)
	if request.Method != "HEAD" && request.Method != "GET" {
		writer.WriteHeader(999)
	}
	body := ""
	switch request.URL.Path {
	case "/v1/a/c":
		if request.URL.Query().Get("delimiter") != "/" {
			writer.WriteHeader(999)
		} else {
			switch request.URL.Query().Get("prefix") {
			case "":
				writer.WriteHeader(200)
				body = `[{"subdir":"s001/"}, {"name":"index.html","last_modified":"2017-06-06T15:02:12.628489","bytes":46,"content_type":"text/html","hash":"02d28af81f1ddb353f2be4135f8240d6"}]`
			case "s001/":
				writer.WriteHeader(200)
				body = `[{"name":"s001/one.html","last_modified":"2017-06-06T15:02:12.628489","bytes":46,"content_type":"text/html","hash":"02d28af81f1ddb353f2be4135f8240d6"}]`
			case "s002/":
				writer.WriteHeader(200)
				body = `[{"name":"s002/index.html","last_modified":"2017-06-06T15:02:12.628489","bytes":46,"content_type":"text/html","hash":"02d28af81f1ddb353f2be4135f8240d6"}]`
			default:
				writer.WriteHeader(200)
				body = "[]"
			}
		}
	case "/v1/a/c2/":
		writer.WriteHeader(404)
	case "/v1/a/c/o001":
		writer.WriteHeader(200)
		body = "Just a test"
	case "/v1/a/c/s001":
		writer.WriteHeader(404)
	case "/v1/a/c/s001/":
		writer.WriteHeader(404)
	case "/v1/a/c/s002":
		writer.WriteHeader(404)
	case "/v1/a/c/s002/":
		writer.WriteHeader(404)
	case "/v1/a/c/o002":
		writer.WriteHeader(404)
	case "/v1/a/c/index.html":
		writer.WriteHeader(200)
		body = "Just a test index file"
	case "/v1/a/c/s002/index.html":
		writer.WriteHeader(200)
		body = "Just another test index file"
	case "/v1/a/c/notfound":
		writer.WriteHeader(404)
	case "/v1/a/c/error001":
		writer.WriteHeader(404)
	case "/v1/a/c/error002":
		writer.WriteHeader(401)
	case "/v1/a/c/404error.html":
		writer.WriteHeader(200)
		body = "404errorpage"
	case "/v1/a/c/401error.html":
		writer.WriteHeader(200)
		body = "401errorpage"
	default:
		writer.WriteHeader(999)
	}
	if request.Method != "HEAD" && body != "" {
		writer.Write([]byte(body))
	}
}

func TestStaticWebGetObject(t *testing.T) {
	next := &testNext{}
	s, m := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/o001", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C:                      client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{"Web-Index": "index.html"}}}),
		accountInfoCache:       map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if m.Value() != 1 {
		t.Fatal(m.Value())
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	// HEAD to check for object existence; if it didn't exist an attempt to treat it as a directory request would be made.
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/o001" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	// GET for the object to return to the client.
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c/o001" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebGetObjectNotThere(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/o002", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	if resp.StatusCode != 404 {
		t.Fatal(resp.StatusCode)
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	// HEAD to check for object existence; since it doesn't exist an attempt will be made to treat it as a directory.
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/o002" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	// GET the prefixed container listing; which returns an empty list so overall 404 for the object request.
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c" || next.requests[i].URL.Query().Get("prefix") != "o002/" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path + "?prefix=" + next.requests[i].URL.Query().Get("prefix"))
	}
}

func TestStaticWebGetSubdirRedirect(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/s001", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	// 301 because we didn't request with a trailing /
	if resp.StatusCode != 301 {
		t.Fatal(resp.StatusCode)
	}
	if resp.Header.Get("Location") != "/v1/a/c/s001/" {
		t.Fatal(resp.Header.Get("Location"))
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	// HEAD to check for object existence; since it doesn't exist an attempt will be made to treat it as a directory.
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/s001" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	// GET the prefixed container listing.
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c" || next.requests[i].URL.Query().Get("prefix") != "s001/" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path + "?prefix=" + next.requests[i].URL.Query().Get("prefix"))
	}
}

func TestStaticWebGetSubdir(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/s001/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	// Just quick checks that we got what looks like the right listing.
	if !strings.Contains(body, "Listing of /v1/a/c/s001/") {
		t.Fatal("")
	}
	if !strings.Contains(body, `"one.html"`) {
		t.Fatal("")
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	// HEAD to check for object existence; since it doesn't exist an attempt will be made to treat it as a directory.
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/s001/" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	// GET the prefixed container listing.
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c" || next.requests[i].URL.Query().Get("prefix") != "s001/" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path + "?prefix=" + next.requests[i].URL.Query().Get("prefix"))
	}
}

func TestStaticWebGetContainerRedirect(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	// 301 because we didn't request with a trailing /
	if resp.StatusCode != 301 {
		t.Fatal(resp.StatusCode)
	}
	if resp.Header.Get("Location") != "/v1/a/c/" {
		t.Fatal(resp.Header.Get("Location"))
	}
	if len(next.requests) != 0 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
}

func TestStaticWebGetContainer(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	// Just quick checks that we got what looks like the right listing.
	if !strings.Contains(body, "Listing of /v1/a/c/") {
		t.Fatal("")
	}
	if !strings.Contains(body, `"s001/"`) {
		t.Fatal("")
	}
	if len(next.requests) != 1 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebGetWithWebIndex(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
			"Web-Index":    "index.html",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if body != "Just a test index file" {
		t.Fatal("")
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/index.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c/index.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebGetSubdirWithWebIndex(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/s002/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
			"Web-Index":    "index.html",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if body != "Just another test index file" {
		t.Fatal("")
	}
	if len(next.requests) != 3 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/s002/" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/s002/index.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c/s002/index.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebGetSubdirWithWebIndexRedirect(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/s002", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
			"Web-Index":    "index.html",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	if resp.StatusCode != 301 {
		t.Fatal(resp.StatusCode)
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/s002" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/s002/index.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebGetContainerNoListings(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Index": "notfound",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 404 {
		t.Fatal(resp.StatusCode)
	}
	// Quick check that we got something that looks like a Web Listing Disabled help page.
	if !strings.Contains(body, "Web Listing Disabled") {
		t.Fatal("")
	}
	if len(next.requests) != 1 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/notfound" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebGetContainerCustomCSS(t *testing.T) {
	// First section to show CSS reference.
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings":     "true",
			"Web-Listings-Css": "listings.css",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	// Quick check that we got something that looks like a CSS reference.
	if !strings.Contains(body, `href="listings.css"`) {
		t.Fatal("")
	}
	if len(next.requests) != 1 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}

	// Second section to show no CSS reference.
	next = &testNext{}
	s, _ = newTestStaticWebHandler(next)
	request, err = http.NewRequest("GET", "/v1/a/c/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Listings": "true",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp = rec.Result()
	bodyBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body = string(bodyBytes)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	// Quick check that we did not get something that looks like a CSS reference.
	if strings.Contains(body, `href="listings.css"`) {
		t.Fatal("")
	}
	if len(next.requests) != 1 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i = 0
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebCustomErrorPages(t *testing.T) {
	// First section for 404 test.
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c/error001", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Error": "error.html",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	request.Header.Set("X-Web-Mode", "t")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != 404 {
		t.Fatal(resp.StatusCode)
	}
	if body != "404errorpage" {
		t.Fatal(body)
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/error001" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c/404error.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}

	// First section for 401 test.
	next = &testNext{}
	s, _ = newTestStaticWebHandler(next)
	request, err = http.NewRequest("GET", "/v1/a/c/error002", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C: client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c": {Metadata: map[string]string{
			"Web-Error": "error.html",
		}}}),
		accountInfoCache: map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp = rec.Result()
	bodyBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body = string(bodyBytes)
	if resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
	if body != "401errorpage" {
		t.Fatal("")
	}
	if len(next.requests) != 2 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i = 0
	if next.requests[i].Method != "HEAD" || next.requests[i].URL.Path != "/v1/a/c/error002" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
	i++
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c/401error.html" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}

func TestStaticWebNoContainerInfo(t *testing.T) {
	next := &testNext{}
	s, _ := newTestStaticWebHandler(next)
	request, err := http.NewRequest("GET", "/v1/a/c2/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request = request.WithContext(context.WithValue(request.Context(), "proxycontext", &ProxyContext{
		ProxyContextMiddleware: &ProxyContextMiddleware{next: s},
		Logger:                 zap.NewNop(),
		C:                      client.NewProxyClient(&client.ProxyDirectClient{}, nil, map[string]*client.ContainerInfo{"container/a/c2": client.NilContainerInfo}),
		accountInfoCache:       map[string]*AccountInfo{"account/a": {Metadata: map[string]string{}}},
	}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, request)
	resp := rec.Result()
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal(resp.StatusCode)
	}
	if len(next.requests) != 1 {
		for _, r := range next.requests {
			fmt.Printf("%s %s\n", r.Method, r.URL.Path)
		}
		t.Fatal(len(next.requests))
	}
	i := 0
	if next.requests[i].Method != "GET" || next.requests[i].URL.Path != "/v1/a/c2/" {
		t.Fatal(next.requests[i].Method + " " + next.requests[i].URL.Path)
	}
}
