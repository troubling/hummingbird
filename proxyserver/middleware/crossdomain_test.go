package middleware

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"

	"github.com/stretchr/testify/require"
)

func TestCrossDomain(t *testing.T) {
	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(200)
		w.Write([]byte("NO"))
	})
	section := conf.Section{}
	testScope := common.NewTestScope()
	crossdomain, _ := NewCrossDomain(section, testScope)
	h := crossdomain(passthrough)
	req, err := http.NewRequest("GET", "/crossdomain.xml", nil)
	require.Nil(t, err)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	require.Equal(t, 200, resp.StatusCode)
	// Now this is ugly :)
	require.Equal(t, "<?xml version=\"1.0\"?>\n<!DOCTYPE cross-domain-policy SYSTEM "+
		"\"http://www.adobe.com/xml/dtds/cross-domain-policy.dtd\" >\n"+
		"<cross-domain-policy>\n<allow-access-from domain=\"*\" secure=\"false\" />\n"+
		"</cross-domain-policy>", string(body))

	req, err = http.NewRequest("GET", "/anythingelse", nil)
	require.Nil(t, err)

	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "NO", string(body))
}
