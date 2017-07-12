package middleware

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
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

	p := &PipeResponse{}

	body, header, code := p.Get("/ver/a/c/o", dummy, "test", nil)

	require.Equal(t, 200, code)
	buf := make([]byte, 1024)
	num, err := body.Read(buf)
	require.Nil(t, err)
	require.Equal(t, 5, num)
	require.Equal(t, "stuff", string(buf[:5]))
	require.NotNil(t, header)
	require.NotNil(t, body)
}
