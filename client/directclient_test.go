package client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/ring"
)

func TestAddUpdateHeaders(t *testing.T) {
	devices := []*ring.Device{
		{Ip: "127.0.0.1", Port: 1212, Device: "sda"},
		{Ip: "127.0.0.2", Port: 2345, Device: "sdb"},
		{Ip: "127.0.0.3", Port: 6789, Device: "sdc"},
	}
	// 3 object replicas
	headers := make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 0, 3)
	require.Equal(t, "127.0.0.1:1212", headers.Get("X-Container-Host"))
	require.Equal(t, "sda", headers.Get("X-Container-Device"))

	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 1, 3)
	require.Equal(t, "127.0.0.2:2345", headers.Get("X-Container-Host"))
	require.Equal(t, "sdb", headers.Get("X-Container-Device"))

	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 2, 3)
	require.Equal(t, "127.0.0.3:6789", headers.Get("X-Container-Host"))
	require.Equal(t, "sdc", headers.Get("X-Container-Device"))

	// 2 object replicas - comma-separated entries
	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 0, 2)
	require.Equal(t, "127.0.0.1:1212,127.0.0.3:6789", headers.Get("X-Container-Host"))
	require.Equal(t, "sda,sdc", headers.Get("X-Container-Device"))

	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 1, 2)
	require.Equal(t, "127.0.0.2:2345", headers.Get("X-Container-Host"))
	require.Equal(t, "sdb", headers.Get("X-Container-Device"))

	// 4 object replicas - last one should be empty.
	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 0, 4)
	require.Equal(t, "127.0.0.1:1212", headers.Get("X-Container-Host"))
	require.Equal(t, "sda", headers.Get("X-Container-Device"))

	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 1, 4)
	require.Equal(t, "127.0.0.2:2345", headers.Get("X-Container-Host"))
	require.Equal(t, "sdb", headers.Get("X-Container-Device"))

	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 2, 4)
	require.Equal(t, "127.0.0.3:6789", headers.Get("X-Container-Host"))
	require.Equal(t, "sdc", headers.Get("X-Container-Device"))

	headers = make(http.Header)
	addUpdateHeaders("X-Container", headers, devices, 3, 4)
	require.Equal(t, "", headers.Get("X-Container-Host"))
	require.Equal(t, "", headers.Get("X-Container-Device"))
}
