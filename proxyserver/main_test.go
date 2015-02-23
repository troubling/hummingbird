package proxyserver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthCheck(t *testing.T) {
	expectedBody := "OK"
	conf := "/etc/swift/proxy-server.conf"
	ip, port, handler, _ := GetServer(conf)
	recorder := httptest.NewRecorder()
	url := fmt.Sprintf("http://%s:%d/healthcheck", ip, port)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Error("Unable to create new Request")
	}
	handler.ServeHTTP(recorder, req)

	if recorder.Body.String() != expectedBody {
		t.Error("Excepting ", expectedBody, " got ", recorder.Body.String())
	}
}
