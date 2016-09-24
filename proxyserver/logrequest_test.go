package proxyserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type SysLogMock struct {
}

func (s *SysLogMock) Err(string) error {
	return nil
}

func (s *SysLogMock) Info(string) error {
	return nil
}

func (s *SysLogMock) Debug(string) error {
	return nil
}

func TestTransactionIdHeader(t *testing.T) {
	proxy := &ProxyServer{logger: &SysLogMock{}}
	ts := httptest.NewServer(proxy.LogRequest(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	})))
	defer ts.Close()

	req, err := http.NewRequest("GET", ts.URL, nil)
	if err != nil {
		t.Error("failed to create http request")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error("failed to send http request")
	}

	if resp.Header.Get("X-Trans-Id") == "" {
		t.Error("Response should contain X-Trans-Id header")
	}
}
