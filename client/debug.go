package client

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type autoCloseResponses struct {
	transport http.RoundTripper
}

func (a *autoCloseResponses) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := a.transport.RoundTrip(req)
	resp.Body = &autoClosingBody{delegate: resp.Body, timer: time.AfterFunc(time.Minute, func() { autoCloseReport(req, resp) })}
	return resp, err
}

type autoClosingBody struct {
	delegate io.ReadCloser
	timer    *time.Timer
}

func (a *autoClosingBody) Read(buf []byte) (int, error) {
	return a.delegate.Read(buf)
}

func (a *autoClosingBody) Close() error {
	a.timer.Stop()
	return a.delegate.Close()
}

func autoCloseReport(req *http.Request, resp *http.Response) {
	fmt.Fprintf(os.Stderr, `
################################################################################
# Auto Closing Response
# Request: %s %#v %#v
# Response: %d %#v
################################################################################
`, req.Method, req.URL, req.Header, resp.StatusCode, resp.Header)
}
