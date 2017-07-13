package client

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type autoCloseResponses struct {
	http.RoundTripper
}

func (a *autoCloseResponses) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := a.RoundTripper.RoundTrip(req)
	resp.Body = &autoClosingBody{ReadCloser: resp.Body, timer: time.AfterFunc(time.Minute, func() { autoCloseReport(req, resp) })}
	return resp, err
}

type autoClosingBody struct {
	io.ReadCloser
	timer *time.Timer
}

func (a *autoClosingBody) Close() error {
	a.timer.Stop()
	return a.ReadCloser.Close()
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
