//  Copyright (c) 2018 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package client

import (
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"

	"crypto/tls"
	"fmt"
	"net/http/httptrace"
)

// ErrValidTracerRequired error
var ErrValidTracerRequired = errors.New("valid tracer required")

// TracingClient holds a opentracing instrumented HTTP Client.
type TracingClient struct {
	*http.Client
	tracer    opentracing.Tracer
	httpTrace bool
}

// NewTracingClient returns an HTTP Client adding Opentracing instrumentation around an
// embedded standard Go http.Client.
func NewTracingClient(tracer opentracing.Tracer, httpClient *http.Client, enableHTTPTrace bool) (*TracingClient, error) {
	if tracer == nil {
		return nil, ErrValidTracerRequired
	}

	tc := &TracingClient{tracer: tracer, Client: httpClient, httpTrace: enableHTTPTrace}

	transport, err := NewTransport(tracer, tc.Client.Transport, enableHTTPTrace)
	if err != nil {
		return nil, err
	}
	tc.Client.Transport = transport

	return tc, nil
}

// Do wraps http.Client's Do with tracing using an application span.
func (tc *TracingClient) Do(req *http.Request) (*http.Response, error) {
	var parentContext opentracing.SpanContext

	if span := opentracing.SpanFromContext(req.Context()); span != nil {
		parentContext = span.Context()
	}

	appSpan := tc.tracer.StartSpan("HTTP Client", opentracing.ChildOf(parentContext))

	ext.HTTPMethod.Set(appSpan, req.Method)
	ext.HTTPUrl.Set(appSpan, req.URL.String())

	res, err := tc.Client.Do(
		req.WithContext(opentracing.ContextWithSpan(req.Context(), appSpan)),
	)
	if err != nil {
		ext.Error.Set(appSpan, true)
		appSpan.SetTag("error", err.Error())
		appSpan.Finish()
		return res, err
	}

	ext.HTTPStatusCode.Set(appSpan, uint16(res.StatusCode))
	if res.StatusCode > 399 {
		ext.Error.Set(appSpan, true)
	}
	if req.Method == "HEAD" {
		appSpan.Finish()
	} else {
		res.Body = &spanCloser{
			ReadCloser:   res.Body,
			sp:           appSpan,
			traceEnabled: tc.httpTrace,
		}
	}
	return res, nil
}

type transport struct {
	tracer    opentracing.Tracer
	rt        http.RoundTripper
	httpTrace bool
}

// NewTransport returns a new Opentracing instrumented http RoundTripper
func NewTransport(tracer opentracing.Tracer, rt http.RoundTripper, enableHTTPTrace bool) (http.RoundTripper, error) {
	if tracer == nil {
		return nil, ErrValidTracerRequired
	}
	if rt == nil {
		rt = http.DefaultTransport
	}
	t := &transport{
		tracer:    tracer,
		rt:        rt,
		httpTrace: enableHTTPTrace,
	}

	return t, nil
}

// RoundTrip satisfies the RoundTripper interface.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	parent := opentracing.SpanFromContext(req.Context())
	var spanctx opentracing.SpanContext
	if parent != nil {
		spanctx = parent.Context()
	}
	sp := t.tracer.StartSpan("HTTP "+req.Method, opentracing.ChildOf(spanctx))
	ext.SpanKindRPCClient.Set(sp)

	if t.httpTrace {
		sptr := spanTrace{
			Span: sp,
		}
		sptr.c = &httptrace.ClientTrace{
			GetConn:              sptr.getConn,
			GotConn:              sptr.gotConn,
			PutIdleConn:          sptr.putIdleConn,
			GotFirstResponseByte: sptr.gotFirstResponseByte,
			Got100Continue:       sptr.got100Continue,
			DNSStart:             sptr.dnsStart,
			DNSDone:              sptr.dnsDone,
			ConnectStart:         sptr.connectStart,
			ConnectDone:          sptr.connectDone,
			TLSHandshakeStart:    sptr.tlsHandshakeStart,
			TLSHandshakeDone:     sptr.tlsHandshakeDone,
			WroteHeaders:         sptr.wroteHeaders,
			Wait100Continue:      sptr.wait100Continue,
			WroteRequest:         sptr.wroteRequest,
		}

		req = req.WithContext(
			httptrace.WithClientTrace(req.Context(), sptr.c),
		)
	}

	ext.HTTPMethod.Set(sp, req.Method)
	ext.HTTPUrl.Set(
		sp,
		fmt.Sprintf("%s://%s%s", req.URL.Scheme, req.URL.Host, req.URL.Path),
	)
	host, portString, err := net.SplitHostPort(req.URL.Host)
	if err == nil {
		ext.PeerHostname.Set(sp, host)
		if port, err := strconv.Atoi(portString); err != nil {
			ext.PeerPort.Set(sp, uint16(port))
		}
	} else {
		ext.PeerHostname.Set(sp, req.URL.Host)
	}

	carrier := opentracing.HTTPHeadersCarrier(req.Header)
	//TODO nadeem: check & log error
	sp.Tracer().Inject(sp.Context(), opentracing.TextMap, carrier)

	res, err := t.rt.RoundTrip(req)

	if err != nil {
		sp.SetTag("error", err.Error())
		sp.Finish()
		return res, err
	}

	ext.HTTPStatusCode.Set(sp, uint16(res.StatusCode))
	if res.StatusCode > 399 {
		ext.Error.Set(sp, true)
	}

	sp.Finish()
	return res, nil
}

type spanTrace struct {
	opentracing.Span
	c *httptrace.ClientTrace
}

func (s *spanTrace) getConn(hostPort string) {
	s.LogFields(log.String("event", "GetConn"))
	ext.HTTPUrl.Set(s, hostPort)
}

func (s *spanTrace) gotConn(info httptrace.GotConnInfo) {
	s.LogFields(log.String("event", "GotConn"))
	s.SetTag("httptrace.got_connection.reused", fmt.Sprintf("%t", info.Reused))
	s.SetTag("httptrace.got_connection.was_idle", fmt.Sprintf("%t", info.WasIdle))
	if info.WasIdle {
		s.SetTag("httptrace.got_connection.idle_time", info.IdleTime.String())
	}
}

func (s *spanTrace) putIdleConn(err error) {
	s.LogFields(log.String("event", "PutIdleConn"))
	if err != nil {
		s.SetTag("httptrace.put_idle_connection.error", err.Error())
	}
}

func (s *spanTrace) gotFirstResponseByte() {
	s.LogFields(log.String("event", "GotFirstResponseByte"))
}

func (s *spanTrace) got100Continue() {
	s.LogFields(log.String("event", "Got100Continue"))
}

func (s *spanTrace) dnsStart(info httptrace.DNSStartInfo) {
	s.LogFields(
		log.String("event", "DNSStart"),
		log.String("host", info.Host),
	)
}

func (s *spanTrace) dnsDone(info httptrace.DNSDoneInfo) {
	fields := []log.Field{log.String("event", "DNSDone")}
	for _, addr := range info.Addrs {
		fields = append(fields, log.String("addr", addr.String()))
	}
	if info.Err != nil {
		fields = append(fields, log.Error(info.Err))
	}
	s.LogFields(fields...)
}

func (s *spanTrace) connectStart(network, addr string) {
	s.LogFields(
		log.String("event", "ConnectStart"),
		log.String("network", network),
		log.String("addr", addr),
	)
}

func (s *spanTrace) connectDone(network, addr string, err error) {
	if err != nil {
		s.LogFields(
			log.String("message", "ConnectDone"),
			log.String("network", network),
			log.String("addr", addr),
			log.String("event", "error"),
			log.Error(err),
		)
	} else {
		s.LogFields(
			log.String("event", "ConnectDone"),
			log.String("network", network),
			log.String("addr", addr),
		)
	}
}

func (s *spanTrace) tlsHandshakeStart() {
	s.LogFields(log.String("event", "TLS Handshake Start"))
}

func (s *spanTrace) tlsHandshakeDone(_ tls.ConnectionState, err error) {
	s.LogFields(log.String("event", "TLS Handshake Done"))
	if err != nil {
		s.SetTag("httptrace.tls_handshake_done.error", err.Error())
	}
}

func (s *spanTrace) wroteHeaders() {
	s.LogFields(log.String("event", "WroteHeaders"))
}

func (s *spanTrace) wait100Continue() {
	s.LogFields(log.String("event", "Wait100Continue"))
}

func (s *spanTrace) wroteRequest(info httptrace.WroteRequestInfo) {
	if info.Err != nil {
		s.LogFields(
			log.String("message", "WroteRequest"),
			log.String("event", "error"),
			log.Error(info.Err),
		)
		ext.Error.Set(s, true)
	} else {
		s.LogFields(log.String("event", "WroteRequest"))
	}
}

type spanCloser struct {
	io.ReadCloser
	sp           opentracing.Span
	traceEnabled bool
}

func (s *spanCloser) Close() (err error) {
	if s.traceEnabled {
		s.sp.LogFields(log.String("event", "ClosedBody"))
	}
	err = s.ReadCloser.Close()
	s.sp.Finish()
	return
}
