//  Copyright (c) 2015 Rackspace
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

package srv

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/http2"
)

var responseTemplate = "<html><h1>%s</h1><p>%s</p></html>"

var responseBodies = map[int]string{
	100: "",
	200: "",
	201: "",
	202: fmt.Sprintf(responseTemplate, "Accepted", "The request is accepted for processing."),
	204: "",
	206: "",
	301: fmt.Sprintf(responseTemplate, "Moved Permanently", "The resource has moved permanently."),
	302: fmt.Sprintf(responseTemplate, "Found", "The resource has moved temporarily."),
	303: fmt.Sprintf(responseTemplate, "See Other", "The response to the request can be found under a different URI."),
	304: "",
	307: fmt.Sprintf(responseTemplate, "Temporary Redirect", "The resource has moved temporarily."),
	400: fmt.Sprintf(responseTemplate, "Bad Request", "The server could not comply with the request since it is either malformed or otherwise incorrect."),
	401: fmt.Sprintf(responseTemplate, "Unauthorized", "This server could not verify that you are authorized to access the document you requested."),
	402: fmt.Sprintf(responseTemplate, "Payment Required", "Access was denied for financial reasons."),
	403: fmt.Sprintf(responseTemplate, "Forbidden", "Access was denied to this resource."),
	404: fmt.Sprintf(responseTemplate, "Not Found", "The resource could not be found."),
	405: fmt.Sprintf(responseTemplate, "Method Not Allowed", "The method is not allowed for this resource."),
	406: fmt.Sprintf(responseTemplate, "Not Acceptable", "The resource is not available in a format acceptable to your browser."),
	408: fmt.Sprintf(responseTemplate, "Request Timeout", "The server has waited too long for the request to be sent by the client."),
	409: fmt.Sprintf(responseTemplate, "Conflict", "There was a conflict when trying to complete your request."),
	410: fmt.Sprintf(responseTemplate, "Gone", "This resource is no longer available."),
	411: fmt.Sprintf(responseTemplate, "Length Required", "Content-Length header required."),
	412: "",
	413: fmt.Sprintf(responseTemplate, "Request Entity Too Large", "The body of your request was too large for this server."),
	414: fmt.Sprintf(responseTemplate, "Request URI Too Long", "The request URI was too long for this server."),
	415: fmt.Sprintf(responseTemplate, "Unsupported Media Type", "The request media type is not supported by this server."),
	416: fmt.Sprintf(responseTemplate, "Requested Range Not Satisfiable", "The Range requested is not available."),
	417: fmt.Sprintf(responseTemplate, "Expectation Failed", "Expectation failed."),
	422: fmt.Sprintf(responseTemplate, "Unprocessable Entity", "Unable to process the contained instructions"),
	498: fmt.Sprintf(responseTemplate, "Ratelimited", "The client is sending too many requests and should slow down."),
	499: fmt.Sprintf(responseTemplate, "Client Disconnect", "The client was disconnected during request."),
	500: fmt.Sprintf(responseTemplate, "Internal Error", "The server has either erred or is incapable of performing the requested operation."),
	501: fmt.Sprintf(responseTemplate, "Not Implemented", "The requested method is not implemented by this server."),
	502: fmt.Sprintf(responseTemplate, "Bad Gateway", "Bad gateway."),
	503: fmt.Sprintf(responseTemplate, "Service Unavailable", "The server is currently unavailable. Please try again at a later time."),
	504: fmt.Sprintf(responseTemplate, "Gateway Timeout", "A timeout has occurred speaking to a backend server."),
}

type customWriter struct {
	http.ResponseWriter
	f func(w http.ResponseWriter, status int) int
}

type IpPort struct {
	Ip                string
	Port              int
	CertFile, KeyFile string
}

func (w *customWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(w.f(w.ResponseWriter, status))
}

func (w *customWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

// NewCustomWriter creates an http.ResponseWriter wrapper that calls your function on WriteHeader.
func NewCustomWriter(w http.ResponseWriter, f func(w http.ResponseWriter, status int) int) http.ResponseWriter {
	return &customWriter{ResponseWriter: w, f: f}
}

// ResponseWriter that saves its status - used for logging.

type WebWriter struct {
	http.ResponseWriter
	Status          int
	ResponseStarted time.Time
	ByteCount       int
}

func (w *WebWriter) WriteHeader(status int) {
	w.Status = status
	w.ResponseStarted = time.Now()
	w.ResponseWriter.WriteHeader(status)
}

func (w WebWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

func (w *WebWriter) Response() (time.Time, int) {
	return w.ResponseStarted, w.Status
}

func (w *WebWriter) Write(b []byte) (n int, err error) {
	n, err = w.ResponseWriter.Write(b)
	w.ByteCount += n
	return n, err
}

type CountingReadCloser struct {
	io.ReadCloser
	ByteCount int
}

func (crc *CountingReadCloser) Read(b []byte) (n int, err error) {
	n, err = crc.ReadCloser.Read(b)
	crc.ByteCount += n
	return n, err
}

func CopyResponseHeaders(w http.ResponseWriter, src *http.Response) {
	for key := range src.Header {
		w.Header().Set(key, src.Header.Get(key))
	}
}

func StandardResponse(w http.ResponseWriter, statusCode int) {
	body := responseBodies[statusCode]
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

func SimpleErrorResponse(w http.ResponseWriter, statusCode int, body string) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

func CustomErrorResponse(w http.ResponseWriter, statusCode int, vars map[string]string) {
	body := ""
	switch statusCode {
	case 507:
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		if vars["Method"] != "HEAD" {
			body = fmt.Sprintf("<html><h1>Insufficient Storage</h1><p>There was not enough space to save the resource. Drive: %s</p></html>", vars["device"])
		}
	}
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

func CopyRequestHeaders(r *http.Request, dst *http.Request) {
	for key := range r.Header {
		dst.Header.Set(key, r.Header.Get(key))
	}
}

type WebWriterInterface interface {
	http.ResponseWriter
	Response() (time.Time, int)
}

func ValidateRequest(w http.ResponseWriter, r *http.Request) bool {
	// if invalid request will right own response and return false, otherwise true
	if !utf8.ValidString(r.URL.Path) || strings.Contains(r.URL.Path, "\x00") {
		SimpleErrorResponse(w, 412, "Invalid UTF8 or contains NULL")
		return false
	}
	if !utf8.ValidString(r.Header.Get("Content-Type")) || strings.Contains(r.Header.Get("Content-Type"), "\x00") {
		SimpleErrorResponse(w, 400, "Invalid UTF8 or contains NULL")
		return false
	}
	return true
}

type LowLevelLogger interface {
	Error(msg string, fields ...zapcore.Field)
	Info(msg string, fields ...zapcore.Field)
	Debug(msg string, fields ...zapcore.Field)
	With(fields ...zapcore.Field) *zap.Logger
}

func LogPanics(logger LowLevelLogger, msg string) {
	if e := recover(); e != nil {
		recoveredMsg := fmt.Sprintf("PANIC (%s)", msg)
		logger.Error(recoveredMsg, zap.Any("err", e))
	}
}

// SetupLogger configures structured logging using uber's zap library.
func SetupLogger(prefix string, atomicLevel *zap.AtomicLevel, flags *flag.FlagSet) (LowLevelLogger, error) {

	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= atomicLevel.Level() && lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= atomicLevel.Level() && lvl < zapcore.ErrorLevel
	})

	var lowPrioFile, highPrioFile zapcore.WriteSyncer
	var err error
	name := "stdout"
	if lFlag := flags.Lookup("l"); lFlag != nil {
		name = lFlag.Value.(flag.Getter).Get().(string)
	}
	if name == "" {
		name = "stdout"
	}
	switch name {
	case "stdout", "stderr":
		lowPrioFile, _, err = zap.Open(name)
		if err != nil {
			return nil, fmt.Errorf("Unable to open low priority logger: %s %v", name, err)
		}
	default:
		lowPrioFile, err = os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("Unable to open file for low priority logger: %s %v", name, err)
		}
		err := syscall.Dup2(int(lowPrioFile.(*os.File).Fd()), int(os.Stdout.Fd()))
		if err != nil {
			return nil, fmt.Errorf("Unable to redirect STDOUT: %s", err)
		}
	}
	name = "stderr"
	if eFlag := flags.Lookup("e"); eFlag != nil {
		name = eFlag.Value.(flag.Getter).Get().(string)
	}
	if name == "" {
		name = "stderr"
	}
	switch name {
	case "stdout", "stderr":
		highPrioFile, _, err = zap.Open(name)
		if err != nil {
			return nil, fmt.Errorf("Unable to open high priority logger: %s %v", name, err)
		}
	default:
		highPrioFile, err = os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("Unable to open file for high priority logger: %s %v", name, err)
		}
		err := syscall.Dup2(int(highPrioFile.(*os.File).Fd()), int(os.Stderr.Fd()))
		if err != nil {
			return nil, fmt.Errorf("Unable to redirect STDERR: %s", err)
		}
	}

	infos := zapcore.AddSync(lowPrioFile)
	errors := zapcore.AddSync(highPrioFile)

	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, infos, lowPriority),
		zapcore.NewCore(encoder, errors, highPriority),
	)

	baseLogger := zap.New(core)
	logger := baseLogger.With(zap.String("name", prefix))
	return logger, nil
}

// LogRequest is a middleware that logs requests and also sets up a logger in
// the request context.
func LogRequest(logger LowLevelLogger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		start := time.Now()
		newWriter := &WebWriter{ResponseWriter: writer, Status: 500}
		newReader := &CountingReadCloser{ReadCloser: request.Body}
		request.Body = newReader
		logr := logger.With(zap.String("txn", request.Header.Get("X-Trans-Id")))
		request = SetLogger(request, logr)
		next.ServeHTTP(newWriter, request)
		LogRequestLine(logr, request, start, newWriter, newReader)
	})
}

func LogRequestLine(logger *zap.Logger, request *http.Request, start time.Time, newWriter *WebWriter, newReader *CountingReadCloser) {
	if newWriter.Status/100 != 2 || request.Header.Get("X-Backend-Suppress-2xx-Logging") != "t" || logger.Core().Enabled(zap.DebugLevel) {
		extraInfo := "-"
		if request.Header.Get("X-Force-Acquire") == "true" {
			extraInfo = "FA"
		}
		logger.Info(
			"Request log",
			zap.String("remoteAddr", common.GetDefault(request.Header, "X-Forwarded-For", request.RemoteAddr)),
			zap.String("eventTime", time.Now().Format("02/Jan/2006:15:04:05 -0700")),
			zap.String("method", request.Method),
			zap.String("urlPath", common.Urlencode(request.URL.Path)),
			zap.Int("status", newWriter.Status),
			zap.Int("contentBytesIn", newReader.ByteCount),
			zap.Int("contentBytesOut", newWriter.ByteCount),
			zap.String("contentLengthIn", common.GetDefault(request.Header, "Content-Length", "-")),
			zap.String("contentLengthOut", common.GetDefault(newWriter.Header(), "Content-Length", "-")),
			zap.String("referer", common.GetDefault(request.Header, "Referer", "-")),
			zap.String("userAgent", common.GetDefault(request.Header, "User-Agent", "-")),
			zap.Float64("requestTimeSeconds", time.Since(start).Seconds()),
			zap.Float64("requestTimeToHeaderSeconds", newWriter.ResponseStarted.Sub(start).Seconds()),
			zap.String("extraInfo", extraInfo),
		)
	}
}

type ConfigLoader interface {
	GetHashPrefixAndSuffix() (string, string, error)
	GetPolicies() (conf.PolicyList, error)
	GetSyncRealms() (conf.SyncRealmList, error)
	GetRing(ringType, prefix, suffix string, policy int) (ring.Ring, error)
}

type DefaultConfigLoader struct{}

func (d DefaultConfigLoader) GetHashPrefixAndSuffix() (string, string, error) {
	return conf.GetHashPrefixAndSuffix()
}

func (d DefaultConfigLoader) GetPolicies() (conf.PolicyList, error) {
	return conf.GetPolicies()
}

func (d DefaultConfigLoader) GetSyncRealms() (conf.SyncRealmList, error) {
	return conf.GetSyncRealms()
}

func (d DefaultConfigLoader) GetRing(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
	return ring.GetRing(ringType, prefix, suffix, policy)
}

type TestConfigLoader struct {
	DefaultConfigLoader
	GetHashPrefixAndSuffixFunc func() (string, string, error)
	GetPoliciesFunc            func() (conf.PolicyList, error)
	GetSyncRealmsFunc          func() (conf.SyncRealmList, error)
	GetRingFunc                func(ringType, prefix, suffix string, policy int) (ring.Ring, error)
}

func (t *TestConfigLoader) GetHashPrefixAndSuffix() (string, string, error) {
	if t.GetHashPrefixAndSuffixFunc != nil {
		return t.GetHashPrefixAndSuffixFunc()
	} else {
		return t.DefaultConfigLoader.GetHashPrefixAndSuffix()
	}
}

func (t *TestConfigLoader) GetPolicies() (conf.PolicyList, error) {
	if t.GetPoliciesFunc != nil {
		return t.GetPoliciesFunc()
	} else {
		return t.DefaultConfigLoader.GetPolicies()
	}
}

func (t *TestConfigLoader) GetSyncRealms() (conf.SyncRealmList, error) {
	if t.GetSyncRealmsFunc != nil {
		return t.GetSyncRealmsFunc()
	} else {
		return t.DefaultConfigLoader.GetSyncRealms()
	}
}

func (t *TestConfigLoader) GetRing(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
	if t.GetRingFunc != nil {
		return t.GetRingFunc(ringType, prefix, suffix, policy)
	} else {
		return t.DefaultConfigLoader.GetRing(ringType, prefix, suffix, policy)
	}
}

func NewTestConfigLoader(testRing ring.Ring) *TestConfigLoader {
	confLoader := &TestConfigLoader{
		GetRingFunc: func(ringType, prefix, suffix string, policy int) (ring.Ring, error) {
			return testRing, nil
		},
		GetPoliciesFunc: func() (conf.PolicyList, error) {
			return conf.PolicyList(map[int]*conf.Policy{
				0: {
					Index:      0,
					Type:       "replication",
					Name:       "Policy-0",
					Aliases:    nil,
					Default:    false,
					Deprecated: false,
				},
				1: {
					Index:      1,
					Type:       "replication",
					Name:       "Policy-1",
					Aliases:    nil,
					Default:    false,
					Deprecated: false,
				},
				2: {
					Index:      2,
					Type:       "hec",
					Name:       "HecPolicy-2",
					Aliases:    nil,
					Default:    false,
					Deprecated: false,
					Config:     map[string]string{"data_frags": "2", "parity_frags": "1", "nursery_replicas": "1"},
				},
			}), nil
		},
		GetHashPrefixAndSuffixFunc: func() (string, string, error) {
			return "changeme", "changeme", nil
		},
	}
	return confLoader
}

/* http.Server that knows how to shut down gracefully */

type HummingbirdServer struct {
	*http.Server
	logger   LowLevelLogger
	finalize func()
}

func RetryListen(ip string, port int) (net.Listener, error) {
	address := fmt.Sprintf("%s:%d", ip, port)
	started := time.Now()
	for {
		if sock, err := net.Listen("tcp", address); err == nil {
			return sock, nil
		} else if time.Now().Sub(started) > 10*time.Second {
			return nil, fmt.Errorf("Failed to bind for 10 seconds (%v)", err)
		}
		time.Sleep(time.Second / 5)
	}
}

func DumpGoroutinesStackTrace(pid int) {
	filename := filepath.Join("/tmp", strconv.Itoa(pid)+".dump")
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	ioutil.WriteFile(filename, buf, 0644)
}

type Server interface {
	Type() string
	Background(flags *flag.FlagSet) chan struct{}
	GetHandler(config conf.Config, metricsPrefix string) http.Handler
	Finalize() // This is called before stoping gracefully so that a server can clean up before closing
}

func RunServers(getServer func(conf.Config, *flag.FlagSet, ConfigLoader) (*IpPort, Server, LowLevelLogger, error), flags *flag.FlagSet) {
	var servers []*HummingbirdServer

	if flags.NArg() != 0 {
		flags.Usage()
		return
	}
	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	configs, err := conf.LoadConfigs(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error finding configs: %v\n", err)
		return
	}
	var wg *sync.WaitGroup

	for _, config := range configs {
		ipPort, server, logger, err := getServer(config, flags, DefaultConfigLoader{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		var metricsPrefix string
		if len(configs) == 1 {
			metricsPrefix = fmt.Sprintf("hb_%s", server.Type())
		} else {
			metricsPrefix = fmt.Sprintf("hb_%s_%s_%d", server.Type(), ipPort.Ip, ipPort.Port)
		}
		metricsPrefix = strings.Replace(metricsPrefix, "-", "_", -1)
		metricsPrefix = strings.Replace(metricsPrefix, ".", "_", -1)
		sock, err := RetryListen(ipPort.Ip, ipPort.Port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listening: %v\n", err)
			logger.Error("Error listening", zap.Error(err))
			os.Exit(1)
		}
		var srv HummingbirdServer
		if ipPort.CertFile != "" && ipPort.KeyFile != "" {
			tlsConf := &tls.Config{
				PreferServerCipherSuites: true,
				MinVersion:               tls.VersionTLS12,
			}
			if server.Type() != "proxy" {
				tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
			}
			httpServer := http.Server{
				Handler:      server.GetHandler(config, metricsPrefix),
				ReadTimeout:  24 * time.Hour,
				WriteTimeout: 24 * time.Hour,
				TLSConfig:    tlsConf,
			}
			err := http2.ConfigureServer(&httpServer, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error enabling http2 on server: %v\n", err)
				logger.Error("Error enabling http2 on server", zap.Error(err))
				os.Exit(1)
			}
			srv = HummingbirdServer{
				Server:   &httpServer,
				logger:   logger,
				finalize: server.Finalize,
			}
			go srv.ServeTLS(sock, ipPort.CertFile, ipPort.KeyFile)
		} else {
			srv = HummingbirdServer{
				Server: &http.Server{
					Handler:      server.GetHandler(config, metricsPrefix),
					ReadTimeout:  24 * time.Hour,
					WriteTimeout: 24 * time.Hour,
				},
				logger:   logger,
				finalize: server.Finalize,
			}
			go srv.Serve(sock)
		}
		ch := server.Background(flags)
		if ch != nil {
			if wg == nil {
				wg = &sync.WaitGroup{}
			}
			wg.Add(1)
			go func(ch2 chan struct{}) {
				defer wg.Done()
				<-ch2
			}(ch)
		}
		servers = append(servers, &srv)
		logger.Info("Server started", zap.Int("port", ipPort.Port))
	}

	if wg != nil {
		wg.Wait()
		return
	}

	if len(servers) > 0 {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT)
		s := <-c
		switch s {
		case syscall.SIGTERM, syscall.SIGHUP: // graceful shutdown
			var wg sync.WaitGroup
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			for _, srv := range servers {
				// Shutdown the HTTP server
				wg.Add(1)
				go func(hserv *HummingbirdServer) {
					defer wg.Done()
					if err := hserv.Shutdown(ctx); err != nil {
						// failure/timeout shutting down the server gracefully
						hserv.logger.Error("Error with graceful shutdown", zap.Error(err))
					}
					// Wait for any async processes to quit
					hserv.finalize()
				}(srv)
			}
			// Wait for everything to complete
			wgc := make(chan struct{})
			go func() {
				defer close(wgc)
				wg.Wait()
			}()
			select {
			case <-wgc:
				// Everything has completed
				fmt.Println("Graceful shutdown complete.")
				return
			case <-ctx.Done():
				// Timeout before everything completing
				fmt.Println("Forcing shutdown after timeout.")
				return
			}
		case syscall.SIGABRT, syscall.SIGQUIT: // drop a traceback
			pid := os.Getpid()
			DumpGoroutinesStackTrace(pid)
		default:
			for _, srv := range servers {
				if err := srv.Close(); err != nil {
					srv.logger.Error("Error shutdown", zap.Error(err))
				}
			}
		}
	}
}
