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
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/troubling/hummingbird/common/conf"
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

// ResponseWriter that saves its status - used for logging.

type WebWriter struct {
	http.ResponseWriter
	Status          int
	ResponseStarted bool
}

func (w *WebWriter) WriteHeader(status int) {
	w.Status = status
	w.ResponseStarted = true
	w.ResponseWriter.WriteHeader(status)
}

func (w WebWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

func (w *WebWriter) Response() (bool, int) {
	return w.ResponseStarted, w.Status
}

func CopyResponseHeaders(w http.ResponseWriter, src *http.Response) {
	for key := range src.Header {
		w.Header().Set(key, src.Header.Get(key))
	}
}

func StandardResponse(w http.ResponseWriter, statusCode int) {
	body := responseBodies[statusCode]
	w.Header().Set("Content-Type", "text/html")
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

type RequestLogger struct {
	Request *http.Request
	Logger  LowLevelLogger
	W       interface {
		http.ResponseWriter
		Response() (bool, int)
	}
}

func (r RequestLogger) LogError(format string, args ...interface{}) {
	transactionId := r.Request.Header.Get("X-Trans-Id")
	r.Logger.Err(fmt.Sprintf(format, args...) + " (txn:" + transactionId + ")")
}

func (r RequestLogger) LogInfo(format string, args ...interface{}) {
	transactionId := r.Request.Header.Get("X-Trans-Id")
	r.Logger.Info(fmt.Sprintf(format, args...) + " (txn:" + transactionId + ")")
}

func (r RequestLogger) LogDebug(format string, args ...interface{}) {
	transactionId := r.Request.Header.Get("X-Trans-Id")
	r.Logger.Debug(fmt.Sprintf(format, args...) + " (txn:" + transactionId + ")")
}

func (r RequestLogger) LogPanics(msg string) {
	if e := recover(); e != nil {
		transactionId := r.Request.Header.Get("X-Trans-Id")
		r.Logger.Err(fmt.Sprintf("PANIC (%s): %s: %s", msg, e, debug.Stack()) + " (txn:" + transactionId + ")")
		// if we haven't set a status code yet, we can send a 500 response.
		if started, _ := r.W.Response(); !started {
			StandardResponse(r.W, http.StatusInternalServerError)
		}
	}
}

func ValidateRequest(r *http.Request) bool {
	return utf8.ValidString(r.URL.Path) && utf8.ValidString(r.Header.Get("Content-Type"))
}

type LoggingContext interface {
	LogError(format string, args ...interface{})
	LogInfo(format string, args ...interface{})
	LogDebug(format string, args ...interface{})
	LogPanics(format string)
}

type LowLevelLogger interface {
	Err(string) error
	Info(string) error
	Debug(string) error
}

type consoleLogger struct{}

func (c *consoleLogger) Err(m string) error {
	fmt.Println("ERROR:", m)
	return nil
}

func (c *consoleLogger) Info(m string) error {
	fmt.Println("INFO:", m)
	return nil
}

func (c *consoleLogger) Debug(m string) error {
	fmt.Println("DEBUG:", m)
	return nil
}

var syslogFacilityMapping = map[string]syslog.Priority{"LOG_USER": syslog.LOG_USER,
	"LOG_MAIL": syslog.LOG_MAIL, "LOG_DAEMON": syslog.LOG_DAEMON,
	"LOG_AUTH": syslog.LOG_AUTH, "LOG_SYSLOG": syslog.LOG_SYSLOG,
	"LOG_LPR": syslog.LOG_LPR, "LOG_NEWS": syslog.LOG_NEWS,
	"LOG_UUCP": syslog.LOG_UUCP, "LOG_CRON": syslog.LOG_CRON,
	"LOG_AUTHPRIV": syslog.LOG_AUTHPRIV, "LOG_FTP": syslog.LOG_FTP,
	"LOG_LOCAL0": syslog.LOG_LOCAL0, "LOG_LOCAL1": syslog.LOG_LOCAL1,
	"LOG_LOCAL2": syslog.LOG_LOCAL2, "LOG_LOCAL3": syslog.LOG_LOCAL3,
	"LOG_LOCAL4": syslog.LOG_LOCAL4, "LOG_LOCAL5": syslog.LOG_LOCAL5,
	"LOG_LOCAL6": syslog.LOG_LOCAL6, "LOG_LOCAL7": syslog.LOG_LOCAL7}

// SetupLogger pulls configuration information from the config and flags to create a UDP syslog logger.
// If -d was not specified, it also logs to the console.
func SetupLogger(conf conf.Config, flags *flag.FlagSet, section, prefix string) (LowLevelLogger, error) {
	vFlag := flags.Lookup("v")
	dFlag := flags.Lookup("d")
	if vFlag != nil && dFlag != nil && vFlag.Value.(flag.Getter).Get().(bool) && !dFlag.Value.(flag.Getter).Get().(bool) {
		return &consoleLogger{}, nil
	}
	facility := conf.GetDefault(section, "log_facility", "LOG_LOCAL0")
	host := conf.GetDefault(section, "log_udp_host", "127.0.0.1")
	port := conf.GetInt(section, "log_udp_port", 514)
	dialHost := fmt.Sprintf("%s:%d", host, port)
	logger, err := syslog.Dial("udp", dialHost, syslogFacilityMapping[facility], prefix)
	if err != nil {
		return nil, fmt.Errorf("Unable to dial logger: %v", err)
	}
	return logger, nil
}

/* http.Server that knows how to shut down gracefully */

type HummingbirdServer struct {
	http.Server
	Listener net.Listener
	logger   LowLevelLogger
	finalize func()
}

func ShutdownStdio() {
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0600)
	if err != nil {
		panic("Error opening /dev/null")
	}
	syscall.Dup2(int(devnull.Fd()), int(os.Stdin.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stdout.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stderr.Fd()))
	devnull.Close()
}

func RetryListen(ip string, port int) (net.Listener, error) {
	address := fmt.Sprintf("%s:%d", ip, port)
	started := time.Now()
	for {
		if sock, err := net.Listen("tcp", address); err == nil {
			return sock, nil
		} else if time.Now().Sub(started) > 10*time.Second {
			return nil, errors.New(fmt.Sprintf("Failed to bind for 10 seconds (%v)", err))
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
	GetHandler(conf.Config) http.Handler
	Finalize() // This is called before stoping gracefully so that a server can clean up before closing
}

/*
	SIGINT - graceful shutdown
	SIGTERM, SIGQUIT - immediate shutdown
	SIGABRT - dump goroutines stacktrace

	Graceful shutdown/restart gives any open connections 5 minutes to complete, then exits.
*/
func RunServers(GetServer func(conf.Config, *flag.FlagSet) (string, int, Server, LowLevelLogger, error), flags *flag.FlagSet) {
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

	for _, config := range configs {
		ip, port, server, logger, err := GetServer(config, flags)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		sock, err := RetryListen(ip, port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listening: %v\n", err)
			logger.Err(fmt.Sprintf("Error listening: %v", err))
			os.Exit(1)
		}
		srv := HummingbirdServer{
			Server: http.Server{
				Handler:      server.GetHandler(config),
				ReadTimeout:  24 * time.Hour,
				WriteTimeout: 24 * time.Hour,
			},
			Listener: sock,
			logger:   logger,
			finalize: server.Finalize,
		}
		go srv.Serve(sock)
		servers = append(servers, &srv)
		logger.Err(fmt.Sprintf("Server started on port %d", port))
		fmt.Printf("Server started on port %d\n", port)
	}

	if len(servers) > 0 {
		if flags.Lookup("d").Value.(flag.Getter).Get() == true {
			ShutdownStdio()
		}
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT)
		s := <-c
		if s == syscall.SIGINT {
			var wg sync.WaitGroup
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			for _, srv := range servers {
				// Shutdown the HTTP server
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := srv.Shutdown(ctx); err != nil {
						// failure/timeout shutting down the server gracefully
						srv.logger.Err(fmt.Sprintf("Error with graceful shutdown: %v", err))
					}
					// Wait for any async processes to quit
					srv.finalize()
				}()
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
		} else if s == syscall.SIGABRT {
			pid := os.Getpid()
			DumpGoroutinesStackTrace(pid)
		} else {
			for _, srv := range servers {
				if err := srv.Close(); err != nil {
					srv.logger.Err(fmt.Sprintf("Error shutdown: %v", err))
				}
			}
		}
	}
}

type Daemon interface {
	Run()
	RunForever()
	LogError(format string, args ...interface{})
}

func RunDaemon(GetDaemon func(conf.Config, *flag.FlagSet) (Daemon, error), flags *flag.FlagSet) {
	var daemons []Daemon

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

	once := flags.Lookup("once").Value.(flag.Getter).Get() == true

	for _, config := range configs {
		if daemon, err := GetDaemon(config, flags); err == nil {
			if once {
				daemon.Run()
				fmt.Fprintf(os.Stderr, "Daemon pass completed.\n")
				daemon.LogError("Daemon pass completed.")
			} else {
				daemons = append(daemons, daemon)
				go daemon.RunForever()
				fmt.Fprintf(os.Stderr, "Daemon started.\n")
				daemon.LogError("Daemon started.")
			}
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create daemon: %v", err)
		}
	}

	if len(daemons) > 0 {
		if flags.Lookup("d").Value.(flag.Getter).Get() == true {
			ShutdownStdio()
		}
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		<-c
	}
}
