package hummingbird

import (
	"fmt"
	"log/syslog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// ResponseWriter that saves its status - used for logging.

type WebWriter struct {
	http.ResponseWriter
	Status int
}

func (w *WebWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.Status = status
}

func (w *WebWriter) CopyResponseHeaders(src *http.Response) {
	for key := range src.Header {
		w.Header().Set(key, src.Header.Get(key))
	}
}

// http.Request that also contains swift-specific info about the request

type WebRequest struct {
	*http.Request
	TransactionId string
	XTimestamp    string
	Start         time.Time
	Logger        *syslog.Writer
}

func (r *WebRequest) CopyRequestHeaders(dst *http.Request) {
	for key := range r.Header {
		dst.Header.Set(key, r.Header.Get(key))
	}
	dst.Header.Set("X-Timestamp", r.XTimestamp)
	dst.Header.Set("X-Trans-Id", r.TransactionId)
}

func (r *WebRequest) NillableFormValue(key string) *string {
	if r.Form == nil {
		r.ParseForm()
	}
	if vs, ok := r.Form[key]; !ok {
		return nil
	} else {
		return &vs[0]
	}
}

func (r WebRequest) LogError(format string, args ...interface{}) {
	r.Logger.Err(fmt.Sprintf(format, args...) + " (txn:" + r.TransactionId + ")")
}

func (r WebRequest) LogInfo(format string, args ...interface{}) {
	r.Logger.Info(fmt.Sprintf(format, args...) + " (txn:" + r.TransactionId + ")")
}

func (r WebRequest) LogDebug(format string, args ...interface{}) {
	r.Logger.Debug(fmt.Sprintf(format, args...) + " (txn:" + r.TransactionId + ")")
}

type LoggingContext interface {
	LogError(format string, args ...interface{})
	LogInfo(format string, args ...interface{})
	LogDebug(format string, args ...interface{})
}

/* http.Server that knows how to shut down gracefully */

type HummingbirdServer struct {
	http.Server
	Listener net.Listener
	wg       sync.WaitGroup
}

func (srv *HummingbirdServer) ConnStateChange(conn net.Conn, state http.ConnState) {
	if state == http.StateNew {
		srv.wg.Add(1)
	} else if state == http.StateClosed {
		srv.wg.Done()
	}
}

func (srv *HummingbirdServer) BeginShutdown() {
	srv.SetKeepAlivesEnabled(false)
	srv.Listener.Close()
}

func (srv *HummingbirdServer) Wait() {
	srv.wg.Wait()
}

/*
	SIGHUP - graceful restart
	SIGINT - graceful shutdown
	SIGTERM, SIGQUIT - immediate shutdown

	Graceful shutdown/restart gives any open connections 5 minutes to complete, then exits.
*/
func RunServers(configFile string, GetServer func(string) (string, int, http.Handler)) {
	var servers []*HummingbirdServer
	configFiles, err := filepath.Glob(fmt.Sprintf("%s/*.conf", configFile))
	if err != nil || len(configFiles) <= 0 {
		configFiles = []string{configFile}
	}
	for _, configFile := range configFiles {
		ip, port, handler := GetServer(configFile)
		sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
		if err != nil {
			panic("Error listening on socket!")
		}
		srv := HummingbirdServer{}
		srv.Handler = handler
		srv.ConnState = srv.ConnStateChange
		srv.Listener = sock
		go srv.Serve(sock)
		servers = append(servers, &srv)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	s := <-c
	if s == syscall.SIGINT {
		for _, srv := range servers {
			srv.BeginShutdown()
		}
		go func() {
			time.Sleep(time.Minute * 5)
			os.Exit(0)
		}()
		for _, srv := range servers {
			srv.Wait()
			time.Sleep(time.Second * 5)
		}
	}
	os.Exit(0)
}
