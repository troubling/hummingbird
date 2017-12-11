package middleware

import (
	"bufio"
	"fmt"
	"net"
	"net/http"

	"github.com/uber-go/tally"
)

type recordStatusWriter struct {
	http.ResponseWriter
	status int
}

func (mw *recordStatusWriter) WriteHeader(status int) {
	mw.status = status
	mw.ResponseWriter.WriteHeader(status)
}

func (mw *recordStatusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return mw.ResponseWriter.(http.Hijacker).Hijack()
}

func Metrics(metricsScope tally.Scope) func(http.Handler) http.Handler {
	requestsMetric := metricsScope.Counter("requests")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			w := &recordStatusWriter{ResponseWriter: writer}
			next.ServeHTTP(w, request)
			requestsMetric.Inc(1)
			metricsScope.Counter(request.Method + "_requests").Inc(1)
			metricsScope.Counter(fmt.Sprintf("%d_responses", w.status)).Inc(1)
		})
	}
}
