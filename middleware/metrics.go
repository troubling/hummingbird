package middleware

import (
	"fmt"
	"net/http"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
)

func Metrics(config *conf.Config, metricsScope tally.Scope) func(http.Handler) http.Handler {
	requestsMetric := metricsScope.Counter("requests")
	xSourceCode := false
	if config != nil {
		xSourceCode = config.GetBool("debug", "debug_x_source_code", false)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			var status int
			w := srv.NewCustomWriter(xSourceCode, writer, func(w http.ResponseWriter, s int) int {
				status = s
				return s
			})
			next.ServeHTTP(w, request)
			requestsMetric.Inc(1)
			metricsScope.Counter(request.Method + "_requests").Inc(1)
			metricsScope.Counter(fmt.Sprintf("%d_responses", status)).Inc(1)
		})
	}
}
