package hummingbird

import (
	"net/http"
	"time"
)

// ResponseWriter that saves its status - used for logging.

type SwiftWriter struct {
	http.ResponseWriter
	Status int
}

func (w *SwiftWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.Status = status
}

func (w *SwiftWriter) CopyResponseHeaders(src *http.Response) {
	for key := range src.Header {
		w.Header().Set(key, src.Header.Get(key))
	}
}

// http.Request that also contains swift-specific info about the request

type SwiftRequest struct {
	*http.Request
	TransactionId string
	XTimestamp    string
	Start         time.Time
}

func (r *SwiftRequest) CopyRequestHeaders(dst *http.Request) {
	for key := range r.Header {
		dst.Header.Set(key, r.Header.Get(key))
	}
	dst.Header.Set("X-Timestamp", r.XTimestamp)
	dst.Header.Set("X-Trans-Id", r.TransactionId)
}

func (r *SwiftRequest) NillableFormValue(key string) *string {
	if r.Form == nil {
		r.ParseForm()
	}
	if vs, ok := r.Form[key]; !ok {
		return nil
	} else {
		return &vs[0]
	}
}
