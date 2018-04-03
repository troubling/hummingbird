package middleware

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

func RingHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "PUT" {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, "PUT only\n")
		return
	}
	ringPath := "/" + srv.GetVars(req)["ring_path"]
	if ringPath == "" {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, "No ring path\n")
		return
	}
	dir, file := path.Split(ringPath)
	if dir != "/etc/hummingbird/" && dir != "/etc/swift/" {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, "Invalid ring directory\n")
		return
	}
	if !strings.HasSuffix(file, ".ring.gz") {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, "Invalid ring extension\n")
		return
	}
	typ := strings.SplitN(strings.SplitN(file, ".", 2)[0], "-", 2)[0]
	if typ != "account" && typ != "container" && typ != "object" {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, "Invalid ring type\n")
		return
	}
	etag := req.Header.Get("Etag")
	if etag == "" {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, "No ETag\n")
		return
	}
	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		zap.L().Error("middleware.RingHandler TempFile error", zap.String("dir", dir), zap.String("file", file), zap.Error(err))
		srv.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	defer f.Close()
	tempPath := f.Name()
	_, err = io.Copy(f, req.Body)
	if err != nil {
		zap.L().Error("middleware.RingHandler Copy", zap.String("temp path", tempPath), zap.Error(err))
		srv.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	f.Close()
	f, err = os.Open(tempPath)
	defer f.Close()
	if err != nil {
		zap.L().Error("middleware.RingHandler Open", zap.String("temp path", tempPath), zap.Error(err))
		srv.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	hsh := md5.New()
	io.Copy(hsh, f)
	tempEtag := fmt.Sprintf("%x", hsh.Sum(nil))
	f.Close()
	if tempEtag != etag {
		srv.SimpleErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Etag mismatch: calculated %s but was given %s", tempEtag, etag))
		return
	}
	err = os.Rename(tempPath, ringPath)
	if err != nil {
		zap.L().Error("middleware.RingHandler Rename", zap.String("temp path", tempPath), zap.String("ring path", ringPath), zap.Error(err))
		srv.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	srv.StandardResponse(w, http.StatusCreated)
}
