package objectserver

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hummingbird/common"
)

type ObjectHandler struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	checkMounts    bool
	disableFsync   bool
	asyncFinalize  bool
	asyncFsync     bool
	dropCache      bool
	allowedHeaders map[string]bool
	logger         *syslog.Writer
	diskLimit      int64
	diskInUse      map[string]int64
}

func (server *ObjectHandler) ObjGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	headers := writer.Header()
	hashDir, err := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix, server.checkMounts)
	if err != nil {
		http.Error(writer, "Insufficent Storage", 507)
		return
	}
	dataFile, metaFile := ObjectFiles(hashDir)
	if dataFile == "" || strings.HasSuffix(dataFile, ".ts") {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	metadata, err := ObjectMetadata(dataFile, metaFile)
	if err != nil {
		request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	contentLength, err := strconv.ParseInt(metadata["Content-Length"].(string), 10, 64)

	file, err := os.Open(dataFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer file.Close()

	if stat, _ := file.Stat(); stat.Size() != contentLength {
		if QuarantineHash(hashDir) == nil {
			InvalidateHash(hashDir, !server.disableFsync)
		}
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	if deleteAt, ok := metadata["X-Delete-At"].(string); ok {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err == nil && deleteTime.Before(time.Now()) {
			http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
	}

	lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"].(string))
	if err != nil {
		request.LogError("Error getting timestamp from %s: %s", dataFile, err.Error())
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	lastModifiedHeader := lastModified
	if lastModified.Nanosecond() > 0 { // for some reason, Last-Modified is ceil(X-Timestamp)
		lastModifiedHeader = lastModified.Truncate(time.Second).Add(time.Second)
	}
	headers.Set("Last-Modified", lastModifiedHeader.Format(time.RFC1123))
	headers.Set("ETag", fmt.Sprintf("\"%s\"", metadata["ETag"].(string)))
	headers.Set("X-Timestamp", metadata["X-Timestamp"].(string))
	for key, value := range metadata {
		if allowed, ok := server.allowedHeaders[key.(string)]; (ok && allowed) || strings.HasPrefix(key.(string), "X-Object-Meta-") {
			headers.Set(key.(string), value.(string))
		}
	}

	if im := request.Header.Get("If-Match"); im != "" && !strings.Contains(im, metadata["ETag"].(string)) {
		http.Error(writer, http.StatusText(http.StatusPreconditionFailed), http.StatusPreconditionFailed)
		return
	}
	if inm := request.Header.Get("If-None-Match"); inm != "" && strings.Contains(inm, metadata["ETag"].(string)) {
		http.Error(writer, http.StatusText(http.StatusNotModified), http.StatusNotModified)
		return
	}
	if ius, err := hummingbird.ParseDate(request.Header.Get("If-Unmodified-Since")); err == nil && lastModified.After(ius) {
		http.Error(writer, http.StatusText(http.StatusPreconditionFailed), http.StatusPreconditionFailed)
		return
	}
	if ims, err := hummingbird.ParseDate(request.Header.Get("If-Modified-Since")); err == nil && lastModified.Before(ims) {
		http.Error(writer, http.StatusText(http.StatusNotModified), http.StatusNotModified)
		return
	}
	headers.Set("Content-Type", metadata["Content-Type"].(string))
	headers.Set("Content-Length", metadata["Content-Length"].(string))

	if rangeHeader := request.Header.Get("Range"); rangeHeader != "" {
		ranges, err := hummingbird.ParseRange(rangeHeader, contentLength)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusRequestedRangeNotSatisfiable), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if ranges != nil && len(ranges) == 1 {
			_, _ = file.Seek(ranges[0].Start, os.SEEK_SET)
			writer.Header().Set("Content-Length", strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))
			writer.WriteHeader(http.StatusPartialContent)
			io.CopyN(writer, file, ranges[0].End-ranges[0].Start)
			return
		}
	}
	writer.WriteHeader(http.StatusOK)
	if request.Method == "GET" {
		io.Copy(writer, file)
		if server.dropCache {
			go hummingbird.DropBufferCache(int(file.Fd()), contentLength)
		}
	} else {
		writer.Write([]byte{})
	}
}

func (server *ObjectHandler) ObjPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	outHeaders := writer.Header()
	hashDir, err := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix, server.checkMounts)
	if err != nil {
		http.Error(writer, "Insufficent Storage", 507)
		return
	}
	if inm := request.Header.Get("If-None-Match"); inm == "*" {
		dataFile, _ := ObjectFiles(hashDir)
		if dataFile != "" && !strings.HasSuffix(dataFile, ".ts") {
			http.Error(writer, http.StatusText(http.StatusPreconditionFailed), http.StatusPreconditionFailed)
			return
		}
	}

	if os.MkdirAll(hashDir, 0770) != nil || os.MkdirAll(ObjTempDir(vars, server.driveRoot), 0770) != nil {
		request.LogError("Error creating temp directory: %s", ObjTempDir(vars, server.driveRoot))
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	fileName := fmt.Sprintf("%s/%s.data", hashDir, request.Header.Get("X-Timestamp"))
	tempFile, err := ioutil.TempFile(ObjTempDir(vars, server.driveRoot), "PUT")
	if err != nil {
		request.LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if contentLength, err := strconv.ParseInt(request.Header.Get("Content-Length"), 10, 64); err == nil {
		syscall.Fallocate(int(tempFile.Fd()), 1, 0, contentLength)
	}
	defer tempFile.Close()
	defer os.RemoveAll(tempFile.Name())
	metadata := make(map[string]interface{})
	metadata["name"] = fmt.Sprintf("/%s/%s/%s", vars["account"], vars["container"], vars["obj"])
	metadata["X-Timestamp"] = request.Header.Get("X-Timestamp")
	metadata["Content-Type"] = request.Header.Get("Content-Type")
	for key := range request.Header {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) || strings.HasPrefix(key, "X-Object-Meta-") {
			metadata[key] = request.Header.Get(key)
		}
	}
	hash := md5.New()
	totalSize, err := io.Copy(hash, io.TeeReader(request.Body, tempFile))
	if err != nil {
		request.LogError("Error writing to file %s: %s", tempFile.Name(), err.Error())
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	metadata["Content-Length"] = strconv.FormatInt(totalSize, 10)
	metadata["ETag"] = fmt.Sprintf("%x", hash.Sum(nil))
	requestEtag := request.Header.Get("ETag")
	if requestEtag != "" && requestEtag != metadata["ETag"].(string) {
		http.Error(writer, "Unprocessable Entity", 422)
		return
	}
	outHeaders.Set("ETag", metadata["ETag"].(string))
	WriteMetadata(int(tempFile.Fd()), metadata)

	if !server.disableFsync {
		if server.asyncFsync {
			go tempFile.Sync()
		} else {
			tempFile.Sync()
		}
	}
	os.Rename(tempFile.Name(), fileName)
	if server.dropCache {
		go hummingbird.DropBufferCache(int(tempFile.Fd()), totalSize)
	}

	finalize := func() {
		UpdateContainer(metadata, request, vars, hashDir)
		if request.Header.Get("X-Delete-At") != "" || request.Header.Get("X-Delete-After") != "" {
			UpdateDeleteAt(request, vars, metadata, hashDir)
		}
		CleanupHashDir(hashDir)
		InvalidateHash(hashDir, !server.disableFsync)
	}
	if server.asyncFinalize {
		go finalize()
	} else {
		finalize()
	}
	http.Error(writer, http.StatusText(http.StatusCreated), http.StatusCreated)
}

func (server *ObjectHandler) ObjDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	hashDir, err := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix, server.checkMounts)
	if err != nil {
		http.Error(writer, "Insufficent Storage", 507)
		return
	}
	dataFile, metaFile := ObjectFiles(hashDir)
	if ida := request.Header.Get("X-If-Delete-At"); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		if dataFile != "" && !strings.HasSuffix(dataFile, ".ts") {
			http.Error(writer, http.StatusText(http.StatusPreconditionFailed), http.StatusPreconditionFailed)
			return
		}
		metadata, err := ObjectMetadata(dataFile, metaFile)
		if err != nil {
			request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if _, ok := metadata["X-Delete-At"]; ok {
			if ida != metadata["X-Delete-At"] {
				http.Error(writer, http.StatusText(http.StatusPreconditionFailed), http.StatusPreconditionFailed)
				return
			}
		}
	}

	if os.MkdirAll(hashDir, 0770) != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	fileName := fmt.Sprintf("%s/%s.ts", hashDir, request.Header.Get("X-Timestamp"))
	tempFile, err := ioutil.TempFile(ObjTempDir(vars, server.driveRoot), "PUT")
	if err != nil {
		request.LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer tempFile.Close()
	defer os.RemoveAll(tempFile.Name())
	metadata := make(map[string]interface{})
	metadata["X-Timestamp"] = request.Header.Get("X-Timestamp")
	metadata["name"] = fmt.Sprintf("/%s/%s/%s", vars["account"], vars["container"], vars["obj"])
	WriteMetadata(int(tempFile.Fd()), metadata)

	if !server.disableFsync {
		if server.asyncFsync {
			go tempFile.Sync()
		} else {
			tempFile.Sync()
		}
	}
	os.Rename(tempFile.Name(), fileName)

	finalize := func() {
		UpdateContainer(metadata, request, vars, hashDir)
		if _, ok := metadata["X-Delete-At"]; ok {
			UpdateDeleteAt(request, vars, metadata, hashDir)
		}
		CleanupHashDir(hashDir)
		InvalidateHash(hashDir, !server.disableFsync)
	}
	if server.asyncFinalize {
		go finalize()
	} else {
		finalize()
	}

	if !strings.HasSuffix(dataFile, ".data") {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	} else {
		http.Error(writer, "", http.StatusNoContent)
	}
}

func (server *ObjectHandler) ObjReplicateHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	hashes, err := GetHashes(server.driveRoot, vars["device"], vars["partition"], strings.Split(vars["suffixes"], "-"))
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(hummingbird.PickleDumps(hashes))
}

func GetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

func (server *ObjectHandler) AcquireDisk(disk string) bool {
	if _, ok := server.diskInUse[disk]; !ok {
		server.diskInUse[disk] = 0
	}
	if server.diskInUse[disk] > server.diskLimit {
		return false
	}
	server.diskInUse[disk] += 1
	return true
}

func (server *ObjectHandler) ReleaseDisk(disk string) {
	server.diskInUse[disk] -= 1
}

func (server *ObjectHandler) LogRequest(writer *hummingbird.WebWriter, request *hummingbird.WebRequest) {
	go server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		request.URL.Path,
		writer.Status,
		GetDefault(writer.Header(), "Content-Length", "-"),
		GetDefault(request.Header, "Referer", "-"),
		GetDefault(request.Header, "X-Trans-Id", "-"),
		GetDefault(request.Header, "User-Agent", "-"),
		time.Since(request.Start).Seconds(),
		"-")) // TODO: "additional info"?
}

func (server ObjectHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthcheck" {
		writer.Header().Set("Content-Length", "2")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
		return
	} else if strings.HasPrefix(request.URL.Path, "/recon/") {
		hummingbird.ReconHandler(server.driveRoot, writer, request)
		return
	} else if request.URL.Path == "/diskusage" {
		data, err := json.Marshal(server.diskInUse)
		if err == nil {
			writer.WriteHeader(http.StatusOK)
			writer.Write(data)
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
			writer.Write([]byte(err.Error()))
		}
		return
	}
	parts := strings.SplitN(request.URL.Path, "/", 6)
	vars := make(map[string]string)
	if len(parts) > 1 {
		vars["device"] = parts[1]
		if len(parts) > 2 {
			vars["partition"] = parts[2]
			if len(parts) > 3 {
				vars["account"] = parts[3]
				vars["suffixes"] = parts[3]
				if len(parts) > 4 {
					vars["container"] = parts[4]
					if len(parts) > 5 {
						vars["obj"] = parts[5]
					}
				}
			}
		}
	}

	newWriter := &hummingbird.WebWriter{writer, 200}
	newRequest := &hummingbird.WebRequest{request, request.Header.Get("X-Trans-Id"), request.Header.Get("X-Timestamp"), time.Now(), server.logger}
	defer newRequest.LogPanics()
	defer server.LogRequest(newWriter, newRequest) // log the request after return

	if !server.AcquireDisk(vars["device"]) {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer server.ReleaseDisk(vars["device"])

	switch request.Method {
	case "GET":
		server.ObjGetHandler(newWriter, newRequest, vars)
	case "HEAD":
		server.ObjGetHandler(newWriter, newRequest, vars)
	case "PUT":
		server.ObjPutHandler(newWriter, newRequest, vars)
	case "DELETE":
		server.ObjDeleteHandler(newWriter, newRequest, vars)
	case "REPLICATE":
		server.ObjReplicateHandler(newWriter, newRequest, vars)
	}
}

func GetServer(conf string) (string, int, http.Handler) {
	handler := ObjectHandler{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		checkMounts: true, disableFsync: false, asyncFinalize: false,
		allowedHeaders: map[string]bool{"Content-Disposition": true,
			"Content-Encoding":      true,
			"X-Delete-At":           true,
			"X-Object-Manifest":     true,
			"X-Static-Large-Object": true,
		},
		diskInUse: map[string]int64{},
	}

	if swiftconf, err := hummingbird.LoadIniFile("/etc/hummingbird/hummingbird.conf"); err == nil {
		handler.hashPathPrefix = swiftconf.GetDefault("swift-hash", "swift_hash_path_prefix", "")
		handler.hashPathSuffix = swiftconf.GetDefault("swift-hash", "swift_hash_path_suffix", "")
	} else if swiftconf, err := hummingbird.LoadIniFile("/etc/swift/swift.conf"); err == nil {
		handler.hashPathPrefix = swiftconf.GetDefault("swift-hash", "swift_hash_path_prefix", "")
		handler.hashPathSuffix = swiftconf.GetDefault("swift-hash", "swift_hash_path_suffix", "")
	}

	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		panic(fmt.Sprintf("Unable to load %s", conf))
	}
	handler.driveRoot = serverconf.GetDefault("DEFAULT", "devices", "/srv/node")
	handler.checkMounts = hummingbird.LooksTrue(serverconf.GetDefault("DEFAULT", "mount_check", "true"))
	handler.disableFsync = hummingbird.LooksTrue(serverconf.GetDefault("DEFAULT", "disable_fsync", "false"))
	handler.asyncFinalize = hummingbird.LooksTrue(serverconf.GetDefault("DEFAULT", "async_finalize", "false"))
	handler.asyncFsync = hummingbird.LooksTrue(serverconf.GetDefault("DEFAULT", "async_fsync", "false"))
	handler.diskLimit, err = strconv.ParseInt(serverconf.GetDefault("DEFAULT", "disk_limit", "100"), 10, 64)
	handler.dropCache = hummingbird.LooksTrue(serverconf.GetDefault("DEFAULT", "drop_cache", "true"))
	if err != nil {
		panic("Invalid disk_limit format")
	}
	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort, err := strconv.ParseInt(serverconf.GetDefault("DEFAULT", "bind_port", "6000"), 10, 64)
	if err != nil {
		panic("Invalid bind port format")
	}
	if allowedHeaders, ok := serverconf.Get("DEFAULT", "allowed_headers"); ok {
		headers := strings.Split(allowedHeaders, ",")
		for i := range headers {
			handler.allowedHeaders[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(headers[i]))] = true
		}
	}

	handler.logger = hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "object-server")
	hummingbird.DropPrivileges(serverconf.GetDefault("DEFAULT", "user", "swift"))

	return bindIP, int(bindPort), handler
}
