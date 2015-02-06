package objectserver

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hummingbird "hummingbird/common"
)

type ObjectHandler struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	checkEtags       bool
	checkMounts      bool
	disableFsync     bool
	asyncFinalize    bool
	asyncFsync       bool
	dropCache        bool
	allowedHeaders   map[string]bool
	logger           *syslog.Writer
	diskLimit        int64
	diskInUse        map[string]*int64
	diskInUseLock    sync.Mutex
	fallocateReserve int64
}

func (server *ObjectHandler) ObjGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	headers := writer.Header()
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	dataFile, metaFile := ObjectFiles(hashDir)
	if dataFile == "" || strings.HasSuffix(dataFile, ".ts") {
		if im := request.Header.Get("If-Match"); im != "" && strings.Contains(im, "*") {
			writer.StandardResponse(http.StatusPreconditionFailed)
			return
		} else {
			writer.StandardResponse(http.StatusNotFound)
			return
		}
	}

	metadata, err := ObjectMetadata(dataFile, metaFile)
	if err != nil {
		request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
		if QuarantineHash(hashDir) == nil {
			InvalidateHash(hashDir, !server.disableFsync)
		}
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
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

	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"].(string))
	if deleteAt, ok := metadata["X-Delete-At"].(string); ok {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err == nil && deleteTime.Before(time.Now()) {
			writer.StandardResponse(http.StatusNotFound)
			return
		}
	}

	lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"].(string))
	if err != nil {
		request.LogError("Error getting timestamp from %s: %s", dataFile, err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	lastModifiedHeader := lastModified
	if lastModified.Nanosecond() > 0 { // for some reason, Last-Modified is ceil(X-Timestamp)
		lastModifiedHeader = lastModified.Truncate(time.Second).Add(time.Second)
	}
	headers.Set("Last-Modified", lastModifiedHeader.Format(time.RFC1123))
	headers.Set("ETag", "\""+metadata["ETag"].(string)+"\"")
	xTimestamp, err := hummingbird.GetEpochFromTimestamp(metadata["X-Timestamp"].(string))
	if err != nil {
		request.LogError("Error getting the epoch time from x-timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	headers.Set("X-Timestamp", xTimestamp)
	for key, value := range metadata {
		if allowed, ok := server.allowedHeaders[key.(string)]; (ok && allowed) ||
			strings.HasPrefix(key.(string), "X-Object-Meta-") ||
			strings.HasPrefix(key.(string), "X-Object-Sysmeta-") {
			headers.Set(key.(string), value.(string))
		}
	}

	if im := request.Header.Get("If-Match"); im != "" && !strings.Contains(im, metadata["ETag"].(string)) && !strings.Contains(im, "*") {
		writer.StandardResponse(http.StatusPreconditionFailed)
		return
	}

	if inm := request.Header.Get("If-None-Match"); inm != "" && (strings.Contains(inm, metadata["ETag"].(string)) || strings.Contains(inm, "*")) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	if ius, err := hummingbird.ParseDate(request.Header.Get("If-Unmodified-Since")); err == nil && lastModified.After(ius) {
		writer.StandardResponse(http.StatusPreconditionFailed)
		return
	}

	if ims, err := hummingbird.ParseDate(request.Header.Get("If-Modified-Since")); err == nil && lastModified.Before(ims) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	headers.Set("Accept-Ranges", "bytes")
	headers.Set("Content-Type", metadata["Content-Type"].(string))
	headers.Set("Content-Length", metadata["Content-Length"].(string))

	if rangeHeader := request.Header.Get("Range"); rangeHeader != "" {
		ranges, err := hummingbird.ParseRange(rangeHeader, contentLength)
		if err != nil {
			headers.Set("Content-Length", "0")
			writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		} else if ranges != nil && len(ranges) == 1 {
			headers.Set("Content-Length", strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))
			headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, contentLength))
			writer.WriteHeader(http.StatusPartialContent)
			file.Seek(ranges[0].Start, os.SEEK_SET)
			io.CopyN(writer, file, ranges[0].End-ranges[0].Start)
			return
		} else if ranges != nil && len(ranges) > 1 {
			w := multipart.NewWriter(writer)
			responseLength := int64(6 + len(w.Boundary()) + (len(w.Boundary())+len(metadata["Content-Type"].(string))+47)*len(ranges))
			for _, rng := range ranges {
				responseLength += int64(len(fmt.Sprintf("%d-%d/%d", rng.Start, rng.End-1, contentLength))) + rng.End - rng.Start
			}
			headers.Set("Content-Length", strconv.FormatInt(responseLength, 10))
			headers.Set("Content-Type", "multipart/byteranges;boundary="+w.Boundary())
			writer.WriteHeader(http.StatusPartialContent)
			for _, rng := range ranges {
				part, _ := w.CreatePart(textproto.MIMEHeader{"Content-Type": []string{metadata["Content-Type"].(string)},
					"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End-1, contentLength)}})
				file.Seek(rng.Start, os.SEEK_SET)
				io.CopyN(part, file, rng.End-rng.Start)
			}
			w.Close()
			return
		}
	}
	writer.WriteHeader(http.StatusOK)
	if request.Method == "GET" {
		if server.checkEtags {
			hash := md5.New()
			hummingbird.Copy(file, writer, hash)
			if hex.EncodeToString(hash.Sum(nil)) != metadata["ETag"].(string) && QuarantineHash(hashDir) == nil {
				InvalidateHash(hashDir, !server.disableFsync)
			}
		} else {
			io.Copy(writer, file)
		}
		if server.dropCache {
			go hummingbird.DropBufferCache(int(file.Fd()), contentLength)
		}
	} else {
		writer.Write([]byte{})
	}
}

func (server *ObjectHandler) ObjPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	outHeaders := writer.Header()
	if !hummingbird.ValidTimestamp(request.Header.Get("X-Timestamp")) {
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	if request.Header.Get("Content-Type") == "" {
		http.Error(writer, "No content type", http.StatusBadRequest)
		return
	}
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	tempDir := ObjTempDir(vars, server.driveRoot)

	if deleteAt := request.Header.Get("X-Delete-At"); deleteAt != "" {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err != nil || deleteTime.Before(time.Now()) {
			http.Error(writer, "Bad Request", 400)
			return
		}
	}

	if inm := request.Header.Get("If-None-Match"); inm == "*" {
		dataFile, _ := ObjectFiles(hashDir)
		if dataFile != "" && !strings.HasSuffix(dataFile, ".ts") {
			writer.StandardResponse(http.StatusPreconditionFailed)
			return
		}
	}

	if os.MkdirAll(hashDir, 0770) != nil || os.MkdirAll(tempDir, 0770) != nil {
		request.LogError("Error creating temp directory: %s", tempDir)
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		request.LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}

	fileName := hashDir + "/" + requestTimestamp + ".data"
	tempFile, err := ioutil.TempFile(tempDir, "PUT")
	if err != nil {
		request.LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	defer tempFile.Close()
	defer os.RemoveAll(tempFile.Name())
	contentLength, cLErr := strconv.ParseInt(request.Header.Get("Content-Length"), 10, 64)

	var st syscall.Statfs_t
	if server.fallocateReserve > 0 && syscall.Fstatfs(int(tempFile.Fd()), &st) == nil && (int64(st.Frsize)*int64(st.Bavail)-contentLength) < server.fallocateReserve {
		writer.CustomErrorResponse(507, vars)
		return
	}
	if cLErr == nil && contentLength > 0 {
		syscall.Fallocate(int(tempFile.Fd()), 1, 0, contentLength)
	}
	metadata := make(map[string]interface{})
	metadata["name"] = "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"]
	metadata["X-Timestamp"] = requestTimestamp
	metadata["Content-Type"] = request.Header.Get("Content-Type")
	for key := range request.Header {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			metadata[key] = request.Header.Get(key)
		}
	}
	hash := md5.New()
	totalSize, err := hummingbird.Copy(request.Body, tempFile, hash)
	if err != nil {
		request.LogError("Error writing to file %s: %s", tempFile.Name(), err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	metadata["Content-Length"] = strconv.FormatInt(totalSize, 10)
	metadata["ETag"] = hex.EncodeToString(hash.Sum(nil))
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
		HashCleanupListDir(hashDir, request)
		InvalidateHash(hashDir, !server.disableFsync)
	}
	if server.asyncFinalize {
		go finalize()
	} else {
		finalize()
	}
	writer.StandardResponse(http.StatusCreated)
}

func (server *ObjectHandler) ObjDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	headers := writer.Header()
	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		request.LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	tempDir := ObjTempDir(vars, server.driveRoot)
	responseStatus := http.StatusNotFound

	dataFile, metaFile := ObjectFiles(hashDir)
	if ida := request.Header.Get("X-If-Delete-At"); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			writer.StandardResponse(http.StatusBadRequest)
			return
		}
		if dataFile != "" && !strings.HasSuffix(dataFile, ".ts") {
			writer.StandardResponse(http.StatusPreconditionFailed)
			return
		}
		metadata, err := ObjectMetadata(dataFile, metaFile)
		if err != nil {
			request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			writer.StandardResponse(http.StatusInternalServerError)
			return
		}
		if _, ok := metadata["X-Delete-At"]; ok {
			if ida != metadata["X-Delete-At"] {
				writer.StandardResponse(http.StatusPreconditionFailed)
				return
			}
		}
	}

	if dataFile != "" {
		if strings.HasSuffix(dataFile, ".data") {
			responseStatus = http.StatusNoContent
		}
		origMetadata, err := ObjectMetadata(dataFile, metaFile)
		if err == nil {
			// compare the timestamps here
			if origTimestamp, ok := origMetadata["X-Timestamp"]; ok && origTimestamp.(string) >= requestTimestamp {
				headers.Set("X-Backend-Timestamp", origTimestamp.(string))
				if strings.HasSuffix(dataFile, ".data") {
					writer.StandardResponse(http.StatusConflict)
					return
				} else {
					writer.StandardResponse(http.StatusNotFound)
					return
				}
			}
		} else {
			request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			if qerr := QuarantineHash(hashDir); qerr == nil {
				InvalidateHash(hashDir, !server.disableFsync)
			}
			responseStatus = http.StatusNotFound
		}
	}

	if os.MkdirAll(hashDir, 0770) != nil || os.MkdirAll(tempDir, 0770) != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	fileName := hashDir + "/" + requestTimestamp + ".ts"
	tempFile, err := ioutil.TempFile(tempDir, "PUT")
	if err != nil {
		request.LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	defer tempFile.Close()
	defer os.RemoveAll(tempFile.Name())
	metadata := make(map[string]interface{})
	metadata["X-Timestamp"] = requestTimestamp
	metadata["name"] = "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"]
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
		HashCleanupListDir(hashDir, request)
		InvalidateHash(hashDir, !server.disableFsync)
	}
	if server.asyncFinalize {
		go finalize()
	} else {
		finalize()
	}
	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"].(string))
	writer.StandardResponse(responseStatus)
}

func (server *ObjectHandler) ObjReplicateHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}
	hashes, err := GetHashes(server.driveRoot, vars["device"], vars["partition"], recalculate, request)
	if err != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		// TODO: need to check if this is  507 instead of a 500 for the drive unmounted
		//        if err.(*BackEnd)
		//		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(hummingbird.PickleDumps(hashes))
}

func (server *ObjectHandler) AcquireDisk(disk string) bool {
	if server.checkMounts {
		devicePath := server.driveRoot + "/" + disk
		if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
			return false
		}
	}

	if val, ok := server.diskInUse[disk]; !ok {
		server.diskInUseLock.Lock()
		if _, ok := server.diskInUse[disk]; !ok {
			server.diskInUse[disk] = new(int64)
		}
		server.diskInUseLock.Unlock()
	} else if *val > server.diskLimit {
		return false
	}
	atomic.AddInt64(server.diskInUse[disk], 1)
	return true
}

func (server *ObjectHandler) ReleaseDisk(disk string) {
	atomic.AddInt64(server.diskInUse[disk], -1)
}

func (server *ObjectHandler) LogRequest(writer *hummingbird.WebWriter, request *hummingbird.WebRequest) {
	go server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		hummingbird.Urlencode(request.URL.Path),
		writer.Status,
		hummingbird.GetDefault(writer.Header(), "Content-Length", "-"),
		hummingbird.GetDefault(request.Header, "Referer", "-"),
		hummingbird.GetDefault(request.Header, "X-Trans-Id", "-"),
		hummingbird.GetDefault(request.Header, "User-Agent", "-"),
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

	newWriter := &hummingbird.WebWriter{writer, 500, false}
	newRequest := &hummingbird.WebRequest{request, request.Header.Get("X-Trans-Id"), request.Header.Get("X-Timestamp"), time.Now(), server.logger}
	defer newRequest.LogPanics(newWriter)
	defer server.LogRequest(newWriter, newRequest) // log the request after return

	if !newRequest.ValidateRequest() {
		newWriter.StandardResponse(400)
		return
	}

	if !server.AcquireDisk(vars["device"]) {
		vars["Method"] = request.Method
		newWriter.CustomErrorResponse(507, vars)
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

func GetServer(conf string) (string, int, http.Handler, *syslog.Writer) {
	handler := ObjectHandler{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		allowedHeaders: map[string]bool{"Content-Disposition": true,
			"Content-Encoding":      true,
			"X-Delete-At":           true,
			"X-Object-Manifest":     true,
			"X-Static-Large-Object": true,
		},
		diskInUse: make(map[string]*int64),
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
	handler.driveRoot = serverconf.GetDefault("app:object-server", "devices", "/srv/node")
	handler.checkMounts = serverconf.GetBool("app:object-server", "mount_check", true)
	handler.disableFsync = serverconf.GetBool("app:object-server", "disable_fsync", false)
	handler.asyncFinalize = serverconf.GetBool("app:object-server", "async_finalize", false)
	handler.asyncFsync = serverconf.GetBool("app:object-server", "async_fsync", false)
	handler.dropCache = serverconf.GetBool("app:object-server", "drop_cache", true)
	handler.diskLimit = serverconf.GetInt("app:object-server", "disk_limit", 100)
	handler.checkEtags = serverconf.GetBool("app:object-server", "check_etags", false)
	bindIP := serverconf.GetDefault("app:object-server", "bind_ip", "0.0.0.0")
	bindPort := serverconf.GetInt("app:object-server", "bind_port", 6000)
	if allowedHeaders, ok := serverconf.Get("app:object-server", "allowed_headers"); ok {
		headers := strings.Split(allowedHeaders, ",")
		for i := range headers {
			handler.allowedHeaders[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(headers[i]))] = true
		}
	}

	handler.logger = hummingbird.SetupLogger(serverconf.GetDefault("app:object-server", "log_facility", "LOG_LOCAL0"), "object-server")
	hummingbird.DropPrivileges(serverconf.GetDefault("app:object-server", "user", "swift"))

	return bindIP, int(bindPort), handler, handler.logger
}
