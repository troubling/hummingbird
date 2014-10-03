package containerserver

import (
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/syslog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"hummingbird/common"
)

type ContainerHandler struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	checkMounts    bool
	logger         *syslog.Writer
}

var saveHeaders = map[string]bool{"X-Container-Read": true, "X-Container-Write": true, "X-Container-Sync-Key": true, "X-Container-Sync-To": true}

func ContainerLocation(vars map[string]string, server ContainerHandler) (string, error) {
	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%s/%s/%s%s", server.hashPathPrefix, vars["account"],
		vars["container"], server.hashPathSuffix))
	hexHash := fmt.Sprintf("%x", h.Sum(nil))
	suffix := hexHash[29:32]
	devicePath := fmt.Sprintf("%s/%s", server.driveRoot, vars["device"])
	if server.checkMounts {
		if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
			return "", errors.New("Not mounted")
		}
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s.db", devicePath, "containers", vars["partition"], suffix, hexHash, hexHash), nil
}

func (server ContainerHandler) ContainerGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	db, err := OpenDatabase(containerFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	info, err := db.GetInfo()
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	headers := writer.Header()
	headers.Set("X-Backend-Timestamp", info.CreatedAt)
	headers.Set("X-Backend-PUT-Timestamp", info.PutTimestamp)
	headers.Set("X-Backend-DELETE-Timestamp", info.DeleteTimestamp)
	headers.Set("X-Backend-Status-Changed-At", info.StatusChangedAt)
	headers.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(info.StoragePolicyIndex))
	if db.IsDeleted() {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	} else {
		headers.Set("X-Container-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
		headers.Set("X-Container-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
		headers.Set("X-Timestamp", info.CreatedAt)
		headers.Set("X-PUT-Timestamp", info.PutTimestamp)
		for key, value := range db.GetMetadata() {
			headers.Set(key, value)
		}
	}
	limit, _ := strconv.ParseInt(request.FormValue("limit"), 10, 64)
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	marker := request.NillableFormValue("marker")
	endMarker := request.NillableFormValue("end_marker")
	prefix := request.NillableFormValue("prefix")
	delimiter := request.NillableFormValue("delimiter")
	path := request.NillableFormValue("path")
	storagePolicyIndex := 0
	objects, err := db.ListObjects(int(limit), marker, endMarker, prefix, delimiter, path, storagePolicyIndex)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	format := request.FormValue("format")
	if format == "" { /* TODO: real accept parsing */
		accept := request.Header.Get("Accept")
		if strings.Contains(accept, "application/json") {
			format = "json"
		} else if strings.Contains(accept, "application/xml") || strings.Contains(accept, "text/xml") {
			format = "xml"
		} else {
			format = "text"
		}
	}
	if format == "text" {
		response := ""
		for _, obj := range objects {
			response += obj.(ObjectRecord).Name + "\n"
		}
		if len(response) > 0 {
			headers.Set("Content-Length", strconv.Itoa(len(response)))
			writer.WriteHeader(200)
			writer.Write([]byte(response))
		} else {
			headers.Set("Content-Length", "0")
			writer.WriteHeader(204)
			writer.Write([]byte(""))
		}
	} else if format == "json" {
		output, _ := json.MarshalIndent(objects, "", "  ")
		headers.Set("Content-Type", "application/json; charset=utf-8")
		headers.Set("Content-Length", strconv.Itoa(len(output)))
		writer.WriteHeader(200)
		writer.Write(output)
	} else if format == "xml" {
		type Container struct {
			XMLName xml.Name `xml:"container"`
			Name    string   `xml:"name,attr"`
			Objects []interface{}
		}
		container := &Container{Name: vars["container"], Objects: objects}
		writer.Header().Set("Content-Type", "application/xml; charset=utf-8")
		output, _ := xml.MarshalIndent(container, "", "  ")
		headers.Set("Content-Length", strconv.Itoa(len(output)+39))
		writer.WriteHeader(200)
		writer.Write([]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"))
		writer.Write(output)
	}
}

func (server ContainerHandler) ContainerHeadHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	db, err := OpenDatabase(containerFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	info, err := db.GetInfo()
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	headers := writer.Header()
	headers.Set("X-Backend-Timestamp", info.CreatedAt)
	headers.Set("X-Backend-PUT-Timestamp", info.PutTimestamp)
	headers.Set("X-Backend-DELETE-Timestamp", info.DeleteTimestamp)
	headers.Set("X-Backend-Status-Changed-At", info.StatusChangedAt)
	headers.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(info.StoragePolicyIndex))
	if db.IsDeleted() {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	} else {
		headers.Set("X-Container-Object-Count", strconv.FormatInt(info.ObjectCount, 10))
		headers.Set("X-Container-Bytes-Used", strconv.FormatInt(info.BytesUsed, 10))
		headers.Set("X-Timestamp", info.CreatedAt)
		headers.Set("X-PUT-Timestamp", info.PutTimestamp)
		for key, value := range db.GetMetadata() {
			headers.Set(key, value)
		}
	}
	writer.WriteHeader(204)
	writer.Write([]byte(""))
}

func (server ContainerHandler) ContainerPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	timestamp := request.Header.Get("X-Timestamp")
	policyIndex := 0
	metadata := make(map[string][]string)
	for key, _ := range request.Header {
		_, inSaveHeaders := saveHeaders[key]
		if !(strings.HasPrefix(key, "X-Container-Meta-") || strings.HasPrefix(key, "X-Container-Sysmeta") || inSaveHeaders) {
			continue
		} else if request.Header.Get(key) == "" {
			continue
		}
		metadata[key] = []string{request.Header.Get(key), timestamp}
	}
	created, err := CreateDatabase(containerFile, vars["account"], vars["container"], timestamp, metadata, policyIndex)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	db, err := OpenDatabase(containerFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	info, _ := db.GetInfo()
	UpdateAccount(info, request, vars)
	if created {
		http.Error(writer, http.StatusText(201), 201)
	} else {
		http.Error(writer, http.StatusText(202), 202)
	}
}

func (server ContainerHandler) ContainerDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	db, err := OpenDatabase(containerFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	db.CommitPending()
	timestamp := request.Header.Get("X-Timestamp")
	info, _ := db.GetInfo()
	if info.ObjectCount > 0 {
		http.Error(writer, http.StatusText(409), 409)
		return
	}
	db.Delete(timestamp)
	UpdateAccount(info, request, vars)
	writer.WriteHeader(204)
	writer.Write([]byte(""))
}

func (server ContainerHandler) ContainerPostHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	timestamp := request.Header.Get("X-Timestamp")
	updates := make(map[string][]string)
	for key, _ := range request.Header {
		_, inSaveHeaders := saveHeaders[key]
		if !(strings.HasPrefix(key, "X-Container-Meta-") || strings.HasPrefix(key, "X-Container-Sysmeta") || inSaveHeaders) {
			continue
		}
		updates[key] = []string{request.Header.Get(key), timestamp}
	}
	db, err := OpenDatabase(containerFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	db.UpdateMetadata(updates)
	writer.WriteHeader(204)
	writer.Write([]byte(""))
}

func (server ContainerHandler) ObjPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	timestamp := request.Header.Get("X-Timestamp")
	size, _ := strconv.ParseInt(request.Header.Get("X-Size"), 10, 64)
	contentType := request.Header.Get("X-Content-Type")
	etag := request.Header.Get("X-Etag")
	deleted := 0
	storagePolicyIndex := 0 // TODO: figure out where this comes from in real life
	err = PutObject(containerFile, vars["obj"], timestamp, size, contentType, etag, deleted, storagePolicyIndex)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	http.Error(writer, http.StatusText(201), 201)
}

func (server ContainerHandler) ObjDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	containerFile, err := ContainerLocation(vars, server)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	timestamp := request.Header.Get("X-Timestamp")
	storagePolicyIndex := 0 // TODO: figure out where this comes from in real life
	err = PutObject(containerFile, vars["obj"], timestamp, 0, "", "", 1, storagePolicyIndex)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(204)
	writer.Write([]byte(""))
}

func GetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

func (server ContainerHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	request.Body.Close()
	if request.URL.Path == "/healthcheck" {
		writer.Header().Set("Content-Length", "2")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
		return
	} else if strings.HasPrefix(request.URL.Path, "/recon/") {
		hummingbird.ReconHandler(server.driveRoot, writer, request)
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
	if len(parts) == 5 {
		switch newRequest.Method {
		case "GET":
			server.ContainerGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.ContainerHeadHandler(newWriter, newRequest, vars)
		case "PUT":
			server.ContainerPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.ContainerDeleteHandler(newWriter, newRequest, vars)
		case "POST":
			server.ContainerPostHandler(newWriter, newRequest, vars)
		}
	} else if len(parts) == 6 {
		switch newRequest.Method {
		case "PUT":
			server.ObjPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.ObjDeleteHandler(newWriter, newRequest, vars)
		}
	}

	go server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		request.URL.Path,
		newWriter.Status,
		GetDefault(writer.Header(), "Content-Length", "-"),
		GetDefault(request.Header, "Referer", "-"),
		GetDefault(request.Header, "X-Trans-Id", "-"),
		GetDefault(request.Header, "User-Agent", "-"),
		time.Since(newRequest.Start).Seconds(),
		"-")) // TODO: "additional info"
}

func GetServer(conf string) (string, int, http.Handler) {
	InitializeDatabase()

	handler := ContainerHandler{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		checkMounts: true,
	}

	if swiftconf, err := hummingbird.LoadIniFile("/etc/swift/swift.conf"); err == nil {
		handler.hashPathPrefix = swiftconf.GetDefault("swift-hash", "swift_hash_path_prefix", "")
		handler.hashPathSuffix = swiftconf.GetDefault("swift-hash", "swift_hash_path_suffix", "")
	}

	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		panic(fmt.Sprintf("Unable to load %s", conf))
	}
	handler.driveRoot = serverconf.GetDefault("DEFAULT", "devices", "/srv/node")
	handler.checkMounts = hummingbird.LooksTrue(serverconf.GetDefault("DEFAULT", "mount_check", "true"))
	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort, err := strconv.ParseInt(serverconf.GetDefault("DEFAULT", "bind_port", "6001"), 10, 64)
	if err != nil {
		panic("Invalid bind port format")
	}
	handler.logger = hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "object-server")
	hummingbird.DropPrivileges(serverconf.GetDefault("DEFAULT", "user", "swift"))

	return bindIP, int(bindPort), handler
}
