package main

import (
	"fmt"
	"io"
	"log/syslog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"hummingbird/common"
)

type ProxyServer struct {
	objectRing    *hummingbird.Ring
	containerRing *hummingbird.Ring
	accountRing   *hummingbird.Ring
	client        *http.Client
	logger        *syslog.Writer
	mc            *memcache.Client
}

// object that performs some number of requests asynchronously and aggregates the results

type MultiClient struct {
	client        *http.Client
	request_count int
	done          []chan int
}

func (mc *MultiClient) Do(req *http.Request) {
	donech := make(chan int)
	mc.done = append(mc.done, donech)
	go func(client *http.Client, req *http.Request, done chan int) {
		resp, err := client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err != nil {
			fmt.Println(err.Error())
			done <- 500
		} else {
			if resp.StatusCode/100 == 5 {
				blah := make([]byte, 8192)
				length, _ := resp.Body.Read(blah)
				fmt.Println("Error", string(blah[0:length]))
			}
			done <- resp.StatusCode
		}
	}(mc.client, req, donech)
	mc.request_count += 1
}

func (mc MultiClient) BestResponse(writer *hummingbird.SwiftWriter) {
	var responses []int
	quorum := (mc.request_count / 2) + 1
	for chanIndex, done := range mc.done {
		responses = append(responses, <-done)
		for status_range := 200; status_range <= 400; status_range += 100 {
			range_count := 0
			for _, response := range responses {
				if response >= status_range && response < (status_range+100) {
					range_count += 1
					if range_count >= quorum {
						http.Error(writer, http.StatusText(response), response)
						go func() { // wait for any remaining connections to finish
							for i := chanIndex + 1; i < len(mc.done); i++ {
								<-mc.done[i]
							}
						}()
						return
					}
				}
			}
		}
	}
	http.Error(writer, http.StatusText(500), 500)
}

// request handlers

func (server ProxyServer) ObjectGetHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	for _, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			fmt.Println(err)
			continue
		}
		request.CopyRequestHeaders(req)
		resp, err := server.client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err != nil {
			fmt.Println(err)
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			writer.CopyResponseHeaders(resp)
			writer.WriteHeader(resp.StatusCode)
			if request.Method == "GET" {
				io.Copy(writer, resp.Body)
			} else {
				writer.Write([]byte(""))
			}
			return
		}
	}
}

func (server ProxyServer) ObjectPutHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	container_partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	container_devices := server.containerRing.GetNodes(container_partition)
	var writers []*io.PipeWriter
	resultSet := MultiClient{server.client, 0, nil}
	for i, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		rp, wp := io.Pipe()
		defer wp.Close()
		defer rp.Close()
		req, err := http.NewRequest("PUT", url, rp)
		writers = append(writers, wp)
		if err != nil {
			server.logger.Err(err.Error())
			fmt.Printf("ERROR %s\n", err)
			continue
		}
		request.CopyRequestHeaders(req)
		req.ContentLength = request.ContentLength
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(container_partition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", container_devices[i].Ip, container_devices[i].Port))
		req.Header.Set("X-Container-Device", container_devices[i].Device)
		resultSet.Do(req)
	}
	mw := io.MultiWriter(writers[0], writers[1], writers[2])
	io.Copy(mw, request.Body)
	for _, writer := range writers {
		writer.Close()
	}
	resultSet.BestResponse(writer)
}

func (server ProxyServer) ObjectDeleteHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%s/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, _ := http.NewRequest("DELETE", url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(req)
	}
	rs.BestResponse(writer)
}

func (server ProxyServer) ContainerGetHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s?%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), request.URL.RawQuery)
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			server.logger.Err(err.Error())
			fmt.Printf("ERROR %s\n", err)
			continue
		}
		request.CopyRequestHeaders(req)
		request.CopyRequestHeaders(req)
		resp, err := server.client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			writer.CopyResponseHeaders(resp)
			writer.WriteHeader(http.StatusOK)
			if request.Method == "GET" {
				io.Copy(writer, resp.Body)
			} else {
				writer.Write([]byte(""))
			}
			return
		}
	}
}

func (server ProxyServer) ContainerPutHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(req)
	}
	rs.BestResponse(writer)
}

func (server ProxyServer) ContainerDeleteHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(req)
	}
	rs.BestResponse(writer)
}

func (server ProxyServer) AccountGetHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	fmt.Println("ACCOUNT GET?!")
	http.Error(writer, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
}

func (server ProxyServer) AccountPutHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], "", "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(vars["account"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(req)
	}
	rs.BestResponse(writer)
}

func (server ProxyServer) AccountDeleteHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], "", "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(vars["account"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(req)
	}
	rs.BestResponse(writer)
}

func (server ProxyServer) AuthHandler(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest, vars map[string]string) {
	token := make([]byte, 32)
	for i := range token {
		token[i] = byte('A' + (rand.Int() % 26))
	}
	user := request.Header.Get("X-Auth-User")
	key := fmt.Sprintf("auth/AUTH_%s/%s", user, string(token))
	server.mc.Set(&memcache.Item{Key: key, Value: []byte("VALID")})
	writer.Header().Set("X-Storage-Token", string(token))
	writer.Header().Set("X-Auth-Token", string(token))
	writer.Header().Set("X-Storage-URL", fmt.Sprintf("http://%s/v1/AUTH_%s", request.Host, user))
	http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
}

// access log

func (server ProxyServer) LogRequest(writer *hummingbird.SwiftWriter, request *hummingbird.SwiftRequest) {
	go server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		request.URL.Path,
		writer.Status,
		hummingbird.HeaderGetDefault(writer.Header(), "Content-Length", "-"),
		hummingbird.HeaderGetDefault(request.Header, "Referer", "-"),
		request.TransactionId,
		hummingbird.HeaderGetDefault(request.Header, "User-Agent", "-"),
		time.Since(request.Start).Seconds(),
		"-")) // TODO: "additional info", probably saved in request?
}

func (server ProxyServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthcheck" {
		writer.Header().Set("Content-Length", "2")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
		return
	}
	parts := strings.SplitN(request.URL.Path, "/", 5)
	vars := make(map[string]string)
	if len(parts) > 2 {
		vars["account"] = parts[2]
		if len(parts) > 3 {
			vars["container"] = parts[3]
			if len(parts) > 4 {
				vars["obj"] = parts[4]
			}
		}
	}
	newWriter := &hummingbird.SwiftWriter{writer, 500}
	newRequest := &hummingbird.SwiftRequest{request, hummingbird.GetTransactionId(), hummingbird.GetTimestamp(), time.Now()}
	defer server.LogRequest(newWriter, newRequest) // log the request after return

	if len(parts) >= 1 && parts[1] == "auth" {
		server.AuthHandler(newWriter, newRequest, vars)
		return
	} else if val := request.Header.Get("X-Auth-Token"); val == "" {
		http.Error(writer, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	key := fmt.Sprintf("auth/%s/%s", vars["account"], request.Header.Get("X-Auth-Token"))
	it, err := server.mc.Get(key)
	if err != nil || string(it.Value) != "VALID" {
		http.Error(writer, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	if len(parts) == 5 && parts[1] == "v1" {
		switch request.Method {
		case "GET":
			server.ObjectGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.ObjectGetHandler(newWriter, newRequest, vars)
		case "PUT":
			server.ObjectPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.ObjectDeleteHandler(newWriter, newRequest, vars)
		}
	} else if len(parts) == 4 && parts[1] == "v1" {
		switch request.Method {
		case "GET":
			server.ContainerGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.ContainerGetHandler(newWriter, newRequest, vars)
		case "PUT":
			server.ContainerPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.ContainerDeleteHandler(newWriter, newRequest, vars)
		}
	} else if len(parts) == 3 && parts[1] == "v1" {
		switch request.Method {
		case "GET":
			server.AccountGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.AccountGetHandler(newWriter, newRequest, vars)
		case "PUT":
			server.AccountPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.AccountDeleteHandler(newWriter, newRequest, vars)
		}
	} else {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}
}

func RunServer(conf string) {
	rand.Seed(time.Now().Unix())
	server := ProxyServer{}

	transport := http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
	}

	server.client = &http.Client{Transport: &transport}
	server.mc = memcache.New("127.0.0.1:11211")
	hashPathPrefix := ""
	hashPathSuffix := ""

	if swiftconf, err := hummingbird.LoadIniFile("/etc/swift/swift.conf"); err == nil {
		hashPathPrefix = swiftconf.GetDefault("swift-hash", "swift_hash_path_prefix", "")
		hashPathSuffix = swiftconf.GetDefault("swift-hash", "swift_hash_path_suffix", "")
	}

	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		panic(fmt.Sprintf("Unable to load %s", conf))
	}
	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort, err := strconv.ParseInt(serverconf.GetDefault("DEFAULT", "bind_port", "8080"), 10, 64)
	if err != nil {
		panic("Invalid bind port format")
	}

	sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindIP, bindPort))
	if err != nil {
		panic(fmt.Sprintf("Unable to bind %s:%d", bindIP, bindPort))
	}
	server.logger = hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "proxy-server")
	server.objectRing = hummingbird.LoadRing("/etc/swift/object.ring.gz", hashPathPrefix, hashPathSuffix)
	server.containerRing = hummingbird.LoadRing("/etc/swift/container.ring.gz", hashPathPrefix, hashPathSuffix)
	server.accountRing = hummingbird.LoadRing("/etc/swift/account.ring.gz", hashPathPrefix, hashPathSuffix)
	hummingbird.DropPrivileges(serverconf.GetDefault("DEFAULT", "user", "swift"))
	srv := &http.Server{Handler: server}
	srv.Serve(sock)
}

func main() {
	hummingbird.UseMaxProcs()
	if os.Args[1] == "saio" {
		go RunServer("/etc/swift/proxy-server.conf")
		for {
			time.Sleep(10000)
		}
	}
	RunServer(os.Args[1])
}
