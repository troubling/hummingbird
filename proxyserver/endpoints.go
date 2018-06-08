package proxyserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/proxyserver/middleware"
	"go.uber.org/zap"
)

func (server *ProxyServer) EndpointsObjectGetHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		server.logger.Error("could not get proxy context")
		srv.StandardResponse(writer, 500)
		return
	}
	vars := srv.GetVars(request)
	ring, resp := ctx.C.ObjectRingFor(request.Context(), vars["account"], vars["container"])
	if resp != nil {
		body, _ := ioutil.ReadAll(resp.Body)
		srv.SimpleErrorResponse(writer, 500, string(body))
		return
	}
	partition := ring.GetPartition(vars["account"], vars["container"], vars["obj"])
	endpoints := []string{}
	for _, device := range ring.GetNodes(partition) {
		endpoints = append(endpoints, fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(vars["account"]), common.Urlencode(vars["container"]), common.Urlencode(vars["obj"])))
	}
	body, err := json.Marshal(endpoints)
	if err != nil {
		server.logger.Error("could not marshal endpoints", zap.Any("endpoints", endpoints), zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(200)
	writer.Write(body)
}

func (server *ProxyServer) EndpointsContainerGetHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		server.logger.Error("could not get proxy context")
		srv.StandardResponse(writer, 500)
		return
	}
	vars := srv.GetVars(request)
	ring := ctx.C.ContainerRing()
	partition := ring.GetPartition(vars["account"], vars["container"], "")
	endpoints := []string{}
	for _, device := range ring.GetNodes(partition) {
		endpoints = append(endpoints, fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(vars["account"]), common.Urlencode(vars["container"])))
	}
	body, err := json.Marshal(endpoints)
	if err != nil {
		server.logger.Error("could not marshal endpoints", zap.Any("endpoints", endpoints), zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(200)
	writer.Write(body)
}

func (server *ProxyServer) EndpointsAccountGetHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		server.logger.Error("could not get proxy context")
		srv.StandardResponse(writer, 500)
		return
	}
	vars := srv.GetVars(request)
	ring := ctx.C.AccountRing()
	partition := ring.GetPartition(vars["account"], "", "")
	endpoints := []string{}
	for _, device := range ring.GetNodes(partition) {
		endpoints = append(endpoints, fmt.Sprintf("%s://%s:%d/%s/%d/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(vars["account"])))
	}
	body, err := json.Marshal(endpoints)
	if err != nil {
		server.logger.Error("could not marshal endpoints", zap.Any("endpoints", endpoints), zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(200)
	writer.Write(body)
}

func (server *ProxyServer) EndpointsObjectGetHandler2(writer http.ResponseWriter, request *http.Request) {
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		server.logger.Error("could not get proxy context")
		srv.StandardResponse(writer, 500)
		return
	}
	vars := srv.GetVars(request)
	containerInfo, err := ctx.C.GetContainerInfo(request.Context(), vars["account"], vars["container"])
	if err != nil {
		server.logger.Error("could not obtain container info", zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	ring, resp := ctx.C.ObjectRingFor(request.Context(), vars["account"], vars["container"])
	if resp != nil {
		body, _ := ioutil.ReadAll(resp.Body)
		srv.SimpleErrorResponse(writer, 500, string(body))
		return
	}
	partition := ring.GetPartition(vars["account"], vars["container"], vars["obj"])
	data := struct {
		Endpoints []string          `json:"endpoints"`
		Headers   map[string]string `json:"headers"`
	}{Headers: map[string]string{}}
	data.Headers["X-Backend-Storage-Policy-Index"] = strconv.Itoa(containerInfo.StoragePolicyIndex)
	for _, device := range ring.GetNodes(partition) {
		data.Endpoints = append(data.Endpoints, fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(vars["account"]), common.Urlencode(vars["container"]), common.Urlencode(vars["obj"])))
	}
	body, err := json.Marshal(data)
	if err != nil {
		server.logger.Error("could not marshal data", zap.Any("data", data), zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(200)
	writer.Write(body)
}

func (server *ProxyServer) EndpointsContainerGetHandler2(writer http.ResponseWriter, request *http.Request) {
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		server.logger.Error("could not get proxy context")
		srv.StandardResponse(writer, 500)
		return
	}
	vars := srv.GetVars(request)
	containerInfo, err := ctx.C.GetContainerInfo(request.Context(), vars["account"], vars["container"])
	if err != nil {
		server.logger.Error("could not obtain container info", zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	ring := ctx.C.ContainerRing()
	partition := ring.GetPartition(vars["account"], vars["container"], "")
	data := struct {
		Endpoints []string          `json:"endpoints"`
		Headers   map[string]string `json:"headers"`
	}{Headers: map[string]string{}}
	data.Headers["X-Backend-Storage-Policy-Index"] = strconv.Itoa(containerInfo.StoragePolicyIndex)
	for _, device := range ring.GetNodes(partition) {
		data.Endpoints = append(data.Endpoints, fmt.Sprintf("%s://%s:%d/%s/%d/%s/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(vars["account"]), common.Urlencode(vars["container"])))
	}
	body, err := json.Marshal(data)
	if err != nil {
		server.logger.Error("could not marshal data", zap.Any("data", data), zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(200)
	writer.Write(body)
}

func (server *ProxyServer) EndpointsAccountGetHandler2(writer http.ResponseWriter, request *http.Request) {
	ctx := middleware.GetProxyContext(request)
	if ctx == nil {
		server.logger.Error("could not get proxy context")
		srv.StandardResponse(writer, 500)
		return
	}
	vars := srv.GetVars(request)
	ring := ctx.C.AccountRing()
	partition := ring.GetPartition(vars["account"], "", "")
	data := struct {
		Endpoints []string          `json:"endpoints"`
		Headers   map[string]string `json:"headers"`
	}{Headers: map[string]string{}}
	for _, device := range ring.GetNodes(partition) {
		data.Endpoints = append(data.Endpoints, fmt.Sprintf("%s://%s:%d/%s/%d/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, common.Urlencode(vars["account"])))
	}
	body, err := json.Marshal(data)
	if err != nil {
		server.logger.Error("could not marshal data", zap.Any("data", data), zap.Error(err))
		srv.StandardResponse(writer, 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(200)
	writer.Write(body)
}
