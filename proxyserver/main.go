//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package proxyserver

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/troubling/hummingbird/proxyserver/middleware"

	"github.com/justinas/alice"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
)

type ProxyServer struct {
	logger            srv.LowLevelLogger
	logLevel          zap.AtomicLevel
	mc                ring.MemcacheRing
	accountAutoCreate bool
	proxyDirectClient *client.ProxyDirectClient
	metricsCloser     io.Closer
}

func (server *ProxyServer) Type() string {
	return "proxy"
}

func (server *ProxyServer) Finalize() {
	if server.metricsCloser != nil {
		server.metricsCloser.Close()
	}
}

func (server *ProxyServer) GetHandler(config conf.Config, metricsPrefix string) http.Handler {
	var metricsScope tally.Scope
	metricsScope, server.metricsCloser = tally.NewRootScope(tally.ScopeOptions{
		Prefix:         metricsPrefix,
		Tags:           map[string]string{},
		CachedReporter: promreporter.NewReporter(promreporter.Options{}),
		Separator:      promreporter.DefaultSeparator,
	}, time.Second)
	router := srv.NewRouter()
	router.Get("/metrics", prometheus.Handler())
	router.Get("/loglevel", server.logLevel)
	router.Put("/loglevel", server.logLevel)
	router.Get("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectGetHandler))
	router.Head("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectHeadHandler))
	router.Put("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectPutHandler))
	router.Delete("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectDeleteHandler))
	router.Post("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectPostHandler))
	router.Options("/v1/:account/:container/*obj", http.HandlerFunc(server.OptionsHandler))

	router.Get("/v1/:account/:container", http.HandlerFunc(server.ContainerGetHandler))
	router.Get("/v1/:account/:container/", http.HandlerFunc(server.ContainerGetHandler))
	router.Head("/v1/:account/:container", http.HandlerFunc(server.ContainerHeadHandler))
	router.Head("/v1/:account/:container/", http.HandlerFunc(server.ContainerHeadHandler))
	router.Put("/v1/:account/:container", http.HandlerFunc(server.ContainerPutHandler))
	router.Put("/v1/:account/:container/", http.HandlerFunc(server.ContainerPutHandler))
	router.Delete("/v1/:account/:container", http.HandlerFunc(server.ContainerDeleteHandler))
	router.Delete("/v1/:account/:container/", http.HandlerFunc(server.ContainerDeleteHandler))
	router.Post("/v1/:account/:container", http.HandlerFunc(server.ContainerPostHandler))
	router.Post("/v1/:account/:container/", http.HandlerFunc(server.ContainerPostHandler))
	router.Options("/v1/:account/:container", http.HandlerFunc(server.OptionsHandler))
	router.Options("/v1/:account/:container/", http.HandlerFunc(server.OptionsHandler))

	router.Get("/v1/:account", http.HandlerFunc(server.AccountGetHandler))
	router.Get("/v1/:account/", http.HandlerFunc(server.AccountGetHandler))
	router.Head("/v1/:account", http.HandlerFunc(server.AccountHeadHandler))
	router.Head("/v1/:account/", http.HandlerFunc(server.AccountHeadHandler))
	router.Put("/v1/:account", http.HandlerFunc(server.AccountPutHandler))
	router.Put("/v1/:account/", http.HandlerFunc(server.AccountPutHandler))
	router.Delete("/v1/:account", http.HandlerFunc(server.AccountDeleteHandler))
	router.Delete("/v1/:account/", http.HandlerFunc(server.AccountDeleteHandler))
	router.Post("/v1/:account", http.HandlerFunc(server.AccountPostHandler))
	router.Post("/v1/:account/", http.HandlerFunc(server.AccountPostHandler))
	router.Options("/v1/:account", http.HandlerFunc(server.OptionsHandler))
	router.Options("/v1/:account/", http.HandlerFunc(server.OptionsHandler))

	if config.GetBool("proxy-server", "pprof_enabled", false) {
		router.Get("/debug/pprof/:parm", http.DefaultServeMux)
		router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	}

	tempAuth := config.GetBool("proxy-server", "tempauth_enabled", true)
	var middlewares []struct {
		construct func(conf.Section, tally.Scope) (func(http.Handler) http.Handler, error)
		section   string
	}
	// TODO: make this all dynamical and stuff
	if tempAuth {
		middlewares = []struct {
			construct func(conf.Section, tally.Scope) (func(http.Handler) http.Handler, error)
			section   string
		}{
			{middleware.NewCatchError, "filter:catch_errors"},
			{middleware.NewHealthcheck, "filter:healthcheck"},
			{middleware.NewRequestLogger, "filter:proxy-logging"},
			{middleware.NewCors, "filter:cors"}, // TODO: i dont want to have to have a seciton for this
			{middleware.NewFormPost, "filter:formpost"},
			{middleware.NewTempURL, "filter:tempurl"},
			{middleware.NewTempAuth, "filter:tempauth"},
			{middleware.NewBulk, "filter:bulk"},
			{middleware.NewMultirange, "filter:multirange"},
			{middleware.NewRatelimiter, "filter:ratelimit"},
			{middleware.NewStaticWeb, "filter:staticweb"},
			{middleware.NewCopyMiddleware, "filter:copy"},
			{middleware.NewVersionedWrites, "filter:versioned_writes"},
			{middleware.NewXlo, "filter:slo"},
		}
	} else {
		middlewares = []struct {
			construct func(conf.Section, tally.Scope) (func(http.Handler) http.Handler, error)
			section   string
		}{
			{middleware.NewCatchError, "filter:catch_errors"},
			{middleware.NewHealthcheck, "filter:healthcheck"},
			{middleware.NewRequestLogger, "filter:proxy-logging"},
			{middleware.NewCors, "filter:cors"},
			{middleware.NewFormPost, "filter:formpost"},
			{middleware.NewTempURL, "filter:tempurl"},
			{middleware.NewAuthToken, "filter:authtoken"},
			{middleware.NewKeystoneAuth, "filter:keystoneauth"},
			{middleware.NewBulk, "filter:bulk"},
			{middleware.NewMultirange, "filter:multirange"},
			{middleware.NewRatelimiter, "filter:ratelimit"},
			{middleware.NewStaticWeb, "filter:staticweb"},
			{middleware.NewCopyMiddleware, "filter:copy"},
			{middleware.NewVersionedWrites, "filter:versioned_writes"},
			{middleware.NewXlo, "filter:slo"},
		}
	}
	pipeline := alice.New(middleware.NewContext(config.GetBool("debug", "debug_x_source_code", false),
		server.mc, server.logger, server.proxyDirectClient))
	for _, m := range middlewares {
		mid, err := m.construct(config.GetSection(m.section), metricsScope)
		if err != nil {
			// TODO: propagate error upwards instead of panicking
			panic("Unable to construct middleware")
		}
		pipeline = pipeline.Append(mid)
	}
	return pipeline.Then(router)
}

func NewServer(serverconf conf.Config, flags *flag.FlagSet, cnf srv.ConfigLoader) (bindIP string, bindPort int, serv srv.Server, logger srv.LowLevelLogger, err error) {
	server := &ProxyServer{}
	server.mc, err = ring.NewMemcacheRingFromConfig(serverconf)
	if err != nil {
		return "", 0, nil, nil, err
	}

	bindIP = serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort = int(serverconf.GetInt("DEFAULT", "bind_port", 8080))

	logLevelString := serverconf.GetDefault("app:proxy-server", "log_level", "INFO")
	server.logLevel = zap.NewAtomicLevel()
	server.logLevel.UnmarshalText([]byte(strings.ToLower(logLevelString)))
	server.accountAutoCreate = serverconf.GetBool("app:proxy-server", "account_autocreate", false)
	if server.logger, err = srv.SetupLogger("proxy-server", &server.logLevel, flags); err != nil {
		return "", 0, nil, nil, fmt.Errorf("Error setting up logger: %v", err)
	}
	policies, err := cnf.GetPolicies()
	if err != nil {
		return "", 0, nil, nil, err
	}
	server.proxyDirectClient, err = client.NewProxyDirectClient(policies, cnf)
	if err != nil {
		return "", 0, nil, nil, fmt.Errorf("Error setting up proxyDirectClient: %v", err)
	}
	info := map[string]interface{}{
		"version":                  common.Version,
		"strict_cors_mode":         true,
		"policies":                 policies.GetPolicyInfo(),
		"account_autocreate":       server.accountAutoCreate,
		"allow_account_management": true,
	}
	for k, v := range common.DEFAULT_CONSTRAINTS {
		info[k] = v
	}
	middleware.RegisterInfo("swift", info)
	return bindIP, int(bindPort), server, server.logger, nil
}
