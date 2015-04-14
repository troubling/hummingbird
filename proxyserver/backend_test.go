// //  Copyright (c) 2015 Rackspace
// //
// //  Licensed under the Apache License, Version 2.0 (the "License");
// //  you may not use this file except in compliance with the License.
// //  You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// //  Unless required by applicable law or agreed to in writing, software
// //  distributed under the License is distributed on an "AS IS" BASIS,
// //  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// //  implied.
// //  See the License for the specific language governing permissions and
// //  limitations under the License.

package proxyserver

// import (
// 	"net"
// 	"net/http"
// 	"net/http/httptest"
// 	"testing"

// 	hummingbird "hummingbird/common"

// 	"github.com/stretchr/testify/assert"
// )

// func TestLogRequest(t *testing.T) {
// 	conf := "/etc/swift/proxy-server.conf"
// 	serverconf, _ := hummingbird.LoadIniFile(conf)
// 	addr := net.UDPAddr{
// 		Port: 9229,
// 		IP:   net.ParseIP("127.0.0.1"),
// 	}
// 	conn, err := net.ListenUDP("udp", &addr)
// 	defer conn.Close()
// 	if err != nil {
// 		panic(err)
// 	}

// 	logger := hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "proxy-server", "127.0.0.1:9229")
// 	writer := httptest.NewRecorder()
// 	request, _ := http.NewRequest("GET", "127.0.0.1:1234/v1/a/c/o", nil)
// 	w, r := CreateWriterAndRequest(writer, request, logger)
// 	r.TransactionId = "transactionId"
// 	LogRequest(w, r, logger)
// 	msg := make([]byte, 1024)
// 	conn.ReadFromUDP(msg)
// 	assert.Contains(t, string(msg), "\"GET 127.0.0.1:1234/v1/a/c/o\" 500 - \"-\" \"transactionId\" \"-\"")
// }

// func TestCreateWriterAndRequest(t *testing.T) {
// 	conf := "/etc/swift/proxy-server.conf"
// 	serverconf, _ := hummingbird.LoadIniFile(conf)
// 	logger := hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "proxy-server", "127.0.0.1:9229")
// 	writer := httptest.NewRecorder()
// 	request, _ := http.NewRequest("GET", "127.0.0.1:1234/v1/a", nil)

// 	newWriter, newRequest := CreateWriterAndRequest(writer, request, logger)
// 	assert.Equal(t, newWriter.ResponseWriter, writer)
// 	assert.Equal(t, 500, newWriter.Status)
// 	assert.Equal(t, false, newWriter.ResponseStarted)

// 	assert.Equal(t, newRequest.Request, request)
// 	assert.NotNil(t, newRequest.TransactionId)
// 	assert.NotNil(t, newRequest.XTimestamp)
// 	assert.NotNil(t, newRequest.Start)
// 	assert.Equal(t, newRequest.logger, logger)
// }
