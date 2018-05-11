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

package ring

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"

	opentracing "github.com/opentracing/opentracing-go"
)

const (
	jsonFlag    = 2
	opGet       = byte(0x00)
	opSet       = byte(0x01)
	opDelete    = byte(0x04)
	opIncrement = byte(0x05)
	opDecrement = byte(0x06)
	confSection = "filter:cache"
)

type MemcacheRing interface {
	Decr(ctx context.Context, key string, delta int64, timeout int) (int64, error)
	Delete(ctx context.Context, key string) error
	Get(ctx context.Context, key string) (interface{}, error)
	GetStructured(ctx context.Context, key string, val interface{}) error
	GetMulti(ctx context.Context, serverKey string, keys []string) (map[string]interface{}, error)
	Incr(ctx context.Context, key string, delta int64, timeout int) (int64, error)
	Set(ctx context.Context, key string, value interface{}, timeout int) error
	SetMulti(ctx context.Context, serverKey string, values map[string]interface{}, timeout int) error
}

type tracingMemcacheRing struct {
	MemcacheRing
	tracer opentracing.Tracer
}

func NewTracingMemcacheRing(mc MemcacheRing, tracer opentracing.Tracer) *tracingMemcacheRing {
	return &tracingMemcacheRing{MemcacheRing: mc, tracer: tracer}
}

func (r *tracingMemcacheRing) getSpanContext(ctx context.Context) opentracing.SpanContext {
	var parentContext opentracing.SpanContext
	if span := opentracing.SpanFromContext(ctx); span != nil {
		parentContext = span.Context()
	}
	return parentContext
}

func (r *tracingMemcacheRing) addKey(span opentracing.Span, key string) {
	if strings.HasPrefix(key, "account/") || strings.HasPrefix(key, "container/") {
		span.SetTag("key", key)
	}
}

func (r *tracingMemcacheRing) Decr(ctx context.Context, key string, delta int64, timeout int) (int64, error) {
	mcSpan := r.tracer.StartSpan("Memcache Decr", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, key)
	defer mcSpan.Finish()
	return r.MemcacheRing.Decr(ctx, key, delta, timeout)
}

func (r *tracingMemcacheRing) Delete(ctx context.Context, key string) error {
	mcSpan := r.tracer.StartSpan("Memcache Delete", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, key)
	defer mcSpan.Finish()
	return r.MemcacheRing.Delete(ctx, key)
}

func (r *tracingMemcacheRing) Get(ctx context.Context, key string) (interface{}, error) {
	mcSpan := r.tracer.StartSpan("Memcache Get", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, key)
	defer mcSpan.Finish()
	return r.MemcacheRing.Get(ctx, key)
}

func (r *tracingMemcacheRing) GetStructured(ctx context.Context, key string, val interface{}) error {
	mcSpan := r.tracer.StartSpan("Memcache GetStructured", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, key)
	defer mcSpan.Finish()
	return r.MemcacheRing.GetStructured(ctx, key, val)
}

func (r *tracingMemcacheRing) GetMulti(ctx context.Context, serverKey string, keys []string) (map[string]interface{}, error) {
	mcSpan := r.tracer.StartSpan("Memcache GetMulti", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, serverKey)
	defer mcSpan.Finish()
	return r.MemcacheRing.GetMulti(ctx, serverKey, keys)
}

func (r *tracingMemcacheRing) Incr(ctx context.Context, key string, delta int64, timeout int) (int64, error) {
	mcSpan := r.tracer.StartSpan("Memcache Incr", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, key)
	defer mcSpan.Finish()
	return r.MemcacheRing.Incr(ctx, key, delta, timeout)
}

func (r *tracingMemcacheRing) Set(ctx context.Context, key string, value interface{}, timeout int) error {
	mcSpan := r.tracer.StartSpan("Memcache Set", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, key)
	defer mcSpan.Finish()
	return r.MemcacheRing.Set(ctx, key, value, timeout)
}

func (r *tracingMemcacheRing) SetMulti(ctx context.Context, serverKey string, values map[string]interface{}, timeout int) error {
	mcSpan := r.tracer.StartSpan("Memcache SetMulti", opentracing.ChildOf(r.getSpanContext(ctx)))
	r.addKey(mcSpan, serverKey)
	defer mcSpan.Finish()
	return r.MemcacheRing.SetMulti(ctx, serverKey, values, timeout)
}

type memcacheRing struct {
	ring                        map[string]string
	serverKeys                  []string
	servers                     map[string]*server
	connTimeout                 int64
	responseTimeout             int64
	maxFreeConnectionsPerServer int64
	tries                       int64
	nodeWeight                  int64
	tracing                     bool
}

func NewMemcacheRing(confPath string) (*memcacheRing, error) {
	config, err := conf.LoadConfig(confPath)
	if err != nil {
		return nil, fmt.Errorf("Unable to load conf file: %s: %s", confPath, err)
	}
	return NewMemcacheRingFromConfig(config)
}

func NewMemcacheRingFromConfig(config conf.Config) (*memcacheRing, error) {
	ring := &memcacheRing{}
	ring.ring = make(map[string]string)
	ring.serverKeys = make([]string, 0)
	ring.servers = make(map[string]*server)

	ring.maxFreeConnectionsPerServer = config.GetInt(confSection, "max_free_connections_per_server", 100)
	ring.connTimeout = config.GetInt(confSection, "conn_timeout", 100)
	ring.responseTimeout = config.GetInt(confSection, "response_timeout", 100)
	ring.nodeWeight = config.GetInt(confSection, "node_weight", 50)
	ring.tries = config.GetInt(confSection, "tries", 5)
	for _, s := range strings.Split(config.GetDefault(confSection, "memcache_servers", ""), ",") {
		err := ring.addServer(s)
		if err != nil {
			return nil, err
		}
	}
	if len(ring.servers) == 0 {
		ring.addServer("127.0.0.1:11211")
	}
	ring.sortServerKeys()
	if int64(len(ring.servers)) < ring.tries {
		ring.tries = int64(len(ring.servers))
	}
	return ring, nil
}

func hashKey(s string) string {
	h := md5.New()
	io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
}

func (ring *memcacheRing) addServer(serverString string) error {
	server, err := newServer(serverString, ring.connTimeout, ring.responseTimeout, ring.maxFreeConnectionsPerServer)
	if err != nil {
		return err
	}
	ring.servers[serverString] = server
	for i := 0; int64(i) < ring.nodeWeight; i++ {
		ring.ring[hashKey(fmt.Sprintf("%s-%d", serverString, i))] = serverString
	}
	return nil
}

func (ring *memcacheRing) sortServerKeys() {
	ring.serverKeys = make([]string, 0)
	for k := range ring.ring {
		ring.serverKeys = append(ring.serverKeys, k)
	}
	sort.Strings(ring.serverKeys)
}

func (ring *memcacheRing) Decr(ctx context.Context, key string, delta int64, timeout int) (int64, error) {
	return ring.Incr(ctx, key, -delta, timeout)
}

func (ring *memcacheRing) Delete(ctx context.Context, key string) error {
	fn := func(conn *connection) error {
		_, _, err := conn.roundTripPacket(opDelete, hashKey(key), nil, nil)
		if err != nil && err != CacheMiss {
			return err
		}
		return nil
	}
	return ring.loop(key, fn)
}

func (ring *memcacheRing) GetStructured(ctx context.Context, key string, val interface{}) error {
	fn := func(conn *connection) error {
		value, extras, err := conn.roundTripPacket(opGet, hashKey(key), nil, nil)
		if err != nil {
			return err
		}
		flags := binary.BigEndian.Uint32(extras[0:4])
		if flags&jsonFlag == 0 {
			return errors.New("Not json data")
		}
		if err := json.Unmarshal(value, val); err != nil {
			return err
		}
		return nil
	}
	return ring.loop(key, fn)
}

func (ring *memcacheRing) Get(ctx context.Context, key string) (interface{}, error) {
	type Return struct {
		value interface{}
	}
	ret := &Return{}
	fn := func(conn *connection) error {
		value, extras, err := conn.roundTripPacket(opGet, hashKey(key), nil, nil)
		if err != nil {
			return err
		}
		flags := binary.BigEndian.Uint32(extras[0:4])
		if flags&jsonFlag != 0 {
			if err := json.Unmarshal(value, &ret.value); err != nil {
				return err
			}
		} else {
			ret.value = value
		}
		return nil
	}
	return ret.value, ring.loop(key, fn)
}

func (ring *memcacheRing) GetMulti(ctx context.Context, serverKey string, keys []string) (map[string]interface{}, error) {
	type Return struct {
		value map[string]interface{}
	}
	ret := &Return{}
	fn := func(conn *connection) error {
		ret.value = make(map[string]interface{})
		for _, key := range keys {
			if err := conn.sendPacket(opGet, hashKey(key), nil, nil); err != nil {
				return err
			}
		}
		for _, key := range keys {
			value, extras, err := conn.receivePacket()
			if err != nil {
				if err == CacheMiss {
					continue
				}
				return err
			}
			flags := binary.BigEndian.Uint32(extras[0:4])
			if flags&jsonFlag != 0 {
				var v interface{}
				if err := json.Unmarshal(value, &v); err != nil {
					return err
				}
				ret.value[key] = v
			} else {
				ret.value[key] = value
			}
		}
		return nil
	}
	return ret.value, ring.loop(serverKey, fn)
}

func (ring *memcacheRing) Incr(ctx context.Context, key string, delta int64, timeout int) (int64, error) {
	type Return struct {
		value int64
	}
	ret := &Return{}
	op := opIncrement
	dfl := delta
	if delta < 0 {
		op = opDecrement
		delta = 0 - delta
		dfl = 0
	}
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras[0:8], uint64(delta))
	binary.BigEndian.PutUint64(extras[8:16], uint64(dfl))
	binary.BigEndian.PutUint32(extras[16:20], uint32(timeout))
	fn := func(conn *connection) error {
		value, _, err := conn.roundTripPacket(op, hashKey(key), nil, extras)
		if err != nil {
			return err
		}
		ret.value = int64(binary.BigEndian.Uint64(value[0:8]))
		return nil
	}
	return ret.value, ring.loop(key, fn)
}

func (ring *memcacheRing) Set(ctx context.Context, key string, value interface{}, timeout int) error {
	serl, err := json.Marshal(value)
	if err != nil {
		return err
	}
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras[0:4], uint32(jsonFlag))
	binary.BigEndian.PutUint32(extras[4:8], uint32(timeout))
	fn := func(conn *connection) error {
		_, _, err = conn.roundTripPacket(opSet, hashKey(key), serl, extras)
		return err
	}
	return ring.loop(key, fn)
}

func (ring *memcacheRing) SetMulti(ctx context.Context, serverKey string, values map[string]interface{}, timeout int) error {
	fn := func(conn *connection) error {
		for key, value := range values {
			serl, err := json.Marshal(value)
			if err != nil {
				return err
			}
			extras := make([]byte, 8)
			binary.BigEndian.PutUint32(extras[0:4], uint32(jsonFlag))
			binary.BigEndian.PutUint32(extras[4:8], uint32(timeout))
			if err = conn.sendPacket(opSet, hashKey(key), serl, extras); err != nil {
				return err
			}
		}
		for range values {
			if _, _, err := conn.receivePacket(); err != nil {
				return err
			}
		}
		return nil
	}
	return ring.loop(serverKey, fn)
}

type serverIterator struct {
	ring    *memcacheRing
	key     string
	current int
	servers []string
}

func (ring *memcacheRing) newServerIterator(key string) *serverIterator {
	return &serverIterator{ring, hashKey(key), -1, make([]string, 0)}
}

func (it *serverIterator) next() bool {
	return int64(len(it.servers)) < it.ring.tries
}

func (it *serverIterator) value() *server {
	if int64(len(it.servers)) > it.ring.tries {
		panic("serverIterator.Value() called when there are no more tries left")
	}
	if it.current == -1 {
		it.current = sort.SearchStrings(it.ring.serverKeys, it.key) % len(it.ring.serverKeys)
	} else {
		for common.StringInSlice(it.ring.ring[it.ring.serverKeys[it.current]], it.servers) {
			it.current = (it.current + 1) % len(it.ring.serverKeys)
		}
	}
	serverString := it.ring.ring[it.ring.serverKeys[it.current]]
	it.servers = append(it.servers, serverString)
	return it.ring.servers[serverString]
}

var noServersError = errors.New("no memcache servers in ring")

func (ring *memcacheRing) loop(key string, fn func(*connection) error) error {
	err := noServersError
	it := ring.newServerIterator(key)
	for it.next() {
		server := it.value()
		var conn *connection
		conn, err = server.getConnection()
		if err != nil {
			continue
		}
		err = fn(conn)
		server.releaseConnection(conn, err)
		if err == nil {
			return nil
		} else if err == CacheMiss {
			return err
		}
	}
	return err
}

type server struct {
	serverString       string
	addr               net.Addr
	lock               sync.Mutex
	connTimeout        time.Duration
	requestTimeout     time.Duration
	maxFreeConnections int64
	connections        []*connection
}

func newServer(serverString string, connTimeout int64, requestTimeout int64, maxFreeConnections int64) (*server, error) {
	var addr net.Addr
	var err error
	s := server{serverString: serverString}
	if strings.Contains(serverString, "/") {
		addr, err = net.ResolveUnixAddr("unix", serverString)
		if err != nil {
			return nil, err
		}
	} else {
		if !strings.Contains(serverString, ":") {
			serverString = fmt.Sprintf("%s:11211", serverString)
		}
		addr, err = net.ResolveTCPAddr("tcp", serverString)
		if err != nil {
			return nil, err
		}
	}
	s.addr = addr
	s.connTimeout = time.Duration(connTimeout) * time.Millisecond
	s.requestTimeout = time.Duration(requestTimeout) * time.Millisecond
	s.maxFreeConnections = maxFreeConnections
	s.connections = make([]*connection, 0)
	return &s, nil
}

func (s *server) connectionCount() uint64 {
	return uint64(len(s.connections))
}

func (s *server) getExistingConnection() *connection {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.connections) == 0 {
		return nil
	}
	conn := s.connections[len(s.connections)-1]
	s.connections = s.connections[:len(s.connections)-1]
	return conn
}

func (s *server) getConnection() (*connection, error) {
	conn := s.getExistingConnection()
	if conn != nil {
		return conn, nil
	}
	return newConnection(s.serverString, s.connTimeout, s.requestTimeout)
}

func (s *server) releaseConnection(conn *connection, err error) {
	if err == nil || err == CacheMiss {
		s.lock.Lock()
		defer s.lock.Unlock()
		if int64(len(s.connections)) < s.maxFreeConnections {
			s.connections = append(s.connections, conn)
			return
		}
	}
	conn.close()
}

var CacheMiss = fmt.Errorf("Server cache miss")

type connection struct {
	conn       net.Conn
	rw         *bufio.ReadWriter
	reqTimeout time.Duration
	packetBuf  []byte
}

func newConnection(address string, connTimeout time.Duration, requestTimeout time.Duration) (*connection, error) {
	domain := "tcp"
	if strings.Contains(address, "/") {
		domain = "unix"
	} else if !strings.Contains(address, ":") {
		address = fmt.Sprintf("%s:%d", address, 11211)
	}
	conn, err := net.DialTimeout(domain, address, connTimeout)
	if err != nil {
		return nil, err
	}
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetNoDelay(true)
	}
	return &connection{
		conn:       conn,
		rw:         bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		reqTimeout: requestTimeout,
		packetBuf:  make([]byte, 256),
	}, nil
}

func (c *connection) close() error {
	return c.conn.Close()
}

func (c *connection) receivePacket() ([]byte, []byte, error) {
	packet := c.packetBuf[0:24]
	if _, err := io.ReadFull(c.rw, packet); err != nil {
		c.close()
		return nil, nil, err
	}
	keyLen := binary.BigEndian.Uint16(packet[2:4])
	extrasLen := packet[4]
	status := binary.BigEndian.Uint16(packet[6:8])
	bodyLen := int(binary.BigEndian.Uint32(packet[8:12]))
	for cap(c.packetBuf) < bodyLen {
		c.packetBuf = append(c.packetBuf, ' ')
	}
	body := c.packetBuf[0:bodyLen]
	if _, err := io.ReadFull(c.rw, body); err != nil {
		c.close()
		return nil, nil, err
	}
	switch status {
	case 0:
		return body[int(keyLen)+int(extrasLen) : int(bodyLen)], body[int(keyLen):int(extrasLen)], nil
	case 1:
		return nil, nil, CacheMiss
	default:
		return nil, nil, fmt.Errorf("Error response from memcache: %d", status)
	}
}

func (c *connection) roundTripPacket(opcode byte, hashKey string, value []byte, extras []byte) ([]byte, []byte, error) {
	if err := c.sendPacket(opcode, hashKey, value, extras); err != nil {
		return nil, nil, err
	}
	return c.receivePacket()
}

func (c *connection) sendPacket(opcode byte, hashKey string, value []byte, extras []byte) error {
	key := []byte(hashKey)
	c.conn.SetDeadline(time.Now().Add(c.reqTimeout))
	packet := c.packetBuf[0:24]
	packet[0], packet[1], packet[4], packet[5] = 0x80, opcode, byte(len(extras)), 0
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(key)))
	packet[6], packet[7] = 0, 0
	binary.BigEndian.PutUint32(packet[8:12], uint32(len(key)+len(value)+len(extras)))
	packet[12], packet[13], packet[14], packet[15] = 0, 0, 0, 0
	packet[16], packet[17], packet[18], packet[19], packet[20], packet[21], packet[22], packet[23] = 0, 0, 0, 0, 0, 0, 0, 0
	packet = append(append(append(packet, extras...), key...), value...)
	if _, err := c.rw.Write(packet); err != nil || c.rw.Flush() != nil {
		c.close()
		return err
	}
	return nil
}
