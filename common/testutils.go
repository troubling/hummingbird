package common

import (
	"sync"
	"sync/atomic"

	"github.com/uber-go/tally"
)

type testScope struct {
	lock     sync.RWMutex
	counters map[string]tally.Counter
}

func NewTestScope() *testScope {
	return &testScope{counters: map[string]tally.Counter{}}
}

func (t *testScope) Counter(name string) tally.Counter {
	t.lock.RLock()
	c := t.counters[name]
	t.lock.RUnlock()
	if c == nil {
		t.lock.Lock()
		c = t.counters[name]
		if c == nil {
			c = &TestCounter{}
			t.counters[name] = c
		}
		t.lock.Unlock()
	}
	return c
}

func (t *testScope) Gauge(name string) tally.Gauge {
	panic("Not implemented yet.")
}

func (t *testScope) Timer(name string) tally.Timer {
	panic("Not implemented yet.")
}

func (t *testScope) Histogram(name string, buckets tally.Buckets) tally.Histogram {
	panic("Not implemented yet.")
}

func (t *testScope) Tagged(tags map[string]string) tally.Scope {
	panic("Not implemented yet.")
}

func (t *testScope) SubScope(name string) tally.Scope {
	panic("Not implemented yet.")
}

func (t *testScope) Capabilities() tally.Capabilities {
	panic("Not implemented yet.")
}

type TestCounter struct {
	count int64
}

func (c *TestCounter) Inc(delta int64) {
	atomic.AddInt64(&c.count, delta)
}

func (c *TestCounter) Value() int64 {
	return atomic.LoadInt64(&c.count)
}
