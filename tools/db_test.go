package tools

import (
	"fmt"
	"sync/atomic"
)

var dbTestNameCounter int64

func dbTestName(name string) string {
	return fmt.Sprintf("%s%d", name, atomic.AddInt64(&dbTestNameCounter, 1))
}
