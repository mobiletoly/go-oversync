package oversqlite

import (
	"fmt"
	"sync/atomic"
)

var testSourceCounter atomic.Int64

func newTestSourceID() string {
	return fmt.Sprintf("test-source-%d", testSourceCounter.Add(1))
}
