package oversqlite_e2e

import (
	"fmt"
	"sync/atomic"
)

var e2eSourceCounter atomic.Int64

func newE2ESourceID() string {
	return fmt.Sprintf("e2e-source-%d", e2eSourceCounter.Add(1))
}
