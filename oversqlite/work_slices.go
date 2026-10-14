package oversqlite

import (
	"context"
	"fmt"
	"runtime"
)

// A phase keeps its validated input while private preparation is checkpointed.
// It does not re-enter a public operation or reset its transport/attempt state.
type workSliceKey struct{}
type workSlice struct {
	checkpointed bool
	durable      bool
}

type pushPlanningWorkspaceKey struct{}
type pushPlanningWorkspace struct {
	writer                 queryRower
	base                   *workspaceCharge
	guard                  *pushPlanningGuard
	plan                   *pushPlan
	planLoaded, outboxNone bool
}

type pushPlanningGuard struct {
	hash, initialization string
	next, entries, bytes int64
}

var pushPlanningGuardCharge = workspaceCharge{entries: 4, bytes: 8192}

// The holder contains writer-local scalar charges, flags and references only.
// Its plan is the same current descriptor, covered by per-step push admission.
var pushPlanningWorkspaceCharge = workspaceCharge{entries: 2, bytes: 256}

// SyncWorkNoProgressError identifies a stuck local continuation, not a retryable
// network failure. No pending intent or checkpoint is discarded.
type SyncWorkNoProgressError struct{ Phase string }

func (e *SyncWorkNoProgressError) Error() string {
	return fmt.Sprintf("sync_work_no_progress: phase=%s", e.Phase)
}

func workSliceFrom(ctx context.Context) *workSlice {
	slice, _ := ctx.Value(workSliceKey{}).(*workSlice)
	return slice
}

func checkpointWorkStep(ctx context.Context) {
	if slice := workSliceFrom(ctx); slice != nil {
		slice.checkpointed = true
	}
}

func commitWorkStep(ctx context.Context) {
	if slice := workSliceFrom(ctx); slice != nil && slice.checkpointed {
		slice.durable = true
	}
}

func (c *Client) nextWorkSlice(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.a3Stats.workSlices++
	c.a3Stats.peakSliceWork = max(c.a3Stats.peakSliceWork, c.syncWorkUsed)
	c.syncWorkUsed = 0
	c.readyAtomicUnitDispatched = false
	runtime.Gosched()
	return ctx.Err()
}

func completeWorkPhase[T any](ctx context.Context, c *Client, phase string, step func(context.Context) (T, error)) (T, error) {
	var zero T
	// Entry is a safe boundary: the preceding phase has completed and released
	// its transaction/handles. Keep its report and confirmed-prefix ownership.
	if c.syncWorkUsed >= c.config.MaxSyncWorkUnitsPerInvocation {
		if err := c.nextWorkSlice(ctx); err != nil {
			return zero, err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		slice := &workSlice{}
		value, err := step(context.WithValue(ctx, workSliceKey{}, slice))
		if !pureApplyWork(err) {
			if err == nil {
				checkpointWorkStep(ctx)
				commitWorkStep(ctx)
			}
			return value, err
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if !slice.durable {
			return zero, &SyncWorkNoProgressError{Phase: phase}
		}
		if err := c.nextWorkSlice(ctx); err != nil {
			return zero, err
		}
	}
}

func (c *Client) completeWork(ctx context.Context, phase string, step func(context.Context) error) error {
	_, err := completeWorkPhase(ctx, c, phase, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, step(ctx)
	})
	return err
}
