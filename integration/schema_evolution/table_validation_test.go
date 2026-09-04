package schemaevolution

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTableEvolution_ValidationPagesResume(t *testing.T) {
	for _, boundary := range []string{"validation_before_checkpoint", "validation_after_checkpoint"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			pool, old := tableBaseline(t)
			for _, scope := range []string{"a", "b", "c"} {
				_, err := CreateScope(ctx, pool, scope)
				require.NoError(t, err)
			}
			var last uuid.UUID
			for i := 1; i <= 5; i++ {
				last = uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
				require.NoError(t, fixtureWrite(t, old, "a", `INSERT INTO main.people VALUES('a',$1,'Person')`, last))
			}
			bounds := tableTestBounds()
			bounds.ScopePage = 1
			proof, cleanup := tableCleanupStart(t, pool, old, bounds)
			backfill := tableBackfillStart(t, pool, proof, cleanup)
			stopped, cancel := context.WithCancel(ctx)
			defer cancel()
			var saved tableValidationCursor
			interrupted := false
			w := &TableWorker{Pool: pool, Service: backfill, Capacity: tableTestCapacity}
			w.boundary = func(point string) {
				if point != boundary || interrupted {
					return
				}
				run, err := loadTableRun(ctx, pool)
				require.NoError(t, err)
				if run.Validation.Stage != 3 || run.Validation.Key == nil {
					return
				}
				saved = run.Validation
				interrupted = true
				cancel()
			}
			require.Error(t, w.Populate(stopped))
			require.True(t, interrupted)
			paused, err := loadTableRun(ctx, pool)
			require.NoError(t, err)
			require.Equal(t, saved, paused.Validation, "cancelled transaction must not advance its checkpoint")
			require.Equal(t, "cleaned", paused.Phase)
			require.True(t, bounds.Deadline.Equal(paused.Bounds.Deadline))
			var certificateMissing bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT certificate IS NULL FROM app_evolution.table_run`).Scan(&certificateMissing))
			require.True(t, certificateMissing)

			// A later page still has to validate its captured history after takeover.
			var captured []byte
			require.NoError(t, pool.QueryRow(ctx, `DELETE FROM sync.row_state WHERE table_id=2 AND key_bytes=uuid_send($1::uuid) RETURNING to_jsonb(row_state)`, last).Scan(&captured))
			resumed := &TableWorker{Pool: pool, Service: backfill, Capacity: tableTestCapacity}
			first := true
			resumed.boundary = func(point string) {
				if point != "validation_after_checkpoint" || !first {
					return
				}
				first = false
				run, err := loadTableRun(ctx, pool)
				require.NoError(t, err)
				require.Equal(t, saved.Pages+1, run.Validation.Pages, "resume must continue the durable pass, not restart it")
			}
			require.ErrorContains(t, resumed.Populate(ctx), "table population validation failed")
			require.False(t, first)
			failed, err := loadTableRun(ctx, pool)
			require.NoError(t, err)
			require.Equal(t, 3, failed.Validation.Stage)
			require.Equal(t, "cleaned", failed.Phase)
			_, err = pool.Exec(ctx, `INSERT INTO sync.row_state SELECT * FROM jsonb_populate_record(NULL::sync.row_state,$1::jsonb)`, captured)
			require.NoError(t, err)
			require.NoError(t, resumed.Populate(ctx))
			complete, err := loadTableRun(ctx, pool)
			require.NoError(t, err)
			require.Equal(t, tableValidationDone, complete.Validation.Stage)
			require.Greater(t, complete.Validation.Pages, int64(12))
			require.Equal(t, paused.ID, complete.ID)
			require.Equal(t, paused.Epoch, complete.Epoch)
			require.Equal(t, paused.Bounds, complete.Bounds)
			assertTableServingState(t, pool, "populated", false)
			// Even a completed worker pass never substitutes for fresh readiness checks.
			_, err = pool.Exec(ctx, `DELETE FROM sync.row_state WHERE table_id=2 AND key_bytes=uuid_send($1::uuid)`, last)
			require.NoError(t, err)
			validation, err := validateTablePopulation(ctx, pool, true)
			require.NoError(t, err)
			require.NotZero(t, validation.Violations)
		})
	}
}
