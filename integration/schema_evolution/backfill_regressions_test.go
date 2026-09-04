package schemaevolution

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_BackfillCanonicalExpansionRollback(t *testing.T) {
	for _, values := range [][]string{{"A|" + strings.Repeat("B", 600<<10)}, {"A|" + strings.Repeat("B", 300<<10), "C|" + strings.Repeat("D", 300<<10)}} {
		t.Run(string(rune('0'+len(values))), func(t *testing.T) {
			pool, svc, _ := fixtureMaintainingRows(t, values)
			w := fixtureWorker(pool, svc)
			ctx := context.Background()
			var historyBefore, receiptBefore int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log`).Scan(&historyBefore))
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.scope_write_receipts`).Scan(&receiptBefore))
			require.ErrorContains(t, w.Run(ctx), "configured limits")
			var history, receipts, changed, scanned int
			require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM sync.bundle_log),(SELECT count(*) FROM sync.scope_write_receipts),(SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL),(SELECT scanned FROM app_evolution.jobs)`).Scan(&history, &receipts, &changed, &scanned))
			require.Equal(t, historyBefore, history)
			require.Equal(t, receiptBefore, receipts)
			require.Zero(t, changed)
			require.Zero(t, scanned)
		})
	}
}

func TestEvolution_BackfillConversionFailureDurable(t *testing.T) {
	pool, svc, _ := fixtureMaintainingRows(t, []string{"A|B", "invalid"})
	ctx := context.Background()
	w := fixtureWorker(pool, svc)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	stats, err := w.Telemetry(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Failed)
	require.Positive(t, stats.Unresolved)
	require.Zero(t, stats.Changed)
	var digestBefore, category string
	require.NoError(t, pool.QueryRow(ctx, `SELECT input_digest,category FROM app_evolution.row_work`).Scan(&digestBefore, &category))
	require.Equal(t, digest([]byte("invalid")), digestBefore)
	require.ErrorContains(t, fixtureWorker(pool, svc).Run(ctx), "invalid_historical_input")
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.row_work`).Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL`).Scan(&n))
	require.Zero(t, n)
}

func TestEvolution_BackfillFrozenRunCapacity(t *testing.T) {
	for _, mode := range []string{"history", "evidence", "deadline", "disk"} {
		t.Run(mode, func(t *testing.T) {
			pool, svc, _ := fixtureMaintainingRows(t, []string{"A|B", "C|D"})
			ctx := context.Background()
			w := fixtureWorker(pool, svc)
			// Controlled pre-execution fixture limits, not a production limit-changing API.
			w.Bounds.Rows = 1
			switch mode {
			case "history":
				w.Bounds.MaxBundles = 1
			case "evidence":
				w.Bounds.MaxEvidenceBytes = 1
			case "deadline":
				w.Bounds.Deadline = time.Now().Add(-time.Second)
			case "disk":
				w.Capacity = func(context.Context) (Capacity, error) {
					return Capacity{Available: true, FreeDiskBytes: 1, RetentionRemaining: 7 * 24 * time.Hour}, nil
				}
			}
			_, err := pool.Exec(ctx, `UPDATE app_evolution.jobs SET bounds=$1`, w.Bounds)
			require.NoError(t, err)
			err = w.Run(ctx)
			require.Error(t, err)
			if mode == "history" {
				require.ErrorContains(t, err, "history_capacity_exceeded")
			}
			var batches int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.batches`).Scan(&batches))
			if mode == "history" {
				require.Equal(t, 1, batches)
			} else {
				require.Zero(t, batches)
			}
			deadline := w.Bounds.Deadline
			restarted := fixtureWorker(pool, svc)
			if mode == "disk" {
				restarted.Capacity = w.Capacity
			}
			require.True(t, deadline.Equal(restarted.Bounds.Deadline))
			require.Error(t, restarted.Run(ctx))
		})
	}
}

func TestEvolution_BackfillCensusCapacity(t *testing.T) {
	pool, _, _ := fixtureMaintaining(t)
	ctx := context.Background()
	b := fixtureBounds()
	b.MaxEvidenceBytes = 1
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.ErrorContains(t, checkEvidenceCapacity(ctx, tx, b, 8192), "evidence_capacity_exceeded")
	b = DefaultBounds()
	require.ErrorContains(t, b.validate(), "explicit total run limits")
}

func TestEvolution_BackfillZeroUUID(t *testing.T) {
	pool, svc, _ := fixtureMaintainingRowsWithKeys(t, []string{"A|B"}, []uuid.UUID{uuid.Nil})
	ctx := context.Background()
	svc = fixtureCoexistence(t, pool, svc)
	var d BatchDescriptor
	require.NoError(t, pool.QueryRow(ctx, `SELECT spec FROM app_evolution.batches`).Scan(&d))
	require.Equal(t, uuid.Nil, d.Rows[0].Key)
	var name string
	require.NoError(t, pool.QueryRow(ctx, `SELECT name FROM main.upgrade_people WHERE id=$1`, uuid.Nil).Scan(&name))
	require.Equal(t, "A", name)
	var seq int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(bundle_seq) FROM sync.bundle_log WHERE source_id='schema-evolution-backfill'`).Scan(&seq))
	cfg, err := Configuration(4, 3, true)
	require.NoError(t, err)
	for _, generation := range cfg.Readiness.Generations {
		actor := oversync.Actor{UserID: "scope", SourceID: "zero-reader-" + uuid.NewString()}
		_, err := svc.Connect(ctx, actor, generation.Proof, &oversync.ConnectRequest{})
		require.NoError(t, err)
		pull, err := svc.ProcessPull(ctx, actor, generation.Proof, seq-1, 1, 0)
		require.NoError(t, err)
		require.Len(t, pull.Bundles, 1)
		require.Len(t, pull.Bundles[0].Rows, 1)
		row := pull.Bundles[0].Rows[0]
		require.Equal(t, uuid.Nil.String(), row.Key["id"])
		var visible map[string]any
		require.NoError(t, json.Unmarshal(row.Payload, &visible))
		if generation.Proof.ID == 1 {
			require.Equal(t, "A|B", visible["legacy_name"])
		} else {
			require.Equal(t, "A", visible["name"])
			require.Equal(t, "B", visible["new_label"])
		}
	}
}

func TestEvolution_BackfillFanoutRollback(t *testing.T) {
	keys := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")}
	pool, svc, _ := fixtureMaintainingRowsWithKeys(t, []string{"A|B", "C|D"}, keys)
	ctx := context.Background()
	w := fixtureWorker(pool, svc)
	w.Bounds.Rows = 1
	_, err := pool.Exec(ctx, `UPDATE app_evolution.jobs SET bounds=$1`, w.Bounds)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE FUNCTION app_evolution.fixture_fanout() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE main.upgrade_people SET legacy_name='C|'||repeat('D',614400) WHERE id='00000000-0000-0000-0000-000000000002'; RETURN NEW; END $$; CREATE TRIGGER zzz_fixture_fanout AFTER UPDATE ON main.upgrade_people FOR EACH ROW WHEN(NEW.id='00000000-0000-0000-0000-000000000001') EXECUTE FUNCTION app_evolution.fixture_fanout()`)
	require.NoError(t, err)
	require.ErrorContains(t, w.Run(ctx), "configured limits")
	var changed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL OR length(legacy_name)>3`).Scan(&changed))
	require.Zero(t, changed)
	var completed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.batches WHERE state='completed'`).Scan(&completed))
	require.Zero(t, completed)
}

func TestEvolution_BackfillExpiryReconciliationIsBounded(t *testing.T) {
	pool, svc, _ := fixtureMaintaining(t)
	ctx := context.Background()
	w := fixtureWorker(pool, svc)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `LOCK TABLE app_evolution.batches IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	w.Bounds.QueryTimeout = 50 * time.Millisecond
	started := time.Now()
	require.Error(t, w.reconcileRunExpiry(ctx))
	require.Less(t, time.Since(started), time.Second)
}
