package schemaevolution

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func tableUnbootstrappedService(t *testing.T, pool *pgxpool.Pool) *oversync.SyncService {
	t.Helper()
	ctx := context.Background()
	cfg, err := TableEvolutionConfiguration(TablesServing)
	require.NoError(t, err)
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	t.Cleanup(pair.Close)
	cfg.Readiness.PoolPair = pair
	s, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func assertTableServingState(t *testing.T, pool *pgxpool.Pool, phase string, open bool) {
	t.Helper()
	var actual string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT phase FROM app_evolution.table_run WHERE singleton`).Scan(&actual))
	require.Equal(t, phase, actual)
	status, err := MaintenanceStatus(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, open, status.AdmissionOpen)
}

func TestTableEvolution_FirstReopenRevalidatesCapture(t *testing.T) {
	for _, boundary := range []string{"publication", "bootstrap", "reopen", "reopen_rollback"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			pool, old := tableBaseline(t)
			_, err := CreateScope(ctx, pool, "scope")
			require.NoError(t, err)
			require.NoError(t, fixtureWrite(t, old, "scope", `INSERT INTO main.people VALUES('scope',$1,'Person')`, uuid.New()))
			proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
			backfill := tableBackfillStart(t, pool, proof, cleanup)
			w := &TableWorker{Pool: pool, Service: backfill, Capacity: tableTestCapacity}
			require.NoError(t, w.Populate(ctx))
			require.NoError(t, backfill.Close(ctx))
			assertTableServingState(t, pool, "populated", false)
			cfg, err := TableEvolutionConfiguration(TablesServing)
			require.NoError(t, err)
			publish := func() error {
				a, err := OpenAuthority(ctx, proof)
				if err != nil {
					return err
				}
				defer a.Close()
				return a.Publish(ctx, cfg)
			}
			if boundary != "publication" {
				require.NoError(t, publish())
			}
			var serving *oversync.SyncService
			if boundary == "reopen" || boundary == "reopen_rollback" {
				serving = tableTestService(t, pool, TablesServing)
			}
			if boundary == "reopen_rollback" {
				// Fail after verifyTableReopen has written its marker, before the
				// same transaction can open admission. Neither may survive rollback.
				_, err = pool.Exec(ctx, `CREATE FUNCTION app_evolution.reject_test_reopen() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT NEW.closed THEN RAISE EXCEPTION 'test reopen rollback'; END IF; RETURN NEW; END $$;
CREATE TRIGGER reject_test_reopen BEFORE UPDATE ON app_evolution.barrier FOR EACH ROW EXECUTE FUNCTION app_evolution.reject_test_reopen()`)
				require.NoError(t, err)
				require.ErrorContains(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{serving}), "test reopen rollback")
				assertTableServingState(t, pool, "populated", false)
				_, err = pool.Exec(ctx, `DROP TRIGGER reject_test_reopen ON app_evolution.barrier; DROP FUNCTION app_evolution.reject_test_reopen()`)
				require.NoError(t, err)
			}
			// Corruption occurs AFTER certification, leaving its stored certificate
			// and physical FK-valid rows intact (for example an inconsistent restore).
			var captured []byte
			require.NoError(t, pool.QueryRow(ctx, `DELETE FROM sync.row_state WHERE table_id=4 RETURNING to_jsonb(row_state)`).Scan(&captured))
			switch boundary {
			case "publication":
				require.ErrorContains(t, publish(), "backfill_validation_failed")
				var revision int64
				require.NoError(t, pool.QueryRow(ctx, `SELECT revision FROM app_evolution.deployment WHERE singleton`).Scan(&revision))
				require.EqualValues(t, TablesBackfill, revision)
			case "bootstrap":
				bad := tableUnbootstrappedService(t, pool)
				require.ErrorContains(t, bad.Bootstrap(ctx), "backfill_validation_failed")
				require.False(t, bad.Readiness().Ready)
				require.NoError(t, bad.Close(ctx))
			default:
				require.ErrorContains(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{serving}), "backfill_validation_failed")
			}
			assertTableServingState(t, pool, "populated", false)
			_, err = pool.Exec(ctx, `INSERT INTO sync.row_state SELECT * FROM jsonb_populate_record(NULL::sync.row_state,$1::jsonb)`, captured)
			require.NoError(t, err)
			if boundary == "publication" {
				require.NoError(t, publish())
			}
			if serving == nil {
				serving = tableTestService(t, pool, TablesServing)
			}
			require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{serving}))
			assertTableServingState(t, pool, "serving", true)
			// Lost reopen response is safe to retry. Later legitimate parent
			// deletion and NULL inserts must not reinstate one-time seed checks.
			require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{serving}))
			require.NoError(t, fixtureWrite(t, serving, "scope", `DELETE FROM main.projects WHERE _sync_scope_id='scope' AND id=$1::uuid`, TableProjectID))
			_, err = CreateScope(ctx, pool, "later-scope")
			require.NoError(t, err)
			require.NoError(t, fixtureWrite(t, serving, "later-scope", `INSERT INTO main.people(_sync_scope_id,id,name) VALUES('later-scope',$1,'Later')`, uuid.New()))
			_, err = Drain(ctx, pool, []*oversync.SyncService{serving}, func(context.Context) error { return nil })
			require.NoError(t, err)
			fresh := tableTestService(t, pool, TablesServing)
			require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{fresh}))
			assertTableServingState(t, pool, "serving", true)
		})
	}
}
