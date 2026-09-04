package schemaevolution

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_RuntimeRejectsOwnershipAndProvisioningMembership(t *testing.T) {
	for _, mode := range []string{"owner_revoked", "inherited_owner", "set_role_owner", "admin"} {
		t.Run(mode, func(t *testing.T) {
			pool, svc, _ := fixtureMaintaining(t)
			ctx := context.Background()
			proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
			require.NoError(t, err)
			a, err := OpenAuthority(ctx, proof)
			require.NoError(t, err)
			defer a.Close()
			role := "r6_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			owner := role + "_owner"
			r := pgx.Identifier{role}.Sanitize()
			o := pgx.Identifier{owner}.Sanitize()
			_, err = a.conn.Exec(ctx, `CREATE ROLE `+r+`; CREATE ROLE `+o)
			require.NoError(t, err)
			defer func() {
				_, err := a.conn.Exec(ctx, `ALTER TABLE main.upgrade_people OWNER TO CURRENT_USER; DROP OWNED BY `+r+`; DROP OWNED BY `+o+`; DROP ROLE `+r+`; DROP ROLE `+o)
				require.NoError(t, err)
			}()
			switch mode {
			case "owner_revoked":
				_, err = a.conn.Exec(ctx, `ALTER TABLE main.upgrade_people OWNER TO `+r+`; REVOKE TRIGGER ON main.upgrade_people FROM `+r)
			case "inherited_owner":
				_, err = a.conn.Exec(ctx, `ALTER TABLE main.upgrade_people OWNER TO `+o+`; GRANT `+o+` TO `+r+` WITH INHERIT TRUE`)
			case "set_role_owner":
				_, err = a.conn.Exec(ctx, `ALTER TABLE main.upgrade_people OWNER TO `+o+`; GRANT `+o+` TO `+r+` WITH INHERIT FALSE, SET TRUE`)
			case "admin":
				_, err = a.conn.Exec(ctx, `ALTER ROLE `+o+` CREATEROLE; GRANT `+o+` TO `+r+` WITH INHERIT FALSE, SET TRUE`)
			}
			require.NoError(t, err)
			if mode == "owner_revoked" || mode == "set_role_owner" {
				var trigger bool
				require.NoError(t, a.conn.QueryRow(ctx, `SELECT has_table_privilege($1,'main.upgrade_people','TRIGGER')`, role).Scan(&trigger))
				require.False(t, trigger)
			}
			require.ErrorContains(t, a.GrantRuntime(ctx, role), "ownership or reachable provisioning")
		})
	}
}

type revision6FaultTracer struct {
	mode  string
	armed atomic.Bool
	fired atomic.Bool
}

func (f *revision6FaultTracer) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if f.mode == "commit" && strings.EqualFold(d.SQL, "commit") && f.armed.CompareAndSwap(true, false) {
		f.fired.Store(true)
		_ = c.Close(ctx)
	}
	return ctx
}
func (f *revision6FaultTracer) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	var pg *pgconn.PgError
	if f.mode == "rollback" && errors.As(d.Err, &pg) && pg.Code == "P6001" && f.armed.CompareAndSwap(true, false) {
		f.fired.Store(true)
		_ = c.Close(ctx)
	}
}

func revision6TracedWorker(t *testing.T, pool *pgxpool.Pool, svc *oversync.SyncService, mode string) (*Worker, *revision6FaultTracer) {
	t.Helper()
	require.NoError(t, svc.Close(context.Background()))
	tracer := &revision6FaultTracer{mode: mode}
	pc := pool.Config().Copy()
	pc.ConnConfig.Tracer = tracer
	execution, err := pgxpool.NewWithConfig(context.Background(), pc)
	require.NoError(t, err)
	t.Cleanup(execution.Close)
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	serving := bootstrapFixture(t, execution, cfg)
	w := fixtureWorker(pool, serving)
	w.boundary = func(at string) {
		if at == "before_apply" {
			tracer.armed.Store(true)
		}
	}
	return w, tracer
}

func TestEvolution_ConversionRollbackUnconfirmedPreservesReservation(t *testing.T) {
	pool, svc, _ := fixtureMaintainingRows(t, []string{"A|B", "invalid"})
	w, trace := revision6TracedWorker(t, pool, svc, "rollback")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := w.Run(ctx)
	var unconfirmed *oversync.WriteRollbackUnconfirmedError
	require.ErrorAs(t, err, &unconfirmed)
	require.True(t, trace.fired.Load())
	var failed, changed, pending int
	var reserved int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.row_work),(SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL),(SELECT count(*) FROM app_evolution.batches WHERE state='pending'),(SELECT sum(history_reserve) FROM app_evolution.batches)`).Scan(&failed, &changed, &pending, &reserved))
	require.Zero(t, failed)
	require.Zero(t, changed)
	require.Equal(t, 1, pending)
	require.EqualValues(t, w.Bounds.Bytes, reserved)
	stats, err := w.Telemetry(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Failed)
	require.Positive(t, stats.Unresolved)
}

func TestEvolution_HistoryByteReservationSurvivesAmbiguousCommit(t *testing.T) {
	pool, svc, _ := fixtureMaintainingRows(t, []string{"A|B", "C|D"})
	w, trace := revision6TracedWorker(t, pool, svc, "commit")
	w.Bounds.Rows = 1
	w.Bounds.Bytes = 1024
	w.Bounds.MaxHistoryBytes = 1024
	w.Bounds.Retries = 1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `UPDATE app_evolution.jobs SET bounds=$1`, w.Bounds)
	require.NoError(t, err)
	err = w.Run(ctx)
	var unknown *oversync.CommitOutcomeUnknownError
	require.ErrorAs(t, err, &unknown)
	require.True(t, trace.fired.Load())
	var operation string
	var historyReserve, evidenceReserve int64
	var frozen []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id,history_reserve,evidence_reserve,descriptor FROM app_evolution.batches`).Scan(&operation, &historyReserve, &evidenceReserve, &frozen))
	require.EqualValues(t, 1024, historyReserve)
	// Fresh worker state, same immutable job and descriptor, now healthy transport.
	restarted := &Worker{Pool: pool, Manager: w.Manager, Bounds: w.Bounds, Capacity: w.Capacity}
	require.ErrorContains(t, restarted.Run(ctx), "history_capacity_exceeded")
	var afterOperation string
	var actual, afterReserve, afterEvidence, bundles int64
	var afterFrozen []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id,history_bytes,history_reserve,evidence_reserve,bundles,descriptor FROM app_evolution.batches`).Scan(&afterOperation, &actual, &afterReserve, &afterEvidence, &bundles, &afterFrozen))
	require.Equal(t, operation, afterOperation)
	require.Equal(t, frozen, afterFrozen)
	require.Equal(t, historyReserve, afterReserve)
	require.Equal(t, evidenceReserve, afterEvidence)
	require.Positive(t, actual)
	require.Less(t, actual, int64(1024))
	require.EqualValues(t, 1, bundles)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.batches`).Scan(&count))
	require.Equal(t, 1, count)
	var charged int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT history_bytes FROM app_evolution.jobs`).Scan(&charged))
	require.Equal(t, actual, charged)
	require.ErrorContains(t, restarted.Run(ctx), "history_capacity_exceeded")
	var persisted Bounds
	require.NoError(t, pool.QueryRow(ctx, `SELECT bounds FROM app_evolution.jobs`).Scan(&persisted))
	require.Equal(t, encodeJSON(w.Bounds), encodeJSON(persisted))
	require.NoError(t, pool.QueryRow(ctx, `SELECT history_bytes FROM app_evolution.jobs`).Scan(&charged))
	require.Equal(t, actual, charged)
}

func revision6EvidenceUsage(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var used int64
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.census_progress x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.repair_evidence x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.certificates x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.census_rows x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.census_scopes x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.jobs x),0)+
 COALESCE((SELECT sum(pg_column_size(x)+evidence_reserve) FROM app_evolution.batches x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.row_work x),0)`).Scan(&used))
	return used
}

func TestEvolution_DiagnosticGrowthAndCertificateEvidenceLimits(t *testing.T) {
	t.Run("retained_diagnostics", func(t *testing.T) {
		pool, svc, _ := fixtureMaintainingRows(t, []string{"invalid"})
		w := fixtureWorker(pool, svc)
		ctx := context.Background()
		require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
		used := revision6EvidenceUsage(t, pool)
		d, err := w.pendingDescriptor(ctx)
		require.NoError(t, err)
		require.NotNil(t, d)
		frozen, _ := d.CanonicalBytes()
		var reserve int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT evidence_reserve FROM app_evolution.batches`).Scan(&reserve))
		require.Positive(t, reserve)
		// Replay the same diagnostic: no new reservation or row. Retained diagnostic
		// bytes plus the existing conservative reservation are both accounted.
		require.ErrorContains(t, fixtureWorker(pool, svc).Run(ctx), "invalid_historical_input")
		after, _ := w.pendingDescriptor(ctx)
		afterFrozen, _ := after.CanonicalBytes()
		require.Equal(t, frozen, afterFrozen)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		b := w.Bounds
		b.MaxEvidenceBytes = revision6EvidenceUsage(t, pool)
		require.GreaterOrEqual(t, b.MaxEvidenceBytes, used)
		require.NoError(t, checkEvidenceCapacity(ctx, tx, b, 0))
		require.ErrorContains(t, checkEvidenceCapacity(ctx, tx, b, 1), "evidence_capacity_exceeded")
		b.MaxEvidenceBytes += 512
		// Real retained row growth, rather than an empty-database one-byte allowance.
		_, err = tx.Exec(ctx, `INSERT INTO app_evolution.row_work(job,incarnation,row_key,input_digest,category) SELECT job,incarnation,$1,input_digest,repeat('x',512) FROM app_evolution.row_work LIMIT 1`, uuid.New())
		require.NoError(t, err)
		require.ErrorContains(t, checkEvidenceCapacity(ctx, tx, b, 0), "evidence_capacity_exceeded")
		require.NoError(t, tx.Rollback(ctx))
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.row_work`).Scan(&n))
		require.Equal(t, 1, n)
	})
	t.Run("certificate_growth", func(t *testing.T) {
		pool, svc, _ := fixtureMaintaining(t)
		ctx := context.Background()
		require.NoError(t, fixtureWorker(pool, svc).Run(ctx))
		proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
		require.NoError(t, err)
		a, err := OpenAuthority(ctx, proof)
		require.NoError(t, err)
		defer a.Close()
		// Controlled completed-job fixture boundary: capacity cannot cover a new
		// certificate. Validate must leave the old job state and evidence unchanged.
		b := fixtureBounds()
		b.MaxEvidenceBytes = revision6EvidenceUsage(t, pool) + 64
		_, err = a.conn.Exec(ctx, `UPDATE app_evolution.jobs SET bounds=$1`, b)
		require.NoError(t, err)
		require.ErrorContains(t, a.Certify(ctx, 3), "evidence_capacity_exceeded")
		var n int
		var state string
		require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.certificates),state FROM app_evolution.jobs`).Scan(&n, &state))
		require.Zero(t, n)
		require.Equal(t, "traversed", state)
	})
}
