package schemaevolution

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_RepairFailedConversion(t *testing.T) {
	for _, mode := range []string{"before_expiry", "after_expiry", "rollback", "descriptor_restart"} {
		t.Run(mode, func(t *testing.T) {
			pool, svc, keys := fixtureMaintainingRows(t, []string{"A|B", "invalid historical"})
			ctx := context.Background()
			w := fixtureWorker(pool, svc)
			require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
			var d BatchDescriptor
			require.NoError(t, pool.QueryRow(ctx, `SELECT spec FROM app_evolution.batches WHERE state='pending'`).Scan(&d))
			failed := d.OperationID
			if mode == "after_expiry" {
				d.ValidUntil = time.Now().Add(-time.Hour).Truncate(time.Microsecond)
				encoded, err := d.CanonicalBytes()
				require.NoError(t, err)
				_, err = pool.Exec(ctx, `UPDATE app_evolution.batches SET spec=$2,descriptor=$3,hash=$4 WHERE operation_id=$1`, failed, d, encoded, digest(encoded))
				require.NoError(t, err)
			}
			var frozen []byte
			require.NoError(t, pool.QueryRow(ctx, `SELECT descriptor FROM app_evolution.batches WHERE operation_id=$1`, failed).Scan(&frozen))
			if mode == "rollback" {
				w.boundary = func(at string) {
					if at == "inside_repair" {
						panic("injected repair interruption")
					}
				}
				// ScopeManager protects callback panics by rolling the transaction back.
				func() { defer func() { _ = recover() }(); _ = w.Repair(ctx, failed, keys[1], "C|D") }()
				var legacy string
				require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name FROM main.upgrade_people WHERE id=$1`, keys[1]).Scan(&legacy))
				require.Equal(t, "invalid historical", legacy)
				w.boundary = nil
			}
			if mode == "descriptor_restart" {
				canceled, cancel := context.WithCancel(ctx)
				w.boundary = func(at string) {
					if at == "after_repair_descriptor" {
						cancel()
					}
				}
				require.Error(t, w.Repair(canceled, failed, keys[1], "C|D"))
				w = fixtureWorker(pool, svc)
			}
			require.NoError(t, w.Repair(ctx, failed, keys[1], "C|D"))
			require.NoError(t, w.Repair(ctx, failed, keys[1], "C|D"))
			require.ErrorContains(t, w.Repair(ctx, failed, keys[1], "Different|Approval"), "immutable repair")
			var preserved []byte
			var state, category string
			require.NoError(t, pool.QueryRow(ctx, `SELECT descriptor,state FROM app_evolution.batches WHERE operation_id=$1`, failed).Scan(&preserved, &state))
			require.Equal(t, frozen, preserved)
			require.Equal(t, "repaired", state)
			require.NoError(t, pool.QueryRow(ctx, `SELECT category FROM app_evolution.repair_evidence WHERE failed_operation=$1`, failed).Scan(&category))
			require.Equal(t, "invalid_historical_input", category)
			var unresolved int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.row_work`).Scan(&unresolved))
			require.Zero(t, unresolved)
			var receipts int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.scope_write_receipts WHERE operation_id=(SELECT repair_operation FROM app_evolution.repair_evidence WHERE failed_operation=$1)`, failed).Scan(&receipts))
			require.Equal(t, 1, receipts)
			svc = fixtureCoexistence(t, pool, svc)
			require.True(t, svc.Readiness().Ready)
		})
	}
}

func TestEvolution_RuntimeDefaultsAndHooks(t *testing.T) {
	for _, mode := range []string{"role", "database", "role_database", "AfterConnect", "PrepareConn", "ShouldPing", "pgconn_ValidateConnect", "pgconn_AfterConnect"} {
		t.Run(mode, func(t *testing.T) {
			pool, svc, _ := fixtureMaintaining(t)
			ctx := context.Background()
			proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
			require.NoError(t, err)
			a, err := OpenAuthority(ctx, proof)
			require.NoError(t, err)
			defer func() { a.Close() }()
			role := "r9_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			r := pgx.Identifier{role}.Sanitize()
			_, err = a.conn.Exec(ctx, `CREATE ROLE `+r)
			require.NoError(t, err)
			defer func() { _, e := a.conn.Exec(ctx, `DROP OWNED BY `+r+`; DROP ROLE `+r); require.NoError(t, e) }()
			var database string
			require.NoError(t, a.conn.QueryRow(ctx, `SELECT current_database()`).Scan(&database))
			db := pgx.Identifier{database}.Sanitize()
			defaults := map[string]string{"role": `ALTER ROLE ` + r, "database": `ALTER DATABASE ` + db, "role_database": `ALTER ROLE ` + r + ` IN DATABASE ` + db}
			if prefix, ok := defaults[mode]; ok {
				_, err = a.conn.Exec(ctx, prefix+` SET session_replication_role=replica`)
				require.NoError(t, err)
				require.Error(t, a.GrantRuntime(ctx, role))
				_, err = a.conn.Exec(ctx, prefix+` RESET session_replication_role`)
				require.NoError(t, err)
				require.NoError(t, a.GrantRuntime(ctx, role))
				return
			}
			require.NoError(t, a.GrantRuntime(ctx, role))
			a.Close()
			pc := pool.Config().Copy()
			mutate := func(ctx context.Context, c *pgx.Conn) error {
				_, e := c.Exec(ctx, `SET session_replication_role=replica; SET ROLE `+r)
				return e
			}
			switch mode {
			case "AfterConnect":
				pc.AfterConnect = mutate
			case "PrepareConn":
				pc.PrepareConn = func(ctx context.Context, c *pgx.Conn) (bool, error) { return true, mutate(ctx, c) }
			case "ShouldPing":
				pc.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
			case "pgconn_ValidateConnect":
				pc.ConnConfig.Config.ValidateConnect = func(context.Context, *pgconn.PgConn) error { return nil }
			case "pgconn_AfterConnect":
				pc.ConnConfig.Config.AfterConnect = func(context.Context, *pgconn.PgConn) error { return nil }
			}
			_, err = oversync.NewReadinessPoolPair(ctx, pc, 2)
			require.ErrorContains(t, err, "hooks are prohibited")
			var rows, history int
			require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM main.upgrade_people),(SELECT count(*) FROM sync.bundle_log)`).Scan(&rows, &history))
			require.Equal(t, 1, rows)
			require.Equal(t, 1, history)
			// The authority socket was released before startup; reopen only for role cleanup.
			proof, err = Drain(ctx, pool, nil, func(context.Context) error { return nil })
			require.NoError(t, err)
			a, err = OpenAuthority(ctx, proof)
			require.NoError(t, err)
		})
	}
}

func TestEvolution_HistoricalDescriptorVectorsRejected(t *testing.T) {
	pool, svc, _ := fixtureMaintaining(t)
	w := fixtureWorker(pool, svc)
	ctx := context.Background()
	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			raw, err := migrationFiles.ReadFile("operation-encoding-" + version + ".json")
			require.NoError(t, err)
			var vector struct {
				Descriptor  BatchDescriptor
				Hex, SHA256 string
			}
			require.NoError(t, json.Unmarshal(raw, &vector))
			encoded, err := hex.DecodeString(vector.Hex)
			require.NoError(t, err)
			require.Equal(t, vector.SHA256, digest(encoded))
			_, err = pool.Exec(ctx, `INSERT INTO app_evolution.batches(operation_id,job,ordinal,descriptor,spec,hash) VALUES($1,$2,1,$3,$4,$5)`, vector.Descriptor.OperationID, JobID, encoded, vector.Descriptor, vector.SHA256)
			require.NoError(t, err)
			_, err = w.pendingDescriptor(ctx)
			require.Error(t, err)
			_, err = pool.Exec(ctx, `DELETE FROM app_evolution.batches`)
			require.NoError(t, err)
		})
	}
}

func TestEvolution_CensusPagesResumeWithFrozenLimits(t *testing.T) {
	b := fixtureBounds()
	b.Rows = 1
	b.ScopePage = 1
	pool, svc, _ := fixtureMaintainingBounds(t, []string{"A|B", "C|D", "E|F"}, nil, b)
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	// Return this isolated fixture to the point before its first activation.
	_, err = a.conn.Exec(ctx, `DELETE FROM app_evolution.census_progress; DELETE FROM app_evolution.jobs; DELETE FROM app_evolution.census_rows; DELETE FROM app_evolution.census_scopes`)
	require.NoError(t, err)
	require.NoError(t, a.initializeCensus(ctx, b))
	for i := 0; i < 3; i++ {
		done, e := a.captureCensusPage(ctx, b)
		require.NoError(t, e)
		require.False(t, done)
	}
	var count int
	var hash []byte
	var key string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.census_rows),hash_state,row_key::text FROM app_evolution.census_progress`).Scan(&count, &hash, &key))
	require.Equal(t, 1, count)
	a.Close()
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer func() { a.Close() }()
	wider := b
	wider.Deadline = wider.Deadline.Add(time.Hour)
	require.ErrorContains(t, a.Activate(ctx, wider, fixtureDiskCapacity()), "immutable census contract")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, a.Activate(canceled, b, fixtureDiskCapacity()))
	var unchanged []byte
	var sameKey string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT hash_state,row_key::text FROM app_evolution.census_progress`).Scan(&unchanged, &sameKey))
	require.Equal(t, hash, unchanged)
	require.Equal(t, key, sameKey)
	require.NoError(t, a.Activate(ctx, b, fixtureDiskCapacity()))
	require.NoError(t, verifyCensus(ctx, a.conn))
	var frozen Bounds
	var state string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT bounds,state FROM app_evolution.jobs`).Scan(&frozen, &state))
	require.Equal(t, encodeJSON(b), encodeJSON(frozen))
	require.Equal(t, "paused", state)
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT count(*) FROM app_evolution.census_rows`).Scan(&count))
	require.Equal(t, 3, count)
}

func TestEvolution_RepairCommittedReplyLost(t *testing.T) {
	pool, svc, keys := fixtureMaintainingRows(t, []string{"invalid historical"})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := fixtureWorker(pool, svc)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	var failed uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id FROM app_evolution.batches WHERE state='pending'`).Scan(&failed))
	require.NoError(t, svc.Close(ctx))
	trace := &revision7CommitTracer{}
	pc := pool.Config().Copy()
	pc.MaxConns = 1
	pc.ConnConfig.Tracer = trace
	dial := pc.ConnConfig.DialFunc
	pc.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &revision7CommitTransport{Conn: c}, nil
	}
	execution, err := pgxpool.NewWithConfig(ctx, pc)
	require.NoError(t, err)
	defer execution.Close()
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	serving := bootstrapFixture(t, execution, cfg)
	defer serving.Close(ctx)
	w = fixtureWorker(pool, serving)
	w.boundary = func(at string) {
		if at == "inside_repair" {
			trace.armed.Store(true)
		}
	}
	err = w.Repair(ctx, failed, keys[0], "A|B")
	var unknown *oversync.CommitOutcomeUnknownError
	require.ErrorAs(t, err, &unknown)
	require.NotNil(t, trace.transport)
	require.True(t, trace.transport.committed.Load())
	var state, legacy string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM app_evolution.batches WHERE operation_id=$1`, failed).Scan(&state))
	require.Equal(t, "repaired", state)
	require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name FROM main.upgrade_people WHERE id=$1`, keys[0]).Scan(&legacy))
	require.Equal(t, "A|B", legacy)
	restarted := fixtureWorker(pool, serving)
	require.NoError(t, restarted.Repair(ctx, failed, keys[0], "A|B"))
	var count int
	var measured bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM sync.scope_write_receipts r WHERE r.operation_id=b.operation_id),metrics_complete FROM app_evolution.batches b JOIN app_evolution.repair_evidence e ON e.repair_operation=b.operation_id WHERE e.failed_operation=$1`, failed).Scan(&count, &measured))
	require.Equal(t, 1, count)
	require.True(t, measured)
	require.NoError(t, restarted.Run(ctx))
}

func TestEvolution_CensusCapacityExhaustionPreservesPages(t *testing.T) {
	values := make([]string, 200)
	for i := range values {
		values[i] = "A|B"
	}
	pool, svc, _ := fixtureMaintainingRows(t, values)
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	_, err = a.conn.Exec(ctx, `DELETE FROM app_evolution.census_progress; DELETE FROM app_evolution.jobs; DELETE FROM app_evolution.census_rows; DELETE FROM app_evolution.census_scopes`)
	require.NoError(t, err)
	b := fixtureBounds()
	b.Rows = 10
	b.MaxEvidenceBytes = 9000
	require.NoError(t, a.initializeCensus(ctx, b))
	var capacityErr error
	for i := 0; i < 30; i++ {
		done, e := a.captureCensusPage(ctx, b)
		if e != nil {
			capacityErr = e
			break
		}
		require.False(t, done)
	}
	require.ErrorContains(t, capacityErr, "evidence_capacity_exceeded")
	var count int
	var hash []byte
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.census_rows),hash_state FROM app_evolution.census_progress`).Scan(&count, &hash))
	require.Positive(t, count)
	require.Less(t, count, len(values))
	a.Close()
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	require.ErrorContains(t, a.Activate(ctx, b, fixtureDiskCapacity()), "evidence_capacity_exceeded")
	var after int
	var unchanged []byte
	var frozen Bounds
	var closed bool
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.census_rows),hash_state FROM app_evolution.census_progress`).Scan(&after, &unchanged))
	require.Equal(t, count, after)
	require.Equal(t, hash, unchanged)
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT bounds,(SELECT requested FROM app_evolution.maintenance) FROM app_evolution.jobs`).Scan(&frozen, &closed))
	require.Equal(t, encodeJSON(b), encodeJSON(frozen))
	require.True(t, closed)
}
