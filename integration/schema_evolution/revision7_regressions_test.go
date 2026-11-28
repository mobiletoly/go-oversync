package schemaevolution

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_RuntimeRejectsParameterAndColumnAuthority(t *testing.T) {
	for _, privilege := range []string{"parameter", "column_update", "column_insert", "scope_update", "scope_delete"} {
		for _, mode := range []string{"direct", "inherited", "set_role"} {
			t.Run(privilege+"/"+mode, func(t *testing.T) {
				pool, svc, _ := fixtureMaintaining(t)
				ctx := context.Background()
				proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
				require.NoError(t, err)
				a, err := OpenAuthority(ctx, proof)
				require.NoError(t, err)
				defer a.Close()
				role := "r7_" + strings.ReplaceAll(uuid.NewString(), "-", "")
				parent := role + "_parent"
				r, p := pgx.Identifier{role}.Sanitize(), pgx.Identifier{parent}.Sanitize()
				_, err = a.conn.Exec(ctx, `CREATE ROLE `+r+`; CREATE ROLE `+p)
				require.NoError(t, err)
				defer func() {
					_, err := a.conn.Exec(ctx, `DROP OWNED BY `+r+`; DROP OWNED BY `+p+`; DROP ROLE `+r+`; DROP ROLE `+p)
					require.NoError(t, err)
				}()
				target := r
				if mode != "direct" {
					target = p
					options := " WITH INHERIT TRUE"
					if mode == "set_role" {
						options = " WITH INHERIT FALSE, SET TRUE"
					}
					_, err = a.conn.Exec(ctx, `GRANT `+p+` TO `+r+options)
					require.NoError(t, err)
				}
				grant := `GRANT SET ON PARAMETER session_replication_role TO ` + target
				if privilege == "column_update" {
					grant = `GRANT UPDATE(requested) ON app_evolution.maintenance TO ` + target + `; GRANT UPDATE(closed) ON app_evolution.barrier TO ` + target
				}
				if privilege == "column_insert" {
					grant = `GRANT INSERT(backend) ON app_evolution.worker_transactions TO ` + target
				}
				if privilege == "scope_update" {
					grant = `GRANT UPDATE(incarnation) ON app_evolution.scope_inventory TO ` + target
				}
				if privilege == "scope_delete" {
					grant = `GRANT DELETE ON app_evolution.scope_inventory TO ` + target
				}
				_, err = a.conn.Exec(ctx, grant)
				require.NoError(t, err)
				require.ErrorContains(t, a.GrantRuntime(ctx, role), "ownership or reachable provisioning")
				// GrantRuntime's table/schema grants must all roll back on rejection.
				var businessAccess bool
				require.NoError(t, a.conn.QueryRow(ctx, `SELECT has_table_privilege($1,'main.upgrade_people','DELETE')`, role).Scan(&businessAccess))
				require.False(t, businessAccess)
				var rows, history int64
				require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM main.upgrade_people),(SELECT count(*) FROM sync.bundle_log)`).Scan(&rows, &history))
				_, err = a.conn.Exec(ctx, `SET ROLE `+r)
				require.NoError(t, err)
				_, deleteErr := a.conn.Exec(ctx, `DELETE FROM main.upgrade_people`)
				_, err = a.conn.Exec(ctx, `RESET ROLE`)
				require.NoError(t, err)
				require.Error(t, deleteErr)
				var afterRows, afterHistory int64
				var requested, closed bool
				require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM main.upgrade_people),(SELECT count(*) FROM sync.bundle_log),(SELECT requested FROM app_evolution.maintenance),(SELECT closed FROM app_evolution.barrier)`).Scan(&afterRows, &afterHistory, &requested, &closed))
				require.Equal(t, rows, afterRows)
				require.Equal(t, history, afterHistory)
				require.True(t, requested)
				require.True(t, closed)
			})
		}
	}
}

// The server's COMMIT response is consumed at the transport boundary and never
// delivered to pgx. Independent database reads below prove the committed outcome.
type revision7CommitTransport struct {
	net.Conn
	drop      atomic.Bool
	committed atomic.Bool
}

func (c *revision7CommitTransport) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == 0 || !c.drop.CompareAndSwap(true, false) {
		return n, err
	}
	reader := io.MultiReader(bytes.NewReader(p[:n]), c.Conn)
	for {
		var header [5]byte
		if _, err = io.ReadFull(reader, header[:]); err != nil {
			break
		}
		size := int(binary.BigEndian.Uint32(header[1:])) - 4
		if size < 0 || size > 1<<20 {
			err = fmt.Errorf("invalid PostgreSQL response size")
			break
		}
		payload := make([]byte, size)
		if _, err = io.ReadFull(reader, payload); err != nil {
			break
		}
		if header[0] == 'C' && string(payload) == "COMMIT\x00" {
			c.committed.Store(true)
		}
		if header[0] == 'Z' {
			break
		}
	}
	_ = c.Conn.Close()
	return 0, io.ErrUnexpectedEOF
}

type revision7CommitTracer struct {
	armed     atomic.Bool
	transport *revision7CommitTransport
}

func (f *revision7CommitTracer) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.EqualFold(d.SQL, "commit") && f.armed.CompareAndSwap(true, false) {
		f.transport = c.PgConn().Conn().(*revision7CommitTransport)
		f.transport.drop.Store(true)
	}
	return ctx
}
func (*revision7CommitTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func revision7AssertCapacity(t *testing.T, pool *pgxpool.Pool, b Bounds) {
	t.Helper()
	used := revision6EvidenceUsage(t, pool)
	require.LessOrEqual(t, used, b.MaxEvidenceBytes)
	var persisted Bounds
	var closed bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT bounds,(SELECT requested FROM app_evolution.maintenance) FROM app_evolution.jobs`).Scan(&persisted, &closed))
	require.Equal(t, encodeJSON(b), encodeJSON(persisted))
	require.True(t, closed)
}

func TestEvolution_WorkerCommittedReplyLostAccounting(t *testing.T) {
	b := fixtureBounds()
	b.Rows = 1
	b.Bytes = 1024
	b.MaxHistoryBytes = 1024
	b.MaxEvidenceBytes = 14000
	b.Retries = 1
	pool, svc, _ := fixtureMaintainingBounds(t, []string{"A|B", "C|D"}, nil, b)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, svc.Close(ctx))
	trace := &revision7CommitTracer{}
	pc := pool.Config().Copy()
	pc.MaxConns = 1
	pc.ConnConfig.Tracer = trace
	dial := pc.ConnConfig.DialFunc
	pc.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &revision7CommitTransport{Conn: conn}, nil
	}
	execution, err := pgxpool.NewWithConfig(ctx, pc)
	require.NoError(t, err)
	defer execution.Close()
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	serving := bootstrapFixture(t, execution, cfg)
	defer serving.Close(context.Background())
	w := fixtureWorker(pool, serving)
	w.Bounds = b
	var frozen []byte
	var operation uuid.UUID
	var reserved int64
	w.boundary = func(at string) {
		if at == "before_apply" {
			require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id,descriptor,evidence_reserve FROM app_evolution.batches`).Scan(&operation, &frozen, &reserved))
			trace.armed.Store(true)
		}
	}
	err = w.Run(ctx)
	var unknown *oversync.CommitOutcomeUnknownError
	require.ErrorAs(t, err, &unknown)
	require.NotNil(t, trace.transport)
	require.True(t, trace.transport.committed.Load(), "server COMMIT reply must have been consumed before simulating loss")
	var changed, receipts, unmeasured int
	require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL),(SELECT count(*) FROM sync.scope_write_receipts WHERE operation_id=$1 AND receipt_state='completed'),(SELECT count(*) FROM app_evolution.batches WHERE state='completed' AND NOT metrics_complete)`, operation).Scan(&changed, &receipts, &unmeasured))
	require.Equal(t, 1, changed)
	require.Equal(t, 1, receipts)
	require.Equal(t, 1, unmeasured)
	revision7AssertCapacity(t, pool, b)
	// New worker object, same durable job, no mutation of frozen limits or descriptor.
	restarted := fixtureWorker(pool, serving)
	restarted.Bounds = b
	for i := 0; i < 2; i++ {
		require.ErrorContains(t, restarted.Run(ctx), "history_capacity_exceeded")
		revision7AssertCapacity(t, pool, b)
		var got []byte
		var reserve, jobBytes, batchBytes, bundles int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT descriptor,evidence_reserve,history_bytes,bundles,(SELECT history_bytes FROM app_evolution.jobs) FROM app_evolution.batches WHERE operation_id=$1`, operation).Scan(&got, &reserve, &batchBytes, &bundles, &jobBytes))
		require.Equal(t, frozen, got)
		require.Equal(t, reserved, reserve)
		require.Positive(t, batchBytes)
		require.Equal(t, batchBytes, jobBytes)
		require.EqualValues(t, 1, bundles)
	}
}

func TestEvolution_WorkerFrozenEvidenceBoundary(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint("invalid=", invalid), func(t *testing.T) {
			b := fixtureBounds()
			b.Rows = 1
			b.Bytes = 1024
			b.MaxEvidenceBytes = 22000
			values := []string{"A|B", "C|D", "E|F", "G|H"}
			if invalid {
				values[1] = "invalid"
			}
			keys := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002"), uuid.MustParse("00000000-0000-0000-0000-000000000003"), uuid.MustParse("00000000-0000-0000-0000-000000000004")}
			pool, svc, _ := fixtureMaintainingBounds(t, values, keys, b)
			w := fixtureWorker(pool, svc)
			w.Bounds = b
			w.boundary = func(at string) {
				if at == "after_descriptor" || at == "after_commit" {
					revision7AssertCapacity(t, pool, b)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			expected := "evidence_capacity_exceeded"
			if invalid {
				expected = "invalid_historical_input"
			}
			require.ErrorContains(t, w.Run(ctx), expected)
			revision7AssertCapacity(t, pool, b)
			var descriptors [][]byte
			rows, err := pool.Query(ctx, `SELECT descriptor FROM app_evolution.batches ORDER BY ordinal`)
			require.NoError(t, err)
			for rows.Next() {
				var v []byte
				require.NoError(t, rows.Scan(&v))
				descriptors = append(descriptors, v)
			}
			require.NoError(t, rows.Err())
			rows.Close()
			require.NotEmpty(t, descriptors)
			var jobBytes, reserves int64
			require.NoError(t, pool.QueryRow(ctx, `SELECT history_bytes,(SELECT sum(evidence_reserve) FROM app_evolution.batches) FROM app_evolution.jobs`).Scan(&jobBytes, &reserves))
			require.Positive(t, jobBytes)
			// A fresh worker replays the real conversion failure or repeats exhausted
			// admission. Neither case may allocate another descriptor or extend bounds.
			for i := 0; i < 2; i++ {
				next := fixtureWorker(pool, svc)
				next.Bounds = b
				require.ErrorContains(t, next.Run(ctx), expected)
				revision7AssertCapacity(t, pool, b)
				var count, failures int
				var bytesAfter, reservesAfter int64
				require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.batches),(SELECT count(*) FROM app_evolution.row_work),history_bytes,(SELECT sum(evidence_reserve) FROM app_evolution.batches) FROM app_evolution.jobs`).Scan(&count, &failures, &bytesAfter, &reservesAfter))
				require.Equal(t, len(descriptors), count)
				require.Equal(t, jobBytes, bytesAfter)
				require.Equal(t, reserves, reservesAfter)
				if invalid {
					require.Equal(t, 1, failures)
				} else {
					require.Zero(t, failures)
				}
				for ordinal, want := range descriptors {
					var got []byte
					require.NoError(t, pool.QueryRow(ctx, `SELECT descriptor FROM app_evolution.batches WHERE ordinal=$1`, ordinal+1).Scan(&got))
					require.Equal(t, want, got)
				}
			}
			used := revision6EvidenceUsage(t, pool)
			t.Logf("frozen limit=%d retained plus reservations=%d descriptors=%d", b.MaxEvidenceBytes, used, len(descriptors))
			require.Greater(t, used, b.MaxEvidenceBytes/2)
		})
	}
}

func TestEvolution_CertificateCapacityFrozenAtActivation(t *testing.T) {
	b := fixtureBounds()
	b.Rows = 1
	b.Bytes = 1024
	b.MaxEvidenceBytes = 17000
	pool, svc, _ := fixtureMaintainingBounds(t, []string{"A|B", "C|D"}, nil, b)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w := fixtureWorker(pool, svc)
	w.Bounds = b
	require.NoError(t, w.Run(ctx))
	revision7AssertCapacity(t, pool, b)
	proof, err := Drain(ctx, pool, []*oversync.SyncService{svc}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	before := revision6EvidenceUsage(t, pool)
	require.ErrorContains(t, a.Certify(ctx, 3), "evidence_capacity_exceeded")
	require.Equal(t, before, revision6EvidenceUsage(t, pool))
	revision7AssertCapacity(t, pool, b)
	var count int
	var state string
	require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.certificates),state FROM app_evolution.jobs`).Scan(&count, &state))
	require.Zero(t, count)
	require.Equal(t, "traversed", state)
}
