package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type tablePush struct {
	actor   oversync.Actor
	proof   oversync.SchemaGenerationProof
	request oversync.OneShotPushRequest
}

func tablePushRequest(t *testing.T, s *oversync.SyncService, count int) tablePush {
	t.Helper()
	ctx := context.Background()
	cfg, err := TableEvolutionConfiguration(TablesBaseline)
	require.NoError(t, err)
	p := tablePush{actor: oversync.Actor{UserID: "scope", SourceID: uuid.NewString()}, proof: cfg.Readiness.Generations[0].Proof}
	_, err = s.Connect(ctx, p.actor, p.proof, &oversync.ConnectRequest{})
	require.NoError(t, err)
	var hashed []protocolhash.PushRow
	for i := 0; i < count; i++ {
		key := uuid.NewString()
		payload := json.RawMessage(fmt.Sprintf(`{"id":%q,"body":"offline"}`, key))
		row := oversync.PushRequestRow{Schema: "main", Table: "legacy_notes", Key: oversync.SyncKey{"id": key}, Op: oversync.OpInsert, Payload: payload}
		p.request.Rows = append(p.request.Rows, row)
		hashed = append(hashed, protocolhash.PushRow{Schema: row.Schema, Table: row.Table, Key: map[string]any(row.Key), Op: row.Op, Payload: payload})
	}
	hash, _, err := protocolhash.GenerationPinnedPushRequest(fmt.Sprint(p.proof.ID), p.proof.Fingerprint, hashed)
	require.NoError(t, err)
	p.request.SourceBundleID = 1
	p.request.CanonicalRequestHash = hash
	return p
}
func tableAuthoritativeDigest(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var d string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT md5(COALESCE(string_agg(v::text,',' ORDER BY kind,v::text),'')) FROM (
 SELECT 'source' kind,to_jsonb(s) v FROM sync.source_state s UNION ALL SELECT 'row',to_jsonb(r) FROM sync.row_state r UNION ALL SELECT 'log',to_jsonb(b) FROM sync.bundle_log b UNION ALL SELECT 'rows',to_jsonb(b) FROM sync.bundle_rows b UNION ALL SELECT 'receipt',to_jsonb(r) FROM sync.scope_write_receipts r) evidence`).Scan(&d))
	return d
}
func TestTableEvolution_CleanupExpiryBoundsAndReplay(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	committed := tablePushRequest(t, old, 3)
	receipt, err := old.PushOneShot(ctx, committed.actor, committed.proof, &committed.request)
	require.NoError(t, err)
	pending := tablePushRequest(t, old, 5)
	snapshot, err := old.CreateSnapshotSession(ctx, committed.actor, committed.proof)
	require.NoError(t, err)
	b := tableTestBounds()
	b.CleanupRows = 2
	b.CleanupSessions = 2
	proof, cleanup := tableCleanupStart(t, pool, old, b)
	before := tableAuthoritativeDigest(t, pool)
	_, err = cleanup.PushOneShot(ctx, pending.actor, pending.proof, &pending.request)
	require.Error(t, err)
	_, err = cleanup.GetSnapshotChunk(ctx, committed.actor, committed.proof, snapshot.SnapshotID, 0, 100, 0)
	require.Error(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = cleanup.CleanupExpiredStaging(ctx, tx)
	require.ErrorContains(t, err, "capability")
	require.NoError(t, tx.Rollback(ctx))
	w := &TableWorker{Pool: pool, Service: cleanup, Capacity: tableTestCapacity}
	// Unexpired rows survive; a timeout never grants permission to expand.
	short, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	require.Error(t, w.Cleanup(short))
	cancel()
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	blocked := a.ApplyTableEvolution(ctx, TablesAppend)
	a.Close()
	require.ErrorContains(t, blocked, "staging is not empty")
	a.Close()
	// Test clock setup: the snapshot was created before closure; expire within
	// the frozen window. PostgreSQL, not the worker clock, decides eligibility.
	_, err = pool.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=clock_timestamp()+interval '100 milliseconds'`)
	require.NoError(t, err)
	require.NoError(t, w.Cleanup(ctx))
	require.Equal(t, before, tableAuthoritativeDigest(t, pool))
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.snapshot_sessions`).Scan(&n))
	require.Zero(t, n)
	// Complete the same epoch, then replay retained success and submit the
	// previously unsent direct request.
	require.NoError(t, cleanup.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesAppend))
	tablePublish(t, a, TablesAppend)
	app := tableTestService(t, pool, TablesAppend)
	require.NoError(t, app.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesBackfill))
	tablePublish(t, a, TablesBackfill)
	w.Service = tableTestService(t, pool, TablesBackfill)
	require.NoError(t, w.Populate(ctx))
	require.NoError(t, w.Service.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	tablePublish(t, a, TablesServing)
	final := tableTestService(t, pool, TablesServing)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{final}))
	replay, err := final.PushOneShot(ctx, committed.actor, committed.proof, &committed.request)
	require.NoError(t, err)
	require.Equal(t, receipt.Bundle.BundleSeq, replay.Bundle.BundleSeq)
	_, err = final.PushOneShot(ctx, pending.actor, pending.proof, &pending.request)
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.legacy_notes`).Scan(&n))
	require.Equal(t, 8, n)
}

func TestTableEvolution_CleanupExpiredSnapshot(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	p := tablePushRequest(t, old, 1)
	_, err = old.PushOneShot(ctx, p.actor, p.proof, &p.request)
	require.NoError(t, err)
	_, err = old.CreateSnapshotSession(ctx, p.actor, p.proof)
	require.NoError(t, err)
	_, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	_, err = pool.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=clock_timestamp()-interval '1 second'`)
	require.NoError(t, err)
	w := &TableWorker{Pool: pool, Service: cleanup, Capacity: tableTestCapacity}
	require.NoError(t, w.Cleanup(ctx))
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.snapshot_sessions`).Scan(&count))
	require.Zero(t, count)
}
