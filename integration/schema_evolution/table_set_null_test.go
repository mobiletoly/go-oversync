package schemaevolution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type tableSetNullView struct {
	Generation int                  `json:"generation"`
	Rows       []oversync.BundleRow `json:"expected_rows"`
	Snapshot   oversync.SnapshotRow `json:"expected_snapshot_row"`
}

func TestTableEvolution_CapturedSetNull(t *testing.T) {
	v := readTableCases(t)
	var c tableCase
	for _, candidate := range v.Cases {
		if candidate.ID == "captured-set-null" {
			c = candidate
		}
	}
	require.Equal(t, "captured-set-null", c.ID)
	require.Equal(t, "POST", c.Method)
	require.Equal(t, "/sync/push", c.Path)
	require.Len(t, c.Views, 2)
	ctx := context.Background()
	pool, old := tableBaseline(t)
	for _, scope := range []string{v.Scope, "other-scope"} {
		_, err := CreateScope(ctx, pool, scope)
		require.NoError(t, err)
		require.NoError(t, fixtureWrite(t, old, scope, `INSERT INTO main.people VALUES($1,$2,'Person')`, scope, uuid.MustParse(v.Person)))
	}
	proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	backfill := tableBackfillStart(t, pool, proof, cleanup)
	w := &TableWorker{Pool: pool, Service: backfill, Capacity: tableTestCapacity}
	require.NoError(t, w.Populate(ctx))
	require.NoError(t, backfill.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	tablePublish(t, a, TablesServing)
	cfg, err := TableEvolutionConfiguration(TablesServing)
	require.NoError(t, err)
	cfg.BundleChangeWatch.Enabled = true
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	t.Cleanup(pair.Close)
	cfg.Readiness.PoolPair = pair
	s, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	require.NoError(t, s.Bootstrap(ctx))
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{s}))
	startTableWatchListener(t, pool, s)
	actors := []oversync.Actor{{UserID: v.Scope, SourceID: uuid.NewString()}, {UserID: v.Scope, SourceID: uuid.NewString()}}
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var watches []<-chan oversync.BundleChangeEvent
	for i, actor := range actors {
		g := cfg.Readiness.Generations[i].Proof
		_, err = s.Connect(ctx, actor, g, &oversync.ConnectRequest{})
		require.NoError(t, err)
		ch, err := s.SubscribeBundleChanges(watchCtx, actor, g, 2)
		require.NoError(t, err)
		watches = append(watches, ch)
	}
	target := cfg.Readiness.Generations[c.Generation-1].Proof
	require.Equal(t, fmt.Sprint(target.ID), c.Headers[oversync.SchemaGenerationIDHeader])
	require.Equal(t, target.Fingerprint, c.Headers[oversync.SchemaGenerationFingerprintHeader])
	hash, _, err := protocolhash.GenerationPinnedPushRequest(fmt.Sprint(target.ID), target.Fingerprint, []protocolhash.PushRow{{Schema: c.Row.Schema, Table: c.Row.Table, Key: map[string]any(c.Row.Key), Op: c.Row.Op, BaseRowVersion: c.Row.BaseRowVersion, Payload: c.Row.Payload}})
	require.NoError(t, err)
	require.Equal(t, c.Request.CanonicalRequestHash, hash)
	request := c.Request.oneShot([]oversync.PushRequestRow{c.Row})
	committed, err := s.PushOneShot(ctx, actors[1], target, &request)
	require.NoError(t, err)
	require.EqualValues(t, 3, committed.Bundle.BundleSeq)
	require.EqualValues(t, 2, committed.Bundle.RowCount) // project DELETE + captured person UPDATE
	assertCapturedSetNullState(t, pool, v)
	for i, view := range c.Views {
		require.Equal(t, i+1, view.Generation)
		actor, g := actors[i], cfg.Readiness.Generations[i].Proof
		select {
		case event, ok := <-watches[i]:
			require.True(t, ok)
			require.Equal(t, committed.Bundle.BundleSeq, event.BundleSeq)
			wire, err := json.Marshal(event)
			require.NoError(t, err)
			require.JSONEq(t, `{"bundle_seq":3}`, string(wire)) // reduced metadata-only event, including old view
		case <-watchCtx.Done():
			t.Fatal("SET NULL commit did not wake the generation watch")
		}
		pull, err := s.ProcessPull(ctx, actor, g, 2, 10, 0)
		require.NoError(t, err)
		require.EqualValues(t, 3, pull.StableBundleSeq)
		require.False(t, pull.HasMore)
		require.Len(t, pull.Bundles, 1)
		bundle := pull.Bundles[0]
		assertSetNullRows(t, view.Rows, bundle.Rows)
		expectedHash, expectedBytes := setNullDescriptor(t, view.Rows)
		require.EqualValues(t, len(view.Rows), bundle.RowCount)
		require.Equal(t, expectedBytes, bundle.ByteCount)
		require.Equal(t, expectedHash, bundle.BundleHash)
		if i == 1 {
			require.Equal(t, committed.Bundle.BundleHash, bundle.BundleHash)
			require.Equal(t, committed.Bundle.ByteCount, bundle.ByteCount)
			replayRequest := c.Request.oneShot([]oversync.PushRequestRow{c.Row})
			replay, err := s.PushOneShot(ctx, actor, g, &replayRequest)
			require.NoError(t, err)
			require.Equal(t, committed.Bundle.BundleSeq, replay.Bundle.BundleSeq)
			require.Equal(t, expectedHash, replay.Bundle.BundleHash)
			require.Equal(t, expectedBytes, replay.Bundle.ByteCount)
			require.Equal(t, committed.Bundle.RowCount, replay.Bundle.RowCount)
		}
		snapshot, err := s.CreateSnapshotSession(ctx, actor, g)
		require.NoError(t, err)
		require.EqualValues(t, 3, snapshot.SnapshotBundleSeq)
		require.EqualValues(t, 1, snapshot.RowCount)
		chunk, err := s.GetSnapshotChunk(ctx, actor, g, snapshot.SnapshotID, 0, 100, 1<<20)
		require.NoError(t, err)
		require.Len(t, chunk.Rows, 1)
		expected, err := json.Marshal(view.Snapshot)
		require.NoError(t, err)
		actual, err := json.Marshal(chunk.Rows[0])
		require.NoError(t, err)
		require.JSONEq(t, string(expected), string(actual))
		// Snapshot accounting is the compact logical-row total, not the JSON
		// rendering size of this expanded test representation.
		require.Positive(t, chunk.ByteCount)
		require.Equal(t, snapshot.ByteCount, chunk.ByteCount)
		require.False(t, chunk.HasMore)
	}
}

func startTableWatchListener(t *testing.T, pool *pgxpool.Pool, s *oversync.SyncService) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- s.RunBundleChangeListener(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			require.True(t, err == nil || errors.Is(err, context.Canceled), "listener: %v", err)
		case <-time.After(5 * time.Second):
			t.Error("table watch listener did not stop")
		}
	})
	require.Eventually(t, func() bool {
		var listening bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND state='idle' AND query LIKE 'LISTEN %')`).Scan(&listening)
		return err == nil && listening
	}, 5*time.Second, 10*time.Millisecond)
}

func assertCapturedSetNullState(t *testing.T, pool *pgxpool.Pool, v tableCases) {
	t.Helper()
	ctx := context.Background()
	var scope string
	var project *string
	var version int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT p._sync_scope_id,p.project_id::text,r.bundle_seq FROM main.people p JOIN sync.user_state u ON u.user_id=p._sync_scope_id JOIN sync.row_state r ON r.user_pk=u.user_pk AND r.table_id=2 AND r.key_bytes=uuid_send(p.id) WHERE p._sync_scope_id=$1 AND p.id=$2::uuid AND NOT r.deleted`, v.Scope, v.Person).Scan(&scope, &project, &version))
	require.Equal(t, v.Scope, scope)
	require.Nil(t, project)
	require.EqualValues(t, 3, version)
	require.NoError(t, pool.QueryRow(ctx, `SELECT p.project_id::text,r.bundle_seq FROM main.people p JOIN sync.user_state u ON u.user_id=p._sync_scope_id JOIN sync.row_state r ON r.user_pk=u.user_pk AND r.table_id=2 AND r.key_bytes=uuid_send(p.id) WHERE p._sync_scope_id='other-scope' AND p.id=$1::uuid`, v.Person).Scan(&project, &version))
	require.NotNil(t, project)
	require.Equal(t, v.Project, *project)
	require.EqualValues(t, 2, version)
	var effects, deleted int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE b.table_id=4 AND b.op_code=3) FROM sync.bundle_rows b JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1 AND b.bundle_seq=3 AND ((b.table_id=2 AND b.op_code=2 AND b.payload_wire::jsonb->'project_id'='null'::jsonb) OR (b.table_id=4 AND b.op_code=3))`, v.Scope).Scan(&effects, &deleted))
	require.Equal(t, 2, effects)
	require.Equal(t, 1, deleted)
}

func assertSetNullRows(t *testing.T, expected, actual []oversync.BundleRow) {
	t.Helper()
	want, err := json.Marshal(expected)
	require.NoError(t, err)
	got, err := json.Marshal(actual)
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got))
}

func setNullDescriptor(t *testing.T, rows []oversync.BundleRow) (string, int64) {
	t.Helper()
	var logical []protocolhash.BundleRow
	for _, row := range rows {
		logical = append(logical, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: map[string]any(row.Key), Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload})
	}
	hash, bytes, err := protocolhash.CommittedBundle(logical)
	require.NoError(t, err)
	return hash, bytes
}
