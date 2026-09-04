package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const rollbackFailureDriverName = "sqlite3_oversqlite_rollback_failure"

var (
	registerRollbackFailureDriver sync.Once
	denyRollback                  atomic.Bool
	rollbackDriverConnections     atomic.Int64
)

func ensureRollbackFailureDriver() {
	registerRollbackFailureDriver.Do(func() {
		sql.Register(rollbackFailureDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				rollbackDriverConnections.Add(1)
				conn.RegisterAuthorizer(func(op int, arg1, _, _ string) int {
					if denyRollback.Load() && op == sqlite3.SQLITE_TRANSACTION && strings.EqualFold(arg1, "ROLLBACK") {
						return sqlite3.SQLITE_DENY
					}
					return sqlite3.SQLITE_OK
				})
				return nil
			},
		})
	})
}

func stageUserSnapshotForGate(t *testing.T, client *Client, snapshotID, rowID string, bundleSeq int64, payload []byte) *oversync.SnapshotSession {
	t.Helper()
	rows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": rowID}, RowVersion: bundleSeq, Payload: payload}}
	byteCount := testSnapshotRowsByteCount(rows)
	chunk := &oversync.SnapshotChunkResponse{
		SnapshotID: snapshotID, SnapshotBundleSeq: bundleSeq,
		Rows:           rows,
		NextRowOrdinal: 1, ByteCount: byteCount,
	}
	require.NoError(t, client.stageSnapshotChunk(context.Background(), chunk, 0))
	return &oversync.SnapshotSession{SnapshotID: snapshotID, SnapshotBundleSeq: bundleSeq, RowCount: 1, ByteCount: byteCount, ExpiresAt: "2030-01-01T00:00:00Z"}
}

func stageUserSnapshotRowsForGate(t *testing.T, client *Client, snapshotID string, bundleSeq int64, rows []oversync.SnapshotRow) *oversync.SnapshotSession {
	t.Helper()
	byteCount := testSnapshotRowsByteCount(rows)
	chunk := &oversync.SnapshotChunkResponse{
		SnapshotID: snapshotID, SnapshotBundleSeq: bundleSeq,
		Rows:           rows,
		NextRowOrdinal: int64(len(rows)), ByteCount: byteCount,
	}
	require.NoError(t, client.stageSnapshotChunk(context.Background(), chunk, 0))
	return &oversync.SnapshotSession{SnapshotID: snapshotID, SnapshotBundleSeq: bundleSeq, RowCount: int64(len(rows)), ByteCount: byteCount, ExpiresAt: "2030-01-01T00:00:00Z"}
}

func TestSnapshotFinalGate_LocalWriteDuringDownloadPreservesEverything(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('old','Old','old@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 1, 3)
	remoteRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 7, Payload: mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"})}}
	remoteBytes := testSnapshotRowsByteCount(remoteRows)

	var retired atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-race", SnapshotBundleSeq: 7, RowCount: 1, ByteCount: remoteBytes, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/snapshot-race":
			_, writeErr := db.Exec(`INSERT INTO users(id,name,email) VALUES('local-race','Local','local@example.com')`)
			require.NoError(t, writeErr)
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "snapshot-race", SnapshotBundleSeq: 7,
				Rows:           remoteRows,
				NextRowOrdinal: 1,
			}), nil
		case r.Method == http.MethodDelete && r.URL.Path == "/sync/snapshot-sessions/snapshot-race":
			retired.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	_, err = client.Rebuild(ctx)
	var gateErr *SnapshotFinalApplyGateError
	var dirtyErr *DirtyStateRejectedError
	require.ErrorAs(t, err, &gateErr)
	require.ErrorAs(t, err, &dirtyErr)
	require.Equal(t, string(snapshotModeClearAll), gateErr.Mode)
	require.Equal(t, int64(1), retired.Load())
	requireUserCount(t, db, "old", 1)
	requireUserCount(t, db, "local-race", 1)
	requireUserCount(t, db, "remote", 0)
	requireLastBundleSeqSeen(t, client, ctx, 3)
	require.Equal(t, 1, snapshotStageCount(t, db))
	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, outboxStateNone, requireOutboxBundle(t, db).State)
	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.True(t, attachment.RebuildRequired)
}

func TestImmediateTransaction_WriteAfterOwnershipWaitsAndIsNotErased(t *testing.T) {
	path := filepath.Join(t.TempDir(), "immediate.sqlite")
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=2000&_journal_mode=WAL")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(2)
	_, err = db.Exec(`CREATE TABLE rows(id TEXT PRIMARY KEY, value TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO rows VALUES('old','old')`)
	require.NoError(t, err)

	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(context.Background(), `DELETE FROM rows`)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `INSERT INTO rows VALUES('remote','remote')`)
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := db.Exec(`INSERT INTO rows VALUES('local-after-gate','local')`)
		writeDone <- writeErr
	}()
	select {
	case writeErr := <-writeDone:
		t.Fatalf("writer completed before immediate owner committed: %v", writeErr)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(context.Background()))
	require.NoError(t, <-writeDone)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM rows WHERE id IN ('remote','local-after-gate')`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestSnapshotFinalGate_PreparedSourceRecoveryReappliesAndRebindsAtomically(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	seedOutboxBundleForTest(t, db, outboxBundleRecord{
		State: outboxStatePrepared, SourceID: client.sourceID, SourceBundleID: 8,
		CanonicalRequestHash: "prepared-hash", RowCount: 1,
	})
	seedOutboxUserInsertForTest(t, db, 8, "intent", "Intent", "intent@example.com")
	originalBase := baselineText(t, db, `SELECT base_provenance FROM _sync_outbox_rows`)
	originalWire := baselineText(t, db, `SELECT wire_payload FROM _sync_outbox_rows`)
	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	attachment.RebuildRequired = true
	require.NoError(t, persistAttachmentState(ctx, db, attachment))
	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindSourceRecovery, Reason: string(SourceRecoveryHistoryPruned), ReplacementSourceID: "replacement-source"})

	options := snapshotApplyOptions{
		RotateSource: true, NewSourceID: "replacement-source", ReplacementReason: string(SourceRecoveryHistoryPruned),
		PreserveOutbox: true, ClearSourceRecovery: true, RequireFreshRotatedSource: true,
	}
	guard, err := client.pinSnapshotApplyGuard(ctx, options)
	require.NoError(t, err)
	options.PinnedGuard = guard
	session := stageUserSnapshotForGate(t, client, "snapshot-source-recovery", "remote", 9, mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}))
	require.NoError(t, client.applyStagedSnapshotLocked(ctx, session, options))

	requireUserCount(t, db, "old", 0)
	requireUserCount(t, db, "remote", 1)
	requireUserCount(t, db, "intent", 1)
	require.Equal(t, 0, snapshotStageCount(t, db))
	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, "replacement-source", outbox.SourceID)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	var reboundBundleID int64
	require.NoError(t, db.QueryRow(`SELECT source_bundle_id FROM _sync_outbox_rows`).Scan(&reboundBundleID))
	require.Equal(t, int64(1), reboundBundleID)
	operation, err := loadOperationState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, operationKindNone, operation.Kind)
	attachment, err = loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, "replacement-source", attachment.CurrentSourceID)
	require.Equal(t, int64(9), attachment.LastBundleSeqSeen)
	require.False(t, attachment.RebuildRequired)
	require.Equal(t, originalBase, baselineText(t, db, `SELECT base_provenance FROM _sync_outbox_rows`))
	require.Equal(t, originalWire, baselineText(t, db, `SELECT wire_payload FROM _sync_outbox_rows`))
	require.Equal(t, "absent", baselineText(t, db, `SELECT server_kind FROM _sync_row_state WHERE key_json='{"id":"intent"}'`))
	require.Equal(t, "9", baselineText(t, db, `SELECT json_extract(server_provenance,'$.bundle_sequence') FROM _sync_row_state WHERE key_json='{"id":"intent"}'`))
	require.Equal(t, "Remote", baselineText(t, db, `SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json='{"id":"remote"}'`))
}

func TestSnapshotFinalGate_ChangedFingerprintAndCommittedMisclassificationFailClosed(t *testing.T) {
	t.Run("changed prepared fingerprint", func(t *testing.T) {
		ctx := context.Background()
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
		require.NoError(t, err)
		_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
		require.NoError(t, err)
		seedOutboxBundleForTest(t, db, outboxBundleRecord{State: outboxStatePrepared, SourceID: client.sourceID, SourceBundleID: 3, CanonicalRequestHash: "hash-a", RowCount: 1})
		seedOutboxUserInsertForTest(t, db, 3, "intent", "Intent", "intent@example.com")
		attachment, err := loadAttachmentState(ctx, db)
		require.NoError(t, err)
		attachment.RebuildRequired = true
		require.NoError(t, persistAttachmentState(ctx, db, attachment))
		setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindSourceRecovery, ReplacementSourceID: "replacement"})
		options := snapshotApplyOptions{RotateSource: true, NewSourceID: "replacement", PreserveOutbox: true, ClearSourceRecovery: true, RequireFreshRotatedSource: true}
		guard, err := client.pinSnapshotApplyGuard(ctx, options)
		require.NoError(t, err)
		options.PinnedGuard = guard
		session := stageUserSnapshotForGate(t, client, "snapshot-fingerprint", "remote", 8, mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}))
		_, err = db.Exec(`UPDATE _sync_outbox_bundle SET canonical_request_hash = 'hash-b' WHERE singleton_key = 1`)
		require.NoError(t, err)
		err = client.applyStagedSnapshotLocked(ctx, session, options)
		var gateErr *SnapshotFinalApplyGateError
		var pendingErr *PendingPushReplayError
		require.ErrorAs(t, err, &gateErr)
		require.ErrorAs(t, err, &pendingErr)
		requireUserCount(t, db, "old", 1)
		requireUserCount(t, db, "remote", 0)
		require.Equal(t, 1, snapshotStageCount(t, db))
	})

}
func TestSnapshotApplyFailureRollsBackAuthoritativeStateAndStageDeletion(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 1, 4)
	require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
	options := snapshotApplyOptions{}
	guard, err := client.pinSnapshotApplyGuard(ctx, options)
	require.NoError(t, err)
	options.PinnedGuard = guard
	session := stageUserSnapshotForGate(t, client, "snapshot-malformed-apply", "remote", 9, []byte(`{"id":"remote","name":"missing-email"}`))
	err = client.applyStagedSnapshotLocked(ctx, session, options)
	require.Error(t, err)
	requireUserCount(t, db, "old", 1)
	requireUserCount(t, db, "remote", 0)
	requireLastBundleSeqSeen(t, client, ctx, 4)
	require.Equal(t, 1, snapshotStageCount(t, db))
	attachment, loadErr := loadAttachmentState(ctx, db)
	require.NoError(t, loadErr)
	require.True(t, attachment.RebuildRequired)
	stats := client.SnapshotTransferDiagnostics()
	require.Equal(t, int64(1), stats.MaxLiveStagedApplyRows)
	require.Greater(t, stats.MaxLiveStagedApplyTextBytes, int64(0))
	require.Equal(t, int64(1), stats.MaxAppliedInMemoryRows)
}

func TestImmediateTxRollbackFailureDiscardsConnectionAndPreservesPrimaryError(t *testing.T) {
	ensureRollbackFailureDriver()
	denyRollback.Store(false)
	t.Cleanup(func() { denyRollback.Store(false) })

	db, err := sql.Open(rollbackFailureDriverName, filepath.Join(t.TempDir(), "rollback-failure.db"))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, func() error {
		_, execErr := db.Exec(`CREATE TABLE rollback_probe (id TEXT PRIMARY KEY NOT NULL)`)
		return execErr
	}())

	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `INSERT INTO rollback_probe(id) VALUES('uncommitted')`)
	require.NoError(t, err)
	connectionsBeforeFailure := rollbackDriverConnections.Load()

	primaryErr := &RebuildRequiredError{}
	denyRollback.Store(true)
	err = func() (resultErr error) {
		defer tx.rollbackOnReturn(&resultErr, "rollback failure test transaction")
		return primaryErr
	}()
	denyRollback.Store(false)

	var rebuildErr *RebuildRequiredError
	require.ErrorAs(t, err, &rebuildErr)
	require.ErrorContains(t, err, "failed to roll back rollback failure test transaction")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM rollback_probe`).Scan(&count))
	require.Zero(t, count)
	require.Greater(t, rollbackDriverConnections.Load(), connectionsBeforeFailure)
}

func TestSnapshotApplyErrorRedactsStructuredPrimaryKey(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 1, 4)
	require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
	options := snapshotApplyOptions{}
	options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
	require.NoError(t, err)

	const sensitiveKey = "snapshot-secret-primary-key"
	session := stageUserSnapshotForGate(t, client, "snapshot-redacted-key", sensitiveKey, 9, mustJSONPayload(t, map[string]any{
		"id": sensitiveKey, "name": "Remote", "email": "remote@example.com",
	}))
	_, err = db.Exec(`
		CREATE TRIGGER reject_snapshot_row_state
		BEFORE INSERT ON _sync_row_state
		WHEN NEW.table_name = 'users'
		BEGIN
			SELECT RAISE(ABORT, 'forced row state failure');
		END
	`)
	require.NoError(t, err)

	err = client.applyStagedSnapshotLocked(ctx, session, options)
	require.ErrorContains(t, err, "main.users")
	require.ErrorContains(t, err, "forced row state failure")
	require.NotContains(t, err.Error(), sensitiveKey)
	requireUserCount(t, db, "old", 1)
	requireUserCount(t, db, sensitiveKey, 0)
	require.Equal(t, 1, snapshotStageCount(t, db))
}

func TestSnapshotApplyFailureBoundariesRollBackCompleteTransition(t *testing.T) {
	t.Run("staged count mismatch", func(t *testing.T) {
		ctx := context.Background()
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
		require.NoError(t, err)
		_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
		require.NoError(t, err)
		setCurrentSourceBundleState(t, db, 1, 4)
		require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
		options := snapshotApplyOptions{}
		options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
		require.NoError(t, err)
		session := stageUserSnapshotForGate(t, client, "snapshot-count-mismatch", "remote", 9, mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}))
		session.RowCount = 2

		err = client.applyStagedSnapshotLocked(ctx, session, options)
		require.ErrorContains(t, err, "staged snapshot row count 1 does not match expected row_count 2")
		requireUserCount(t, db, "old", 1)
		requireUserCount(t, db, "remote", 0)
		requireLastBundleSeqSeen(t, client, ctx, 4)
		require.Equal(t, 1, snapshotStageCount(t, db))
	})

	t.Run("cancellation after destructive row work", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
		require.NoError(t, err)
		_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
		require.NoError(t, err)
		setCurrentSourceBundleState(t, db, 1, 4)
		require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
		options := snapshotApplyOptions{}
		options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
		require.NoError(t, err)
		session := stageUserSnapshotForGate(t, client, "snapshot-cancel-apply", "remote", 9, mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}))
		client.afterSnapshotRowsHook = cancel

		err = client.applyStagedSnapshotLocked(ctx, session, options)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		requireUserCount(t, db, "old", 1)
		requireUserCount(t, db, "remote", 0)
		requireLastBundleSeqSeen(t, client, context.Background(), 4)
		require.Equal(t, 1, snapshotStageCount(t, db))
	})

	t.Run("real SQLite full error", func(t *testing.T) {
		ctx := context.Background()
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
		require.NoError(t, err)
		_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
		require.NoError(t, err)
		setCurrentSourceBundleState(t, db, 1, 4)
		require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
		options := snapshotApplyOptions{}
		options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
		require.NoError(t, err)
		payload := mustJSONPayload(t, map[string]any{"id": "remote", "name": strings.Repeat("n", 2<<20), "email": "remote@example.com"})
		session := stageUserSnapshotForGate(t, client, "snapshot-sqlite-full", "remote", 9, payload)
		var pageCount int
		require.NoError(t, db.QueryRow(`PRAGMA page_count`).Scan(&pageCount))
		var acceptedMax int
		require.NoError(t, db.QueryRow(`PRAGMA max_page_count = `+strconv.Itoa(pageCount)).Scan(&acceptedMax))
		require.Equal(t, pageCount, acceptedMax)

		err = client.applyStagedSnapshotLocked(ctx, session, options)
		require.ErrorContains(t, err, "database or disk is full")
		requireUserCount(t, db, "old", 1)
		requireUserCount(t, db, "remote", 0)
		requireLastBundleSeqSeen(t, client, ctx, 4)
		require.Equal(t, 1, snapshotStageCount(t, db))
	})

	t.Run("missing dependency preserves primary state", func(t *testing.T) {
		ctx := context.Background()
		client, db := newBundleClient(t, "main", []SyncTable{
			{TableName: "parents", SyncKeyColumnName: "id"},
			{TableName: "children", SyncKeyColumnName: "id"},
		}, `
			PRAGMA foreign_keys = ON;
			CREATE TABLE parents (id TEXT PRIMARY KEY NOT NULL);
			CREATE TABLE children (
				id TEXT PRIMARY KEY NOT NULL,
				parent_id TEXT NOT NULL,
				FOREIGN KEY(parent_id) REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)
		_, err := db.Exec(`INSERT INTO parents VALUES('parent-old')`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO children VALUES('old','parent-old')`)
		require.NoError(t, err)
		_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
		require.NoError(t, err)
		setCurrentSourceBundleState(t, db, 1, 4)
		require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
		options := snapshotApplyOptions{}
		options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
		require.NoError(t, err)
		chunk := &oversync.SnapshotChunkResponse{
			SnapshotID: "snapshot-commit-failure", SnapshotBundleSeq: 9,
			Rows: []oversync.SnapshotRow{{
				Schema: "main", Table: "children", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 9,
				Payload: mustJSONPayload(t, map[string]any{"id": "remote", "parent_id": "missing-parent"}),
			}},
			NextRowOrdinal: 1,
		}
		chunk.ByteCount = testSnapshotRowsByteCount(chunk.Rows)
		require.NoError(t, client.stageSnapshotChunk(ctx, chunk, 0))
		session := &oversync.SnapshotSession{SnapshotID: chunk.SnapshotID, SnapshotBundleSeq: 9, RowCount: 1, ByteCount: chunk.ByteCount, ExpiresAt: "2030-01-01T00:00:00Z"}

		before := owningRemediationPrimaryState(t, db, "parents", "children")
		err = client.applyStagedSnapshotLocked(ctx, session, options)
		var pending *FKReconciliationRequiredError
		require.ErrorAs(t, err, &pending)
		require.Equal(t, "dependency_unavailable", pending.Detail)
		require.Len(t, pending.Unit, 64)
		require.Equal(t, before, owningRemediationPrimaryState(t, db, "parents", "children"))
		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM children WHERE id = 'old'`).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM children WHERE id = 'remote'`).Scan(&count))
		require.Zero(t, count)
		requireLastBundleSeqSeen(t, client, ctx, 4)
		require.Equal(t, 1, snapshotStageCount(t, db))
	})
}

func TestSnapshotFinalGateErrorsRetainTypedFamilies(t *testing.T) {
	err := &SnapshotFinalApplyGateError{Mode: string(snapshotModeClearAll), Reason: "test", Cause: &RebuildRequiredError{}}
	var rebuildErr *RebuildRequiredError
	require.True(t, errors.As(err, &rebuildErr))
}
