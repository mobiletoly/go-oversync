// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPullToStable_HistoryPrunedFallsBackToSnapshot(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	snapshotRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 9, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")}}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		switch r.URL.Path {
		case "/sync/pull":
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "history_pruned",
				Message: "after_bundle_seq 0 is below retained floor 5",
			}), nil
		case "/sync/snapshot-sessions":
			if r.Method == http.MethodPost {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "snapshot-pruned",
					SnapshotBundleSeq: 9,
					RowCount:          1,
					ByteCount:         snapshotBytes,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
		case "/sync/snapshot-sessions/snapshot-pruned":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-pruned",
					SnapshotBundleSeq: 9,
					Rows:              snapshotRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		default:
			return nil, io.EOF
		}
		return nil, io.EOF
	})}

	mustPullToStable(t, client, ctx)
	require.GreaterOrEqual(t, requests, 2)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "user-1").Scan(&name))
	require.Equal(t, "Ada", name)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(9), lastBundleSeq)
}

func TestPullToStable_CheckpointAheadFallsBackToSnapshot(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	snapshotRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 9, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")}}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)
	_, err := db.ExecContext(ctx, `UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99 WHERE singleton_key = 1`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/pull":
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "checkpoint_ahead",
				Message: "requested checkpoint 99 is ahead of current committed bundle sequence 9",
			}), nil
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-ahead",
				SnapshotBundleSeq: 9,
				RowCount:          1,
				ByteCount:         snapshotBytes,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-ahead":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-ahead",
					SnapshotBundleSeq: 9,
					Rows:              snapshotRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	report, err := client.PullToStable(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	require.Equal(t, int64(9), report.Status.LastBundleSeqSeen)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))

	var rebuildRequired int
	require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&rebuildRequired))
	require.Zero(t, rebuildRequired)
	name, email, exists := loadUserForKey(t, db, "user-1")
	require.True(t, exists)
	require.Equal(t, "Ada", name)
	require.Equal(t, "ada@example.com", email)
}

func TestRebuild_KeepsManagedSourceAndBundleStateOutsideSourceRecovery(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY NOT NULL,
			body TEXT NOT NULL
		)
	`)

	originalSourceID := client.sourceID
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "stale", "Stale", "stale@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	snapshotRows := []oversync.SnapshotRow{
		{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 8, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")},
		{Schema: "main", Table: "users", Key: mustBundleKey("user-2"), RowVersion: 9, Payload: mustBundlePayload(t, "user-2", "Grace", "grace@example.com")},
	}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)
	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, client.seedFixtureRowStateInTx(ctx, tx, "main", "users", rowKeyJSON("stale"), 4, false))
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`
		INSERT INTO _sync_push_stage (
			bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		) VALUES (99, 0, 'main', 'users', ?, 'UPDATE', 4, ?)
	`, rowKeyJSON("stale"), `{"id":"stale","name":"Stage","email":"stage@example.com"}`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-stale', 1, 'main', 'users', ?, 4, ?)
	`, rowKeyJSON("stale"), `{"id":"stale","name":"Snapshot","email":"snapshot@example.com"}`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 5, 4)
	_, err = db.Exec(`INSERT INTO legacy_docs (id, body) VALUES (?, ?)`, "legacy-1", "stale body")
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_managed_tables (schema_name, table_name)
		VALUES ('main', 'legacy_docs')
		ON CONFLICT(schema_name, table_name) DO NOTHING
	`)
	require.NoError(t, err)
	seedLegacyDocBaseline(t, client, db)

	client.config.SnapshotChunkRows = 1
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			var rebuildRequired int
			require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&rebuildRequired))
			require.Equal(t, 1, rebuildRequired)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-recover",
				SnapshotBundleSeq: 9,
				RowCount:          2,
				ByteCount:         snapshotBytes,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-recover":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-recover",
						SnapshotBundleSeq: 9,
						Rows:              snapshotRows[:1],
						NextRowOrdinal:    1,
						HasMore:           true,
					}), nil
				case "1":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-recover",
						SnapshotBundleSeq: 9,
						Rows:              snapshotRows[1:],
						NextRowOrdinal:    2,
						HasMore:           false,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx)
	require.Equal(t, originalSourceID, client.sourceID)

	sourceID, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	var rebuildRequired int
	require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&rebuildRequired))
	require.Equal(t, client.sourceID, sourceID)
	require.Equal(t, int64(5), nextSourceBundleID)
	require.Equal(t, int64(9), lastBundleSeq)
	require.Equal(t, 0, rebuildRequired)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-2'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`
		SELECT COUNT(*)
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, rowKeyJSON("stale")).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state WHERE table_name = 'legacy_docs'`).Scan(&count))
	require.Equal(t, 0, count)
}

func TestRebuildRequired_NormalSyncAutomaticallyResumesCheckpointRecovery(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)
	originalSourceID := client.sourceID
	snapshotRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 7, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")}}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)

	_, err := db.Exec(`UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID)
	require.NoError(t, err)

	snapshotRequests := 0
	pullRequests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			snapshotRequests++
			var rebuildRequired int
			require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&rebuildRequired))
			require.Equal(t, 1, rebuildRequired)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-rebuild-required",
				SnapshotBundleSeq: 7,
				RowCount:          1,
				ByteCount:         snapshotBytes,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-rebuild-required":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-rebuild-required",
					SnapshotBundleSeq: 7,
					Rows:              snapshotRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/pull":
			require.Equal(t, http.MethodGet, r.Method)
			pullRequests++
			require.Equal(t, "7", r.URL.Query().Get("after_bundle_seq"))
			return jsonResponse(oversync.PullResponse{
				StableBundleSeq: 7,
				HasMore:         false,
				Bundles:         []oversync.Bundle{},
			}), nil
		}
		return nil, io.EOF
	})}

	report, err := client.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.RemoteOutcome)
	require.Equal(t, 1, snapshotRequests)
	require.Equal(t, 0, pullRequests)

	var rebuildRequired int
	require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&rebuildRequired))
	require.Equal(t, 0, rebuildRequired)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Equal(t, originalSourceID, client.sourceID)

	_, err = client.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, pullRequests)

	name, email, exists := loadUserForKey(t, db, "user-1")
	require.True(t, exists)
	require.Equal(t, "Ada", name)
	require.Equal(t, "ada@example.com", email)
}

func TestCheckpointRecovery_PausedPendingWorkReturnsActionableBlocker(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('local-1', 'Local', 'local@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99, rebuild_required = 1 WHERE singleton_key = 1`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_operation_state SET kind = 'none', reason = 'checkpoint_ahead' WHERE singleton_key = 1`)
	require.NoError(t, err)
	client.PauseUploads()

	_, err = client.Sync(ctx)
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompleteUploadsPaused, incomplete.Reason)
	require.Equal(t, 1, incomplete.DirtyCount)
	require.Zero(t, incomplete.OutboundCount)

	var (
		dirtyCount      int
		lastBundleSeq   int64
		rebuildRequired int
	)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen, rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&lastBundleSeq, &rebuildRequired))
	require.Equal(t, int64(99), lastBundleSeq)
	require.Equal(t, 1, rebuildRequired)
	require.Equal(t, "checkpoint_ahead", requireOperationReason(t, db))

	name, email, exists := loadUserForKey(t, db, "local-1")
	require.True(t, exists)
	require.Equal(t, "Local", name)
	require.Equal(t, "local@example.com", email)
}

func TestCheckpointRecovery_FailedPushReportsDurableFrozenState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('local-1', 'Local', 'local@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99, rebuild_required = 1 WHERE singleton_key = 1`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_operation_state SET kind = 'none', reason = 'checkpoint_ahead' WHERE singleton_key = 1`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push" {
			return errorJSONResponse(http.StatusUnauthorized, oversync.ErrorResponse{
				Error:   "unauthorized",
				Message: "reconcile denied",
			}), nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	_, err = client.Sync(ctx)
	var blockedErr *CheckpointRecoveryBlockedError
	require.NotErrorAs(t, err, &blockedErr)
	require.ErrorContains(t, err, "401")

	var (
		dirtyCount      int
		outboundCount   int
		lastBundleSeq   int64
		rebuildRequired int
		replayState     string
	)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1`).Scan(&replayState))
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen, rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&lastBundleSeq, &rebuildRequired))
	require.Zero(t, dirtyCount)
	require.Equal(t, 1, outboundCount)
	require.Equal(t, "prepared", replayState)
	require.Equal(t, int64(99), lastBundleSeq)
	require.Equal(t, 1, rebuildRequired)
	require.Equal(t, "checkpoint_ahead", requireOperationReason(t, db))

	name, email, exists := loadUserForKey(t, db, "local-1")
	require.True(t, exists)
	require.Equal(t, "Local", name)
	require.Equal(t, "local@example.com", email)
}
