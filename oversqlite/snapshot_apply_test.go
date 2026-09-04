// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestApplyStagedSnapshot_FailsClosedWhenAttachedStateIsMissing(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "local-1", "Local", "local@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-missing-state', 1, 'main', 'users', ?, 7, ?)
	`, rowKeyJSON("remote-1"), `{"id":"remote-1","name":"Remote","email":"remote@example.com"}`)
	require.NoError(t, err)
	_, err = db.Exec(`
		UPDATE _sync_attachment_state
		SET binding_state = 'anonymous', attached_user_id = '', pending_initialization_id = ''
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	err = client.applyStagedSnapshotLocked(ctx, &oversync.SnapshotSession{
		SnapshotID:        "snapshot-missing-state",
		SnapshotBundleSeq: 7,
		RowCount:          1,
	}, snapshotApplyOptions{})
	var connectErr *AttachRequiredError
	require.ErrorAs(t, err, &connectErr)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "local-1").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "remote-1").Scan(&count))
	require.Equal(t, 0, count)
}

func TestRebuildKeepSource_CheckpointUnchangedOnFailedFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-bad-final",
				SnapshotBundleSeq: 4,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-bad-final":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-bad-final",
					SnapshotBundleSeq: 4,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 4,
						Payload:    []byte(`{"id":"user-1","name":"Broken"}`),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx)
	require.Error(t, err)
	require.Equal(t, 1, snapshotStageCount(t, db))

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestRebuildRotateSource_ReusesDurableReplacementSourceAcrossRetry(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)
	client.sourceIDGenerator = func() string { return "reserved-rotated-device" }

	var requestedReplacementSourceIDs []string
	createPruned := true
	sessionCreates := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/push":
			if createPruned {
				createPruned = false
				return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
					Error:   "history_pruned",
					Message: "source tuple is below retained history floor",
				}), nil
			}
			return nil, io.EOF
		case "/sync/snapshot-sessions":
			var req oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.NotNil(t, req.SourceReplacement)
			requestedReplacementSourceIDs = append(requestedReplacementSourceIDs, req.SourceReplacement.NewSourceID)
			sessionCreates++
			if sessionCreates == 1 {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "retry-rotate-a",
					SnapshotBundleSeq: 9,
					RowCount:          1,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "retry-rotate-b",
				SnapshotBundleSeq: 9,
				RowCount:          0,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/retry-rotate-a":
			switch r.Method {
			case http.MethodGet:
				return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
					Error:   "snapshot_session_not_found",
					Message: "snapshot session retry-rotate-a was not found",
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/snapshot-sessions/retry-rotate-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "retry-rotate-b",
					SnapshotBundleSeq: 9,
					Rows:              nil,
					NextRowOrdinal:    0,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)
	require.Equal(t, "reserved-rotated-device", requireOperationReplacementSourceID(t, db))

	_, err = client.Rebuild(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_session_not_found")
	require.Equal(t, "bundle-device", client.sourceID)
	require.Equal(t, "reserved-rotated-device", requireOperationReplacementSourceID(t, db))

	report, err := client.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	require.Equal(t, "reserved-rotated-device", client.sourceID)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Equal(t, "", requireOperationReplacementSourceID(t, db))
	require.Equal(t, []string{"reserved-rotated-device", "reserved-rotated-device"}, requestedReplacementSourceIDs)
}

func TestRebuildRotateSource_SourceIDDoesNotRotateOnFailedFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalSourceID := client.sourceID
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 5, 4)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-bad-recover",
				SnapshotBundleSeq: 9,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-bad-recover":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-bad-recover",
					SnapshotBundleSeq: 9,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 9,
						Payload:    []byte(`{"id":"user-1","name":"Broken"}`),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx)
	require.Error(t, err)
	require.Equal(t, originalSourceID, client.sourceID)

	sourceID, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	require.Equal(t, originalSourceID, sourceID)
	require.Equal(t, int64(5), nextSourceBundleID)
	require.Equal(t, int64(4), lastBundleSeq)
}

func TestRebuildKeepSource_DeferredFKRowsApplyAcrossChunkBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		tables     []SyncTable
		ddl        []string
		snapshotID string
		bundleSeq  int64
		chunks     [][]oversync.SnapshotRow
		verify     func(t *testing.T, db *sql.DB)
	}{
		{
			name:       "self referential rows",
			tables:     []SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}},
			snapshotID: "snapshot-self-ref",
			bundleSeq:  3,
			ddl: []string{`
				CREATE TABLE categories (
					id TEXT PRIMARY KEY NOT NULL,
					parent_id TEXT,
					name TEXT NOT NULL,
					FOREIGN KEY (parent_id) REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED
				)
			`},
			chunks: [][]oversync.SnapshotRow{
				{{
					Schema:     "main",
					Table:      "categories",
					Key:        mustBundleKey("child"),
					RowVersion: 2,
					Payload:    []byte(`{"id":"child","parent_id":"root","name":"Child"}`),
				}},
				{{
					Schema:     "main",
					Table:      "categories",
					Key:        mustBundleKey("root"),
					RowVersion: 1,
					Payload:    []byte(`{"id":"root","parent_id":null,"name":"Root"}`),
				}},
			},
			verify: func(t *testing.T, db *sql.DB) {
				t.Helper()
				var count int
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM categories`).Scan(&count))
				require.Equal(t, 2, count)
				var parentID string
				require.NoError(t, db.QueryRow(`SELECT parent_id FROM categories WHERE id = 'child'`).Scan(&parentID))
				require.Equal(t, "root", parentID)
			},
		},
		{
			name: "cyclic rows",
			tables: []SyncTable{
				{TableName: "teams", SyncKeyColumnName: "id"},
				{TableName: "team_members", SyncKeyColumnName: "id"},
			},
			snapshotID: "snapshot-cycle",
			bundleSeq:  5,
			ddl: []string{`
				CREATE TABLE teams (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					captain_member_id TEXT,
					FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
				)
			`, `
				CREATE TABLE team_members (
					id TEXT PRIMARY KEY NOT NULL,
					team_id TEXT NOT NULL,
					name TEXT NOT NULL,
					FOREIGN KEY (team_id) REFERENCES teams(id) DEFERRABLE INITIALLY DEFERRED
				)
			`},
			chunks: [][]oversync.SnapshotRow{
				{{
					Schema:     "main",
					Table:      "team_members",
					Key:        mustBundleKey("member-1"),
					RowVersion: 4,
					Payload:    []byte(`{"id":"member-1","team_id":"team-1","name":"Captain"}`),
				}},
				{{
					Schema:     "main",
					Table:      "teams",
					Key:        mustBundleKey("team-1"),
					RowVersion: 5,
					Payload:    []byte(`{"id":"team-1","name":"Ops","captain_member_id":"member-1"}`),
				}},
			},
			verify: func(t *testing.T, db *sql.DB) {
				t.Helper()
				var captainID string
				require.NoError(t, db.QueryRow(`SELECT captain_member_id FROM teams WHERE id = 'team-1'`).Scan(&captainID))
				require.Equal(t, "member-1", captainID)
				var teamID string
				require.NoError(t, db.QueryRow(`SELECT team_id FROM team_members WHERE id = 'member-1'`).Scan(&teamID))
				require.Equal(t, "team-1", teamID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client, db := newBundleClient(t, "main", tc.tables, tc.ddl...)
			client.config.SnapshotChunkRows = 1
			_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
			require.NoError(t, err)

			setChunkedSnapshotSession(t, client, tc.snapshotID, tc.bundleSeq, tc.chunks)
			mustRebuild(t, client, ctx)
			tc.verify(t, db)
		})
	}
}

func TestSnapshotStageRows_AreIsolatedBySnapshotID(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	require.NoError(t, client.stageSnapshotChunk(ctx, &oversync.SnapshotChunkResponse{
		SnapshotID:        "snapshot-a",
		SnapshotBundleSeq: 1,
		Rows: []oversync.SnapshotRow{{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey("user-a"),
			RowVersion: 1,
			Payload:    mustBundlePayload(t, "user-a", "Ada", "ada@example.com"),
		}},
		NextRowOrdinal: 1,
		HasMore:        false,
	}, 0))
	require.NoError(t, client.stageSnapshotChunk(ctx, &oversync.SnapshotChunkResponse{
		SnapshotID:        "snapshot-b",
		SnapshotBundleSeq: 1,
		Rows: []oversync.SnapshotRow{{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey("user-b"),
			RowVersion: 1,
			Payload:    mustBundlePayload(t, "user-b", "Grace", "grace@example.com"),
		}},
		NextRowOrdinal: 1,
		HasMore:        false,
	}, 0))

	var countA, countB int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id = 'snapshot-a'`).Scan(&countA))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id = 'snapshot-b'`).Scan(&countB))
	require.Equal(t, 1, countA)
	require.Equal(t, 1, countB)
}
