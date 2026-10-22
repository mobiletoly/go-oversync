// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPushPending_RejectsInvalidCommitResponse(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		return jsonResponse(map[string]any{
			"bundle_seq":        1,
			"source_id":         session.SourceID,
			"source_bundle_id":  session.SourceBundleID,
			"row_count":         len(session.Rows),
			"planned_row_count": len(session.Rows),
			"byte_count":        3,
		})
	}
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	// Any forbidden legacy member may be found first when validating the object.
	require.Regexp(t, `unknown member "(bundle_seq|row_count|planned_row_count|byte_count)"`, err.Error())
	require.Empty(t, server.ackRequests)

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'users'`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows WHERE table_name = 'users'`).Scan(&outboundCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 1, outboundCount)

	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1) AND ? IS NOT NULL`, client.UserID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(1), nextSourceBundleID)
}
func TestPushPending_ReplaysDirectRequestAfterCrashBeforeDurableApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}

	snapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 1)

	committed, err := client.commitPushOutboundSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, int64(1), committed.BundleSeq)
	require.NoError(t, client.Close())

	restarted, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	restarted.HTTP = &http.Client{Transport: server}
	mustOpen(t, restarted, ctx)
	result, err := restarted.Attach(ctx, "bundle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	mustPushPending(t, restarted, ctx)
	require.Len(t, server.pushRequests, 1, "durable C settles locally after restart")
	require.Empty(t, server.resolveRequests)
	require.Len(t, server.ackRequests, 1)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)

	var rowVersion int64
	require.NoError(t, db.QueryRow(`
		SELECT row_version
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, rowKeyJSON(userID)).Scan(&rowVersion))
	require.Equal(t, int64(1), rowVersion)

	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1) AND ? IS NOT NULL`, restarted.UserID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(2), nextSourceBundleID)
}
func TestPushPending_AuthoritativeReplayDoesNotGenerateDirtyRowsAndCapturesLaterWrites(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	snapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 1)

	committedRows := []oversync.BundleRow{{
		Schema:     "main",
		Table:      "users",
		Key:        mustBundleKey(userID),
		Op:         oversync.OpInsert,
		RowVersion: 1,
		Payload:    mustJSONPayload(t, map[string]any{"id": userID, "name": "Ada Server", "email": "ada.server@example.com"}),
	}}
	committed := boundCommittedPushForReplayTest(t, client, db, snapshot, 1, committedRows)
	stageCommittedPushBundleRows(t, db, committed.BundleSeq, committedRows)

	require.NoError(t, client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed))
	require.Equal(t, 0, readApplyMode(t, db, client.UserID))

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)

	_, err = db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Ada Local", "ada.local@example.com", userID)
	require.NoError(t, err)

	var dirtyOp string
	require.NoError(t, db.QueryRow(`
		SELECT op
		FROM _sync_dirty_rows
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, rowKeyJSON(userID)).Scan(&dirtyOp))
	require.Equal(t, oversync.OpUpdate, dirtyOp)
}

func TestPushPending_AuthoritativeReplayRollbackDoesNotLeakApplyMode(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
            CHECK(name != 'Broken')
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	snapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 1)

	committedRows := []oversync.BundleRow{{
		Schema:     "main",
		Table:      "users",
		Key:        mustBundleKey(userID),
		Op:         oversync.OpInsert,
		RowVersion: 1,
		Payload:    []byte(`{"id":"` + userID + `","name":"Broken","email":"valid@example.invalid"}`),
	}}
	committed := boundCommittedPushForReplayTest(t, client, db, snapshot, 1, committedRows)
	stageCommittedPushBundleRows(t, db, committed.BundleSeq, committedRows)

	err = client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CHECK constraint failed")
	require.Positive(t, countA3(t, db, `SELECT length(outcome_body) FROM _sync_upload_journal WHERE phase='frozen'`))
	require.Equal(t, 0, readApplyMode(t, db, client.UserID))

	var dirtyCount, outboundCount, stagedCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 1, outboundCount)
	require.Equal(t, 1, stagedCount)
}

func TestPushPending_AuthoritativeReplayRunsWithDeferredForeignKeys(t *testing.T) {
	ctx := context.Background()

	t.Run("self referential child before parent", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}}, `
			CREATE TABLE categories (
				id TEXT PRIMARY KEY NOT NULL,
				name TEXT NOT NULL,
				parent_id TEXT,
				FOREIGN KEY (parent_id) REFERENCES categories(id) DEFERRABLE INITIALLY IMMEDIATE
			)
		`)

		_, err := db.Exec(`INSERT INTO categories (id, name, parent_id) VALUES ('parent', 'Parent', NULL)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO categories (id, name, parent_id) VALUES ('child', 'Child', 'parent')`)
		require.NoError(t, err)

		snapshot, err := client.ensurePushOutboundSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, snapshot.Rows, 2)

		committedRows := []oversync.BundleRow{
			{
				Schema:     "main",
				Table:      "categories",
				Key:        mustBundleKey("child"),
				Op:         oversync.OpInsert,
				RowVersion: 1,
				Payload:    mustJSONPayload(t, map[string]any{"id": "child", "name": "Child", "parent_id": "parent"}),
			},
			{
				Schema:     "main",
				Table:      "categories",
				Key:        mustBundleKey("parent"),
				Op:         oversync.OpInsert,
				RowVersion: 1,
				Payload:    mustJSONPayload(t, map[string]any{"id": "parent", "name": "Parent", "parent_id": nil}),
			},
		}
		committed := boundCommittedPushForReplayTest(t, client, db, snapshot, 1, committedRows)
		stageCommittedPushBundleRows(t, db, committed.BundleSeq, committedRows)

		require.NoError(t, client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed))

		var parentID string
		require.NoError(t, db.QueryRow(`SELECT parent_id FROM categories WHERE id = 'child'`).Scan(&parentID))
		require.Equal(t, "parent", parentID)
	})

	t.Run("cyclic foreign keys", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{
			{TableName: "teams", SyncKeyColumnName: "id"},
			{TableName: "team_members", SyncKeyColumnName: "id"},
		}, `
			CREATE TABLE teams (
				id TEXT PRIMARY KEY NOT NULL,
				name TEXT NOT NULL,
				captain_member_id TEXT,
				FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY IMMEDIATE
			)
		`, `
			CREATE TABLE team_members (
				id TEXT PRIMARY KEY NOT NULL,
				name TEXT NOT NULL,
				team_id TEXT NOT NULL,
				FOREIGN KEY (team_id) REFERENCES teams(id) DEFERRABLE INITIALLY IMMEDIATE
			)
		`)

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`PRAGMA defer_foreign_keys = ON`)
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO teams (id, name, captain_member_id) VALUES ('team-1', 'Core', 'member-1')`)
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO team_members (id, name, team_id) VALUES ('member-1', 'Ada', 'team-1')`)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		snapshot, err := client.ensurePushOutboundSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, snapshot.Rows, 2)

		committedRows := []oversync.BundleRow{
			{
				Schema:     "main",
				Table:      "teams",
				Key:        mustBundleKey("team-1"),
				Op:         oversync.OpInsert,
				RowVersion: 1,
				Payload:    mustJSONPayload(t, map[string]any{"id": "team-1", "name": "Core", "captain_member_id": "member-1"}),
			},
			{
				Schema:     "main",
				Table:      "team_members",
				Key:        mustBundleKey("member-1"),
				Op:         oversync.OpInsert,
				RowVersion: 1,
				Payload:    mustJSONPayload(t, map[string]any{"id": "member-1", "name": "Ada", "team_id": "team-1"}),
			},
		}
		committed := boundCommittedPushForReplayTest(t, client, db, snapshot, 1, committedRows)
		stageCommittedPushBundleRows(t, db, committed.BundleSeq, committedRows)

		require.NoError(t, client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed))

		var captainID, teamID string
		require.NoError(t, db.QueryRow(`SELECT captain_member_id FROM teams WHERE id = 'team-1'`).Scan(&captainID))
		require.NoError(t, db.QueryRow(`SELECT team_id FROM team_members WHERE id = 'member-1'`).Scan(&teamID))
		require.Equal(t, "member-1", captainID)
		require.Equal(t, "team-1", teamID)
	})
}

// Low-level apply witnesses carry the same complete validated C as the public
// path. Staging rows alone is deliberately insufficient for disposition.
func boundCommittedPushForReplayTest(t *testing.T, c *Client, db *sql.DB, snapshot *pushOutboundSnapshot, seq int64, rows []oversync.BundleRow) *committedPushBundle {
	t.Helper()
	ctx := context.Background()
	committed := committedPushForTest(t, c, db, seq, rows)
	journal, err := loadUploadJournal(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, journal)
	groups, err := oversync.CompactGroupsForBundleRows(c.discovery.SchemaGeneration, rows)
	require.NoError(t, err)
	op := journal.Binding.Operation
	outcome := oversync.PushOutcome{SourceID: op.SourceID, GenerationID: op.GenerationID, GenerationFingerprint: op.GenerationFingerprint, SourceBundleID: op.SourceBundleID, CanonicalRequestHash: op.CanonicalRequestHash, InitializationID: op.InitializationID, Status: "committed", Committed: &oversync.CommittedPushEffect{BundleSeq: seq, BundleHash: committed.BundleHash, RowCount: int64(len(rows)), Rows: groups}}
	outcome.OutcomeHash, err = oversync.HashPushOutcome(outcome, c.discovery.SchemaGeneration)
	require.NoError(t, err)
	raw, err := json.Marshal(outcome)
	require.NoError(t, err)
	validated, _, _, err := c.decodeJournalOutcome(ctx, raw, journal, snapshot.Rows)
	require.NoError(t, err)
	require.NoError(t, c.persistUploadOutcome(ctx, journal, raw, validated))
	return committed
}
