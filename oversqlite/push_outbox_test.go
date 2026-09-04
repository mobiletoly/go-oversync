// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestCollectDirtyRowsForPushInTx_LocalInsertBuildsInsertFromDirtyPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := "u-local-insert"
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Local", "local@example.com")
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	sourceBundleID, baseBundleSeq, dirtyRows, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Equal(t, int64(1), sourceBundleID)
	require.Equal(t, int64(0), baseBundleSeq)
	require.Len(t, dirtyRows, 1)
	require.Equal(t, oversync.OpInsert, dirtyRows[0].Op)
	require.Equal(t, rowKeyJSON(userID), dirtyRows[0].KeyJSON)
	require.True(t, dirtyRows[0].LocalPayload.Valid)
	require.Equal(t, "Local", decodeDirtyPayload(t, dirtyRows[0].LocalPayload.String)["name"])
}

func TestCollectDirtyRowsForPushInTx_SyncedUpdateBuildsUpdateFromDirtyPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := "u-synced-update"
	seedSyncedUserRow(t, client, db, userID, "Before", 41)
	_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "After", "after@example.com", userID)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, _, dirtyRows, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Len(t, dirtyRows, 1)
	require.Equal(t, oversync.OpUpdate, dirtyRows[0].Op)
	require.Equal(t, int64(41), dirtyRows[0].BaseRowVersion)
	require.True(t, dirtyRows[0].LocalPayload.Valid)
	require.Equal(t, "After", decodeDirtyPayload(t, dirtyRows[0].LocalPayload.String)["name"])
}

func TestCollectDirtyRowsForPushInTx_UnsyncedInsertDeleteBecomesNoOp(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := "u-noop"
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Temp", "temp@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, _, dirtyRows, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Empty(t, dirtyRows)
	var pending int64
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&pending))
	require.Zero(t, pending)
}

func TestPushPending_UnsyncedInsertDeleteCommitsNoOpPruning(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	userID := "u-noop-committed"
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Temp", "temp@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)

	report, err := client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, PushOutcomeNoChange, report.Outcome)
	var dirty int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Zero(t, dirty)

	report, err = client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, PushOutcomeNoChange, report.Outcome)
}

func TestPushPending_AdmittedHardLimitsFailBeforeFreezeOrSourceSequenceConsumption(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		limits func(*oversync.WireLimits)
		check  func(*testing.T, error)
	}{
		{
			name: "expanded row",
			limits: func(limits *oversync.WireLimits) {
				limits.MaxRowDecodedBytes = 32
			},
			check: func(t *testing.T, err error) {
				var limit *PushRowExceedsAdmittedLimitError
				require.ErrorAs(t, err, &limit)
			},
		},
		{
			name: "complete request",
			limits: func(limits *oversync.WireLimits) {
				limits.MaxPushDecodedBytes = 64
			},
			check: func(t *testing.T, err error) {
				var contract *InvalidAdmittedContractError
				require.ErrorAs(t, err, &contract)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)
			_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), strings.Repeat("N", 256), "limit@example.com")
			require.NoError(t, err)
			testCase.limits(&client.discovery.Limits)
			server := newMockPushServer(t)
			client.HTTP = &http.Client{Transport: server}

			_, err = client.PushPending(ctx)
			testCase.check(t, err)
			require.Empty(t, server.pushRequests)
			var dirtyRows, outboxRows int
			var nextSourceBundleID int64
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
			require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)`).Scan(&nextSourceBundleID))
			require.Equal(t, 1, dirtyRows)
			require.Zero(t, outboxRows)
			require.Equal(t, int64(1), nextSourceBundleID)
		})
	}
}

func newDirtyFKCycleClient(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "teams", SyncKeyColumnName: "id"},
		{TableName: "team_members", SyncKeyColumnName: "id"},
	}, `CREATE TABLE teams (
		id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, captain_member_id TEXT,
		FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY IMMEDIATE
	)`, `CREATE TABLE team_members (
		id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, team_id TEXT NOT NULL,
		FOREIGN KEY (team_id) REFERENCES teams(id) DEFERRABLE INITIALLY IMMEDIATE
	)`)
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`PRAGMA defer_foreign_keys = ON`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO teams (id, name, captain_member_id) VALUES ('team-1', 'Core', 'member-1')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO team_members (id, name, team_id) VALUES ('member-1', 'Ada', 'team-1')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return client, db
}

func TestPushPending_FKCycleCrossesSoftByteTargetAsWholeGroup(t *testing.T) {
	client, db := newDirtyFKCycleClient(t)
	client.config.UploadByteLimit = 1
	snapshot, err := client.ensurePushOutboundSnapshot(context.Background())
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 2)
	var outboxRows, dirtyRows int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
	require.Equal(t, 2, outboxRows)
	require.Zero(t, dirtyRows)
}

func TestPushPending_FKCycleAboveHardLimitDoesNotFreeze(t *testing.T) {
	client, db := newDirtyFKCycleClient(t)
	ctx := context.Background()
	tx, err := db.Begin()
	require.NoError(t, err)
	bundleID, _, rows, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	body, err := client.compactPushRequestBody(bundleID, client.pendingInitializationID, rows)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	client.discovery.Limits.MaxPushDecodedBytes = int64(len(body) - 1)
	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}
	_, err = client.PushPending(ctx)
	var unsendable *PushDependencyGroupExceedsAdmittedLimitError
	require.ErrorAs(t, err, &unsendable)
	require.EqualValues(t, 2, unsendable.ComponentRows)
	require.Empty(t, server.pushRequests)
	var dirtyRows, outboxRows int
	var nextBundleID int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = ?`, client.sourceID).Scan(&nextBundleID))
	require.Equal(t, 2, dirtyRows)
	require.Zero(t, outboxRows)
	require.Equal(t, int64(1), nextBundleID)
}

func TestCommitPushOutboundSnapshot_NewRowLimitPreservesFrozenIntent(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), strings.Repeat("N", 256), "frozen@example.com")
	require.NoError(t, err)
	snapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 1)
	client.discovery.Limits.MaxRowDecodedBytes = 32
	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}

	_, err = client.commitPushOutboundSnapshot(ctx, snapshot)
	var limit *PushRowExceedsAdmittedLimitError
	require.ErrorAs(t, err, &limit)
	require.Empty(t, server.pushRequests)
	var frozenRows, dirtyRows int
	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&frozenRows))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)`).Scan(&nextSourceBundleID))
	require.Equal(t, 1, frozenRows)
	require.Zero(t, dirtyRows)
	require.Equal(t, int64(1), nextSourceBundleID)
}

func TestCollectDirtyRowsForPushInTx_SyncedDeleteBuildsDeleteWithoutPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := "u-synced-delete"
	seedSyncedUserRow(t, client, db, userID, "Gone", 12)
	_, err := db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, _, dirtyRows, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Len(t, dirtyRows, 1)
	require.Equal(t, oversync.OpDelete, dirtyRows[0].Op)
	require.Equal(t, int64(12), dirtyRows[0].BaseRowVersion)
	require.False(t, dirtyRows[0].LocalPayload.Valid)
	require.Equal(t, rowKeyJSON(userID), dirtyRows[0].KeyJSON)
}

func TestPushPending_EmptyDirtyQueueIsNoOpBeforeOutboundStateIsCreated(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		return nil, fmt.Errorf("unexpected network request: %s %s", r.Method, r.URL.Path)
	})}

	mustPushPending(t, client, ctx)
	require.Equal(t, 0, requests)

	var dirtyCount, outboundCount, stageCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stageCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 0, outboundCount)
	require.Equal(t, 0, stageCount)

	_, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestPushPending_DirtySetLargerThanOneChunkSucceeds(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	for idx := 0; idx < 5; idx++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", idx),
			fmt.Sprintf("user%d@example.com", idx),
		)
		require.NoError(t, err)
	}

	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Len(t, server.pushRequests, 1)

	var dirtyCount, outboundCount, stageCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stageCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 0, outboundCount)
	require.Equal(t, 0, stageCount)

	_, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	require.Equal(t, int64(2), nextSourceBundleID)
	require.Equal(t, int64(1), lastBundleSeq)
}

func TestPushPending_RealColumnReplayDoesNotRequeueEquivalentValue(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			score REAL NOT NULL
		)
	`)

	server := newMockPushServer(t)
	server.pushHook = newFixedVersionPushHook(t, server, 2)
	client.HTTP = &http.Client{Transport: server}

	_, err := db.Exec(`INSERT INTO users (id, name, score) VALUES (?, ?, ?)`, "local-user", "Local Bob", 6.57111473696007)
	require.NoError(t, err)

	mustPushPending(t, client, ctx)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)

	var outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.Equal(t, 0, outboundCount)
}

func TestPushPending_OrdersUpsertsParentFirstAndDeletesChildFirst(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY NOT NULL,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
		)
	`)

	userID := uuid.NewString()
	postID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO posts (id, title, author_id) VALUES (?, ?, ?)`, postID, "Hello", userID)
	require.NoError(t, err)

	server := newMockPushServer(t)
	var committedRows [][]oversync.PushRequestRow
	server.pushHook = func(session *mockDirectPush) *http.Response {
		committedRows = append(committedRows, append([]oversync.PushRequestRow(nil), session.Rows...))
		return server.defaultPushResponse(session)
	}
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Len(t, committedRows, 1)
	require.Len(t, committedRows[0], 2)
	require.Equal(t, "users", committedRows[0][0].Table)
	require.Equal(t, "posts", committedRows[0][1].Table)

	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)
	mustPushPending(t, client, ctx)
	require.Len(t, committedRows, 2)
	require.Len(t, committedRows[1], 2)
	require.Equal(t, oversync.OpDelete, committedRows[1][0].Op)
	require.Equal(t, "posts", committedRows[1][0].Table)
	require.Equal(t, "users", committedRows[1][1].Table)
}

func TestPushPending_ConflictLeavesDirtyRowsIntact(t *testing.T) {
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
		return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
			Error:   "push_conflict",
			Message: "bundle conflict",
		})
	}
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "409")

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'users'`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows WHERE table_name = 'users'`).Scan(&outboundCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 1, outboundCount)

	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1) AND ? IS NOT NULL`, client.UserID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(1), nextSourceBundleID)
}

func TestPushPending_ResumesFrozenOutboundSnapshotAfterCrashBeforeCommit(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	for idx := 0; idx < 3; idx++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", idx),
			fmt.Sprintf("user%d@example.com", idx),
		)
		require.NoError(t, err)
	}

	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}
	client.SetBeforePushCommitHook(func(context.Context) error {
		return fmt.Errorf("simulated crash before push commit")
	})

	_, err := client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated crash before push commit")
	require.Empty(t, server.pushRequests)

	var outboundCount, stagedCount, dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 3, outboundCount)
	require.Equal(t, 0, stagedCount)
	require.Equal(t, 0, dirtyCount)
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
	require.Len(t, server.pushRequests, 1)

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, outboundCount)
	require.Equal(t, 0, stagedCount)
	require.Equal(t, 0, dirtyCount)

	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1) AND ? IS NOT NULL`, restarted.UserID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(2), nextSourceBundleID)
}

func TestPushPending_MovesFrozenRowsIntoOutboundAtomically(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	for idx := 0; idx < 2; idx++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", idx),
			fmt.Sprintf("user%d@example.com", idx),
		)
		require.NoError(t, err)
	}

	client.SetBeforePushFreezeHook(func(context.Context) error {
		return fmt.Errorf("simulated freeze rollback")
	})
	_, err := client.ensurePushOutboundSnapshot(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated freeze rollback")

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.Equal(t, 2, dirtyCount)
	require.Equal(t, 0, outboundCount)

	snapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 2)

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 2, outboundCount)
}

func TestPushPending_NewLocalWritesDuringUploadSurviveInDirtyRows(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalID := uuid.NewString()
	newID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, originalID, "Ada", "ada@example.com")
	require.NoError(t, err)

	client.SetBeforePushCommitHook(func(context.Context) error {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, newID, "Grace", "grace@example.com")
		require.NoError(t, err)
		return fmt.Errorf("simulated crash before commit")
	})
	client.HTTP = &http.Client{Transport: newMockPushServer(t)}

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated crash before commit")

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, 1, outboundCount)

	var keyJSON string
	var op string
	require.NoError(t, db.QueryRow(`SELECT key_json, op FROM _sync_dirty_rows`).Scan(&keyJSON, &op))
	require.Equal(t, rowKeyJSON(newID), keyJSON)
	require.Equal(t, oversync.OpInsert, op)
}

func TestPushPending_KeepsOnlyOneFrozenOutboxBundleDurably(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	for idx := 0; idx < 2; idx++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", idx),
			fmt.Sprintf("user%d@example.com", idx),
		)
		require.NoError(t, err)
	}

	firstSnapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, firstSnapshot.Rows, 2)

	secondSnapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, secondSnapshot.Rows, 2)
	require.Equal(t, firstSnapshot.SourceBundleID, secondSnapshot.SourceBundleID)

	var (
		bundleRows        int
		singletonRows     int
		distinctBundleIDs int
	)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&bundleRows))
	require.Equal(t, 2, bundleRows)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_bundle WHERE state = 'prepared'`).Scan(&singletonRows))
	require.Equal(t, 1, singletonRows)
	require.NoError(t, db.QueryRow(`SELECT COUNT(DISTINCT source_bundle_id) FROM _sync_outbox_rows`).Scan(&distinctBundleIDs))
	require.Equal(t, 1, distinctBundleIDs)
}

func TestPushPending_NewLocalWritesDuringReplaySurviveInDirtyRows(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalID := uuid.NewString()
	newID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, originalID, "Ada", "ada@example.com")
	require.NoError(t, err)

	client.SetBeforePushReplayHook(func(context.Context) error {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, newID, "Grace", "grace@example.com")
		require.NoError(t, err)
		return nil
	})
	client.HTTP = &http.Client{Transport: newMockPushServer(t)}

	mustPushPending(t, client, ctx)

	var dirtyCount, outboundCount, stagedCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, 0, outboundCount)
	require.Equal(t, 0, stagedCount)

	var keyJSON string
	require.NoError(t, db.QueryRow(`SELECT key_json FROM _sync_dirty_rows`).Scan(&keyJSON))
	require.Equal(t, rowKeyJSON(newID), keyJSON)
}

func TestDirtyRowCapture_CapturesLocalCascadeDeletes(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY NOT NULL,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
		)
	`)

	userID := uuid.NewString()
	postID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO posts (id, title, author_id) VALUES (?, ?, ?)`, postID, "Hello", userID)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, client.seedFixtureRowStateInTx(context.Background(), tx, "main", "users", rowKeyJSON(userID), 1, false))
	require.NoError(t, client.seedFixtureRowStateInTx(context.Background(), tx, "main", "posts", rowKeyJSON(postID), 2, false))
	require.NoError(t, tx.Commit())

	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE op = 'DELETE'`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestDirtyRowCapture_CapturesLocalTriggerGeneratedWrites(t *testing.T) {
	_, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "audit_logs", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE audit_logs (
			id TEXT PRIMARY KEY NOT NULL,
			user_id TEXT NOT NULL,
			action TEXT NOT NULL
		)
	`, `
		CREATE TRIGGER users_audit_after_insert
		AFTER INSERT ON users
		BEGIN
			INSERT INTO audit_logs (id, user_id, action)
			VALUES (NEW.id || '-audit', NEW.id, 'inserted');
		END
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 2, dirtyCount)
}

func TestDirtyRowCapture_KeyChangingUpdateCreatesDeleteAndUpsert(t *testing.T) {
	_, db := newBundleClient(t, "main", []SyncTable{{TableName: "widgets", SyncKeyColumnName: "id"}}, `
		CREATE TABLE widgets (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO widgets (id, name) VALUES ('old-id', 'Widget')`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE widgets SET id = 'new-id' WHERE id = 'old-id'`)
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'widgets'`).Scan(&count))
	require.Equal(t, 2, count)

	type dirtyKey struct {
		KeyJSON string
		Op      string
	}
	rows, err := db.Query(`SELECT key_json, op FROM _sync_dirty_rows WHERE table_name = 'widgets' ORDER BY key_json`)
	require.NoError(t, err)
	defer rows.Close()

	var got []dirtyKey
	for rows.Next() {
		var keyJSON string
		var op string
		require.NoError(t, rows.Scan(&keyJSON, &op))
		got = append(got, dirtyKey{KeyJSON: keyJSON, Op: op})
	}
	require.NoError(t, rows.Err())
	require.ElementsMatch(t, []dirtyKey{
		{KeyJSON: rowKeyJSON("old-id"), Op: oversync.OpDelete},
		{KeyJSON: rowKeyJSON("new-id"), Op: oversync.OpInsert},
	}, got)
}

func mustPushPayloadName(t *testing.T, payload []byte) string {
	t.Helper()
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(payload, &decoded))
	name, _ := decoded["name"].(string)
	return name
}

func mustBeginTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	return tx
}

// Tests that inspect prepared captures use the production bounded-step owner.
// Freeze consumes that prepared descriptor directly; no second production collector remains.
func (c *Client) collectDirtyRowsForPushInTx(ctx context.Context, tx sqliteTransaction) (int64, int64, []dirtyRowCapture, error) {
	for {
		p, ready, err := c.advancePushPlanInTx(ctx, tx)
		if err != nil {
			return 0, 0, nil, err
		}
		if !ready || (p != nil && p.Phase == "clear") {
			continue
		}
		if p == nil {
			return 0, 0, nil, nil
		}
		if p.CandidateRows == 0 && p.BlockedComponent != 0 {
			return 0, 0, nil, c.pushPlanBlockedErrorInTx(ctx, tx, p)
		}
		rows, err := c.selectedPushCaptures(ctx, tx, p)
		return p.SourceBundleID, 0, rows, err
	}
}
