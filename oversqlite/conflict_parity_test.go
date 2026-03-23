package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func structuredConflictResponse(row oversync.PushRequestRow, serverRowVersion int64, serverRowDeleted bool, serverRow map[string]any) *http.Response {
	var serverRowPayload json.RawMessage
	if serverRow != nil {
		serverRowPayload, _ = json.Marshal(serverRow)
	}
	return errorJSONResponse(http.StatusConflict, oversync.PushConflictResponse{
		Error:   "push_conflict",
		Message: "bundle conflict",
		Conflict: &oversync.PushConflictDetails{
			Schema:           row.Schema,
			Table:            row.Table,
			Key:              row.Key,
			Op:               row.Op,
			BaseRowVersion:   row.BaseRowVersion,
			ServerRowVersion: serverRowVersion,
			ServerRowDeleted: serverRowDeleted,
			ServerRow:        serverRowPayload,
		},
	})
}

func requireClientBundleState(t *testing.T, db *sql.DB, userID string) (int64, int64) {
	t.Helper()
	var nextSourceBundleID, lastBundleSeqSeen int64
	require.NoError(t, db.QueryRow(`
		SELECT next_source_bundle_id, last_bundle_seq_seen
		FROM _sync_client_state
		WHERE user_id = ?
	`, userID).Scan(&nextSourceBundleID, &lastBundleSeqSeen))
	return nextSourceBundleID, lastBundleSeqSeen
}

func TestDecodePushConflictError_RequiresStructuredConflict(t *testing.T) {
	structured := decodePushConflictError(http.StatusConflict, mustJSONPayload(t, map[string]any{
		"error":   "push_conflict",
		"message": "bundle conflict",
		"conflict": map[string]any{
			"schema":             "main",
			"table":              "users",
			"key":                map[string]any{"id": "user-1"},
			"op":                 "UPDATE",
			"base_row_version":   1,
			"server_row_version": 2,
			"server_row_deleted": false,
			"server_row": map[string]any{
				"id":    "user-1",
				"name":  "Server",
				"email": "server@example.com",
			},
		},
	}))
	require.NotNil(t, structured)
	require.NotNil(t, structured.Conflict())

	unstructured := decodePushConflictError(http.StatusConflict, mustJSONPayload(t, map[string]any{
		"error":   "push_conflict",
		"message": "bundle conflict",
	}))
	require.Nil(t, unstructured)
}

func TestPushPending_StructuredConflictServerWinsResolverRecoversToServerState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	seedSyncedUserRow(t, client, db, userID, "Ada", 1)
	_, err := db.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Ada Local", userID)
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
			"id":    userID,
			"name":  "Ada Server",
			"email": "ada@example.com",
		})
	}
	client.HTTP = &http.Client{Transport: server}

	require.NoError(t, client.PushPending(ctx))

	name, email, exists := loadUserForKey(t, db, userID)
	require.True(t, exists)
	require.Equal(t, "Ada Server", name)
	require.Equal(t, "ada@example.com", email)

	rowVersion, deleted, rowStateExists := loadUserRowStateForKey(t, db, rowKeyJSON(userID))
	require.True(t, rowStateExists)
	require.False(t, deleted)
	require.Equal(t, int64(2), rowVersion)

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&outboundCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 0, outboundCount)

	nextSourceBundleID, lastBundleSeqSeen := requireClientBundleState(t, db, client.UserID)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(0), lastBundleSeqSeen)
}

func TestPushPending_StructuredConflictClientWinsResolverAutoRetriesAndWins(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	client.Resolver = &ClientWinsResolver{}

	userID := uuid.NewString()
	seedSyncedUserRow(t, client, db, userID, "Ada", 1)
	_, err := db.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Ada Local", userID)
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	successHook := newFixedVersionCommitHook(t, server, 3)
	var sourceBundleIDs []int64
	attempt := 0
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		attempt++
		sourceBundleIDs = append(sourceBundleIDs, session.SourceBundleID)
		if attempt == 1 {
			return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
				"id":    userID,
				"name":  "Ada Server",
				"email": "ada@example.com",
			})
		}
		require.Equal(t, int64(2), session.Rows[0].BaseRowVersion)
		return successHook(pushID, session)
	}
	client.HTTP = &http.Client{Transport: server}

	require.NoError(t, client.PushPending(ctx))

	name, _, exists := loadUserForKey(t, db, userID)
	require.True(t, exists)
	require.Equal(t, "Ada Local", name)
	require.Equal(t, []int64{1, 1}, sourceBundleIDs)

	rowVersion, deleted, rowStateExists := loadUserRowStateForKey(t, db, rowKeyJSON(userID))
	require.True(t, rowStateExists)
	require.False(t, deleted)
	require.Equal(t, int64(3), rowVersion)

	nextSourceBundleID, lastBundleSeqSeen := requireClientBundleState(t, db, client.UserID)
	require.Equal(t, int64(2), nextSourceBundleID)
	require.Equal(t, int64(1), lastBundleSeqSeen)
}

func TestPushPending_StructuredConflictKeepMergedRetriesMergedPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	seedSyncedUserRow(t, client, db, userID, "Ada", 1)
	_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Ada Local", "local@example.com", userID)
	require.NoError(t, err)
	client.Resolver = &staticResolver{result: KeepMerged{MergedPayload: mustJSONPayload(t, map[string]any{
		"id":    userID,
		"name":  "Ada Merged",
		"email": "merged@example.com",
	})}}

	server := newMockPushSessionServer(t)
	successHook := newFixedVersionCommitHook(t, server, 3)
	attempt := 0
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		attempt++
		if attempt == 1 {
			return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
				"id":    userID,
				"name":  "Ada Server",
				"email": "server@example.com",
			})
		}
		require.JSONEq(t, `{"id":"`+userID+`","name":"Ada Merged","email":"merged@example.com"}`, string(session.Rows[0].Payload))
		require.Equal(t, int64(2), session.Rows[0].BaseRowVersion)
		return successHook(pushID, session)
	}
	client.HTTP = &http.Client{Transport: server}

	require.NoError(t, client.PushPending(ctx))

	name, email, exists := loadUserForKey(t, db, userID)
	require.True(t, exists)
	require.Equal(t, "Ada Merged", name)
	require.Equal(t, "merged@example.com", email)

	rowVersion, deleted, rowStateExists := loadUserRowStateForKey(t, db, rowKeyJSON(userID))
	require.True(t, rowStateExists)
	require.False(t, deleted)
	require.Equal(t, int64(3), rowVersion)
}

func TestPushPending_StructuredConflictPreservesSiblingRowsFromRejectedBundle(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	client.Resolver = &ClientWinsResolver{}

	conflictUserID := uuid.NewString()
	seedSyncedUserRow(t, client, db, conflictUserID, "Grace", 1)

	siblingUserID := uuid.NewString()
	_, err := db.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Grace Local", conflictUserID)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, siblingUserID, "Sibling", "sibling@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	successHook := newFixedVersionCommitHook(t, server, 3)
	attempt := 0
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		attempt++
		if attempt == 1 {
			require.Len(t, session.Rows, 2)
			return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
				"id":    conflictUserID,
				"name":  "Grace Server",
				"email": "grace@example.com",
			})
		}
		require.Len(t, session.Rows, 2)
		require.Equal(t, int64(1), session.SourceBundleID)
		seenIDs := []string{
			session.Rows[0].Key["id"].(string),
			session.Rows[1].Key["id"].(string),
		}
		require.ElementsMatch(t, []string{conflictUserID, siblingUserID}, seenIDs)
		return successHook(pushID, session)
	}
	client.HTTP = &http.Client{Transport: server}

	require.NoError(t, client.PushPending(ctx))

	name, _, exists := loadUserForKey(t, db, conflictUserID)
	require.True(t, exists)
	require.Equal(t, "Grace Local", name)

	siblingName, siblingEmail, siblingExists := loadUserForKey(t, db, siblingUserID)
	require.True(t, siblingExists)
	require.Equal(t, "Sibling", siblingName)
	require.Equal(t, "sibling@example.com", siblingEmail)
}

func TestPushPending_StructuredConflictInvalidKeepMergedForDeleteRestoresReplayableDirtyState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	seedSyncedUserRow(t, client, db, userID, "Ada", 1)
	_, err := db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)
	client.Resolver = &staticResolver{result: KeepMerged{MergedPayload: mustJSONPayload(t, map[string]any{
		"id":    userID,
		"name":  "Ada Merged",
		"email": "merged@example.com",
	})}}

	server := newMockPushSessionServer(t)
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
			"id":    userID,
			"name":  "Ada Server",
			"email": "server@example.com",
		})
	}
	client.HTTP = &http.Client{Transport: server}

	err = client.PushPending(ctx)
	require.Error(t, err)
	var invalidErr *InvalidConflictResolutionError
	require.ErrorAs(t, err, &invalidErr)

	op, baseRowVersion, payload, exists := loadDirtyRowForKey(t, db, rowKeyJSON(userID))
	require.True(t, exists)
	require.Equal(t, oversync.OpDelete, op)
	require.Equal(t, int64(1), baseRowVersion)
	require.False(t, payload.Valid)

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&outboundCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, 0, outboundCount)

	nextSourceBundleID, lastBundleSeqSeen := requireClientBundleState(t, db, client.UserID)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(0), lastBundleSeqSeen)
}

func TestPushPending_StructuredConflictRetryExhaustionLeavesReplayableDirtyState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	client.Resolver = &ClientWinsResolver{}

	userID := uuid.NewString()
	seedSyncedUserRow(t, client, db, userID, "Ada", 1)
	_, err := db.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Ada Local", userID)
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	var sourceBundleIDs []int64
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		sourceBundleIDs = append(sourceBundleIDs, session.SourceBundleID)
		return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
			"id":    userID,
			"name":  "Ada Server",
			"email": "ada@example.com",
		})
	}
	client.HTTP = &http.Client{Transport: server}

	err = client.PushPending(ctx)
	require.Error(t, err)
	var exhaustedErr *PushConflictRetryExhaustedError
	require.ErrorAs(t, err, &exhaustedErr)
	require.Equal(t, 2, exhaustedErr.RetryCount)
	require.Equal(t, 1, exhaustedErr.RemainingDirtyCount)
	require.Equal(t, []int64{1, 1, 1}, sourceBundleIDs)

	op, baseRowVersion, payload, exists := loadDirtyRowForKey(t, db, rowKeyJSON(userID))
	require.True(t, exists)
	require.Equal(t, oversync.OpUpdate, op)
	require.Equal(t, int64(2), baseRowVersion)
	require.True(t, payload.Valid)

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&outboundCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, 0, outboundCount)

	nextSourceBundleID, lastBundleSeqSeen := requireClientBundleState(t, db, client.UserID)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(0), lastBundleSeqSeen)
}
