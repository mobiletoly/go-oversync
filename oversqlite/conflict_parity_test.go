package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type capturingResolver struct {
	result   MergeResult
	conflict ConflictContext
	called   bool
}

func (r *capturingResolver) Resolve(conflict ConflictContext) MergeResult {
	r.called = true
	r.conflict = conflict
	if r.result == nil {
		return AcceptServer{}
	}
	return r.result
}

type latestUpdatedAtTestResolver struct{}

func (r *latestUpdatedAtTestResolver) Resolve(conflict ConflictContext) MergeResult {
	if conflict.LocalOp != oversync.OpUpdate || len(conflict.LocalPayload) == 0 || len(conflict.ServerRowPayload) == 0 {
		return AcceptServer{}
	}

	localPayload, localUpdatedAt, err := parseUpdatedAtTestPayload(conflict.LocalPayload)
	if err != nil {
		return AcceptServer{}
	}
	serverPayload, serverUpdatedAt, err := parseUpdatedAtTestPayload(conflict.ServerRowPayload)
	if err != nil {
		return AcceptServer{}
	}

	chosen := serverPayload
	if localUpdatedAt.After(serverUpdatedAt) || localUpdatedAt.Equal(serverUpdatedAt) {
		chosen = localPayload
	}
	mergedPayload, err := json.Marshal(chosen)
	if err != nil {
		return AcceptServer{}
	}
	return KeepMerged{MergedPayload: mergedPayload}
}

func parseUpdatedAtTestPayload(raw json.RawMessage) (map[string]any, time.Time, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, time.Time{}, err
	}
	updatedAtRaw, ok := payload["updated_at"].(string)
	if !ok {
		return nil, time.Time{}, sql.ErrNoRows
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	if err != nil {
		return nil, time.Time{}, err
	}
	return payload, updatedAt, nil
}

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

func TestBuiltInResolvers_ReturnExpectedResults(t *testing.T) {
	require.IsType(t, AcceptServer{}, (&ServerWinsResolver{}).Resolve(ConflictContext{}))
	require.IsType(t, KeepLocal{}, (&ClientWinsResolver{}).Resolve(ConflictContext{}))
}

func TestPushPending_StructuredConflictBuildsConflictContextFromOutboundSnapshot(t *testing.T) {
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

	resolver := &capturingResolver{result: AcceptServer{}}
	client.Resolver = resolver

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
	require.True(t, resolver.called)
	require.Equal(t, "main", resolver.conflict.Schema)
	require.Equal(t, "users", resolver.conflict.Table)
	require.Equal(t, oversync.OpUpdate, resolver.conflict.LocalOp)
	require.Equal(t, int64(1), resolver.conflict.BaseRowVersion)
	require.Equal(t, int64(2), resolver.conflict.ServerRowVersion)
	require.False(t, resolver.conflict.ServerRowDeleted)
	require.JSONEq(t, `{"id":"`+userID+`","name":"Ada Local","email":"ada@example.com"}`, string(resolver.conflict.LocalPayload))
	require.JSONEq(t, `{"id":"`+userID+`","name":"Ada Server","email":"ada@example.com"}`, string(resolver.conflict.ServerRowPayload))
	require.Equal(t, userID, resolver.conflict.Key["id"])
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
		var nextSourceBundleID int64
		require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_client_state WHERE user_id = ?`, client.UserID).Scan(&nextSourceBundleID))
		require.Equal(t, int64(1), nextSourceBundleID)
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

func TestPushPending_StructuredConflictKeepMergedLatestUpdatedAtWins(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	createdAt := "2026-03-24T18:00:00Z"
	baselineUpdatedAt := "2026-03-24T18:00:00Z"
	_, err := db.Exec(`
		INSERT INTO users (id, name, email, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, userID, "Ada", "ada@example.com", createdAt, baselineUpdatedAt)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, client.updateStructuredRowStateInTx(ctx, tx, "main", "users", rowKeyJSON(userID), 1, false))
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	localUpdatedAt := "2026-03-24T18:02:00Z"
	serverUpdatedAt := "2026-03-24T18:01:00Z"
	_, err = db.Exec(`
		UPDATE users
		SET name = ?, email = ?, updated_at = ?
		WHERE id = ?
	`, "Ada Local Newer", "local-newer@example.com", localUpdatedAt, userID)
	require.NoError(t, err)
	client.Resolver = &latestUpdatedAtTestResolver{}

	server := newMockPushSessionServer(t)
	successHook := newFixedVersionCommitHook(t, server, 3)
	attempt := 0
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		attempt++
		if attempt == 1 {
			return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
				"id":         userID,
				"name":       "Ada Server Older",
				"email":      "server-older@example.com",
				"created_at": createdAt,
				"updated_at": serverUpdatedAt,
			})
		}
		require.JSONEq(t, string(mustJSONPayload(t, map[string]any{
			"id":         userID,
			"name":       "Ada Local Newer",
			"email":      "local-newer@example.com",
			"created_at": createdAt,
			"updated_at": localUpdatedAt,
		})), string(session.Rows[0].Payload))
		require.Equal(t, int64(2), session.Rows[0].BaseRowVersion)
		return successHook(pushID, session)
	}
	client.HTTP = &http.Client{Transport: server}

	require.NoError(t, client.PushPending(ctx))

	var name, email, updatedAt string
	require.NoError(t, db.QueryRow(`
		SELECT name, email, updated_at
		FROM users
		WHERE id = ?
	`, userID).Scan(&name, &email, &updatedAt))
	require.Equal(t, "Ada Local Newer", name)
	require.Equal(t, "local-newer@example.com", email)
	require.Equal(t, localUpdatedAt, updatedAt)

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

func TestPushPending_StructuredConflictInvalidKeepLocalForDeletedAuthoritativeUpdateRestoresReplayableDirtyState(t *testing.T) {
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
	client.Resolver = &staticResolver{result: KeepLocal{}}

	server := newMockPushSessionServer(t)
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		return structuredConflictResponse(session.Rows[0], 2, false, nil)
	}
	client.HTTP = &http.Client{Transport: server}

	err = client.PushPending(ctx)
	require.Error(t, err)
	var invalidErr *InvalidConflictResolutionError
	require.ErrorAs(t, err, &invalidErr)

	op, baseRowVersion, payload, exists := loadDirtyRowForKey(t, db, rowKeyJSON(userID))
	require.True(t, exists)
	require.Equal(t, oversync.OpUpdate, op)
	require.Equal(t, int64(1), baseRowVersion)
	require.True(t, payload.Valid)

	nextSourceBundleID, _ := requireClientBundleState(t, db, client.UserID)
	require.Equal(t, int64(1), nextSourceBundleID)
}

func TestPushPending_StructuredConflictInvalidKeepMergedForDeletedAuthoritativeUpdateRestoresReplayableDirtyState(t *testing.T) {
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
	client.Resolver = &staticResolver{result: KeepMerged{MergedPayload: mustJSONPayload(t, map[string]any{
		"id":    userID,
		"name":  "Ada Merged",
		"email": "merged@example.com",
	})}}

	server := newMockPushSessionServer(t)
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		return structuredConflictResponse(session.Rows[0], 2, false, nil)
	}
	client.HTTP = &http.Client{Transport: server}

	err = client.PushPending(ctx)
	require.Error(t, err)
	var invalidErr *InvalidConflictResolutionError
	require.ErrorAs(t, err, &invalidErr)

	op, baseRowVersion, payload, exists := loadDirtyRowForKey(t, db, rowKeyJSON(userID))
	require.True(t, exists)
	require.Equal(t, oversync.OpUpdate, op)
	require.Equal(t, int64(1), baseRowVersion)
	require.True(t, payload.Valid)
}

func TestPushPending_StructuredConflictRejectsInvalidMergedPayloadShape(t *testing.T) {
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

	cases := []struct {
		name    string
		payload json.RawMessage
	}{
		{
			name:    "non-object",
			payload: json.RawMessage(`["not","an","object"]`),
		},
		{
			name:    "missing column",
			payload: mustJSONPayload(t, map[string]any{"id": userID, "name": "Ada Merged"}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &staticResolver{result: KeepMerged{MergedPayload: tc.payload}}
			client.Resolver = resolver

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
			require.Equal(t, oversync.OpUpdate, op)
			require.Equal(t, int64(1), baseRowVersion)
			require.True(t, payload.Valid)
		})
	}
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
		var nextSourceBundleID int64
		require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_client_state WHERE user_id = ?`, client.UserID).Scan(&nextSourceBundleID))
		require.Equal(t, int64(1), nextSourceBundleID)
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
