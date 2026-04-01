package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func newBundleClient(t *testing.T, schema string, tables []SyncTable, ddl ...string) (*Client, *sql.DB) {
	t.Helper()
	config := DefaultConfig(schema, tables)
	config.RetryPolicy = newTestRetryPolicy()
	return newBundleClientWithConfig(t, config, ddl...)
}

func newBundleClientWithConfig(t *testing.T, config *Config, ddl ...string) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	client, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	attachTestClient(t, client, "bundle-user", "bundle-device")
	return client, db
}

func decodeDirtyPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &payload))
	return payload
}

func rowKeyJSON(id string) string {
	raw, _ := json.Marshal(map[string]any{"id": id})
	return string(raw)
}

func mustJSONPayload(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return raw
}

func stageCommittedPushBundleRows(t *testing.T, db *sql.DB, bundleSeq int64, rows []oversync.BundleRow) {
	t.Helper()
	for idx, row := range rows {
		idValue, ok := row.Key["id"].(string)
		require.True(t, ok)

		var payload any
		if row.Op != oversync.OpDelete {
			payload = string(row.Payload)
		}
		_, err := db.Exec(`
			INSERT INTO _sync_push_stage (
				bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, bundleSeq, idx, row.Schema, row.Table, rowKeyJSON(idValue), row.Op, row.RowVersion, payload)
		require.NoError(t, err)
	}
}

func readApplyMode(t *testing.T, db *sql.DB, userID string) int {
	t.Helper()
	var applyMode int
	require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1 AND ? IS NOT NULL`, userID).Scan(&applyMode))
	return applyMode
}

func loadDirtyRowForKey(t *testing.T, db *sql.DB, keyJSON string) (string, int64, sql.NullString, bool) {
	t.Helper()

	var (
		op             string
		baseRowVersion int64
		payload        sql.NullString
	)
	err := db.QueryRow(`
		SELECT op, base_row_version, payload
		FROM _sync_dirty_rows
		WHERE table_name = 'users' AND key_json = ?
	`, keyJSON).Scan(&op, &baseRowVersion, &payload)
	if err == sql.ErrNoRows {
		return "", 0, sql.NullString{}, false
	}
	require.NoError(t, err)
	return op, baseRowVersion, payload, true
}

func loadUserForKey(t *testing.T, db *sql.DB, id string) (string, string, bool) {
	t.Helper()

	var name, email string
	err := db.QueryRow(`SELECT name, email FROM users WHERE id = ?`, id).Scan(&name, &email)
	if err == sql.ErrNoRows {
		return "", "", false
	}
	require.NoError(t, err)
	return name, email, true
}

func loadUserRowStateForKey(t *testing.T, db *sql.DB, keyJSON string) (int64, bool, bool) {
	t.Helper()

	var (
		rowVersion int64
		deletedInt int
	)
	err := db.QueryRow(`
		SELECT row_version, deleted
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, keyJSON).Scan(&rowVersion, &deletedInt)
	if err == sql.ErrNoRows {
		return 0, false, false
	}
	require.NoError(t, err)
	return rowVersion, deletedInt == 1, true
}

func seedSyncedUserRow(t *testing.T, client *Client, db *sql.DB, id, name string, rowVersion int64) {
	t.Helper()

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, name, strings.ToLower(name)+"@example.com")
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, client.updateStructuredRowStateInTx(context.Background(), tx, "main", "users", rowKeyJSON(id), rowVersion, false))
	require.NoError(t, tx.Commit())

	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
}

func newFixedVersionCommitHook(t *testing.T, server *mockPushSessionServer, rowVersion int64) func(string, *mockPushSession) *http.Response {
	t.Helper()

	return func(pushID string, session *mockPushSession) *http.Response {
		rows := make([]oversync.BundleRow, 0, len(session.Rows))
		for _, row := range session.Rows {
			rows = append(rows, oversync.BundleRow{
				Schema:     row.Schema,
				Table:      row.Table,
				Key:        row.Key,
				Op:         row.Op,
				RowVersion: rowVersion,
				Payload:    row.Payload,
			})
		}
		committed := &mockCommittedBundle{
			BundleSeq:      server.nextBundleSeq,
			SourceID:       session.SourceID,
			SourceBundleID: session.SourceBundleID,
			Rows:           rows,
			BundleHash:     mustBundleHash(t, rows),
		}
		server.nextBundleSeq++
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return jsonResponse(oversync.PushSessionCommitResponse{
			BundleSeq:      committed.BundleSeq,
			SourceID:       committed.SourceID,
			SourceBundleID: committed.SourceBundleID,
			RowCount:       int64(len(committed.Rows)),
			BundleHash:     committed.BundleHash,
		})
	}
}

type mockPushSession struct {
	PushID          string
	SourceID        string
	SourceBundleID  int64
	PlannedRowCount int64
	Rows            []oversync.PushRequestRow
}

type mockCommittedBundle struct {
	BundleSeq      int64
	SourceID       string
	SourceBundleID int64
	Rows           []oversync.BundleRow
	BundleHash     string
}

type mockPushSessionServer struct {
	t                 *testing.T
	nextPushID        int
	nextBundleSeq     int64
	sessions          map[string]*mockPushSession
	committedBySource map[int64]*mockCommittedBundle
	committedBySeq    map[int64]*mockCommittedBundle
	chunkRequests     []oversync.PushSessionChunkRequest
	deletePushIDs     []string
	commitHook        func(pushID string, session *mockPushSession) *http.Response
}

func newMockPushSessionServer(t *testing.T) *mockPushSessionServer {
	t.Helper()
	return &mockPushSessionServer{
		t:                 t,
		nextBundleSeq:     1,
		sessions:          make(map[string]*mockPushSession),
		committedBySource: make(map[int64]*mockCommittedBundle),
		committedBySeq:    make(map[int64]*mockCommittedBundle),
	}
}

func (s *mockPushSessionServer) RoundTrip(r *http.Request) (*http.Response, error) {
	s.t.Helper()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions":
		requireAuthenticatedSyncHeaders(s.t, r, "")
		var req oversync.PushSessionCreateRequest
		require.NoError(s.t, json.NewDecoder(r.Body).Decode(&req))
		if committed := s.committedBySource[req.SourceBundleID]; committed != nil {
			return jsonResponse(oversync.PushSessionCreateResponse{
				Status:         "already_committed",
				BundleSeq:      committed.BundleSeq,
				SourceID:       committed.SourceID,
				SourceBundleID: committed.SourceBundleID,
				RowCount:       int64(len(committed.Rows)),
				BundleHash:     committed.BundleHash,
			}), nil
		}
		s.nextPushID++
		pushID := "push-" + strconv.Itoa(s.nextPushID)
		s.sessions[pushID] = &mockPushSession{
			PushID:          pushID,
			SourceID:        r.Header.Get(oversync.SourceIDHeader),
			SourceBundleID:  req.SourceBundleID,
			PlannedRowCount: req.PlannedRowCount,
		}
		return jsonResponse(oversync.PushSessionCreateResponse{
			PushID:                 pushID,
			Status:                 "staging",
			PlannedRowCount:        req.PlannedRowCount,
			NextExpectedRowOrdinal: 0,
		}), nil

	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sync/push-sessions/") && strings.HasSuffix(r.URL.Path, "/chunks"):
		requireAuthenticatedSyncHeaders(s.t, r, "")
		pushID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sync/push-sessions/"), "/chunks")
		session := s.sessions[pushID]
		require.NotNil(s.t, session)

		var req oversync.PushSessionChunkRequest
		require.NoError(s.t, json.NewDecoder(r.Body).Decode(&req))
		s.chunkRequests = append(s.chunkRequests, req)
		session.Rows = append(session.Rows, req.Rows...)
		return jsonResponse(oversync.PushSessionChunkResponse{
			PushID:                 pushID,
			NextExpectedRowOrdinal: req.StartRowOrdinal + int64(len(req.Rows)),
		}), nil

	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sync/push-sessions/") && strings.HasSuffix(r.URL.Path, "/commit"):
		requireAuthenticatedSyncHeaders(s.t, r, "")
		pushID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sync/push-sessions/"), "/commit")
		session := s.sessions[pushID]
		require.NotNil(s.t, session)
		if s.commitHook != nil {
			return s.commitHook(pushID, session), nil
		}
		return s.defaultCommitResponse(session), nil

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sync/committed-bundles/") && strings.HasSuffix(r.URL.Path, "/rows"):
		requireAuthenticatedSyncHeaders(s.t, r, "")
		trimmed := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sync/committed-bundles/"), "/rows")
		bundleSeq, err := strconv.ParseInt(trimmed, 10, 64)
		require.NoError(s.t, err)
		committed := s.committedBySeq[bundleSeq]
		require.NotNil(s.t, committed)

		maxRows := 1000
		if raw := r.URL.Query().Get("max_rows"); raw != "" {
			maxRows, err = strconv.Atoi(raw)
			require.NoError(s.t, err)
		}

		start := 0
		logicalAfter := int64(-1)
		if raw := r.URL.Query().Get("after_row_ordinal"); raw != "" {
			logicalAfter, err = strconv.ParseInt(raw, 10, 64)
			require.NoError(s.t, err)
			start = int(logicalAfter + 1)
		}
		end := start + maxRows
		if end > len(committed.Rows) {
			end = len(committed.Rows)
		}
		chunkRows := append([]oversync.BundleRow(nil), committed.Rows[start:end]...)
		nextRowOrdinal := logicalAfter
		if len(chunkRows) > 0 {
			nextRowOrdinal = logicalAfter + int64(len(chunkRows))
		}
		return jsonResponse(oversync.CommittedBundleRowsResponse{
			BundleSeq:      committed.BundleSeq,
			SourceID:       committed.SourceID,
			SourceBundleID: committed.SourceBundleID,
			RowCount:       int64(len(committed.Rows)),
			BundleHash:     committed.BundleHash,
			Rows:           chunkRows,
			NextRowOrdinal: nextRowOrdinal,
			HasMore:        end < len(committed.Rows),
		}), nil

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sync/push-sessions/"):
		requireAuthenticatedSyncHeaders(s.t, r, "")
		pushID := strings.TrimPrefix(r.URL.Path, "/sync/push-sessions/")
		s.deletePushIDs = append(s.deletePushIDs, pushID)
		return jsonResponse(map[string]any{"status": "deleted"}), nil
	}

	return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
}

func (s *mockPushSessionServer) defaultCommitResponse(session *mockPushSession) *http.Response {
	rows := make([]oversync.BundleRow, 0, len(session.Rows))
	for idx, row := range session.Rows {
		rows = append(rows, oversync.BundleRow{
			Schema:     row.Schema,
			Table:      row.Table,
			Key:        row.Key,
			Op:         row.Op,
			RowVersion: int64(idx + 1),
			Payload:    row.Payload,
		})
	}
	committed := &mockCommittedBundle{
		BundleSeq:      s.nextBundleSeq,
		SourceID:       session.SourceID,
		SourceBundleID: session.SourceBundleID,
		Rows:           rows,
		BundleHash:     mustBundleHash(s.t, rows),
	}
	s.nextBundleSeq++
	s.committedBySource[session.SourceBundleID] = committed
	s.committedBySeq[committed.BundleSeq] = committed
	return jsonResponse(oversync.PushSessionCommitResponse{
		BundleSeq:      committed.BundleSeq,
		SourceID:       committed.SourceID,
		SourceBundleID: committed.SourceBundleID,
		RowCount:       int64(len(committed.Rows)),
		BundleHash:     committed.BundleHash,
	})
}

func mustBundleHash(t *testing.T, rows []oversync.BundleRow) string {
	t.Helper()

	logicalRows := make([]map[string]any, 0, len(rows))
	for idx, row := range rows {
		payloadValue := any(nil)
		if row.Op != oversync.OpDelete && len(row.Payload) > 0 {
			require.NoError(t, json.Unmarshal(row.Payload, &payloadValue))
		}
		logicalRows = append(logicalRows, map[string]any{
			"row_ordinal": idx,
			"schema":      row.Schema,
			"table":       row.Table,
			"key":         row.Key,
			"op":          row.Op,
			"row_version": row.RowVersion,
			"payload":     payloadValue,
		})
	}
	raw, err := json.Marshal(logicalRows)
	require.NoError(t, err)
	canonical, err := canonicalizeJSONBytes(raw)
	require.NoError(t, err)
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func TestPushPending_RepeatedLocalEditsCollapseIntoOneDirtyRowAndAdvanceBundleState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Ada Updated", "ada.updated@example.com", userID)
	require.NoError(t, err)

	var dirtyCount int
	var dirtyOp string
	var payload string
	require.NoError(t, db.QueryRow(`
		SELECT COUNT(*), COALESCE(MAX(op), ''), COALESCE(MAX(payload), '')
		FROM _sync_dirty_rows
		WHERE table_name = 'users'
	`).Scan(&dirtyCount, &dirtyOp, &payload))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, oversync.OpInsert, dirtyOp)
	require.Equal(t, "Ada Updated", decodeDirtyPayload(t, payload)["name"])

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Len(t, server.chunkRequests, 1)
	require.Len(t, server.chunkRequests[0].Rows, 1)
	require.Equal(t, oversync.OpInsert, server.chunkRequests[0].Rows[0].Op)
	require.Equal(t, "Ada Updated", mustPushPayloadName(t, server.chunkRequests[0].Rows[0].Payload))

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)

	var rowVersion int64
	var deleted int
	require.NoError(t, db.QueryRow(`
		SELECT row_version, deleted
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, rowKeyJSON(userID)).Scan(&rowVersion, &deleted))
	require.Equal(t, int64(1), rowVersion)
	require.Equal(t, 0, deleted)

	_, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	require.Equal(t, int64(2), nextSourceBundleID)
	require.Equal(t, int64(1), lastBundleSeq)
	require.Empty(t, server.deletePushIDs)
}

func TestPushPending_SuccessfulCommitDoesNotDeleteCommittedSession(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Empty(t, server.deletePushIDs)
}

func TestPushPending_RetriesTransientCreateSessionFailureAndSucceeds(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	base := newMockPushSessionServer(t)
	base.nextBundleSeq = 10
	var createRequests int
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" {
			createRequests++
			if createRequests == 1 {
				return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
					Error:   "temporarily_unavailable",
					Message: "retry me",
				}), nil
			}
		}
		return base.RoundTrip(r)
	})}

	mustPushPending(t, client, ctx)
	require.Equal(t, 2, createRequests)
}

func TestPushPending_DoesNotRetryUnauthorized(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	var createRequests int
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" {
			createRequests++
			return errorJSONResponse(http.StatusUnauthorized, oversync.ErrorResponse{
				Error:   "unauthorized",
				Message: "bad token",
			}), nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Equal(t, 1, createRequests)
}

func TestPushPending_CreateHistoryPrunedRequiresSourceRecoveryAndRotatePreservesOutbox(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	base := newMockPushSessionServer(t)
	createPruned := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" && createPruned:
			createPruned = false
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "history_pruned",
				Message: "source tuple is below retained history floor",
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "source-recovery-snapshot",
				SnapshotBundleSeq: 9,
				RowCount:          0,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/source-recovery-snapshot":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID:        "source-recovery-snapshot",
				SnapshotBundleSeq: 9,
				Rows:              nil,
				NextRowOrdinal:    0,
				HasMore:           false,
			}), nil
		case r.Method == http.MethodDelete && r.URL.Path == "/sync/snapshot-sessions/source-recovery-snapshot":
			return jsonResponse(map[string]any{"status": "deleted"}), nil
		default:
			return base.RoundTrip(r)
		}
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, "bundle-device", outbox.SourceID)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	require.Equal(t, int64(1), outbox.RowCount)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoveryHistoryPruned), requireOperationReason(t, db))

	_, err = client.PullToStable(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	_, err = client.Sync(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	rotateReport, err := client.Rebuild(ctx, RebuildRotateSource, "rotated-device")
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, rotateReport.Outcome)
	require.Equal(t, "rotated-device", rotateReport.RotatedSourceID)

	outbox = requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, "rotated-device", outbox.SourceID)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Equal(t, "", requireOperationReason(t, db))

	var localCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 1, localCount)

	report := mustPushPending(t, client, ctx)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	require.Len(t, base.chunkRequests, 1)

	sourceID, nextSourceBundleID, lastBundleSeqSeen := requireBundleState(t, db)
	require.Equal(t, "rotated-device", sourceID)
	require.Equal(t, int64(2), nextSourceBundleID)
	require.GreaterOrEqual(t, lastBundleSeqSeen, int64(9))

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&localCount))
	require.Equal(t, 0, localCount)
	name, _, exists := loadUserForKey(t, db, "user-1")
	require.True(t, exists)
	require.Equal(t, "Ada", name)

	var replacedBy string
	require.NoError(t, db.QueryRow(`SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = ?`, "bundle-device").Scan(&replacedBy))
	require.Equal(t, "rotated-device", replacedBy)
}

func TestPushPending_CreateSourceSequenceOutOfOrderPersistsSourceRecoveryAcrossRestart(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" {
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "source_sequence_out_of_order",
				Message: "expected source_bundle_id 2",
			}), nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoverySequenceOutOfOrder), requireOperationReason(t, db))

	require.NoError(t, client.Close())

	restarted, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	restarted.HTTP = client.HTTP
	mustOpen(t, restarted, ctx, "bundle-device")
	result, err := restarted.Attach(ctx, "bundle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, err = restarted.PushPending(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)

	_, err = restarted.PullToStable(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)

	_, err = restarted.Sync(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)

	_, err = restarted.Rebuild(ctx, RebuildKeepSource, "")
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)
}

func TestPushPending_CommitSourceSequenceChangedRequiresSourceRecovery(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	base := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sync/push-sessions/") && strings.HasSuffix(r.URL.Path, "/commit") {
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "source_sequence_changed",
				Message: "expected source_bundle_id 2 at commit time",
			}), nil
		}
		return base.RoundTrip(r)
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceChanged, recoveryErr.Code)

	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoverySequenceChanged), requireOperationReason(t, db))
}

func TestPushPending_CommittedRemoteReplayPrunedFallsBackToKeepSourceRebuild(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	base := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/sync/committed-bundles/1/rows":
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "history_pruned",
				Message: "bundle 1 is below retained floor 1",
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "committed-remote-rebuild",
				SnapshotBundleSeq: 5,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/committed-remote-rebuild":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID:        "committed-remote-rebuild",
				SnapshotBundleSeq: 5,
				Rows: []oversync.SnapshotRow{{
					Schema:     "main",
					Table:      "users",
					Key:        mustBundleKey("user-1"),
					RowVersion: 1,
					Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
				}},
				NextRowOrdinal: 1,
				HasMore:        false,
			}), nil
		case r.Method == http.MethodDelete && r.URL.Path == "/sync/snapshot-sessions/committed-remote-rebuild":
			return jsonResponse(map[string]any{"status": "deleted"}), nil
		default:
			return base.RoundTrip(r)
		}
	})}

	report := mustPushPending(t, client, ctx)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)

	sourceID, nextSourceBundleID, lastBundleSeqSeen := requireBundleState(t, db)
	require.Equal(t, "bundle-device", sourceID)
	require.Equal(t, int64(2), nextSourceBundleID)
	require.Equal(t, int64(5), lastBundleSeqSeen)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Equal(t, "", requireOperationReason(t, db))

	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStateNone, outbox.State)
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&count))
	require.Equal(t, 0, count)

	name, _, exists := loadUserForKey(t, db, "user-1")
	require.True(t, exists)
	require.Equal(t, "Ada", name)
}

func TestPushPending_RetryExhaustionReturnsTypedError(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	var createRequests int
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" {
			createRequests++
			return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
				Error:   "temporarily_unavailable",
				Message: "retry me",
			}), nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	_, err = client.PushPending(ctx)
	var retryErr *RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, "push_pending", retryErr.Operation)
	require.Equal(t, 3, retryErr.Attempts)
	require.Equal(t, 3, createRequests)
}

func TestPushPending_TextSyncKeyRoundTripsThroughPush(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "docs", SyncKeyColumnName: "doc_id"}}, `
		CREATE TABLE docs (
			doc_id TEXT PRIMARY KEY,
			body TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO docs (doc_id, body) VALUES (?, ?)`, "doc-1", "Hello text key")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Len(t, server.chunkRequests, 1)
	require.Len(t, server.chunkRequests[0].Rows, 1)
	require.Equal(t, oversync.SyncKey{"doc_id": "doc-1"}, server.chunkRequests[0].Rows[0].Key)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(server.chunkRequests[0].Rows[0].Payload, &payload))
	require.Equal(t, "doc-1", payload["doc_id"])
	require.Equal(t, "Hello text key", payload["body"])

	keyJSONBytes, err := json.Marshal(map[string]any{"doc_id": "doc-1"})
	require.NoError(t, err)
	var rowVersion int64
	var deleted int
	require.NoError(t, db.QueryRow(`
		SELECT row_version, deleted
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'docs' AND key_json = ?
	`, string(keyJSONBytes)).Scan(&rowVersion, &deleted))
	require.Equal(t, int64(1), rowVersion)
	require.Equal(t, 0, deleted)
}

func TestPushPending_RejectsNonUUIDBlobSyncKey(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id BLOB PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (x'0102', ?, ?)`, "Blob", "blob@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid blob UUID pk")
}

func TestCollectDirtyRowsForPushInTx_LocalInsertBuildsInsertFromDirtyPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
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

	sourceBundleID, baseBundleSeq, dirtyRows, noOps, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Equal(t, int64(1), sourceBundleID)
	require.Equal(t, int64(0), baseBundleSeq)
	require.Len(t, dirtyRows, 1)
	require.Empty(t, noOps)
	require.Equal(t, oversync.OpInsert, dirtyRows[0].Op)
	require.Equal(t, rowKeyJSON(userID), dirtyRows[0].KeyJSON)
	require.True(t, dirtyRows[0].LocalPayload.Valid)
	require.Equal(t, "Local", decodeDirtyPayload(t, dirtyRows[0].LocalPayload.String)["name"])
}

func TestCollectDirtyRowsForPushInTx_SyncedUpdateBuildsUpdateFromDirtyPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
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

	_, _, dirtyRows, noOps, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Len(t, dirtyRows, 1)
	require.Empty(t, noOps)
	require.Equal(t, oversync.OpUpdate, dirtyRows[0].Op)
	require.Equal(t, int64(41), dirtyRows[0].BaseRowVersion)
	require.True(t, dirtyRows[0].LocalPayload.Valid)
	require.Equal(t, "After", decodeDirtyPayload(t, dirtyRows[0].LocalPayload.String)["name"])
}

func TestCollectDirtyRowsForPushInTx_UnsyncedInsertDeleteBecomesNoOp(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
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

	_, _, dirtyRows, noOps, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Empty(t, dirtyRows)
	require.Equal(t, []string{"main\x00users\x00" + rowKeyJSON(userID)}, noOps)
}

func TestCollectDirtyRowsForPushInTx_SyncedDeleteBuildsDeleteWithoutPayload(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
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

	_, _, dirtyRows, noOps, err := client.collectDirtyRowsForPushInTx(ctx, tx)
	require.NoError(t, err)
	require.Len(t, dirtyRows, 1)
	require.Empty(t, noOps)
	require.Equal(t, oversync.OpDelete, dirtyRows[0].Op)
	require.Equal(t, int64(12), dirtyRows[0].BaseRowVersion)
	require.False(t, dirtyRows[0].LocalPayload.Valid)
	require.Equal(t, rowKeyJSON(userID), dirtyRows[0].KeyJSON)
}

func TestPushPending_EmptyDirtyQueueIsNoOpBeforeOutboundStateIsCreated(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
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
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	client.config.UploadLimit = 2

	for idx := 0; idx < 5; idx++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", idx),
			fmt.Sprintf("user%d@example.com", idx),
		)
		require.NoError(t, err)
	}

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Equal(t, int64(1), client.PushTransferDiagnostics().SessionsCreated)
	require.Equal(t, int64(3), client.PushTransferDiagnostics().ChunksUploaded)
	require.Len(t, server.chunkRequests, 3)
	require.Len(t, server.chunkRequests[0].Rows, 2)
	require.Len(t, server.chunkRequests[1].Rows, 2)
	require.Len(t, server.chunkRequests[2].Rows, 1)

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
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			score REAL NOT NULL
		)
	`)

	server := newMockPushSessionServer(t)
	server.commitHook = newFixedVersionCommitHook(t, server, 2)
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
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`)

	userID := uuid.NewString()
	postID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO posts (id, title, author_id) VALUES (?, ?, ?)`, postID, "Hello", userID)
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Len(t, server.chunkRequests, 1)
	require.Len(t, server.chunkRequests[0].Rows, 2)
	require.Equal(t, "users", server.chunkRequests[0].Rows[0].Table)
	require.Equal(t, "posts", server.chunkRequests[0].Rows[1].Table)

	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	require.NoError(t, err)
	mustPushPending(t, client, ctx)
	require.Len(t, server.chunkRequests, 2)
	require.Len(t, server.chunkRequests[1].Rows, 2)
	require.Equal(t, oversync.OpDelete, server.chunkRequests[1].Rows[0].Op)
	require.Equal(t, "posts", server.chunkRequests[1].Rows[0].Table)
	require.Equal(t, "users", server.chunkRequests[1].Rows[1].Table)
}

func TestPushPending_ConflictLeavesDirtyRowsIntact(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
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

func TestPushPending_RejectsInvalidCommitResponse(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
		return jsonResponse(map[string]any{
			"bundle_seq":       1,
			"source_id":        session.SourceID,
			"source_bundle_id": session.SourceBundleID,
			"row_count":        len(session.Rows),
		})
	}
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bundle_hash")

	var dirtyCount, outboundCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'users'`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows WHERE table_name = 'users'`).Scan(&outboundCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 1, outboundCount)

	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1) AND ? IS NOT NULL`, client.UserID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(1), nextSourceBundleID)
}

func TestPushPending_FailsClosedOnCommittedReplayMetadataMismatch(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name        string
		mutate      func(*oversync.PushSessionCommitResponse)
		errContains string
	}{
		{
			name: "row count mismatch",
			mutate: func(resp *oversync.PushSessionCommitResponse) {
				resp.RowCount++
			},
			errContains: "row_count",
		},
		{
			name: "bundle hash mismatch",
			mutate: func(resp *oversync.PushSessionCommitResponse) {
				resp.BundleHash = strings.Repeat("0", 64)
			},
			errContains: "bundle_hash",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			userID := uuid.NewString()
			_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
			require.NoError(t, err)

			server := newMockPushSessionServer(t)
			server.commitHook = func(pushID string, session *mockPushSession) *http.Response {
				rows := make([]oversync.BundleRow, 0, len(session.Rows))
				for idx, row := range session.Rows {
					rows = append(rows, oversync.BundleRow{
						Schema:     row.Schema,
						Table:      row.Table,
						Key:        row.Key,
						Op:         row.Op,
						RowVersion: int64(idx + 1),
						Payload:    row.Payload,
					})
				}
				committed := &mockCommittedBundle{
					BundleSeq:      server.nextBundleSeq,
					SourceID:       session.SourceID,
					SourceBundleID: session.SourceBundleID,
					Rows:           rows,
					BundleHash:     mustBundleHash(t, rows),
				}
				server.nextBundleSeq++
				server.committedBySource[session.SourceBundleID] = committed
				server.committedBySeq[committed.BundleSeq] = committed

				resp := &oversync.PushSessionCommitResponse{
					BundleSeq:      committed.BundleSeq,
					SourceID:       committed.SourceID,
					SourceBundleID: committed.SourceBundleID,
					RowCount:       int64(len(committed.Rows)),
					BundleHash:     committed.BundleHash,
				}
				tc.mutate(resp)
				return jsonResponse(resp)
			}
			client.HTTP = &http.Client{Transport: server}

			_, err = client.PushPending(ctx)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), tc.errContains)

			var dirtyCount, outboundCount, stagedCount int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
			require.Equal(t, 0, dirtyCount)
			require.Equal(t, 1, outboundCount)
			require.Equal(t, 0, stagedCount)

			_, nextSourceBundleID, lastBundleSeqSeen := requireBundleState(t, db)
			require.Equal(t, int64(1), nextSourceBundleID)
			require.Equal(t, int64(0), lastBundleSeqSeen)
		})
	}
}

func TestPushPending_ReplaysAcceptedBundleAfterCrashBeforeDurableApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	snapshot, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Rows, 1)

	committed, err := client.commitPushOutboundSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.NoError(t, client.fetchCommittedPushBundle(ctx, committed))
	require.NoError(t, client.Close())

	restarted, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
	require.NoError(t, err)
	restarted.HTTP = &http.Client{Transport: server}
	mustOpen(t, restarted, ctx, client.SourceID)
	result, err := restarted.Attach(ctx, "bundle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	mustPushPending(t, restarted, ctx)
	require.Equal(t, int64(0), restarted.PushTransferDiagnostics().SessionsCreated)
	require.Equal(t, int64(0), restarted.PushTransferDiagnostics().ChunksUploaded)
	require.Equal(t, int64(1), restarted.PushTransferDiagnostics().CommittedBundleChunksRead)
	require.Len(t, server.chunkRequests, 1)

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

func TestPushPending_ReuploadsFrozenOutboundSnapshotAfterCrashBeforeCommit(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	client.config.UploadLimit = 2

	for idx := 0; idx < 3; idx++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", idx),
			fmt.Sprintf("user%d@example.com", idx),
		)
		require.NoError(t, err)
	}

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}
	client.SetBeforePushCommitHook(func(context.Context) error {
		return fmt.Errorf("simulated crash before push commit")
	})

	_, err := client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated crash before push commit")
	require.Len(t, server.chunkRequests, 2)
	require.Equal(t, int64(1), client.PushTransferDiagnostics().SessionsCreated)
	require.Equal(t, int64(2), client.PushTransferDiagnostics().ChunksUploaded)
	require.Equal(t, int64(0), client.PushTransferDiagnostics().CommittedBundleChunksRead)

	var outboundCount, stagedCount, dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 3, outboundCount)
	require.Equal(t, 0, stagedCount)
	require.Equal(t, 0, dirtyCount)
	require.NoError(t, client.Close())

	restarted, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
	require.NoError(t, err)
	restarted.config.UploadLimit = 2
	restarted.HTTP = &http.Client{Transport: server}
	mustOpen(t, restarted, ctx, client.SourceID)
	result, err := restarted.Attach(ctx, "bundle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	mustPushPending(t, restarted, ctx)
	require.Len(t, server.chunkRequests, 4)
	require.Equal(t, int64(1), restarted.PushTransferDiagnostics().SessionsCreated)
	require.Equal(t, int64(2), restarted.PushTransferDiagnostics().ChunksUploaded)

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
			id TEXT PRIMARY KEY,
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
			id TEXT PRIMARY KEY,
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
	client.HTTP = &http.Client{Transport: newMockPushSessionServer(t)}

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
			id TEXT PRIMARY KEY,
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
			id TEXT PRIMARY KEY,
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
	client.HTTP = &http.Client{Transport: newMockPushSessionServer(t)}

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

func TestPushPending_AuthoritativeReplayDoesNotGenerateDirtyRowsAndCapturesLaterWrites(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
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
	committed := &committedPushBundle{
		BundleSeq:      1,
		SourceID:       client.SourceID,
		SourceBundleID: snapshot.SourceBundleID,
		RowCount:       int64(len(committedRows)),
		BundleHash:     mustBundleHash(t, committedRows),
	}
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
			id TEXT PRIMARY KEY,
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
		Payload:    []byte(`{"id":"` + userID + `","name":"Broken"}`),
	}}
	committed := &committedPushBundle{
		BundleSeq:      1,
		SourceID:       client.SourceID,
		SourceBundleID: snapshot.SourceBundleID,
		RowCount:       int64(len(committedRows)),
		BundleHash:     mustBundleHash(t, committedRows),
	}
	stageCommittedPushBundleRows(t, db, committed.BundleSeq, committedRows)

	err = client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload for users")
	require.Equal(t, 0, readApplyMode(t, db, client.UserID))

	var dirtyCount, outboundCount, stagedCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 1, outboundCount)
	require.Equal(t, 1, stagedCount)
}

func TestCompareCommittedBundleToCanonicalOutbox_IgnoresEquivalentRowOrder(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	rowA := uuid.NewString()
	rowB := uuid.NewString()

	_, err := db.Exec(`
		UPDATE _sync_outbox_bundle
		SET state = 'prepared', source_id = ?, source_bundle_id = 5, row_count = 2, canonical_request_hash = 'hash'
		WHERE singleton_key = 1
	`, client.SourceID)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_outbox_rows (
			source_bundle_id, row_ordinal, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, wire_payload
		) VALUES
			(5, 0, 'main', 'users', ?, ?, 'DELETE', 3, NULL, NULL),
			(5, 1, 'main', 'users', ?, ?, 'DELETE', 4, NULL, NULL)
	`, rowKeyJSON(rowA), rowKeyJSON(rowA), rowKeyJSON(rowB), rowKeyJSON(rowB))
	require.NoError(t, err)

	committedRows := []oversync.BundleRow{
		{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey(rowB),
			Op:         oversync.OpDelete,
			RowVersion: 5,
		},
		{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey(rowA),
			Op:         oversync.OpDelete,
			RowVersion: 6,
		},
	}
	stageCommittedPushBundleRows(t, db, 11, committedRows)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = client.compareCommittedBundleToCanonicalOutbox(ctx, tx, &committedPushBundle{
		BundleSeq:        11,
		SourceID:         client.SourceID,
		SourceBundleID:   5,
		RowCount:         2,
		BundleHash:       mustBundleHash(t, committedRows),
		AlreadyCommitted: true,
	})
	require.NoError(t, err)
}

func TestPushPending_AuthoritativeReplayRunsWithDeferredForeignKeys(t *testing.T) {
	ctx := context.Background()

	t.Run("self referential child before parent", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}}, `
			CREATE TABLE categories (
				id TEXT PRIMARY KEY,
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
				RowVersion: 2,
				Payload:    mustJSONPayload(t, map[string]any{"id": "parent", "name": "Parent", "parent_id": nil}),
			},
		}
		committed := &committedPushBundle{
			BundleSeq:      1,
			SourceID:       client.SourceID,
			SourceBundleID: snapshot.SourceBundleID,
			RowCount:       int64(len(committedRows)),
			BundleHash:     mustBundleHash(t, committedRows),
		}
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
				id TEXT PRIMARY KEY,
				name TEXT NOT NULL,
				captain_member_id TEXT,
				FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY IMMEDIATE
			)
		`, `
			CREATE TABLE team_members (
				id TEXT PRIMARY KEY,
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
				RowVersion: 2,
				Payload:    mustJSONPayload(t, map[string]any{"id": "member-1", "name": "Ada", "team_id": "team-1"}),
			},
		}
		committed := &committedPushBundle{
			BundleSeq:      1,
			SourceID:       client.SourceID,
			SourceBundleID: snapshot.SourceBundleID,
			RowCount:       int64(len(committedRows)),
			BundleHash:     mustBundleHash(t, committedRows),
		}
		stageCommittedPushBundleRows(t, db, committed.BundleSeq, committedRows)

		require.NoError(t, client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed))

		var captainID, teamID string
		require.NoError(t, db.QueryRow(`SELECT captain_member_id FROM teams WHERE id = 'team-1'`).Scan(&captainID))
		require.NoError(t, db.QueryRow(`SELECT team_id FROM team_members WHERE id = 'member-1'`).Scan(&teamID))
		require.Equal(t, "member-1", captainID)
		require.Equal(t, "team-1", teamID)
	})
}

func TestClient_RejectsOverlappingSyncOperations(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushSessionServer(t)
	client.HTTP = &http.Client{Transport: server}

	entered := make(chan struct{})
	release := make(chan struct{})
	client.SetBeforePushCommitHook(func(context.Context) error {
		close(entered)
		<-release
		return nil
	})

	firstErr := make(chan error, 1)
	go func() {
		_, err := client.PushPending(ctx)
		firstErr <- err
	}()

	<-entered

	cases := []struct {
		name string
		op   func() error
	}{
		{
			name: "push rejects overlap",
			op: func() error {
				_, err := client.PushPending(ctx)
				return err
			},
		},
		{
			name: "pull rejects overlap",
			op: func() error {
				_, err := client.PullToStable(ctx)
				return err
			},
		},
		{
			name: "hydrate rejects overlap",
			op: func() error {
				_, err := client.Rebuild(ctx, RebuildKeepSource, "")
				return err
			},
		},
		{
			name: "recover rejects overlap",
			op: func() error {
				_, err := client.Rebuild(ctx, RebuildRotateSource, newTestSourceID())
				return err
			},
		},
		{
			name: "sync rejects overlap",
			op: func() error {
				_, err := client.Sync(ctx)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op()
			var inProgressErr *SyncOperationInProgressError
			require.ErrorAs(t, err, &inProgressErr)
		})
	}

	close(release)
	require.NoError(t, <-firstErr)
}

func TestPushPending_RebasesSameKeyLocalIntentDuringAuthoritativeReplay(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11

	cases := []struct {
		name              string
		seed              func(*testing.T, *Client, *sql.DB, string)
		firstMutation     func(*testing.T, *sql.DB, string)
		secondMutation    func(*testing.T, *sql.DB, string)
		expectedVisible   bool
		expectedName      string
		expectedDeleted   bool
		expectedDirtyOp   string
		expectedDirty     bool
		expectedDirtyName string
		expectedUploadOp  string
	}{
		{
			name: "update to later update",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Second",
			expectedDeleted:   false,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpUpdate,
			expectedDirtyName: "Second",
			expectedUploadOp:  oversync.OpUpdate,
		},
		{
			name: "update to later delete",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:  false,
			expectedDeleted:  false,
			expectedDirty:    true,
			expectedDirtyOp:  oversync.OpDelete,
			expectedUploadOp: oversync.OpUpdate,
		},
		{
			name: "delete to later recreate",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "Recreated", "recreated@example.com")
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Recreated",
			expectedDeleted:   true,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpInsert,
			expectedDirtyName: "Recreated",
			expectedUploadOp:  oversync.OpDelete,
		},
		{
			name: "insert to later update before replay",
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "First", "first@example.com")
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Second",
			expectedDeleted:   false,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpUpdate,
			expectedDirtyName: "Second",
			expectedUploadOp:  oversync.OpInsert,
		},
		{
			name: "insert to later delete before replay",
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "First", "first@example.com")
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:  false,
			expectedDeleted:  false,
			expectedDirty:    true,
			expectedDirtyOp:  oversync.OpDelete,
			expectedUploadOp: oversync.OpInsert,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			rowID := uuid.NewString()
			if tc.seed != nil {
				tc.seed(t, client, db, rowID)
			}
			tc.firstMutation(t, db, rowID)

			server := newMockPushSessionServer(t)
			server.commitHook = newFixedVersionCommitHook(t, server, committedRowVersion)
			client.HTTP = &http.Client{Transport: server}

			snapshot, err := client.ensurePushOutboundSnapshot(ctx)
			require.NoError(t, err)
			require.Len(t, snapshot.Rows, 1)
			require.Equal(t, tc.expectedUploadOp, snapshot.Rows[0].Op)

			tc.secondMutation(t, db, rowID)

			committed, err := client.commitPushOutboundSnapshot(ctx, snapshot)
			require.NoError(t, err)
			require.NoError(t, client.fetchCommittedPushBundle(ctx, committed))
			require.NoError(t, client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed))

			name, _, exists := loadUserForKey(t, db, rowID)
			require.Equal(t, tc.expectedVisible, exists)
			if tc.expectedVisible {
				require.Equal(t, tc.expectedName, name)
			}

			rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
			require.True(t, metaExists)
			require.Equal(t, committedRowVersion, rowVersion)
			require.Equal(t, tc.expectedDeleted, deleted)

			op, baseRowVersion, payload, dirtyExists := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
			require.Equal(t, tc.expectedDirty, dirtyExists)
			if tc.expectedDirty {
				require.Equal(t, tc.expectedDirtyOp, op)
				require.Equal(t, committedRowVersion, baseRowVersion)
				if tc.expectedDirtyOp == oversync.OpDelete {
					require.False(t, payload.Valid)
				} else {
					require.True(t, payload.Valid)
					payloadObj := decodeDirtyPayload(t, payload.String)
					require.Equal(t, tc.expectedDirtyName, payloadObj["name"])
				}
			}
		})
	}
}

func TestPushPending_RebasesSameKeyLocalIntentMutatedDuringUpload(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11

	cases := []struct {
		name                 string
		seed                 func(*testing.T, *Client, *sql.DB, string)
		firstMutation        func(*testing.T, *sql.DB, string)
		duringUploadMutation func(*testing.T, *sql.DB, string)
		expectedVisible      bool
		expectedName         string
		expectedDeleted      bool
		expectedDirtyOp      string
		expectedDirty        bool
		expectedDirtyName    string
		expectedUploadOp     string
		expectedUploadName   string
	}{
		{
			name: "edit during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:    true,
			expectedName:       "Second",
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpUpdate,
			expectedDirtyName:  "Second",
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "delete during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:    false,
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpDelete,
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "reinsert during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "Recreated", "recreated@example.com")
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Recreated",
			expectedDeleted:   true,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpInsert,
			expectedDirtyName: "Recreated",
			expectedUploadOp:  oversync.OpDelete,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			rowID := uuid.NewString()
			if tc.seed != nil {
				tc.seed(t, client, db, rowID)
			}
			tc.firstMutation(t, db, rowID)

			client.SetBeforePushCommitHook(func(context.Context) error {
				tc.duringUploadMutation(t, db, rowID)
				return nil
			})

			server := newMockPushSessionServer(t)
			server.commitHook = newFixedVersionCommitHook(t, server, committedRowVersion)
			client.HTTP = &http.Client{Transport: server}

			mustPushPending(t, client, ctx)
			require.Len(t, server.chunkRequests, 1)
			require.Len(t, server.chunkRequests[0].Rows, 1)
			require.Equal(t, tc.expectedUploadOp, server.chunkRequests[0].Rows[0].Op)
			if tc.expectedUploadOp != oversync.OpDelete {
				require.Equal(t, tc.expectedUploadName, mustPushPayloadName(t, server.chunkRequests[0].Rows[0].Payload))
			}

			name, _, exists := loadUserForKey(t, db, rowID)
			require.Equal(t, tc.expectedVisible, exists)
			if tc.expectedVisible {
				require.Equal(t, tc.expectedName, name)
			}

			rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
			require.True(t, metaExists)
			require.Equal(t, committedRowVersion, rowVersion)
			require.Equal(t, tc.expectedDeleted, deleted)

			op, baseRowVersion, payload, dirtyExists := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
			require.Equal(t, tc.expectedDirty, dirtyExists)
			if tc.expectedDirty {
				require.Equal(t, tc.expectedDirtyOp, op)
				require.Equal(t, committedRowVersion, baseRowVersion)
				if tc.expectedDirtyOp == oversync.OpDelete {
					require.False(t, payload.Valid)
				} else {
					require.True(t, payload.Valid)
					payloadObj := decodeDirtyPayload(t, payload.String)
					require.Equal(t, tc.expectedDirtyName, payloadObj["name"])
				}
			}
		})
	}
}

func TestPushPending_RebasesSameKeyLocalIntentAfterRestartBeforeReplay(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11

	cases := []struct {
		name                 string
		seed                 func(*testing.T, *Client, *sql.DB, string)
		firstMutation        func(*testing.T, *sql.DB, string)
		duringUploadMutation func(*testing.T, *sql.DB, string)
		expectedVisible      bool
		expectedName         string
		expectedDeleted      bool
		expectedDirtyOp      string
		expectedDirty        bool
		expectedDirtyName    string
		expectedUploadOp     string
		expectedUploadName   string
	}{
		{
			name: "edit during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:    true,
			expectedName:       "Second",
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpUpdate,
			expectedDirtyName:  "Second",
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "delete during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:    false,
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpDelete,
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "reinsert during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "Recreated", "recreated@example.com")
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Recreated",
			expectedDeleted:   true,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpInsert,
			expectedDirtyName: "Recreated",
			expectedUploadOp:  oversync.OpDelete,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			rowID := uuid.NewString()
			if tc.seed != nil {
				tc.seed(t, client, db, rowID)
			}
			tc.firstMutation(t, db, rowID)

			client.SetBeforePushCommitHook(func(context.Context) error {
				tc.duringUploadMutation(t, db, rowID)
				return nil
			})
			client.SetBeforePushReplayHook(func(context.Context) error {
				return fmt.Errorf("simulated crash before replay")
			})

			server := newMockPushSessionServer(t)
			server.commitHook = newFixedVersionCommitHook(t, server, committedRowVersion)
			client.HTTP = &http.Client{Transport: server}

			_, err := client.PushPending(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "simulated crash before replay")
			require.Len(t, server.chunkRequests, 1)
			require.Len(t, server.chunkRequests[0].Rows, 1)
			require.Equal(t, tc.expectedUploadOp, server.chunkRequests[0].Rows[0].Op)
			if tc.expectedUploadOp != oversync.OpDelete {
				require.Equal(t, tc.expectedUploadName, mustPushPayloadName(t, server.chunkRequests[0].Rows[0].Payload))
			}

			require.NoError(t, client.Close())

			restarted, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
				return "token", nil
			}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, restarted.Close()) })
			restarted.HTTP = &http.Client{Transport: server}
			mustOpen(t, restarted, ctx, client.SourceID)
			result, err := restarted.Attach(ctx, "bundle-user")
			require.NoError(t, err)
			require.Equal(t, AttachStatusConnected, result.Status)
			mustPushPending(t, restarted, ctx)
			require.Equal(t, int64(0), restarted.PushTransferDiagnostics().SessionsCreated)
			require.Equal(t, int64(0), restarted.PushTransferDiagnostics().ChunksUploaded)
			require.Equal(t, int64(1), restarted.PushTransferDiagnostics().CommittedBundleChunksRead)
			require.Len(t, server.chunkRequests, 1)

			name, _, exists := loadUserForKey(t, db, rowID)
			require.Equal(t, tc.expectedVisible, exists)
			if tc.expectedVisible {
				require.Equal(t, tc.expectedName, name)
			}

			rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
			require.True(t, metaExists)
			require.Equal(t, committedRowVersion, rowVersion)
			require.Equal(t, tc.expectedDeleted, deleted)

			op, baseRowVersion, payload, dirtyExists := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
			require.Equal(t, tc.expectedDirty, dirtyExists)
			if tc.expectedDirty {
				require.Equal(t, tc.expectedDirtyOp, op)
				require.Equal(t, committedRowVersion, baseRowVersion)
				if tc.expectedDirtyOp == oversync.OpDelete {
					require.False(t, payload.Valid)
				} else {
					require.True(t, payload.Valid)
					payloadObj := decodeDirtyPayload(t, payload.String)
					require.Equal(t, tc.expectedDirtyName, payloadObj["name"])
				}
			}
		})
	}
}

func TestIsExpectedSyncContention(t *testing.T) {
	t.Parallel()

	require.False(t, IsExpectedSyncContention(nil))
	require.True(t, IsExpectedSyncContention(&SyncOperationInProgressError{}))
	require.True(t, IsExpectedSyncContention(fmt.Errorf("wrapped: %w", &SyncOperationInProgressError{})))
	require.False(t, IsExpectedSyncContention(&PendingPushReplayError{OutboundCount: 3}))
	require.False(t, IsExpectedSyncContention(context.Canceled))
}

func TestDirtyRowCapture_CapturesLocalCascadeDeletes(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
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
	require.NoError(t, client.updateStructuredRowStateInTx(context.Background(), tx, "main", "users", rowKeyJSON(userID), 1, false))
	require.NoError(t, client.updateStructuredRowStateInTx(context.Background(), tx, "main", "posts", rowKeyJSON(postID), 2, false))
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
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE audit_logs (
			id TEXT PRIMARY KEY,
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
			id TEXT PRIMARY KEY,
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

func mustProcessPayload(t *testing.T, client *Client, tableName, raw string) []byte {
	t.Helper()
	payload, err := client.processPayloadForUpload(tableName, raw)
	require.NoError(t, err)
	return payload
}

func mustBeginTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	return tx
}
