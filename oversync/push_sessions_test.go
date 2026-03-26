package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type pushSessionFixture struct {
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	writer     Actor
	reader     Actor
}

type pushSessionFixtureOptions struct {
	maxRowsPerPushChunk                int
	pushSessionTTL                     time.Duration
	defaultRowsPerCommittedBundleChunk int
	maxRowsPerCommittedBundleChunk     int
}

func newPushSessionFixture(t *testing.T, ctx context.Context, opts pushSessionFixtureOptions) *pushSessionFixture {
	t.Helper()

	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_sessions_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	userID := "push-sessions-user-" + suffix
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:          1,
		AppName:                            "push-sessions-test",
		MaxRowsPerPushChunk:                opts.maxRowsPerPushChunk,
		PushSessionTTL:                     opts.pushSessionTTL,
		DefaultRowsPerCommittedBundleChunk: opts.defaultRowsPerCommittedBundleChunk,
		MaxRowsPerCommittedBundleChunk:     opts.maxRowsPerCommittedBundleChunk,
		RegisteredTables:                   []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
	}, logger)

	return &pushSessionFixture{
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		writer:     Actor{UserID: userID, SourceID: "writer"},
		reader:     Actor{UserID: userID, SourceID: "reader"},
	}
}

func (f *pushSessionFixture) userRow(id uuid.UUID, name string) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"id":"%s","name":"%s","email":"%s@example.com"}`,
			id,
			name,
			strings.ToLower(name),
		)),
	}
}

func (f *pushSessionFixture) createSession(t *testing.T, ctx context.Context, sourceBundleID, plannedRowCount int64) *PushSessionCreateResponse {
	t.Helper()

	resp, err := f.svc.CreatePushSession(ctx, f.writer, &PushSessionCreateRequest{
		SourceID:        f.writer.SourceID,
		SourceBundleID:  sourceBundleID,
		PlannedRowCount: plannedRowCount,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func (f *pushSessionFixture) uploadChunk(t *testing.T, ctx context.Context, pushID string, startRowOrdinal int64, rows ...PushRequestRow) *PushSessionChunkResponse {
	t.Helper()

	resp, err := f.svc.UploadPushChunk(ctx, f.writer, pushID, &PushSessionChunkRequest{
		StartRowOrdinal: startRowOrdinal,
		Rows:            rows,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func (f *pushSessionFixture) commitSession(t *testing.T, ctx context.Context, pushID string) *PushSessionCommitResponse {
	t.Helper()

	resp, err := f.svc.CommitPushSession(ctx, f.writer, pushID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func (f *pushSessionFixture) businessUserCount(t *testing.T, ctx context.Context) int {
	t.Helper()

	var count int
	require.NoError(t, f.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users`, f.schemaName)).Scan(&count))
	return count
}

func (f *pushSessionFixture) pushSessionRowCount(t *testing.T, ctx context.Context, pushID string) int {
	t.Helper()

	var count int
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_session_rows
		WHERE push_id = $1::uuid
	`, pushID).Scan(&count))
	return count
}

func (f *pushSessionFixture) pushSessionExpiry(t *testing.T, ctx context.Context, pushID string) time.Time {
	t.Helper()

	var expiresAt time.Time
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT expires_at
		FROM sync.push_sessions
		WHERE push_id = $1::uuid
	`, pushID).Scan(&expiresAt))
	return expiresAt
}

func (f *pushSessionFixture) setPushSessionExpiry(t *testing.T, ctx context.Context, pushID string, expiresAt time.Time) {
	t.Helper()

	_, err := f.pool.Exec(ctx, `
		UPDATE sync.push_sessions
		SET expires_at = $2
		WHERE push_id = $1::uuid
	`, pushID, expiresAt)
	require.NoError(t, err)
}

func (f *pushSessionFixture) committedBundleStoredKeys(t *testing.T, ctx context.Context, bundleSeq int64) []string {
	t.Helper()

	rows, err := f.pool.Query(ctx, `
		SELECT key_json
		FROM sync.bundle_rows
		WHERE user_id = $1
		  AND bundle_seq = $2
		ORDER BY row_ordinal
	`, f.writer.UserID, bundleSeq)
	require.NoError(t, err)
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var keyJSON string
		require.NoError(t, rows.Scan(&keyJSON))
		var key SyncKey
		require.NoError(t, json.Unmarshal([]byte(keyJSON), &key))
		keys = append(keys, key["id"].(string))
	}
	require.NoError(t, rows.Err())
	return keys
}

func insertPreparedPushSessionRow(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	svc *SyncService,
	pushID string,
	rowOrdinal int64,
	row PushRequestRow,
) {
	t.Helper()

	preparedRows, err := svc.preparePushRowsPreservingInput([]PushRequestRow{row})
	require.NoError(t, err)
	require.Len(t, preparedRows, 1)

	prepared := preparedRows[0]
	var payload any
	if prepared.op != OpDelete {
		payload = string(prepared.payload)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO sync.push_session_rows (
			push_id, row_ordinal, schema_name, table_name, key_json, op, base_row_version, payload
		) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::jsonb)
	`, pushID, rowOrdinal, prepared.schema, prepared.table, prepared.keyJSON, prepared.op, prepared.baseRowVersion, payload)
	require.NoError(t, err)
}

func doJSONHandlerRequest(
	t *testing.T,
	handler func(http.ResponseWriter, *http.Request),
	method string,
	path string,
	actor Actor,
	body any,
	pathValues map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody []byte
	var err error
	if body != nil {
		reqBody, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))
	req = req.WithContext(ContextWithActor(req.Context(), actor))
	for key, value := range pathValues {
		req.SetPathValue(key, value)
	}

	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func decodeErrorResponse(t *testing.T, rec *httptest.ResponseRecorder) ErrorResponse {
	t.Helper()

	var resp ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return resp
}

func decodePushConflictResponse(t *testing.T, rec *httptest.ResponseRecorder) PushConflictResponse {
	t.Helper()

	var resp PushConflictResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return resp
}

func TestPushSessions_StagedRowsRemainInvisibleUntilCommit(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	rowID := uuid.New()
	session := fixture.createSession(t, ctx, 1, 1)
	fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(rowID, "Alpha"))

	require.Equal(t, 0, fixture.businessUserCount(t, ctx))

	pullBeforeCommit, err := fixture.svc.ProcessPull(ctx, fixture.reader, 0, 10, 0)
	require.NoError(t, err)
	require.Empty(t, pullBeforeCommit.Bundles)
	require.Equal(t, int64(0), pullBeforeCommit.StableBundleSeq)

	commit := fixture.commitSession(t, ctx, session.PushID)
	require.Equal(t, int64(1), commit.RowCount)
	require.Equal(t, 1, fixture.businessUserCount(t, ctx))

	pullAfterCommit, err := fixture.svc.ProcessPull(ctx, fixture.reader, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pullAfterCommit.Bundles, 1)
	require.Equal(t, commit.BundleSeq, pullAfterCommit.Bundles[0].BundleSeq)
}

func TestPushSessions_CommitConflictResponseIncludesStructuredRow(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	rowID := uuid.New()
	_, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{{
		Schema:         fixture.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Server","email":"server@example.com"}`, rowID)),
	}})
	require.NoError(t, err)

	session := fixture.createSession(t, ctx, 2, 1)
	fixture.uploadChunk(t, ctx, session.PushID, 0, PushRequestRow{
		Schema:         fixture.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpUpdate,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Client","email":"client@example.com"}`, rowID)),
	})

	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	rec := doJSONHandlerRequest(
		t,
		handlers.HandleCommitPushSession,
		http.MethodPost,
		"/sync/push-sessions/"+session.PushID+"/commit",
		fixture.writer,
		nil,
		map[string]string{"push_id": session.PushID},
	)

	require.Equal(t, http.StatusConflict, rec.Code)
	resp := decodePushConflictResponse(t, rec)
	require.Equal(t, "push_conflict", resp.Error)
	require.NotNil(t, resp.Conflict)
	require.Equal(t, fixture.schemaName, resp.Conflict.Schema)
	require.Equal(t, "users", resp.Conflict.Table)
	require.Equal(t, SyncKey{"id": rowID.String()}, resp.Conflict.Key)
	require.Equal(t, OpUpdate, resp.Conflict.Op)
	require.Equal(t, int64(0), resp.Conflict.BaseRowVersion)
	require.Equal(t, int64(1), resp.Conflict.ServerRowVersion)
	require.False(t, resp.Conflict.ServerRowDeleted)
	require.JSONEq(t, fmt.Sprintf(`{"id":"%s","name":"Server","email":"server@example.com"}`, rowID), string(resp.Conflict.ServerRow))
}

func TestPushSessions_DeletedConflictReportsNullServerRow(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	rowID := uuid.New()
	_, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{{
		Schema:         fixture.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Server","email":"server@example.com"}`, rowID)),
	}})
	require.NoError(t, err)
	_, err = pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 2, []PushRequestRow{{
		Schema:         fixture.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpDelete,
		BaseRowVersion: 1,
	}})
	require.NoError(t, err)

	session := fixture.createSession(t, ctx, 3, 1)
	fixture.uploadChunk(t, ctx, session.PushID, 0, PushRequestRow{
		Schema:         fixture.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpUpdate,
		BaseRowVersion: 1,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Client","email":"client@example.com"}`, rowID)),
	})

	_, err = fixture.svc.CommitPushSession(ctx, fixture.writer, session.PushID)
	var conflictErr *PushConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.NotNil(t, conflictErr.Conflict)
	require.Equal(t, fixture.schemaName, conflictErr.Conflict.Schema)
	require.Equal(t, "users", conflictErr.Conflict.Table)
	require.Equal(t, SyncKey{"id": rowID.String()}, conflictErr.Conflict.Key)
	require.Equal(t, OpUpdate, conflictErr.Conflict.Op)
	require.Equal(t, int64(1), conflictErr.Conflict.BaseRowVersion)
	require.Equal(t, int64(2), conflictErr.Conflict.ServerRowVersion)
	require.True(t, conflictErr.Conflict.ServerRowDeleted)
	require.Nil(t, conflictErr.Conflict.ServerRow)
}

func TestPushSessions_DuplicateCommitIsIdempotent(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	session := fixture.createSession(t, ctx, 1, 1)
	fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(uuid.New(), "Alpha"))

	firstCommit := fixture.commitSession(t, ctx, session.PushID)
	secondCommit := fixture.commitSession(t, ctx, session.PushID)

	require.Equal(t, firstCommit.BundleSeq, secondCommit.BundleSeq)
	require.Equal(t, firstCommit.RowCount, secondCommit.RowCount)
	require.Equal(t, firstCommit.BundleHash, secondCommit.BundleHash)

	var bundleCount int
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_id = $1
	`, fixture.writer.UserID).Scan(&bundleCount))
	require.Equal(t, 1, bundleCount)
}

func TestPushSessions_UploadAndCommittedBundlePagingContracts(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{
		maxRowsPerPushChunk:                2,
		defaultRowsPerCommittedBundleChunk: 1,
		maxRowsPerCommittedBundleChunk:     2,
	})

	rowIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	session := fixture.createSession(t, ctx, 1, 3)

	chunk1 := fixture.uploadChunk(t, ctx, session.PushID, 0,
		fixture.userRow(rowIDs[0], "Alpha"),
		fixture.userRow(rowIDs[1], "Bravo"),
	)
	require.Equal(t, int64(2), chunk1.NextExpectedRowOrdinal)

	chunk2 := fixture.uploadChunk(t, ctx, session.PushID, chunk1.NextExpectedRowOrdinal,
		fixture.userRow(rowIDs[2], "Charlie"),
	)
	require.Equal(t, int64(3), chunk2.NextExpectedRowOrdinal)

	commit := fixture.commitSession(t, ctx, session.PushID)
	storedKeys := fixture.committedBundleStoredKeys(t, ctx, commit.BundleSeq)
	require.Equal(t, []string{rowIDs[0].String(), rowIDs[1].String(), rowIDs[2].String()}, storedKeys)

	page1, err := fixture.svc.GetCommittedBundleRows(ctx, fixture.reader, commit.BundleSeq, nil, 1)
	require.NoError(t, err)
	require.Len(t, page1.Rows, 1)
	require.Equal(t, rowIDs[0].String(), page1.Rows[0].Key["id"])
	require.Equal(t, int64(0), page1.NextRowOrdinal)
	require.True(t, page1.HasMore)

	after := page1.NextRowOrdinal
	page2, err := fixture.svc.GetCommittedBundleRows(ctx, fixture.reader, commit.BundleSeq, &after, 1)
	require.NoError(t, err)
	require.Len(t, page2.Rows, 1)
	require.Equal(t, rowIDs[1].String(), page2.Rows[0].Key["id"])
	require.Equal(t, int64(1), page2.NextRowOrdinal)
	require.True(t, page2.HasMore)

	after = page2.NextRowOrdinal
	page3, err := fixture.svc.GetCommittedBundleRows(ctx, fixture.reader, commit.BundleSeq, &after, 2)
	require.NoError(t, err)
	require.Len(t, page3.Rows, 1)
	require.Equal(t, rowIDs[2].String(), page3.Rows[0].Key["id"])
	require.Equal(t, int64(2), page3.NextRowOrdinal)
	require.False(t, page3.HasMore)
}

func TestPushSessions_RejectInvalidAndDuplicateChunkUploads(t *testing.T) {
	ctx := context.Background()

	t.Run("empty chunk", func(t *testing.T) {
		fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
		session := fixture.createSession(t, ctx, 1, 1)

		_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, session.PushID, &PushSessionChunkRequest{
			StartRowOrdinal: 0,
			Rows:            nil,
		})
		var invalidErr *PushChunkInvalidError
		require.ErrorAs(t, err, &invalidErr)
		require.Contains(t, invalidErr.Error(), "must not be empty")
	})

	t.Run("oversized chunk", func(t *testing.T) {
		fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{maxRowsPerPushChunk: 1})
		session := fixture.createSession(t, ctx, 1, 2)

		_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, session.PushID, &PushSessionChunkRequest{
			StartRowOrdinal: 0,
			Rows: []PushRequestRow{
				fixture.userRow(uuid.New(), "Alpha"),
				fixture.userRow(uuid.New(), "Bravo"),
			},
		})
		var invalidErr *PushChunkInvalidError
		require.ErrorAs(t, err, &invalidErr)
		require.Contains(t, invalidErr.Error(), "exceeds max_rows_per_push_chunk")
	})

	t.Run("duplicate target within one chunk", func(t *testing.T) {
		fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{maxRowsPerPushChunk: 2})
		session := fixture.createSession(t, ctx, 1, 2)
		rowID := uuid.New()

		_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, session.PushID, &PushSessionChunkRequest{
			StartRowOrdinal: 0,
			Rows: []PushRequestRow{
				fixture.userRow(rowID, "Alpha"),
				fixture.userRow(rowID, "Bravo"),
			},
		})
		var invalidErr *PushChunkInvalidError
		require.ErrorAs(t, err, &invalidErr)
		require.Contains(t, invalidErr.Error(), "duplicate target row")
	})

	t.Run("duplicate target across chunks", func(t *testing.T) {
		fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
		session := fixture.createSession(t, ctx, 1, 2)
		rowID := uuid.New()

		fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(rowID, "Alpha"))
		fixture.uploadChunk(t, ctx, session.PushID, 1, fixture.userRow(rowID, "Bravo"))

		_, err := fixture.svc.CommitPushSession(ctx, fixture.writer, session.PushID)
		var invalidErr *PushCommitInvalidError
		require.ErrorAs(t, err, &invalidErr)
		require.Contains(t, invalidErr.Error(), "duplicate target row")
	})
}

func TestPushSessions_RejectIncompleteOrNonContiguousStagingAtCommit(t *testing.T) {
	ctx := context.Background()

	t.Run("incomplete staged upload", func(t *testing.T) {
		fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
		session := fixture.createSession(t, ctx, 1, 2)
		fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(uuid.New(), "Alpha"))

		_, err := fixture.svc.CommitPushSession(ctx, fixture.writer, session.PushID)
		var invalidErr *PushCommitInvalidError
		require.ErrorAs(t, err, &invalidErr)
		require.Contains(t, invalidErr.Error(), "does not match planned_row_count")
	})

	t.Run("non-contiguous staged upload", func(t *testing.T) {
		fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
		session := fixture.createSession(t, ctx, 1, 2)

		insertPreparedPushSessionRow(t, ctx, fixture.pool, fixture.svc, session.PushID, 0, fixture.userRow(uuid.New(), "Alpha"))
		insertPreparedPushSessionRow(t, ctx, fixture.pool, fixture.svc, session.PushID, 2, fixture.userRow(uuid.New(), "Bravo"))

		_, err := fixture.svc.CommitPushSession(ctx, fixture.writer, session.PushID)
		var invalidErr *PushCommitInvalidError
		require.ErrorAs(t, err, &invalidErr)
		require.Contains(t, invalidErr.Error(), "not contiguous")
	})
}

func TestPushSessions_RestartedSessionDeletesPartialStagingAndInvalidatesOldPushID(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	oldSession := fixture.createSession(t, ctx, 1, 2)
	fixture.uploadChunk(t, ctx, oldSession.PushID, 0, fixture.userRow(uuid.New(), "Alpha"))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, oldSession.PushID))

	newSession := fixture.createSession(t, ctx, 1, 2)
	require.NotEqual(t, oldSession.PushID, newSession.PushID)
	require.Equal(t, 0, fixture.pushSessionRowCount(t, ctx, oldSession.PushID))

	_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, oldSession.PushID, &PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows:            []PushRequestRow{fixture.userRow(uuid.New(), "Bravo")},
	})
	var notFoundErr *PushSessionNotFoundError
	require.ErrorAs(t, err, &notFoundErr)

	var stagingSessionCount int
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_sessions
		WHERE user_id = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
		  AND status = 'staging'
	`, fixture.writer.UserID, fixture.writer.SourceID, int64(1)).Scan(&stagingSessionCount))
	require.Equal(t, 1, stagingSessionCount)
}

func TestPushSessions_UploadRefreshesExpiry(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{
		pushSessionTTL: 250 * time.Millisecond,
	})

	session := fixture.createSession(t, ctx, 1, 1)
	nearExpiry := time.Now().UTC().Add(40 * time.Millisecond)
	fixture.setPushSessionExpiry(t, ctx, session.PushID, nearExpiry)

	fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(uuid.New(), "Alpha"))
	refreshedExpiry := fixture.pushSessionExpiry(t, ctx, session.PushID)
	require.True(t, refreshedExpiry.After(nearExpiry))

	time.Sleep(80 * time.Millisecond)
	_, err := fixture.svc.CommitPushSession(ctx, fixture.writer, session.PushID)
	require.NoError(t, err)
}

func TestPushSessions_AcceptedPushReplaySurvivesNormalHistoryPruning(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	rowID := uuid.New()
	session := fixture.createSession(t, ctx, 1, 1)
	fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(rowID, "Alpha"))
	commit := fixture.commitSession(t, ctx, session.PushID)

	_, err := fixture.pool.Exec(ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = $2
		WHERE user_id = $1
	`, fixture.writer.UserID, commit.BundleSeq)
	require.NoError(t, err)

	_, err = fixture.svc.ProcessPull(ctx, fixture.reader, 0, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, commit.BundleSeq, prunedErr.RetainedFloor)

	page, err := fixture.svc.GetCommittedBundleRows(ctx, fixture.writer, commit.BundleSeq, nil, 10)
	require.NoError(t, err)
	require.Equal(t, commit.BundleSeq, page.BundleSeq)
	require.Equal(t, commit.SourceID, page.SourceID)
	require.Equal(t, commit.SourceBundleID, page.SourceBundleID)
	require.Equal(t, commit.RowCount, page.RowCount)
	require.Equal(t, commit.BundleHash, page.BundleHash)
	require.Len(t, page.Rows, 1)
	require.Equal(t, rowID.String(), page.Rows[0].Key["id"])
	require.Equal(t, int64(0), page.NextRowOrdinal)
	require.False(t, page.HasMore)
}

func TestPushSessions_ConcurrentSessionCreationLeavesOneActiveStagingSession(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	lockConn, err := fixture.pool.Acquire(ctx)
	require.NoError(t, err)
	defer lockConn.Release()

	_, err = lockConn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, fixture.writer.UserID)
	require.NoError(t, err)
	lockHeld := true
	defer func() {
		if !lockHeld {
			return
		}
		_, unlockErr := lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, fixture.writer.UserID)
		require.NoError(t, unlockErr)
	}()

	type createResult struct {
		resp *PushSessionCreateResponse
		err  error
	}

	results := make(chan createResult, 2)
	var wg sync.WaitGroup
	runCreate := func() {
		defer wg.Done()
		resp, err := fixture.svc.CreatePushSession(ctx, fixture.writer, &PushSessionCreateRequest{
			SourceID:        fixture.writer.SourceID,
			SourceBundleID:  1,
			PlannedRowCount: 1,
		})
		results <- createResult{resp: resp, err: err}
	}

	wg.Add(2)
	go runCreate()
	go runCreate()

	select {
	case early := <-results:
		t.Fatalf("expected concurrent create requests to wait on advisory lock, got early result: resp=%v err=%v", early.resp, early.err)
	case <-time.After(250 * time.Millisecond):
	}

	_, err = lockConn.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, fixture.writer.UserID)
	require.NoError(t, err)
	lockHeld = false

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for concurrent create requests")
	}
	close(results)

	collected := make([]createResult, 0, 2)
	for result := range results {
		collected = append(collected, result)
	}
	require.Len(t, collected, 2)
	for _, result := range collected {
		require.NoError(t, result.err)
		require.NotNil(t, result.resp)
		require.Equal(t, "staging", result.resp.Status)
		require.Equal(t, int64(1), result.resp.PlannedRowCount)
		require.Equal(t, int64(0), result.resp.NextExpectedRowOrdinal)
	}
	require.NotEqual(t, collected[0].resp.PushID, collected[1].resp.PushID)

	var activePushID string
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT push_id::text
		FROM sync.push_sessions
		WHERE user_id = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
		  AND status = 'staging'
	`, fixture.writer.UserID, fixture.writer.SourceID, int64(1)).Scan(&activePushID))

	var stagingCount int
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_sessions
		WHERE user_id = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
		  AND status = 'staging'
	`, fixture.writer.UserID, fixture.writer.SourceID, int64(1)).Scan(&stagingCount))
	require.Equal(t, 1, stagingCount)
	require.Contains(t, []string{collected[0].resp.PushID, collected[1].resp.PushID}, activePushID)

	var obsoletePushID string
	if activePushID == collected[0].resp.PushID {
		obsoletePushID = collected[1].resp.PushID
	} else {
		obsoletePushID = collected[0].resp.PushID
	}

	_, err = fixture.svc.UploadPushChunk(ctx, fixture.writer, obsoletePushID, &PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows:            []PushRequestRow{fixture.userRow(uuid.New(), "Obsolete")},
	})
	var notFoundErr *PushSessionNotFoundError
	require.ErrorAs(t, err, &notFoundErr)

	activeChunk := fixture.uploadChunk(t, ctx, activePushID, 0, fixture.userRow(uuid.New(), "Active"))
	require.Equal(t, int64(1), activeChunk.NextExpectedRowOrdinal)
}

func TestPushSessions_UnknownExpiredAndForeignIDsFailClosed(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	foreignActor := Actor{UserID: "other-user-" + uuid.NewString(), SourceID: "other-source"}
	foreignSession := fixture.createSession(t, ctx, 1, 1)

	expiredSession := fixture.createSession(t, ctx, 2, 1)
	fixture.setPushSessionExpiry(t, ctx, expiredSession.PushID, time.Now().UTC().Add(-time.Second))

	validRow := fixture.userRow(uuid.New(), "Alpha")
	unknownPushID := uuid.NewString()

	cases := []struct {
		name        string
		run         func() error
		assertError func(*testing.T, error)
	}{
		{
			name: "upload unknown",
			run: func() error {
				_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, unknownPushID, &PushSessionChunkRequest{
					StartRowOrdinal: 0,
					Rows:            []PushRequestRow{validRow},
				})
				return err
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionNotFoundError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "commit unknown",
			run: func() error {
				_, err := fixture.svc.CommitPushSession(ctx, fixture.writer, unknownPushID)
				return err
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionNotFoundError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "delete unknown",
			run: func() error {
				return fixture.svc.DeletePushSession(ctx, fixture.writer, unknownPushID)
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionNotFoundError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "upload foreign",
			run: func() error {
				_, err := fixture.svc.UploadPushChunk(ctx, foreignActor, foreignSession.PushID, &PushSessionChunkRequest{
					StartRowOrdinal: 0,
					Rows:            []PushRequestRow{validRow},
				})
				return err
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionForbiddenError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "commit foreign",
			run: func() error {
				_, err := fixture.svc.CommitPushSession(ctx, foreignActor, foreignSession.PushID)
				return err
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionForbiddenError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "delete foreign",
			run: func() error {
				return fixture.svc.DeletePushSession(ctx, foreignActor, foreignSession.PushID)
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionForbiddenError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "upload expired",
			run: func() error {
				_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, expiredSession.PushID, &PushSessionChunkRequest{
					StartRowOrdinal: 0,
					Rows:            []PushRequestRow{validRow},
				})
				return err
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionExpiredError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "commit expired",
			run: func() error {
				_, err := fixture.svc.CommitPushSession(ctx, fixture.writer, expiredSession.PushID)
				return err
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionExpiredError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "delete expired",
			run: func() error {
				return fixture.svc.DeletePushSession(ctx, fixture.writer, expiredSession.PushID)
			},
			assertError: func(t *testing.T, err error) {
				t.Helper()
				var target *PushSessionExpiredError
				require.ErrorAs(t, err, &target)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			tc.assertError(t, err)
		})
	}
}

func TestHTTPSyncHandlers_PushSessionErrorMappings(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{maxRowsPerPushChunk: 1})
	handlers := NewHTTPSyncHandlers(fixture.svc, slog.Default())
	foreignActor := Actor{UserID: "other-user-" + uuid.NewString(), SourceID: "other-source"}

	validSession := fixture.createSession(t, ctx, 1, 1)
	outOfOrderSession := fixture.createSession(t, ctx, 2, 1)
	fixture.uploadChunk(t, ctx, outOfOrderSession.PushID, 0, fixture.userRow(uuid.New(), "Alpha"))

	commitInvalidSession := fixture.createSession(t, ctx, 3, 2)
	fixture.uploadChunk(t, ctx, commitInvalidSession.PushID, 0, fixture.userRow(uuid.New(), "Bravo"))

	committedSession := fixture.createSession(t, ctx, 5, 1)
	fixture.uploadChunk(t, ctx, committedSession.PushID, 0, fixture.userRow(uuid.New(), "Charlie"))
	commitResp := fixture.commitSession(t, ctx, committedSession.PushID)

	cases := []struct {
		name           string
		handler        func(http.ResponseWriter, *http.Request)
		method         string
		path           string
		actor          Actor
		body           any
		pathValues     map[string]string
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "create invalid",
			handler:        handlers.HandleCreatePushSession,
			method:         http.MethodPost,
			path:           "/sync/push-sessions",
			actor:          fixture.writer,
			body:           PushSessionCreateRequest{SourceID: "wrong-source", SourceBundleID: 1, PlannedRowCount: 1},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "push_session_invalid",
		},
		{
			name:           "chunk invalid",
			handler:        handlers.HandlePushSessionChunk,
			method:         http.MethodPost,
			path:           "/sync/push-sessions/" + validSession.PushID + "/chunks",
			actor:          fixture.writer,
			body:           PushSessionChunkRequest{StartRowOrdinal: 0},
			pathValues:     map[string]string{"push_id": validSession.PushID},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "push_chunk_invalid",
		},
		{
			name:           "chunk out of order",
			handler:        handlers.HandlePushSessionChunk,
			method:         http.MethodPost,
			path:           "/sync/push-sessions/" + outOfOrderSession.PushID + "/chunks",
			actor:          fixture.writer,
			body:           PushSessionChunkRequest{StartRowOrdinal: 0, Rows: []PushRequestRow{fixture.userRow(uuid.New(), "Delta")}},
			pathValues:     map[string]string{"push_id": outOfOrderSession.PushID},
			expectedStatus: http.StatusConflict,
			expectedCode:   "push_chunk_out_of_order",
		},
		{
			name:           "chunk not found",
			handler:        handlers.HandlePushSessionChunk,
			method:         http.MethodPost,
			path:           "/sync/push-sessions/" + uuid.NewString() + "/chunks",
			actor:          fixture.writer,
			body:           PushSessionChunkRequest{StartRowOrdinal: 0, Rows: []PushRequestRow{fixture.userRow(uuid.New(), "Echo")}},
			pathValues:     map[string]string{"push_id": uuid.NewString()},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "push_session_not_found",
		},
		{
			name:           "chunk forbidden",
			handler:        handlers.HandlePushSessionChunk,
			method:         http.MethodPost,
			path:           "/sync/push-sessions/" + validSession.PushID + "/chunks",
			actor:          foreignActor,
			body:           PushSessionChunkRequest{StartRowOrdinal: 0, Rows: []PushRequestRow{fixture.userRow(uuid.New(), "Foxtrot")}},
			pathValues:     map[string]string{"push_id": validSession.PushID},
			expectedStatus: http.StatusForbidden,
			expectedCode:   "push_session_forbidden",
		},
		{
			name:           "commit invalid",
			handler:        handlers.HandleCommitPushSession,
			method:         http.MethodPost,
			path:           "/sync/push-sessions/" + commitInvalidSession.PushID + "/commit",
			actor:          fixture.writer,
			pathValues:     map[string]string{"push_id": commitInvalidSession.PushID},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "push_commit_invalid",
		},
		{
			name:           "get invalid",
			handler:        handlers.HandleGetCommittedBundleRows,
			method:         http.MethodGet,
			path:           "/sync/committed-bundles/not-a-number/rows",
			actor:          fixture.writer,
			pathValues:     map[string]string{"bundle_seq": "not-a-number"},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "committed_bundle_chunk_invalid",
		},
		{
			name:           "get not found",
			handler:        handlers.HandleGetCommittedBundleRows,
			method:         http.MethodGet,
			path:           "/sync/committed-bundles/999999/rows",
			actor:          fixture.writer,
			pathValues:     map[string]string{"bundle_seq": "999999"},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "committed_bundle_not_found",
		},
		{
			name:           "committed delete invalid",
			handler:        handlers.HandleDeletePushSession,
			method:         http.MethodDelete,
			path:           "/sync/push-sessions/" + committedSession.PushID,
			actor:          fixture.writer,
			pathValues:     map[string]string{"push_id": committedSession.PushID},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "push_chunk_invalid",
		},
		{
			name:           "commit response stays valid after mappings setup",
			handler:        handlers.HandleGetCommittedBundleRows,
			method:         http.MethodGet,
			path:           fmt.Sprintf("/sync/committed-bundles/%d/rows", commitResp.BundleSeq),
			actor:          fixture.writer,
			pathValues:     map[string]string{"bundle_seq": fmt.Sprintf("%d", commitResp.BundleSeq)},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSONHandlerRequest(t, tc.handler, tc.method, tc.path, tc.actor, tc.body, tc.pathValues)
			require.Equal(t, tc.expectedStatus, rec.Code)
			if tc.expectedStatus == http.StatusOK {
				return
			}
			errResp := decodeErrorResponse(t, rec)
			require.Equal(t, tc.expectedCode, errResp.Error)
		})
	}

	expiredSession := fixture.createSession(t, ctx, 6, 1)
	fixture.setPushSessionExpiry(t, ctx, expiredSession.PushID, time.Now().UTC().Add(-time.Second))
	expiredCases := []struct {
		name       string
		handler    func(http.ResponseWriter, *http.Request)
		method     string
		path       string
		body       any
		pathValues map[string]string
	}{
		{
			name:       "chunk expired",
			handler:    handlers.HandlePushSessionChunk,
			method:     http.MethodPost,
			path:       "/sync/push-sessions/" + expiredSession.PushID + "/chunks",
			body:       PushSessionChunkRequest{StartRowOrdinal: 0, Rows: []PushRequestRow{fixture.userRow(uuid.New(), "Expired")}},
			pathValues: map[string]string{"push_id": expiredSession.PushID},
		},
		{
			name:       "commit expired",
			handler:    handlers.HandleCommitPushSession,
			method:     http.MethodPost,
			path:       "/sync/push-sessions/" + expiredSession.PushID + "/commit",
			pathValues: map[string]string{"push_id": expiredSession.PushID},
		},
		{
			name:       "delete expired",
			handler:    handlers.HandleDeletePushSession,
			method:     http.MethodDelete,
			path:       "/sync/push-sessions/" + expiredSession.PushID,
			pathValues: map[string]string{"push_id": expiredSession.PushID},
		},
	}
	for _, tc := range expiredCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSONHandlerRequest(t, tc.handler, tc.method, tc.path, fixture.writer, tc.body, tc.pathValues)
			require.Equal(t, http.StatusGone, rec.Code)
			errResp := decodeErrorResponse(t, rec)
			require.Equal(t, "push_session_expired", errResp.Error)
		})
	}
}

func TestPushSessions_RejectsHiddenOwnerColumnInPayload(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	rowID := uuid.New()
	session := fixture.createSession(t, ctx, 1, 1)
	_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, session.PushID, &PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows: []PushRequestRow{{
			Schema:         fixture.schemaName,
			Table:          "users",
			Key:            SyncKey{"id": rowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"id":"%s","_sync_scope_id":"malicious-user","name":"Mallory","email":"mallory@example.com"}`,
				rowID,
			)),
		}},
	})
	var chunkErr *PushChunkInvalidError
	require.ErrorAs(t, err, &chunkErr)
	require.Contains(t, err.Error(), "must not include hidden scope column")
}

func TestPushSessions_RejectsNonCanonicalUUIDVisibleKey(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	rowID := strings.ToUpper(uuid.New().String())
	session := fixture.createSession(t, ctx, 1, 1)
	_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, session.PushID, &PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows: []PushRequestRow{{
			Schema:         fixture.schemaName,
			Table:          "users",
			Key:            SyncKey{"id": rowID},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"id":"%s","name":"Upper","email":"upper@example.com"}`,
				rowID,
			)),
		}},
	})
	var chunkErr *PushChunkInvalidError
	require.ErrorAs(t, err, &chunkErr)
	require.Contains(t, err.Error(), "canonical dashed lowercase UUID text")
}

func TestPushSessions_AcceptsTextVisibleSyncKey(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_text_key_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			doc_id TEXT NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, doc_id)
		)`, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-text-key-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"doc_id"}},
		},
	}, logger)

	actor := Actor{UserID: "push-text-key-user-" + suffix, SourceID: "writer"}
	bundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "docs",
		Key:            SyncKey{"doc_id": "doc-42"},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(`{"doc_id":"doc-42","body":"Hello text key"}`),
	}})
	require.NoError(t, err)
	require.Len(t, bundle.Rows, 1)
	require.Equal(t, SyncKey{"doc_id": "doc-42"}, bundle.Rows[0].Key)
	require.NotContains(t, string(bundle.Rows[0].Payload), `"_sync_scope_id"`)

	var body string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT body
		FROM %s.docs
		WHERE _sync_scope_id = $1 AND doc_id = $2
	`, schemaIdent), actor.UserID, "doc-42").Scan(&body))
	require.Equal(t, "Hello text key", body)
}
