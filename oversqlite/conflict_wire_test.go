package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestConflictEvidence_RejectsIncompleteOrMismatchedWithoutLosingIntent(t *testing.T) {
	type mutation func(map[string]any, map[string]any)
	cases := map[string]mutation{}
	for _, field := range []string{"source_id", "schema_generation_id", "schema_generation_fingerprint", "source_bundle_id", "canonical_request_hash", "status", "outcome_hash", "rejection"} {
		cases["missing_outer_"+field] = func(e, c map[string]any) { delete(e, field) }
		cases["null_outer_"+field] = func(e, c map[string]any) { e[field] = nil }
	}
	for _, field := range []string{"schema", "table", "key", "op", "base_row_version", "server_row_version", "server_row_deleted", "server_row"} {
		cases["missing_"+field] = func(e, c map[string]any) { delete(c, field) }
		if field != "server_row" {
			cases["null_"+field] = func(e, c map[string]any) { c[field] = nil }
		}
	}
	cases["operation"] = func(e, c map[string]any) { c["op"] = "d" }
	cases["base"] = func(e, c map[string]any) { c["base_row_version"] = 99 }
	cases["live_without_payload"] = func(e, c map[string]any) { c["server_row"] = nil }
	cases["tombstone_with_payload"] = func(e, c map[string]any) { c["server_row_deleted"] = true }
	cases["absence_with_payload"] = func(e, c map[string]any) { c["server_row_version"] = 0 }
	cases["zero_version_tombstone"] = func(e, c map[string]any) {
		c["server_row_version"] = 0
		c["server_row_deleted"] = true
		c["server_row"] = nil
	}
	cases["live_row_wrong_key"] = func(e, c map[string]any) { c["server_row"].(map[string]any)["id"] = uuid.NewString() }
	cases["live_row_missing_column"] = func(e, c map[string]any) { delete(c["server_row"].(map[string]any), "name") }
	cases["live_row_extra_column"] = func(e, c map[string]any) { c["server_row"].(map[string]any)["hidden"] = "leak" }
	cases["live_row_null_nonnullable"] = func(e, c map[string]any) { c["server_row"].(map[string]any)["name"] = nil }
	cases["live_row_invalid_text_codec"] = func(e, c map[string]any) { c["server_row"].(map[string]any)["name"] = 7 }
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			id := uuid.NewString()
			seedSyncedUserRow(t, c, db, id, "Original", 1)
			_, err := db.Exec(`UPDATE users SET name='Offline' WHERE id=?`, id)
			require.NoError(t, err)
			resolver := &capturingResolver{result: KeepLocal{}}
			c.Resolver = resolver
			_, err = c.ensurePushOutboundSnapshot(ctx)
			require.NoError(t, err)
			before := vectorState(t, c)
			server := newMockPushServer(t)
			var beforeResponse *outboxBundleRecord
			server.pushHook = func(s *mockDirectPush) *http.Response {
				var e error
				beforeResponse, e = loadOutboxBundle(ctx, db)
				require.NoError(t, e)
				response := server.retainOutcome(oversync.PushOutcome{SourceID: s.SourceID, GenerationID: s.Generation.ID, GenerationFingerprint: s.Generation.Fingerprint, SourceBundleID: s.SourceBundleID, CanonicalRequestHash: s.CanonicalRequestHash, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "push_conflict", Conflict: &oversync.PushConflictDetails{Schema: "main", Table: "users", Key: mustBundleKey(id), Op: "u", BaseRowVersion: 1, ServerRowVersion: 2, ServerRow: mustJSONPayload(t, map[string]any{"id": id, "name": "Server", "email": "ada@example.com"})}}}, s.Generation)
				raw, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				var envelope map[string]any
				require.NoError(t, json.Unmarshal(raw, &envelope))
				conflict := envelope["rejection"].(map[string]any)["conflict"].(map[string]any)
				mutate(envelope, conflict)
				raw, err = json.Marshal(envelope)
				require.NoError(t, err)
				return rawMockOutcome(raw)
			}
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "/sync/push-outcomes/resolve", r.URL.Path)
				frozen, loadErr := loadOutboxBundle(ctx, db)
				require.NoError(t, loadErr)
				return server.pushHook(&mockDirectPush{SourceID: c.sourceID, SourceBundleID: frozen.SourceBundleID, CanonicalRequestHash: frozen.CanonicalRequestHash, Generation: c.discovery.SchemaGeneration}), nil
			})}
			_, err = c.PushPending(ctx)
			require.Error(t, err)
			require.NotNil(t, beforeResponse, "negative wire body must be reached after public freeze")
			require.False(t, resolver.called)
			pending, pErr := c.PendingConflict(ctx)
			require.NoError(t, pErr)
			require.Nil(t, pending)
			after := vectorState(t, c)
			// Staging identity is transport progress; all frozen intent and business state remain exact.
			delete(before, "outbox")
			delete(after, "outbox")
			require.Equal(t, before, after)
			outbox, err := loadOutboxBundle(ctx, db)
			require.NoError(t, err)
			require.Equal(t, outboxStatePrepared, outbox.State)
			require.Equal(t, beforeResponse, outbox, "invalid conflict evidence cannot mutate the frozen outbox")
			path := filepath.Join(t.TempDir(), "conflict.sqlite")
			_, err = db.Exec("VACUUM INTO ?", path)
			require.NoError(t, err)
			reopenedDB, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			reopenedDB.SetMaxOpenConns(1)
			reopened, err := newTestClient(t, reopenedDB, "http://example.invalid", tokenProviderForTests, c.config)
			require.NoError(t, err)
			defer reopened.Close()
			persisted := vectorState(t, reopened)
			delete(persisted, "outbox")
			require.Equal(t, before, persisted)
		})
	}
}

func TestConflictEvidence_PreservesInt64Versions(t *testing.T) {
	raw := []byte(`{"error":"push_conflict","message":"conflict","conflict":{"schema":"main","table":"users","key":{"id":"a"},"op":"UPDATE","base_row_version":9007199254740993,"server_row_version":9007199254740995,"server_row_deleted":true,"server_row":null}}`)
	response, err := completeConflictResponse(raw)
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), response.Conflict.BaseRowVersion)
	require.Equal(t, int64(9007199254740995), response.Conflict.ServerRowVersion)
}

func TestConflictEvidence_RejectsServerVersionBelowFrozenBase(t *testing.T) {
	ctx := context.Background()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	id := uuid.NewString()
	seedSyncedUserRow(t, c, db, id, "Original", 3)
	_, err := db.Exec(`UPDATE users SET name='Offline' WHERE id=?`, id)
	require.NoError(t, err)
	resolver := &capturingResolver{result: KeepLocal{}}
	c.Resolver = resolver
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		return structuredConflictResponse(session.Rows[0], 2, false, map[string]any{
			"id": id, "name": "Stale Server", "email": "ada@example.com",
		})
	}
	c.HTTP = &http.Client{Transport: server}

	_, err = c.PushPending(ctx)
	require.ErrorContains(t, err, "malformed or mismatched authoritative conflict evidence")
	require.False(t, resolver.called)
	pending, pendingErr := c.PendingConflict(ctx)
	require.NoError(t, pendingErr)
	require.Nil(t, pending)
	var frozen, dirty int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&frozen))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Equal(t, 1, frozen)
	require.Zero(t, dirty)
}
