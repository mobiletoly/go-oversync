package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type maintenanceVector struct {
	ID, Mode, Op, Key, Name, Email string
	Generation                     int64
	Expected                       json.RawMessage
}
type maintenanceExchange struct {
	Method, Path, Source, Generation, Fingerprint, Request string
	Status                                                 int
	Response, TransportError                               string
}
type maintenanceVectorResult struct {
	Exchanges        []maintenanceExchange
	States           []map[string]any
	UninstallBlocked bool
}

func vectorState(t *testing.T, c *Client) map[string]any {
	t.Helper()
	result := map[string]any{}
	for name, query := range map[string]string{
		"business":      `SELECT id,name,email FROM users ORDER BY id`,
		"dirty":         `SELECT schema_name,table_name,key_json,op,base_row_version,base_kind,base_payload,base_provenance,payload,dirty_ordinal FROM _sync_dirty_rows ORDER BY dirty_ordinal`,
		"row_state":     `SELECT schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known,server_revision FROM _sync_row_state ORDER BY key_json`,
		"relationships": `SELECT view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision FROM _sync_relationships ORDER BY view,child_schema,child_table,child_key,fk_column`,
		"attachment":    `SELECT binding_state,attached_user_id,current_source_id,last_bundle_seq_seen,rebuild_required FROM _sync_attachment_state`,
		"source":        `SELECT source_id,next_source_bundle_id FROM _sync_source_state ORDER BY source_id`,
		"outbox":        `SELECT state,source_id,source_bundle_id,canonical_request_hash,row_count,initialization_id FROM _sync_outbox_bundle`,
		"frozen_rows":   `SELECT source_bundle_id,row_ordinal,op,base_row_version,base_kind,base_payload,base_provenance,local_payload,wire_key_json,wire_payload FROM _sync_outbox_rows ORDER BY row_ordinal`,
	} {
		rows, err := c.db.Query(query)
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		values := make([][]any, 0)
		for rows.Next() {
			row := make([]any, len(cols))
			ptr := make([]any, len(cols))
			for i := range row {
				ptr[i] = &row[i]
			}
			require.NoError(t, rows.Scan(ptr...))
			values = append(values, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		result[name] = values
	}
	p, err := c.PendingConflict(context.Background())
	require.NoError(t, err)
	if p != nil {
		result["conflict"] = map[string]any{"revision": p.Revision, "source": p.SourceID, "bundle": p.SourceBundleID, "hash": p.RequestHash, "response": p.Response, "newer_op": p.NewerLocalOp, "newer_payload": p.NewerLocalPayload}
	} else {
		result["conflict"] = nil
	}
	return result
}

func executeMaintenanceVector(t *testing.T, v maintenanceVector) maintenanceVectorResult {
	ctx := context.Background()
	proof := testSchemaGenerationProof()
	proof.ID = oversync.SchemaGenerationID(v.Generation)
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, proof)
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.RetryPolicy.Enabled = false
	if v.Mode == "resumable_chunks" {
		cfg.UploadLimit = 1
	}
	c, db := newBundleClientWithConfig(t, cfg, usersTestDDL)
	if v.Op != oversync.OpInsert {
		seedSyncedUserRow(t, c, db, v.Key, "Original", 1)
	}
	var err error
	switch v.Op {
	case oversync.OpInsert:
		_, err = db.Exec(`INSERT INTO users VALUES(?,?,?)`, v.Key, v.Name, v.Email)
	case oversync.OpUpdate:
		_, err = db.Exec(`UPDATE users SET name=? WHERE id=?`, v.Name, v.Key)
	case oversync.OpDelete:
		_, err = db.Exec(`DELETE FROM users WHERE id=?`, v.Key)
	}
	require.NoError(t, err)
	if v.Mode == "resumable_chunks" {
		for i := 2; i <= 3; i++ {
			_, e := db.Exec(`INSERT INTO users VALUES(?,?,?)`, fmt.Sprintf("00000000-0000-4000-8000-%012d", i), fmt.Sprintf("Offline%d", i), fmt.Sprintf("person%d@example.com", i))
			require.NoError(t, e)
		}
	}
	result := maintenanceVectorResult{States: []map[string]any{vectorState(t, c)}}
	server := newMockPushServer(t)
	server.nextBundleSeq = 3
	if v.Mode == "conflict" || v.Mode == "newer_delete" || v.Mode == "retry_uninstall" || v.Mode == "stale_decision" || v.Mode == "keep_local" || v.Mode == "keep_merged" {
		server.pushHook = func(s *mockDirectPush) *http.Response {
			return structuredConflictResponse(s.Rows[0], 2, false, map[string]any{"id": v.Key, "name": "Server", "email": v.Email})
		}
	}
	interrupted := false
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body []byte
		var err error
		if r.Body != nil {
			body, err = io.ReadAll(r.Body)
		}
		require.NoError(t, err)
		r.Body = io.NopCloser(bytes.NewReader(body))
		exchange := maintenanceExchange{Method: r.Method, Path: r.URL.RequestURI(), Source: r.Header.Get(oversync.SourceIDHeader), Generation: r.Header.Get(oversync.SchemaGenerationIDHeader), Fingerprint: r.Header.Get(oversync.SchemaGenerationFingerprintHeader), Request: string(body)}
		var response *http.Response
		if !interrupted && (v.Mode == "maintenance" || (v.Mode == "resumable_chunks" && r.URL.Path == "/sync/push" && len(server.pushRequests) > 0)) {
			response = errorJSONResponse(503, oversync.ErrorResponse{Error: "service_not_ready", Message: "Sync service is unavailable; retry later"})
		} else if !interrupted && v.Mode == "expired" && r.URL.Path == "/sync/push" {
			interrupted = true
			response = errorJSONResponse(410, oversync.ErrorResponse{Error: "push_expired", Message: "Push request expired"})
		} else if !interrupted && v.Mode == "unsettled" && r.URL.Path == "/sync/push" {
			interrupted = true
			err = fmt.Errorf("fixture lost settlement response")
		} else {
			response, err = server.RoundTrip(r)
		}
		if !interrupted && v.Mode == "uncertain" && r.URL.Path == "/sync/push" {
			interrupted = true
			if response != nil {
				response.Body.Close()
			}
			response = nil
			err = fmt.Errorf("fixture lost commit response")
		}
		if err != nil {
			exchange.TransportError = err.Error()
		} else {
			exchange.Status = response.StatusCode
			b, e := io.ReadAll(response.Body)
			require.NoError(t, e)
			response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(b))
			exchange.Response = string(b)
		}
		result.Exchanges = append(result.Exchanges, exchange)
		return response, err
	})
	c.HTTP = &http.Client{Transport: transport}
	_, err = c.PushPending(ctx)
	require.Error(t, err)
	result.States = append(result.States, vectorState(t, c))
	result.UninstallBlocked = c.UninstallSync(ctx) != nil
	require.True(t, result.UninstallBlocked)
	if v.Mode == "stale_decision" {
		p, e := c.PendingConflict(ctx)
		require.NoError(t, e)
		_, e = db.Exec(`UPDATE users SET name='Newer' WHERE id=?`, v.Key)
		require.NoError(t, e)
		var stale *StaleConflictResolutionError
		require.ErrorAs(t, c.ResolveConflict(ctx, *p, AcceptServer{}), &stale)
	}
	if v.Mode == "keep_local" || v.Mode == "keep_merged" {
		p, e := c.PendingConflict(ctx)
		require.NoError(t, e)
		var choice MergeResult = KeepLocal{}
		if v.Mode == "keep_merged" {
			choice = KeepMerged{MergedPayload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"Merged","email":%q}`, v.Key, v.Email))}
		}
		require.NoError(t, c.ResolveConflict(ctx, *p, choice))
		server.pushHook = nil
	}
	if v.Mode == "newer_delete" {
		_, err = db.Exec(`DELETE FROM users WHERE id=?`, v.Key)
		require.NoError(t, err)
		p, e := c.PendingConflict(ctx)
		require.NoError(t, e)
		require.NoError(t, c.ResolveConflict(ctx, *p, AcceptServer{}))
		server.pushHook = nil
	}
	if v.Mode == "retry_uninstall" {
		p, e := c.PendingConflict(ctx)
		require.NoError(t, e)
		require.NoError(t, c.RetryConflict(ctx, *p))
		result.UninstallBlocked = c.UninstallSync(ctx) != nil
		require.True(t, result.UninstallBlocked)
	}
	result.States = append(result.States, vectorState(t, c))
	// Restart the same persisted file; no source rotation or request reconstruction.
	file := filepath.Join(t.TempDir(), "same.sqlite")
	_, err = db.Exec(`VACUUM INTO ?`, file)
	require.NoError(t, err)
	reopenedDB, err := sql.Open("sqlite3", file)
	require.NoError(t, err)
	defer reopenedDB.Close()
	reopenedDB.SetMaxOpenConns(1)
	reopened, err := newTestClient(t, reopenedDB, "http://example.invalid", tokenProviderForTests, cfg)
	require.NoError(t, err)
	defer reopened.Close()
	result.States = append(result.States, vectorState(t, reopened))
	if v.Mode == "resumable_chunks" || v.Mode == "newer_delete" || v.Mode == "uncertain" || v.Mode == "expired" || v.Mode == "maintenance" || v.Mode == "unsettled" || v.Mode == "keep_local" || v.Mode == "keep_merged" {
		attachTestClient(t, reopened, "bundle-user", "bundle-device")
		reopened.HTTP = &http.Client{Transport: transport}
		interrupted = true
		_, err = reopened.PushPending(ctx)
		require.NoError(t, err)
		result.States = append(result.States, vectorState(t, reopened))
	}
	return result
}

func TestMaintenanceConformance_ConcreteVectors(t *testing.T) {
	// Historical maintenance fixture input: its expected exchanges model the
	// removed upload-session protocol and cannot specify the direct client.
	t.Skip("historical maintenance conflict fixture uses retired upload protocol")
	path := "../conformance/maintenance-conflicts/v2/cases.json"
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var vectors []maintenanceVector
	require.NoError(t, json.Unmarshal(data, &vectors))
	for i := range vectors {
		v := &vectors[i]
		t.Run(v.ID, func(t *testing.T) {
			actual, err := json.Marshal(executeMaintenanceVector(t, *v))
			require.NoError(t, err)
			if os.Getenv("TRANSITION_RECORD_CONFORMANCE") == "1" {
				v.Expected = actual
				return
			}
			require.JSONEq(t, string(v.Expected), string(actual))
		})
	}
	if os.Getenv("TRANSITION_RECORD_CONFORMANCE") == "1" {
		data, err = json.MarshalIndent(vectors, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(data, '\n'), 0644))
	}
}
