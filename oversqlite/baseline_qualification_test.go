package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type qualificationVector map[string][]map[string]any

// Every column retains its SQLite storage class and exact text/hex representation.
func qualificationState(t *testing.T, db *sql.DB, business string) qualificationVector {
	t.Helper()
	result := qualificationVector{}
	names := []string{business, "_sync_row_state", "_sync_dirty_rows", "_sync_relationships", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state", "_sync_conflict_state"}
	for _, name := range names {
		rows, err := db.Query(`PRAGMA table_info("` + name + `")`)
		require.NoError(t, err)
		var columns []string
		for rows.Next() {
			var id, notnull, pk int
			var column, kind string
			var defaultValue any
			require.NoError(t, rows.Scan(&id, &column, &kind, &notnull, &defaultValue, &pk))
			columns = append(columns, column)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.NotEmpty(t, columns, name)
		var projections, ordering []string
		for _, c := range columns {
			q := `"` + c + `"`
			ordering = append(ordering, q)
			projections = append(projections, fmt.Sprintf(`'%s',json_object('storage',typeof(%s),'value',CASE WHEN typeof(%s)='blob' THEN lower(hex(%s)) WHEN %s IS NULL THEN NULL ELSE CAST(%s AS TEXT) END)`, c, q, q, q, q, q))
		}
		rows, err = db.Query(`SELECT json_object(` + strings.Join(projections, ",") + `) FROM "` + name + `" ORDER BY ` + strings.Join(ordering, ","))
		require.NoError(t, err)
		result[name] = []map[string]any{}
		for rows.Next() {
			var raw string
			require.NoError(t, rows.Scan(&raw))
			var row map[string]any
			require.NoError(t, json.Unmarshal([]byte(raw), &row))
			result[name] = append(result[name], row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return result
}
func qualificationText(row map[string]any, column string) any {
	return row[column].(map[string]any)["value"]
}
func qualificationRow(t *testing.T, v qualificationVector, table, key string) map[string]any {
	t.Helper()
	for _, r := range v[table] {
		if qualificationText(r, "key_json") == rowKeyJSON(key) {
			return r
		}
	}
	t.Fatalf("missing %s/%s", table, key)
	return nil
}
func qualificationPayload(t *testing.T, row map[string]any, column string) any {
	t.Helper()
	raw := qualificationText(row, column)
	if raw == nil {
		return nil
	}
	var v any
	require.NoError(t, json.Unmarshal([]byte(raw.(string)), &v))
	return v
}

// Seed the clean fixture before mutation; subsequent writes occur on the actual durable file.
func qualificationDurableClient(t *testing.T) (*Client, *sql.DB, string) {
	t.Helper()
	seed, memory := newA1SurvivorClient(t)
	path := filepath.Join(t.TempDir(), "durable.sqlite")
	_, err := memory.Exec(`VACUUM INTO ?`, path)
	require.NoError(t, err)
	require.NoError(t, seed.Close())
	require.NoError(t, memory.Close())
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	c, err := newAdmittedFixtureClient(db, seed.BaseURL, seed.Token, seed.config)
	require.NoError(t, err)
	c.HTTP = seed.HTTP
	attachTestClient(t, c, seed.UserID, "bundle-device")
	c.HTTP = seed.HTTP
	t.Cleanup(func() { require.NoError(t, c.Close()); require.NoError(t, db.Close()) })
	return c, db, path
}

func qualificationPhysicalRestart(t *testing.T, old *Client, oldDB *sql.DB, path string) (*Client, *sql.DB) {
	t.Helper()
	expected := qualificationState(t, oldDB, "nodes")
	require.NoError(t, old.Close())
	require.NoError(t, oldDB.Close())
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	require.Equal(t, expected, qualificationState(t, db, "nodes"))
	c, err := newAdmittedFixtureClient(db, old.BaseURL, old.Token, old.config)
	require.NoError(t, err)
	c.HTTP = old.HTTP
	attachTestClient(t, c, old.UserID, "bundle-device")
	c.HTTP = old.HTTP
	require.Equal(t, expected, qualificationState(t, db, "nodes"))
	t.Cleanup(func() { require.NoError(t, c.Close()); require.NoError(t, db.Close()) })
	return c, db
}

func TestBaselineQualification_AdoptionLocalDeleteNoopAndChainedRekey(t *testing.T) {
	ctx := context.Background()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}},
		`CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED)`,
		`INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','adopted','P')`)
	adopted := qualificationState(t, db, "nodes")
	for _, key := range []string{"P", "Q", "C"} {
		a := qualificationRow(t, adopted, "_sync_row_state", key)
		b := qualificationRow(t, adopted, "_sync_dirty_rows", key)
		require.Equal(t, "unpublished", qualificationText(a, "server_kind"))
		require.Equal(t, "0", qualificationText(a, "row_version"))
		require.Nil(t, qualificationText(a, "server_payload"))
		require.Equal(t, "absent", qualificationText(b, "base_kind"))
		require.Equal(t, "0", qualificationText(b, "base_row_version"))
		require.Nil(t, qualificationText(b, "base_payload"))
		require.Equal(t, "never_published", qualificationPayload(t, b, "base_provenance").(map[string]any)["origin"])
	}
	require.Equal(t, "P", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired' AND child_key=?`, rowKeyJSON("C")))
	require.Equal(t, "1", baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Empty(t, adopted["_sync_outbox_rows"])
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	_, err := c.PushPending(ctx)
	require.NoError(t, err)
	published := qualificationState(t, db, "nodes")
	_, err = db.Exec(`UPDATE nodes SET parent_id='Q',name='first' WHERE id='C'; UPDATE nodes SET name='second' WHERE id='C'`)
	require.NoError(t, err)
	edited := qualificationState(t, db, "nodes")
	b := qualificationRow(t, edited, "_sync_dirty_rows", "C")
	a := qualificationRow(t, published, "_sync_row_state", "C")
	require.Equal(t, published["_sync_row_state"], edited["_sync_row_state"])
	require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, b, "base_payload"))
	require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, b, "base_provenance"))
	require.JSONEq(t, `{"id":{"storage":"text","value":"C"},"name":{"storage":"text","value":"second"},"parent_id":{"storage":"text","value":"Q"}}`, qualificationText(b, "payload").(string))
	_, err = db.Exec(`DELETE FROM nodes WHERE id='C'`)
	require.NoError(t, err)
	deleted := qualificationState(t, db, "nodes")
	d := qualificationRow(t, deleted, "_sync_dirty_rows", "C")
	for _, col := range []string{"base_kind", "base_row_version", "base_payload", "base_provenance", "dirty_ordinal"} {
		require.Equal(t, b[col], d[col], col)
	}
	require.Equal(t, published["_sync_row_state"], deleted["_sync_row_state"])
	require.Equal(t, "DELETE", qualificationText(d, "op"))
	require.Nil(t, qualificationText(d, "payload"))
	require.Equal(t, "P", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("C")))
	require.Empty(t, deleted["_sync_outbox_rows"])
	require.Equal(t, "0", baselineText(t, db, `SELECT count(*) FROM _sync_relationships WHERE view='desired'`))
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	clean := qualificationState(t, db, "nodes")
	requests := len(server.pushRequests)
	_, err = db.Exec(`INSERT INTO nodes VALUES('N','temporary',NULL); DELETE FROM nodes WHERE id='N'`)
	require.NoError(t, err)
	noOp := qualificationState(t, db, "nodes")
	require.Equal(t, "absent", qualificationText(qualificationRow(t, noOp, "_sync_dirty_rows", "N"), "base_kind"))
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	pruned := qualificationState(t, db, "nodes")
	for _, table := range []string{"nodes", "_sync_source_state", "_sync_attachment_state", "_sync_outbox_rows"} {
		require.Equal(t, clean[table], pruned[table], table)
	}
	require.Empty(t, pruned["_sync_dirty_rows"])
	require.Len(t, server.pushRequests, requests)
	_, err = db.Exec(`UPDATE nodes SET id='R' WHERE id='Q'; UPDATE nodes SET id='S' WHERE id='R'`)
	require.NoError(t, err)
	chain := qualificationState(t, db, "nodes")
	require.Equal(t, qualificationRow(t, published, "_sync_row_state", "Q"), qualificationRow(t, chain, "_sync_row_state", "Q"))
	for _, key := range []string{"Q", "R", "S"} {
		dirty := qualificationRow(t, chain, "_sync_dirty_rows", key)
		if key == "Q" {
			require.Equal(t, "present", qualificationText(dirty, "base_kind"))
			require.Equal(t, qualificationPayload(t, qualificationRow(t, published, "_sync_row_state", "Q"), "server_payload"), qualificationPayload(t, dirty, "base_payload"))
			require.Equal(t, qualificationPayload(t, qualificationRow(t, published, "_sync_row_state", "Q"), "server_provenance"), qualificationPayload(t, dirty, "base_provenance"))
			require.Equal(t, "1", qualificationText(dirty, "base_row_version"))
		} else {
			require.Equal(t, "absent", qualificationText(dirty, "base_kind"))
			require.Nil(t, qualificationText(dirty, "base_payload"))
			require.Equal(t, "0", qualificationText(dirty, "base_row_version"))
			require.Equal(t, "never_published", qualificationPayload(t, dirty, "base_provenance").(map[string]any)["origin"])
		}
	}
	require.Equal(t, "DELETE", qualificationText(qualificationRow(t, chain, "_sync_dirty_rows", "R"), "op"))
	// Hold the actual public request before the mock commits any remote effects.
	server.pushHook = func(session *mockDirectPush) *http.Response {
		return jsonResponse(map[string]any{"held_uncommitted": true})
	}
	_, err = c.PushPending(ctx)
	require.Error(t, err)
	frozen := qualificationState(t, db, "nodes")
	require.Len(t, frozen["_sync_outbox_rows"], 2)
	require.Empty(t, frozen["_sync_dirty_rows"])
	for _, table := range []string{"nodes", "_sync_row_state", "_sync_relationships", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state"} {
		require.Equal(t, chain[table], frozen[table], table)
	}
	for column, value := range chain["_sync_conflict_state"][0] {
		if column == "revision" {
			before, parseErr := strconv.Atoi(qualificationText(chain["_sync_conflict_state"][0], column).(string))
			require.NoError(t, parseErr)
			require.Equal(t, strconv.Itoa(before+3), qualificationText(frozen["_sync_conflict_state"][0], column))
		} else {
			require.Equal(t, value, frozen["_sync_conflict_state"][0][column], column)
		}
	}
	for _, key := range []string{"Q", "S"} {
		dirty := qualificationRow(t, chain, "_sync_dirty_rows", key)
		outbound := qualificationRow(t, frozen, "_sync_outbox_rows", key)
		for _, col := range []string{"op", "base_kind", "base_row_version", "base_payload", "base_provenance"} {
			require.Equal(t, dirty[col], outbound[col], key+"/"+col)
		}
		require.Equal(t, dirty["payload"], outbound["local_payload"])
	}
	request := server.pushRequests[requests]
	rows := expandCapturedPushRequest(t, c, request)
	require.Len(t, rows, 2)
	byKey := map[string]oversync.PushRequestRow{}
	for _, row := range rows {
		byKey[row.Key["id"].(string)] = row
	}
	require.Len(t, byKey, 2)
	require.NotContains(t, byKey, "R")
	require.Equal(t, oversync.OpDelete, byKey["Q"].Op)
	require.Equal(t, int64(1), byKey["Q"].BaseRowVersion)
	require.Empty(t, byKey["Q"].Payload)
	require.Equal(t, oversync.OpInsert, byKey["S"].Op)
	require.Zero(t, byKey["S"].BaseRowVersion)
	require.JSONEq(t, `{"id":"S","name":"other","parent_id":null}`, string(byKey["S"].Payload))
	require.JSONEq(t, string(byKey["S"].Payload), qualificationText(qualificationRow(t, frozen, "_sync_outbox_rows", "S"), "wire_payload").(string))
	hash, err := computeGenerationPinnedPushRequestHash(c.schemaGenerationProof, rows)
	require.NoError(t, err)
	require.Equal(t, hash, request.CanonicalRequestHash)
	require.Equal(t, hash, qualificationText(frozen["_sync_outbox_bundle"][0], "canonical_request_hash"))
	require.Equal(t, "2", qualificationText(frozen["_sync_outbox_bundle"][0], "row_count"))
	require.Equal(t, qualificationText(chain["_sync_attachment_state"][0], "current_source_id"), qualificationText(frozen["_sync_outbox_bundle"][0], "source_id"))
	require.Equal(t, int64(3), request.SourceBundleID)
	require.Equal(t, "3", baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Equal(t, "2", baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	require.Len(t, server.committedBySeq, 2)
	require.Equal(t, int64(3), server.nextBundleSeq)
	server.pushHook = nil
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	require.Len(t, server.pushRequests, requests+1, "resolve cancels uncommitted F; no retransmission")
	require.Len(t, server.resolveRequests, 1)
	require.Equal(t, request.CanonicalRequestHash, server.resolveRequests[0].CanonicalRequestHash)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	successor := server.pushRequests[requests+1]
	require.Equal(t, request.SourceBundleID+1, successor.SourceBundleID)
	require.Equal(t, request.CanonicalRequestHash, successor.CanonicalRequestHash)
	require.Equal(t, request.Rows, successor.Rows)
	require.Len(t, server.pushRequests, requests+2)
	settled := qualificationState(t, db, "nodes")
	require.Empty(t, settled["_sync_dirty_rows"])
	require.Empty(t, settled["_sync_outbox_rows"])
	require.Equal(t, "none", qualificationText(settled["_sync_outbox_bundle"][0], "state"))
	require.Equal(t, "0", qualificationText(settled["_sync_outbox_bundle"][0], "row_count"))
	require.Equal(t, "", qualificationText(settled["_sync_outbox_bundle"][0], "canonical_request_hash"))
	require.Equal(t, chain["nodes"], settled["nodes"])
	require.Equal(t, chain["_sync_relationships"], settled["_sync_relationships"])
	require.Equal(t, qualificationRow(t, chain, "_sync_row_state", "R"), qualificationRow(t, settled, "_sync_row_state", "R"))
	require.Equal(t, "deleted", qualificationText(qualificationRow(t, settled, "_sync_row_state", "Q"), "server_kind"))
	require.Equal(t, "3", qualificationText(qualificationRow(t, settled, "_sync_row_state", "Q"), "row_version"))
	require.Equal(t, "3", qualificationText(qualificationRow(t, settled, "_sync_row_state", "S"), "row_version"))
	require.Equal(t, qualificationText(chain["_sync_attachment_state"][0], "current_source_id"), qualificationText(settled["_sync_attachment_state"][0], "current_source_id"))
	require.Equal(t, qualificationPayload(t, qualificationRow(t, chain, "_sync_dirty_rows", "S"), "payload"), qualificationPayload(t, qualificationRow(t, settled, "_sync_row_state", "S"), "server_payload"))
	require.Equal(t, "5", baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Equal(t, "3", baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	remote := map[string]json.RawMessage{}
	for seq := int64(1); seq < server.nextBundleSeq; seq++ {
		for _, row := range server.committedBySeq[seq].Rows {
			key := row.Key["id"].(string)
			require.NotEqual(t, "R", key, "intermediate must never reach remote history")
			if row.Op == oversync.OpDelete {
				delete(remote, key)
			} else {
				remote[key] = row.Payload
			}
		}
	}
	require.Len(t, remote, 2)
	require.NotContains(t, remote, "Q")
	require.NotContains(t, remote, "R")
	require.JSONEq(t, `{"id":"P","name":"parent","parent_id":null}`, string(remote["P"]))
	require.JSONEq(t, `{"id":"S","name":"other","parent_id":null}`, string(remote["S"]))
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
	t.Log("baseline-unpublished-rekey: frozen Q/S only, distinct B/wire/hash, exact replay, counters and remote exclusion verified")
	t.Log("baseline-qualification: full adoption/reparent/delete/noop/per-key chained bases verified")
}

func TestBaselineQualification_NormalizedSettlementExtraEffectAndUnrelatedTrigger(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	server := c.HTTP.Transport.(*mockPushServer)
	initial := qualificationState(t, db, "nodes")
	_, err := db.Exec(`CREATE TRIGGER disturb_p AFTER UPDATE ON nodes WHEN NEW.id='C' AND NEW.name='normalized' BEGIN UPDATE nodes SET name='trigger-disturbance' WHERE id='P'; END; UPDATE nodes SET name='frozen' WHERE id='C'`)
	require.NoError(t, err)
	var frozen qualificationVector
	c.SetBeforePushCommitHook(func(context.Context) error {
		_, e := db.Exec(`UPDATE nodes SET name='newer' WHERE id='C'; UPDATE nodes SET name='affected-local' WHERE id='Q'; UPDATE nodes SET name='unrelated-local' WHERE id='P'`)
		if e == nil {
			frozen = qualificationState(t, db, "nodes")
		}
		return e
	})
	server.pushHook = func(session *mockDirectPush) *http.Response {
		effects := []oversync.BundleRow{
			{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": "C"}, Op: oversync.OpUpdate, RowVersion: 2, Payload: []byte(`{"id":"C","name":"normalized","parent_id":"Q"}`)},
			{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": "Q"}, Op: oversync.OpUpdate, RowVersion: 2, Payload: []byte(`{"id":"Q","name":"extra-effect","parent_id":null}`)},
		}
		committed := &mockCommittedBundle{BundleSeq: 2, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID, PlannedRowCount: session.RequestRowCount, Rows: effects, BundleHash: mustBundleHash(t, effects), ByteCount: mustBundleByteCount(t, effects), CanonicalRequestHash: session.CanonicalRequestHash, Generation: session.Generation}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[2] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	settled := qualificationState(t, db, "nodes")
	require.Equal(t, initial["_sync_row_state"], frozen["_sync_row_state"])
	o := qualificationRow(t, frozen, "_sync_outbox_rows", "C")
	require.Equal(t, qualificationPayload(t, qualificationRow(t, initial, "_sync_row_state", "C"), "server_payload"), qualificationPayload(t, o, "base_payload"))
	require.JSONEq(t, `{"id":"C","name":"frozen","parent_id":"P"}`, qualificationText(o, "wire_payload").(string))
	for _, key := range []string{"C", "Q"} {
		a := qualificationRow(t, settled, "_sync_row_state", key)
		b := qualificationRow(t, settled, "_sync_dirty_rows", key)
		require.Equal(t, "2", qualificationText(a, "row_version"))
		require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, b, "base_payload"))
		require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, b, "base_provenance"))
	}
	require.JSONEq(t, `{"id":{"storage":"text","value":"C"},"name":{"storage":"text","value":"normalized"},"parent_id":{"storage":"text","value":"Q"}}`, qualificationText(qualificationRow(t, settled, "_sync_row_state", "C"), "server_payload").(string))
	require.JSONEq(t, `{"id":{"storage":"text","value":"Q"},"name":{"storage":"text","value":"extra-effect"},"parent_id":{"storage":"null","value":null}}`, qualificationText(qualificationRow(t, settled, "_sync_row_state", "Q"), "server_payload").(string))
	for _, key := range []string{"P", "D"} {
		require.Equal(t, qualificationRow(t, initial, "_sync_row_state", key), qualificationRow(t, settled, "_sync_row_state", key))
	}
	require.Equal(t, qualificationRow(t, frozen, "_sync_dirty_rows", "P"), qualificationRow(t, settled, "_sync_dirty_rows", "P"))
	require.Equal(t, frozen["nodes"], settled["nodes"])
	require.Empty(t, settled["_sync_outbox_rows"])
	require.Equal(t, "P", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired' AND child_key=?`, rowKeyJSON("C")))
	require.Equal(t, "Q", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("C")))
	require.Equal(t, "3", baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Equal(t, "2", baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	require.Len(t, server.pushRequests, 2)
	require.Len(t, server.committedBySeq[2].Rows, 2)
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
	t.Log("baseline-qualification: full normalized/extra-effect A/B/L, unrelated trigger and atomic counters verified")
}

func TestBaselineQualification_DeferredConflictAndExplicitRebaseFullOwners(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='desired' WHERE id='C'; UPDATE nodes SET name='unrelated' WHERE id='Q'`)
	require.NoError(t, err)
	before := qualificationState(t, db, "nodes")
	server := c.HTTP.Transport.(*mockPushServer)
	remote := map[string]any{"id": "C", "name": "remote", "parent_id": "P"}
	server.pushHook = func(s *mockDirectPush) *http.Response {
		for _, r := range s.Rows {
			if r.Key["id"] == "C" {
				return structuredConflictResponse(r, 2, false, remote)
			}
		}
		t.Fatal("missing frozen conflict key")
		return nil
	}
	_, err = c.PushPending(ctx)
	var needs *ConflictNeedsResolutionError
	require.ErrorAs(t, err, &needs)
	pending := qualificationState(t, db, "nodes")
	for _, table := range []string{"nodes", "_sync_row_state", "_sync_attachment_state"} {
		require.Equal(t, before[table], pending[table], table)
	}
	require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Empty(t, pending["_sync_dirty_rows"])
	for _, key := range []string{"C", "Q"} {
		b := qualificationRow(t, before, "_sync_dirty_rows", key)
		o := qualificationRow(t, pending, "_sync_outbox_rows", key)
		for _, col := range []string{"base_kind", "base_row_version", "base_payload", "base_provenance"} {
			require.Equal(t, b[col], o[col], col)
		}
		require.Equal(t, qualificationPayload(t, b, "payload"), qualificationPayload(t, o, "local_payload"))
	}
	handle, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)
	require.JSONEq(t, string(mustJSONPayload(t, remote)), string(handle.Conflict.ServerRowPayload))
	require.NoError(t, c.ResolveConflict(ctx, *handle, KeepLocal{}))
	resolved := qualificationState(t, db, "nodes")
	a := qualificationRow(t, resolved, "_sync_row_state", "C")
	b := qualificationRow(t, resolved, "_sync_dirty_rows", "C")
	require.Equal(t, "2", qualificationText(a, "row_version"))
	require.JSONEq(t, `{"id":{"storage":"text","value":"C"},"name":{"storage":"text","value":"remote"},"parent_id":{"storage":"text","value":"P"}}`, qualificationText(a, "server_payload").(string))
	require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, b, "base_payload"))
	require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, b, "base_provenance"))
	require.Equal(t, qualificationPayload(t, qualificationRow(t, before, "_sync_dirty_rows", "C"), "payload"), qualificationPayload(t, b, "payload"))
	require.Equal(t, qualificationRow(t, before, "_sync_row_state", "Q"), qualificationRow(t, resolved, "_sync_row_state", "Q"))
	for _, col := range []string{"base_kind", "base_row_version", "base_payload", "base_provenance", "payload"} {
		require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", "Q")[col], qualificationRow(t, resolved, "_sync_dirty_rows", "Q")[col], col)
	}
	for _, table := range []string{"nodes", "_sync_attachment_state"} {
		require.Equal(t, before[table], resolved[table], table)
	}
	require.Empty(t, resolved["_sync_outbox_rows"])
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
	t.Log("baseline-qualification: defer preserves frozen owners and explicit KeepLocal rebases only affected intent")
}

func TestBaselineQualification_TypedFullBaseIndependentFromLocalEdits(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "typed", SyncKeyColumnName: "id"}}, `CREATE TABLE typed(id TEXT PRIMARY KEY NOT NULL,n INTEGER NOT NULL,d TEXT NOT NULL,b BLOB NOT NULL,j TEXT NOT NULL,z TEXT)`)
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO typed VALUES('T',9223372036854775807,'1234567890.123456789',X'00ff','{"exact":"9007199254740993","nested":{"x":null}}',NULL)`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	before := qualificationState(t, db, "typed")
	expected := `{"id":{"storage":"text","value":"T"},"n":{"storage":"integer","value":"9223372036854775807"},"d":{"storage":"text","value":"1234567890.123456789"},"b":{"storage":"blob","value":"00ff"},"j":{"storage":"text","value":"{\"exact\":\"9007199254740993\",\"nested\":{\"x\":null}}"},"z":{"storage":"null","value":null}}`
	require.JSONEq(t, expected, qualificationText(qualificationRow(t, before, "_sync_row_state", "T"), "server_payload").(string))
	_, err = db.Exec(`UPDATE typed SET n=1,j='{"replacement":true}' WHERE id='T'`)
	require.NoError(t, err)
	edited := qualificationState(t, db, "typed")
	require.JSONEq(t, expected, qualificationText(qualificationRow(t, edited, "_sync_dirty_rows", "T"), "base_payload").(string))
	require.Equal(t, qualificationPayload(t, qualificationRow(t, before, "_sync_row_state", "T"), "server_provenance"), qualificationPayload(t, qualificationRow(t, edited, "_sync_dirty_rows", "T"), "base_provenance"))
	for _, table := range []string{"_sync_row_state", "_sync_relationships", "_sync_outbox_rows", "_sync_source_state", "_sync_attachment_state"} {
		require.Equal(t, before[table], edited[table], table)
	}
	require.Equal(t, "1", qualificationText(edited["typed"][0], "n"))
	require.Equal(t, `{"replacement":true}`, qualificationText(edited["typed"][0], "j"))
	t.Log("baseline-qualification: complete exact typed base including atomic JSON and SQL null retained")
}

func TestBaselineQualification_OlderPullAndValidatedSnapshotNeverRegressOrFabricate(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c.setBundleApplyModeInTx(ctx, tx, true))
	_, err = tx.Exec(`UPDATE nodes SET name='newer-authority' WHERE id='C'`)
	require.NoError(t, err)
	require.NoError(t, c.installAuthoritativeBaseline(ctx, tx, "main", "nodes", rowKeyJSON("C"), "live", 3, []byte(`{"id":"C","name":"newer-authority","parent_id":"P"}`), baselineObservation{Origin: "explicit_conflict"}))
	require.NoError(t, c.setBundleApplyModeInTx(ctx, tx, false))
	require.NoError(t, tx.Commit())
	newer := qualificationState(t, db, "nodes")
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("C"), Op: oversync.OpUpdate, RowVersion: 2, Payload: []byte(`{"id":"C","name":"older-pull","parent_id":"Q"}`)})}})
	_, err = c.PullToStable(ctx)
	require.NoError(t, err)
	skipped := qualificationState(t, db, "nodes")
	for _, table := range []string{"nodes", "_sync_row_state", "_sync_relationships", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_source_state"} {
		require.Equal(t, newer[table], skipped[table], table)
	}
	require.Equal(t, "2", baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	snapshot := []oversync.SnapshotRow{
		{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": "P"}, RowVersion: 9, Payload: []byte(`{"id":"P","name":"parent","parent_id":null}`)},
		{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": "C"}, RowVersion: 9, Payload: []byte(`{"id":"C","name":"snapshot-complete","parent_id":"P"}`)},
		{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": "D"}, RowVersion: 9, Payload: []byte(`{"id":"D","name":"grandchild","parent_id":"C"}`)},
	}
	options := snapshotApplyOptions{}
	_, err = db.Exec(`UPDATE _sync_attachment_state SET rebuild_required=1`)
	require.NoError(t, err)
	guard, err := c.pinSnapshotApplyGuard(ctx, options)
	require.NoError(t, err)
	options.PinnedGuard = guard
	session := stageUserSnapshotRowsForGate(t, c, "qualification-clean-snapshot", 9, snapshot)
	require.NoError(t, c.applyStagedSnapshotLocked(ctx, session, options))
	complete := qualificationState(t, db, "nodes")
	require.Len(t, complete["nodes"], 3)
	require.Len(t, complete["_sync_row_state"], 4)
	for _, a := range complete["_sync_row_state"] {
		if qualificationText(a, "key_json") == rowKeyJSON("Q") {
			require.Equal(t, "absent", qualificationText(a, "server_kind"))
			require.Equal(t, "0", qualificationText(a, "row_version"))
			continue
		}
		require.Equal(t, "9", qualificationText(a, "row_version"))
		require.Equal(t, "snapshot", qualificationPayload(t, a, "server_provenance").(map[string]any)["origin"])
	}
	require.Equal(t, "snapshot-complete", baselineText(t, db, `SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("C")))
	require.Equal(t, "P", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("C")))
	require.Empty(t, complete["_sync_dirty_rows"])
	require.Empty(t, complete["_sync_outbox_rows"])
	require.Equal(t, skipped["_sync_source_state"], complete["_sync_source_state"])
	require.Equal(t, "9", baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
	t.Log("baseline-qualification: older hash-validated bundle skipped and complete snapshot exclusion established")
}

func TestBaselineQualification_MismatchedPullVersionRejectsWithoutMutation(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	before := qualificationState(t, db, "nodes")
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("C"), Op: oversync.OpUpdate, RowVersion: 3, Payload: []byte(`{"id":"C","name":"future-version","parent_id":"P"}`)})}})
	_, err := c.PullToStable(context.Background())
	require.ErrorContains(t, err, "row_version 3 does not match bundle_seq 2")
	require.Equal(t, before, qualificationState(t, db, "nodes"))
	t.Log("baseline-invalid-pull: mismatched enclosing sequence rejected before any vector mutation")
}

func TestBaselineQualification_FreezeNewerEditRequeueFullOwners(t *testing.T) {
	c, db, path := qualificationDurableClient(t)
	ctx := context.Background()
	before := qualificationState(t, db, "nodes")
	_, err := db.Exec(`UPDATE nodes SET parent_id='Q',name='frozen' WHERE id='C'`)
	require.NoError(t, err)
	dirty := qualificationState(t, db, "nodes")
	b := qualificationRow(t, dirty, "_sync_dirty_rows", "C")
	a := qualificationRow(t, before, "_sync_row_state", "C")
	require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, b, "base_payload"))
	require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, b, "base_provenance"))
	frozen, err := c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	frozenState := qualificationState(t, db, "nodes")
	o := qualificationRow(t, frozenState, "_sync_outbox_rows", "C")
	for _, col := range []string{"base_kind", "base_row_version", "base_payload", "base_provenance"} {
		require.Equal(t, b[col], o[col], col)
	}
	require.Equal(t, qualificationPayload(t, b, "payload"), qualificationPayload(t, o, "local_payload"))
	_, err = db.Exec(`UPDATE nodes SET name='newer' WHERE id='C'`)
	require.NoError(t, err)
	newer := qualificationState(t, db, "nodes")
	require.Equal(t, before["_sync_row_state"], newer["_sync_row_state"])
	require.Equal(t, frozenState["_sync_outbox_rows"], newer["_sync_outbox_rows"])
	require.Equal(t, frozenState["_sync_outbox_bundle"], newer["_sync_outbox_bundle"])
	current := qualificationRow(t, newer, "_sync_dirty_rows", "C")
	require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, current, "base_payload"))
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	require.EqualValues(t, frozen.SourceBundleID+1, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	requeued := qualificationState(t, db, "nodes")
	r := qualificationRow(t, requeued, "_sync_dirty_rows", "C")
	for _, col := range []string{"base_kind", "base_row_version", "base_payload", "base_provenance"} {
		require.Equal(t, o[col], r[col], col)
	}
	require.Equal(t, qualificationPayload(t, current, "payload"), qualificationPayload(t, r, "payload"))
	require.Empty(t, requeued["_sync_outbox_rows"])
	for _, table := range []string{"nodes", "_sync_row_state", "_sync_attachment_state"} {
		require.Equal(t, newer[table], requeued[table], table)
	}
	require.Equal(t, "P", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("C")))
	require.Equal(t, "Q", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired' AND child_key=?`, rowKeyJSON("C")))
	c, db = qualificationFreshProcessRestart(t, c, db, path)
	require.Equal(t, requeued, qualificationState(t, db, "nodes"))
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	settled := qualificationState(t, db, "nodes")
	require.Equal(t, qualificationPayload(t, r, "payload"), qualificationPayload(t, qualificationRow(t, settled, "_sync_row_state", "C"), "server_payload"))
	require.Empty(t, settled["_sync_dirty_rows"])
	require.Empty(t, settled["_sync_outbox_rows"])
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
	t.Log("baseline-qualification: full freeze/newer-edit/requeue vectors survive physical restart and public settlement")
}

func TestBaselineQualification_SourceRecoveryPreservesFrozenBaseAndWire(t *testing.T) {
	ctx := context.Background()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO users VALUES('C','original','old@example.com')`)
	require.NoError(t, err)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	initial := qualificationState(t, db, "users")
	_, err = db.Exec(`UPDATE users SET name='frozen' WHERE id='C'`)
	require.NoError(t, err)
	_, err = c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	frozen := qualificationState(t, db, "users")
	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	attachment.RebuildRequired = true
	require.NoError(t, persistAttachmentState(ctx, db, attachment))
	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindSourceRecovery, Reason: string(SourceRecoveryHistoryPruned), ReplacementSourceID: "qualification-replacement"})
	options := snapshotApplyOptions{RotateSource: true, NewSourceID: "qualification-replacement", ReplacementReason: string(SourceRecoveryHistoryPruned), ClearSourceRecovery: true, RequireFreshRotatedSource: true}
	before := qualificationState(t, db, "users")
	_, err = c.pinSnapshotApplyGuard(ctx, options)
	var pending *UploadJournalIntegrityError
	require.ErrorAs(t, err, &pending)
	recovered := qualificationState(t, db, "users")
	require.Equal(t, before, recovered)
	o := qualificationRow(t, recovered, "_sync_outbox_rows", "C")
	original := qualificationRow(t, frozen, "_sync_outbox_rows", "C")
	for _, column := range []string{"key_json", "wire_key_json", "op", "base_row_version", "base_kind", "base_payload", "base_provenance", "wire_payload", "local_payload", "row_ordinal"} {
		require.Equal(t, original[column], o[column], column)
	}
	require.Equal(t, qualificationPayload(t, qualificationRow(t, initial, "_sync_row_state", "C"), "server_payload"), qualificationPayload(t, o, "base_payload"))
	require.Equal(t, c.sourceID, qualificationText(recovered["_sync_outbox_bundle"][0], "source_id"))
	t.Log("immutable pending upload blocks snapshot/source replacement; full original base, captures, wire and operation identity preserved")
}

func TestBaselineQualification_CleanupAndAccountGuardsPreserveFullOwners(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='pending' WHERE id='C'`)
	require.NoError(t, err)
	dirty := qualificationState(t, db, "nodes")
	detach, err := c.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detach.Outcome)
	require.Equal(t, dirty, qualificationState(t, db, "nodes"))
	_, err = c.Attach(ctx, "another-account")
	require.Error(t, err)
	require.Equal(t, dirty, qualificationState(t, db, "nodes"))
	require.ErrorContains(t, c.UninstallSync(ctx), "unresolved work")
	require.Equal(t, dirty, qualificationState(t, db, "nodes"))
	_, err = c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	frozen := qualificationState(t, db, "nodes")
	detach, err = c.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detach.Outcome)
	require.Equal(t, frozen, qualificationState(t, db, "nodes"))
	require.ErrorContains(t, c.UninstallSync(ctx), "unresolved work")
	require.Equal(t, frozen, qualificationState(t, db, "nodes"))
	_, err = c.PushPending(ctx)
	require.NoError(t, err) // Resolve cancels the locally frozen, undispatched F.
	require.NotZero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	_, err = c.PushPending(ctx)
	require.NoError(t, err) // A new operation publishes the retained intent.
	settled := qualificationState(t, db, "nodes")
	require.JSONEq(t, `{"id":{"storage":"text","value":"C"},"name":{"storage":"text","value":"pending"},"parent_id":{"storage":"text","value":"P"}}`, qualificationText(qualificationRow(t, settled, "_sync_row_state", "C"), "server_payload").(string))
	detach, err = c.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeDetached, detach.Outcome)
	detached := qualificationState(t, db, "nodes")
	for _, table := range []string{"nodes", "_sync_row_state", "_sync_dirty_rows", "_sync_relationships", "_sync_outbox_rows"} {
		require.Empty(t, detached[table], table)
	}
	require.Equal(t, "none", qualificationText(detached["_sync_outbox_bundle"][0], "state"))
	require.Equal(t, "0", qualificationText(detached["_sync_outbox_bundle"][0], "row_count"))
	require.Equal(t, "", qualificationText(detached["_sync_outbox_bundle"][0], "canonical_request_hash"))
	require.Equal(t, settled["_sync_source_state"], detached["_sync_source_state"])
	require.Equal(t, "anonymous", qualificationText(detached["_sync_attachment_state"][0], "binding_state"))
	require.Equal(t, settled["_sync_attachment_state"][0]["last_bundle_seq_seen"], detached["_sync_attachment_state"][0]["last_bundle_seq_seen"])
	// Reattach uses the retained account-specific provenance and cannot reuse deleted A/B.
	attachTestClient(t, c, "another-account", "bundle-device")
	_, err = db.Exec(`INSERT INTO nodes VALUES('U','new-account',NULL)`)
	require.NoError(t, err)
	adopted := qualificationState(t, db, "nodes")
	require.Len(t, adopted["_sync_row_state"], 1)
	b := qualificationRow(t, adopted, "_sync_dirty_rows", "U")
	require.Equal(t, "absent", qualificationText(b, "base_kind"))
	require.Nil(t, qualificationPayload(t, b, "base_payload"))
	require.Equal(t, "another-account", qualificationPayload(t, b, "base_provenance").(map[string]any)["account_scope"].(map[string]any)["account_id"])
	// Empty anonymous cleanup is allowed; uninstall preserves the application's table.
	_, err = db.Exec(`DELETE FROM nodes WHERE id='U'`)
	require.NoError(t, err)
	// Delete of unpublished U is a no-op only after the capture owner freezes/prunes it.
	_, err = c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	detach, err = c.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeDetached, detach.Outcome)
	require.NoError(t, c.UninstallSync(ctx))
	require.Equal(t, "0", baselineText(t, db, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name GLOB '_sync_*'`))
	_, err = db.Exec(`INSERT INTO nodes VALUES('after','local',NULL)`)
	require.NoError(t, err)
	require.Equal(t, "local", baselineText(t, db, `SELECT name FROM nodes WHERE id='after'`))
	t.Log("baseline-qualification: cleanup guards preserve full referenced state and clean account transition/uninstall release metadata")
}

func TestBaselineQualification_PublishedAndFrozenIntermediateRekeyBases(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint("ambiguous=", ambiguous), func(t *testing.T) {
			ctx := context.Background()
			c, db, path := qualificationDurableClient(t)
			// Reuse the seeded server and its sequence/source receipt identities.
			server := c.HTTP.Transport.(*mockPushServer)
			_, err := db.Exec(`UPDATE nodes SET id='R' WHERE id='Q'`)
			require.NoError(t, err)
			if ambiguous {
				server.pushHook = func(session *mockDirectPush) *http.Response {
					server.pushHook = nil
					response := server.defaultPushResponse(session)
					require.NoError(t, response.Body.Close())
					response.Body = io.NopCloser(strings.NewReader("{"))
					return response
				}
			}
			_, err = c.PushPending(ctx)
			if ambiguous {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			intermediate := qualificationState(t, db, "nodes")
			_, err = db.Exec(`UPDATE nodes SET id='S' WHERE id='R'`)
			require.NoError(t, err)
			chained := qualificationState(t, db, "nodes")
			if ambiguous {
				require.Equal(t, intermediate["_sync_outbox_rows"], chained["_sync_outbox_rows"])
				require.Equal(t, intermediate["_sync_outbox_bundle"], chained["_sync_outbox_bundle"])
			} else {
				r := qualificationRow(t, chained, "_sync_dirty_rows", "R")
				a := qualificationRow(t, intermediate, "_sync_row_state", "R")
				require.Equal(t, "DELETE", qualificationText(r, "op"))
				require.Equal(t, "present", qualificationText(r, "base_kind"))
				require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, r, "base_payload"))
				require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, r, "base_provenance"))
			}
			c, db = qualificationPhysicalRestart(t, c, db, path)
			if ambiguous {
				_, err = c.PushPending(ctx)
				require.NoError(t, err)
				require.Len(t, server.pushRequests, 2)
				require.Len(t, server.resolveRequests, 1)
				ack := qualificationState(t, db, "nodes")
				r := qualificationRow(t, ack, "_sync_dirty_rows", "R")
				a := qualificationRow(t, ack, "_sync_row_state", "R")
				require.Equal(t, "present", qualificationText(r, "base_kind"))
				require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, r, "base_payload"))
				require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, r, "base_provenance"))
				require.Equal(t, chained["nodes"], ack["nodes"])
			}
			_, err = c.PushPending(ctx)
			require.NoError(t, err)
			complete := qualificationState(t, db, "nodes")
			require.Equal(t, "deleted", qualificationText(qualificationRow(t, complete, "_sync_row_state", "R"), "server_kind"))
			require.JSONEq(t, `{"id":{"storage":"text","value":"S"},"name":{"storage":"text","value":"other"},"parent_id":{"storage":"null","value":null}}`, qualificationText(qualificationRow(t, complete, "_sync_row_state", "S"), "server_payload").(string))
			require.Empty(t, complete["_sync_dirty_rows"])
			require.Empty(t, complete["_sync_outbox_rows"])
			require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
			t.Log("baseline-qualification: distinct published/frozen intermediate B and exact physical replay established")
		})
	}
}

func TestBaselineQualification_IndependentOldDeleteCommitSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	c, db, path := qualificationDurableClient(t)
	c.config.UploadLimit = 1
	original := qualificationState(t, db, "nodes")
	// Publish the independent successor first through ordinary captures. No
	// hidden rename pairing or old table-order heuristic determines this order.
	_, err := db.Exec(`INSERT INTO nodes SELECT 'R',name,parent_id FROM nodes WHERE id='Q'; DELETE FROM nodes WHERE id='Q'`)
	require.NoError(t, err)
	before := qualificationState(t, db, "nodes")
	require.Equal(t, qualificationPayload(t, qualificationRow(t, original, "_sync_row_state", "Q"), "server_payload"), qualificationPayload(t, qualificationRow(t, before, "_sync_dirty_rows", "Q"), "base_payload"))
	c, db = qualificationFreshProcessRestart(t, c, db, path)
	// R has the earlier dirty ordinal; each independent operation retains its B.
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	intermediate := qualificationState(t, db, "nodes")
	require.Equal(t, "live", qualificationText(qualificationRow(t, intermediate, "_sync_row_state", "R"), "server_kind"))
	c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(ctx)
	require.ErrorIs(t, err, context.Canceled)
	c.SetBeforePushReplayHook(nil)
	frozen, err := c.loadPushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, frozen.Rows, 1)
	require.Equal(t, rowKeyJSON("Q"), frozen.Rows[0].KeyJSON)
	require.Equal(t, "DELETE", frozen.Rows[0].Op)
	_, err = db.Exec(`UPDATE nodes SET id='S' WHERE id='R'`)
	require.NoError(t, err)
	chained := qualificationState(t, db, "nodes")
	c, db = qualificationFreshProcessRestart(t, c, db, path)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	committed := qualificationState(t, db, "nodes")
	require.Equal(t, "deleted", qualificationText(qualificationRow(t, committed, "_sync_row_state", "Q"), "server_kind"))
	r := qualificationRow(t, committed, "_sync_dirty_rows", "R")
	a := qualificationRow(t, intermediate, "_sync_row_state", "R")
	require.Equal(t, "present", qualificationText(r, "base_kind"))
	require.Equal(t, qualificationPayload(t, a, "server_payload"), qualificationPayload(t, r, "base_payload"))
	require.Equal(t, qualificationPayload(t, a, "server_provenance"), qualificationPayload(t, r, "base_provenance"))
	successor := qualificationRow(t, committed, "_sync_dirty_rows", "S")
	require.Equal(t, "INSERT", qualificationText(successor, "op"))
	require.Equal(t, "absent", qualificationText(successor, "base_kind"))
	require.Nil(t, qualificationPayload(t, successor, "base_payload"))
	require.Empty(t, committed["_sync_outbox_rows"])
	require.Equal(t, chained["nodes"], committed["nodes"])
	c, db = qualificationFreshProcessRestart(t, c, db, path)
	require.Equal(t, committed, qualificationState(t, db, "nodes"))
	c.config.UploadLimit = 200
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	settled := qualificationState(t, db, "nodes")
	require.Equal(t, "live", qualificationText(qualificationRow(t, settled, "_sync_row_state", "S"), "server_kind"))
	require.Equal(t, "deleted", qualificationText(qualificationRow(t, settled, "_sync_row_state", "R"), "server_kind"))
	require.Empty(t, settled["_sync_dirty_rows"])
	require.Empty(t, settled["_sync_outbox_rows"])
	t.Log("baseline-qualification: before-freeze and independently frozen/acknowledged old deletion preserve distinct R/S bases across actual reopen")
}

func TestBaselineQualification_PhysicalAndFreshProcessPreserveCompleteVector(t *testing.T) {
	c, db, dbPath := qualificationDurableClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='frozen' WHERE id='C'`)
	require.NoError(t, err)
	_, err = c.ensurePushOutboundSnapshot(context.Background())
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nodes SET name='newer' WHERE id='C'; UPDATE nodes SET id='R' WHERE id='Q'; UPDATE nodes SET id='S' WHERE id='R'`)
	require.NoError(t, err)
	c, db = qualificationFreshProcessRestart(t, c, db, dbPath)
	require.NoError(t, validateCompleteRetainedRuntime(context.Background(), db))
	t.Log("baseline-qualification: physical writer and fresh process vectors verified")
}

func qualificationFreshProcessRestart(t *testing.T, c *Client, db *sql.DB, dbPath string) (*Client, *sql.DB) {
	t.Helper()
	expected := qualificationState(t, db, "nodes")
	dir := t.TempDir()
	vectorPath := filepath.Join(dir, "expected.json")
	encoded, err := json.Marshal(expected)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(vectorPath, encoded, 0600))
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
	physical, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	require.NoError(t, err)
	physical.SetMaxOpenConns(1)
	require.Equal(t, expected, qualificationState(t, physical, "nodes"))
	require.NoError(t, validateCompleteRetainedRuntime(context.Background(), physical))
	require.NoError(t, physical.Close())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBaselineQualification_ProcessReader$", "-test.v")
	command.Env = append(os.Environ(), "OVERSQLITE_QUALIFICATION_DB="+dbPath, "OVERSQLITE_QUALIFICATION_VECTOR="+vectorPath)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "baseline-process: complete durable vector verified")
	t.Log(string(output))
	reopened, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	require.NoError(t, err)
	reopened.SetMaxOpenConns(1)
	next, err := newAdmittedFixtureClient(reopened, c.BaseURL, c.Token, c.config)
	require.NoError(t, err)
	attachTestClient(t, next, c.UserID, "bundle-device")
	next.HTTP = c.HTTP
	require.Equal(t, expected, qualificationState(t, reopened, "nodes"))
	t.Cleanup(func() { require.NoError(t, next.Close()); require.NoError(t, reopened.Close()) })
	return next, reopened
}

func TestBaselineQualification_ProcessReader(t *testing.T) {
	dbPath := os.Getenv("OVERSQLITE_QUALIFICATION_DB")
	if dbPath == "" {
		t.Skip("child process helper; executed by owning parent")
	}
	raw, err := os.ReadFile(os.Getenv("OVERSQLITE_QUALIFICATION_VECTOR"))
	require.NoError(t, err)
	var expected qualificationVector
	require.NoError(t, json.Unmarshal(raw, &expected))
	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	require.Equal(t, expected, qualificationState(t, db, "nodes"))
	require.NoError(t, validateCompleteRetainedRuntime(context.Background(), db))
	t.Log("baseline-process: complete durable vector verified")
}
