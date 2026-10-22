package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

type convergentTraceTransport struct {
	base   http.RoundTripper
	mu     sync.Mutex
	pushes int
	pulls  int
}

func (t *convergentTraceTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	if request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
		t.pushes++
	}
	if request.Method == http.MethodGet && request.URL.Path == "/sync/pull" {
		t.pulls++
	}
	t.mu.Unlock()
	return t.base.RoundTrip(request)
}

func TestMobileFlow_DirectSyncDrainsSeveralAdmittedBatchesOnce(t *testing.T) {
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range serverpkg.SchemaGenerationsForBusinessSchema("business")[0].Tables {
		if table.Table == "users" {
			generation.Tables = append(generation.Tables, table)
		}
	}
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		JWTSecret:         "direct-sync-multi-batch-secret",
		SchemaGenerations: []oversync.SchemaGeneration{generation},
		MaxRowsPerBundle:  2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("start isolated PostgreSQL server: %v", err)
	}
	defer func() {
		if err := server.CloseWithError(); err != nil {
			t.Errorf("clean isolated PostgreSQL server: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	userID := "direct-sync-" + uuid.NewString()
	writerDB, writer := newNetHTTPWatchClient(t, server, userID, false)
	followerDB, follower := newNetHTTPWatchClient(t, server, userID, false)
	trace := &convergentTraceTransport{base: http.DefaultTransport}
	writer.HTTP = &http.Client{Transport: trace}
	var rowIDs []string
	for i := 0; i < 5; i++ {
		id := uuid.NewString()
		rowIDs = append(rowIDs, id)
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		_, err := writerDB.ExecContext(ctx, `INSERT INTO users (id,name,email,created_at,updated_at) VALUES (?,?,?,?,?)`,
			id, fmt.Sprintf("Direct %d", i), fmt.Sprintf("direct%d@example.com", i), stamp, stamp)
		if err != nil {
			t.Fatalf("insert local row %d: %v", i, err)
		}
	}
	// This is the only writer Sync call. Neither a caller drain nor a retry loop assists it.
	report, err := writer.Sync(ctx)
	if err != nil {
		t.Fatalf("one direct Sync call: %v", err)
	}
	if report.PushOutcome != oversqlite.PushOutcomeCommitted {
		t.Fatalf("push outcome = %s", report.PushOutcome)
	}
	if report.Status.Pending.PendingRowCount != 0 {
		t.Fatalf("pending rows = %d", report.Status.Pending.PendingRowCount)
	}
	trace.mu.Lock()
	pushes, pulls := trace.pushes, trace.pulls
	trace.mu.Unlock()
	if pushes != 3 || pulls != 1 {
		t.Fatalf("writer trace: pushes=%d pulls=%d; want 3 and 1", pushes, pulls)
	}
	if _, err := follower.PullToStable(ctx); err != nil {
		t.Fatalf("follower pull: %v", err)
	}
	for i, id := range rowIDs {
		var name string
		if err := followerDB.QueryRowContext(ctx, `SELECT name FROM users WHERE id=?`, id).Scan(&name); err != nil {
			t.Fatalf("follower row %d: %v", i, err)
		}
		if want := fmt.Sprintf("Direct %d", i); name != want {
			t.Fatalf("follower row %d name = %q, want %q", i, name, want)
		}
	}
	var count int
	if err := writerDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("dirty rows after direct Sync = %d", count)
	}
	if err := writerDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("outbox rows after direct Sync = %d", count)
	}
}

func TestMobileFlow_DirectSyncRecoveryPreservesIntent(t *testing.T) {
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range serverpkg.SchemaGenerationsForBusinessSchema("business")[0].Tables {
		if table.Table == "users" {
			generation.Tables = append(generation.Tables, table)
		}
	}
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		JWTSecret: "direct-sync-cancellation-secret", SchemaGenerations: []oversync.SchemaGeneration{generation},
		MaxRowsPerBundle: 2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.CloseWithError(); err != nil {
			t.Errorf("clean isolated authority: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	userID := "cancelled-sync-" + uuid.NewString()
	token, err := server.GenerateToken(userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	config := oversqlite.DefaultConfig(1, []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	config.DatabaseID, config.ServiceID = "cancelled-sync", "cancelled-sync"
	config.LocalVersion, config.MaxSyncWorkUnitsPerInvocation = 1, 1
	path := filepath.Join(t.TempDir(), "client.sqlite")
	ddl := `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,email TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`
	getToken := func(context.Context) (string, error) { return token, nil }
	client := openWatchClient(t, path, server.URL(), getToken, config, ddl)
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Attach(ctx, userID); err != nil {
		t.Fatal(err)
	}
	for row := range 5 {
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := client.Database().ExecContext(ctx, `INSERT INTO users VALUES(?,?,?,?,?)`, uuid.NewString(),
			fmt.Sprintf("Cancelled %d", row), fmt.Sprintf("cancelled%d@example.com", row), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	interrupted, stop := context.WithCancel(ctx)
	defer stop()
	pushes := 0
	resolves, snapshots := 0, 0
	var cancelledAck bool
	var frozenBody []byte
	var frozenSource string
	var frozenRows map[string]string
	successorRows := make(map[string]string)
	transport := watchRoundTripFunc(func(request *http.Request) (response *http.Response, resultErr error) {
		defer func() {
			if resultErr != nil {
				t.Logf("transport boundary %s: %v", request.URL.Path, resultErr)
			}
		}()
		if request.URL.Path == "/sync/snapshot-sessions" {
			snapshots++
		}
		if request.URL.Path == "/sync/push-outcomes/resolve" {
			resolves++
			if request.Header.Get(oversync.SourceIDHeader) != frozenSource {
				return nil, fmt.Errorf("resolve changed original source")
			}
			raw, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			if err := request.Body.Close(); err != nil {
				return nil, err
			}
			request.Body = io.NopCloser(bytes.NewReader(raw))
			var frozen, resolve map[string]json.RawMessage
			if err := json.Unmarshal(frozenBody, &frozen); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(raw, &resolve); err != nil {
				return nil, err
			}
			for _, key := range []string{"source_bundle_id", "canonical_request_hash"} {
				if !bytes.Equal(frozen[key], resolve[key]) {
					return nil, fmt.Errorf("resolve changed original %s", key)
				}
			}
			t.Logf("reached resolve original source=%s tuple=%s", request.Header.Get("Oversync-Source-ID"), raw)
		}
		if request.URL.Path == "/sync/push-outcomes/ack" && resolves > 0 && pushes == 2 {
			cancelledAck = true
		}
		if request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
			pushes++
			if pushes >= 2 {
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				if err := request.Body.Close(); err != nil {
					return nil, err
				}
				request.Body = io.NopCloser(bytes.NewReader(raw))
				decoded := raw
				if request.Header.Get("Content-Encoding") == "gzip" {
					reader, err := gzip.NewReader(bytes.NewReader(raw))
					if err != nil {
						return nil, err
					}
					decoded, err = io.ReadAll(reader)
					cleanup := reader.Close()
					if err := errors.Join(err, cleanup); err != nil {
						return nil, err
					}
				}
				if pushes == 2 {
					frozenBody = decoded
					frozenSource = request.Header.Get(oversync.SourceIDHeader)
					frozenRows, err = directSyncCompactIntentRows(decoded)
					if err != nil {
						return nil, err
					}
					stop()
					return nil, interrupted.Err()
				}
				var frozen, successor map[string]json.RawMessage
				if err := json.Unmarshal(frozenBody, &frozen); err != nil {
					return nil, err
				}
				if err := json.Unmarshal(decoded, &successor); err != nil {
					return nil, err
				}
				// Cancellation consumes F's sequence and restores its intent to
				// ordinary dirty scheduling. The remaining independent row may
				// share the next batch; compare every original row across all
				// successors, without imposing the old frozen batch boundary.
				var originalSequence, successorSequence int64
				if err := json.Unmarshal(frozen["source_bundle_id"], &originalSequence); err != nil {
					return nil, err
				}
				if err := json.Unmarshal(successor["source_bundle_id"], &successorSequence); err != nil {
					return nil, err
				}
				if !cancelledAck || successorSequence <= originalSequence || request.Header.Get(oversync.SourceIDHeader) != frozenSource {
					return nil, fmt.Errorf("successor must follow original cancellation acknowledgment on the original source with a new sequence")
				}
				preserved, err := directSyncCompactIntentRows(decoded)
				if err != nil {
					return nil, err
				}
				for key, row := range preserved {
					if _, duplicate := successorRows[key]; duplicate {
						return nil, fmt.Errorf("successor repeated row %s", key)
					}
					successorRows[key] = row
				}
			}
		}
		return http.DefaultTransport.RoundTrip(request)
	})
	client.HTTP = &http.Client{Transport: transport}
	if _, err := client.Sync(interrupted); !errors.Is(err, context.Canceled) {
		t.Fatalf("second push cancellation = %v", err)
	}
	var source, hash string
	var bundle, rows, pending, reservations int64
	if err := client.Database().QueryRowContext(ctx, `SELECT source_id,source_bundle_id,canonical_request_hash,row_count FROM _sync_outbox_bundle`).Scan(&source, &bundle, &hash, &rows); err != nil {
		t.Fatal(err)
	}
	var journalRequest []byte
	var frozenCaptures string
	const capturesSQL = `SELECT json_group_array(json_array(source_bundle_id,row_ordinal,schema_name,table_name,key_json,wire_key_json,op,base_row_version,base_kind,base_payload,base_provenance,local_payload,wire_payload)) FROM (SELECT * FROM _sync_outbox_rows ORDER BY row_ordinal)`
	if err := client.Database().QueryRowContext(ctx, `SELECT request_body FROM _sync_upload_journal`).Scan(&journalRequest); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frozenBody, journalRequest) || source != frozenSource {
		t.Fatal("retained journal differs from the actual original request/source")
	}
	if err := client.Database().QueryRowContext(ctx, capturesSQL).Scan(&frozenCaptures); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || hash == "" || len(frozenBody) == 0 {
		t.Fatalf("retained second bundle: rows=%d hash=%q body=%d", rows, hash, len(frozenBody))
	}
	if err := client.Database().QueryRowContext(ctx, `SELECT count(*) FROM _sync_dirty_rows`).Scan(&pending); err != nil || pending+rows != 3 {
		t.Fatalf("first bundle published once: dirty=%d frozen=%d err=%v", pending, rows, err)
	}
	if err := client.Database().QueryRowContext(ctx, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("cancelled operation released reservations: %d err=%v", reservations, err)
	}
	followerDB, follower := newNetHTTPWatchClient(t, server, userID, false)
	if _, err := follower.PullToStable(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := followerDB.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("only first bundle confirmed before reopen: count=%d err=%v", count, err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	client = openWatchClient(t, path, server.URL(), getToken, config, ddl)
	if _, err := client.Attach(ctx, userID); err != nil {
		t.Fatalf("reattach reopened caller: %v", err)
	}
	var reopenedSource, reopenedHash string
	var reopenedBundle int64
	if err := client.Database().QueryRowContext(ctx, `SELECT source_id,source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle`).Scan(&reopenedSource, &reopenedBundle, &reopenedHash); err != nil {
		t.Fatal(err)
	}
	if source != reopenedSource || bundle != reopenedBundle || hash != reopenedHash {
		t.Fatal("close/reopen changed frozen second-bundle identity")
	}
	var reopenedRequest []byte
	var reopenedCaptures string
	if err := client.Database().QueryRowContext(ctx, `SELECT request_body FROM _sync_upload_journal`).Scan(&reopenedRequest); err != nil {
		t.Fatal(err)
	}
	if err := client.Database().QueryRowContext(ctx, capturesSQL).Scan(&reopenedCaptures); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(journalRequest, reopenedRequest) || frozenCaptures != reopenedCaptures {
		t.Fatal("physical reopen changed original bytes, bases or captures")
	}
	client.HTTP = &http.Client{Transport: transport}
	report, err := client.Sync(ctx)
	if err != nil || report.Status.Pending.PendingRowCount != 0 || pushes != 4 {
		t.Fatalf("reopened Sync: report=%+v pushes=%d err=%v", report, pushes, err)
	}
	if len(successorRows) != 3 {
		t.Fatalf("successors retained %d rows; want original three pending rows", len(successorRows))
	}
	for key, row := range frozenRows {
		if successorRows[key] != row {
			t.Fatalf("successor changed original key/op/base/payload: %s", key)
		}
	}
	if resolves != 1 || snapshots != 0 || !cancelledAck {
		t.Fatalf("recovery lifecycle resolve=%d snapshot=%d cancellation acknowledged=%v", resolves, snapshots, cancelledAck)
	}
	if _, err := follower.PullToStable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := followerDB.QueryRowContext(ctx, `SELECT count(*) FROM users WHERE name LIKE 'Cancelled %'`).Scan(&count); err != nil || count != 5 {
		t.Fatalf("authority readback count=%d err=%v", count, err)
	}
	if err := client.Database().QueryRowContext(ctx, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("resumed operation released reservations: %d err=%v", reservations, err)
	}
}

// Ignore transport group ordinals when comparing the same immutable row intent
// across newly scheduled successor bundles. Keep table, key, op, base and values.
func directSyncCompactIntentRows(body []byte) (map[string]string, error) {
	var request oversync.CompactPushRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, group := range request.Rows {
		for _, row := range group.Rows {
			key, err := json.Marshal(row.KeyValues)
			if err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			identity := fmt.Sprintf("%d:%s", group.TableIndex, key)
			if _, exists := result[identity]; exists {
				return nil, fmt.Errorf("duplicate compact intent key %s", identity)
			}
			result[identity] = string(encoded)
		}
	}
	return result, nil
}
