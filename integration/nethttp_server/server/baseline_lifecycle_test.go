package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestBaselineLive_LostResponsePhysicalReopenAndSettlement(t *testing.T) {
	ctx := context.Background()
	authority, err := NewA1SurvivorTestServer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, authority.CloseWithError()) })
	user := "baseline-replay-" + uuid.NewString()
	token, err := authority.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	var loseResponse atomic.Bool
	var duringResponse func()
	var requestBodies [][]byte
	var controlRequests []baselineProtocolRequest
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sync/push-outcomes/resolve" || r.URL.Path == "/sync/push-outcomes/ack" {
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil {
				t.Errorf("read outcome control request: %v", readErr)
				w.WriteHeader(500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			controlRequests = append(controlRequests, baselineProtocolRequest{r.URL.Path, r.Method, r.Header.Clone(), body})
		}
		if r.URL.Path != "/sync/push" {
			authority.Handler.ServeHTTP(w, r)
			return
		}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			http.Error(w, "test request read failed", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		requestBodies = append(requestBodies, body)
		recorder := httptest.NewRecorder()
		authority.Handler.ServeHTTP(recorder, r)
		if loseResponse.Swap(false) {
			if recorder.Code != http.StatusOK {
				t.Errorf("fault injection requires committed response, got %d", recorder.Code)
			}
			duringResponse()
			connection, _, hijackErr := w.(http.Hijacker).Hijack()
			if hijackErr != nil {
				t.Errorf("test hijack failed: %v", hijackErr)
				return
			}
			_ = connection.Close()
			return
		}
		for name, values := range recorder.Header() {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	}))
	t.Cleanup(proxy.Close)
	path := filepath.Join(t.TempDir(), "baseline-replay.sqlite")
	config := defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
	config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	open := func() *oversqlite.Client {
		return newReadyOnlyTestClient(t, path, proxy.URL, func(context.Context) (string, error) { return token, nil }, config,
			`CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`)
	}
	client := open()
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	p, q, c := uuid.NewString(), uuid.NewString(), uuid.NewString()
	db := client.Database()
	_, err = db.Exec(`INSERT INTO categories VALUES(?,'parent',NULL),(?,'other',NULL),(?,'original',?)`, p, q, c, p)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	var original string
	require.NoError(t, db.QueryRow(`SELECT server_payload FROM _sync_row_state WHERE json_extract(key_json,'$.id')=?`, c).Scan(&original))
	_, err = db.Exec(`UPDATE categories SET name='frozen',parent_id=? WHERE id=?`, q, c)
	require.NoError(t, err)
	duringResponse = func() {
		_, editErr := db.Exec(`UPDATE categories SET name='newer' WHERE id=?`, c)
		if editErr != nil {
			t.Errorf("test concurrent capture failed: %v", editErr)
		}
	}
	loseResponse.Store(true)
	_, err = client.PushPending(ctx)
	require.Error(t, err, "response loss must leave a replayable outbox")
	var base, wire, hash, source string
	var nextID, checkpoint int64
	require.NoError(t, db.QueryRow(`SELECT base_payload,wire_payload FROM _sync_outbox_rows`).Scan(&base, &wire))
	require.JSONEq(t, original, base)
	var aProvenance, bProvenance, dirtyBase, dirtyProvenance string
	require.NoError(t, db.QueryRow(`SELECT server_provenance FROM _sync_row_state WHERE json_extract(key_json,'$.id')=?`, c).Scan(&aProvenance))
	require.NoError(t, db.QueryRow(`SELECT base_provenance FROM _sync_outbox_rows`).Scan(&bProvenance))
	require.NoError(t, db.QueryRow(`SELECT base_payload,base_provenance FROM _sync_dirty_rows`).Scan(&dirtyBase, &dirtyProvenance))
	require.JSONEq(t, aProvenance, bProvenance)
	require.JSONEq(t, original, dirtyBase)
	require.JSONEq(t, aProvenance, dirtyProvenance)
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&hash))
	require.Len(t, hash, 64)
	require.NoError(t, db.QueryRow(`SELECT source_id,next_source_bundle_id FROM _sync_source_state`).Scan(&source, &nextID))
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	var frozenBody []byte
	var frozenDigest, frozenPhase, retainedOutcomeHash string
	require.NoError(t, db.QueryRow(`SELECT request_body,request_digest,phase FROM _sync_upload_journal`).Scan(&frozenBody, &frozenDigest, &frozenPhase))
	require.Equal(t, requestBodies[1], frozenBody)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(frozenBody)), frozenDigest)
	require.Equal(t, "frozen", frozenPhase)
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT outcome_hash FROM sync.push_outcomes WHERE source_id=$1`, source).Scan(&retainedOutcomeHash))
	before := baselineLiveVector(t, db)
	var remoteName, remoteParent string
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT name,parent_id::text FROM business.categories WHERE id=$1`, c).Scan(&remoteName, &remoteParent))
	require.Equal(t, "frozen", remoteName)
	require.Equal(t, q, remoteParent)
	require.NoError(t, client.Close())
	client = open()
	db = client.Database()
	require.Equal(t, before, baselineLiveVector(t, db))
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	require.Len(t, requestBodies, 2, "uncertain F resolves its original tuple without retransmitting push")
	require.Equal(t, frozenBody, requestBodies[1])
	require.Len(t, controlRequests, 3) // first commit ack, original F resolve, original C ack
	resolved, acknowledged := controlRequests[1], controlRequests[2]
	require.Equal(t, "/sync/push-outcomes/resolve", resolved.Path)
	require.Equal(t, "/sync/push-outcomes/ack", acknowledged.Path)
	for _, request := range []baselineProtocolRequest{resolved, acknowledged} {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, oversync.SyncProtocolVersion, request.Header.Get(oversync.ProtocolVersionHeader))
		require.Equal(t, source, request.Header.Get(oversync.SourceIDHeader))
		require.Equal(t, "1", request.Header.Get(oversync.SchemaGenerationIDHeader))
		require.NotEmpty(t, request.Header.Get(oversync.SchemaGenerationFingerprintHeader))
	}
	require.Equal(t, resolved.Header.Get(oversync.SchemaGenerationFingerprintHeader), acknowledged.Header.Get(oversync.SchemaGenerationFingerprintHeader))
	require.JSONEq(t, fmt.Sprintf(`{"source_bundle_id":%d,"canonical_request_hash":%q,"initialization_id":null}`, nextID, hash), string(resolved.Body))
	require.JSONEq(t, fmt.Sprintf(`{"source_bundle_id":%d,"canonical_request_hash":%q,"outcome_hash":%q}`, nextID, hash, retainedOutcomeHash), string(acknowledged.Body))
	var a, b, l, serverParent, desiredParent, newSource string
	var settledNext, settledCheckpoint int64
	require.NoError(t, db.QueryRow(`SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE json_extract(key_json,'$.id')=?`, c).Scan(&a))
	require.NoError(t, db.QueryRow(`SELECT json_extract(base_payload,'$.name.value') FROM _sync_dirty_rows`).Scan(&b))
	require.NoError(t, db.QueryRow(`SELECT name FROM categories WHERE id=?`, c).Scan(&l))
	require.Equal(t, "frozen", a)
	require.Equal(t, "frozen", b)
	require.Equal(t, "newer", l)
	var settledA, settledB, settledAProvenance, settledBProvenance string
	require.NoError(t, db.QueryRow(`SELECT server_payload,server_provenance FROM _sync_row_state WHERE json_extract(key_json,'$.id')=?`, c).Scan(&settledA, &settledAProvenance))
	require.NoError(t, db.QueryRow(`SELECT base_payload,base_provenance FROM _sync_dirty_rows`).Scan(&settledB, &settledBProvenance))
	require.JSONEq(t, settledA, settledB)
	require.JSONEq(t, settledAProvenance, settledBProvenance)
	require.NoError(t, db.QueryRow(`SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server'`).Scan(&serverParent))
	require.NoError(t, db.QueryRow(`SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired'`).Scan(&desiredParent))
	require.Equal(t, q, serverParent)
	require.Equal(t, q, desiredParent)
	require.NoError(t, db.QueryRow(`SELECT source_id,next_source_bundle_id FROM _sync_source_state`).Scan(&newSource, &settledNext))
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&settledCheckpoint))
	require.Equal(t, source, newSource)
	require.Equal(t, nextID+1, settledNext)
	require.Equal(t, checkpoint+1, settledCheckpoint)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT name,parent_id::text FROM business.categories WHERE id=$1`, c).Scan(&remoteName, &remoteParent))
	require.Equal(t, "frozen", remoteName)
	require.Equal(t, q, remoteParent)
	t.Logf("physical replay preserved full vector; frozen wire=%s hash=%s; settled=%s", wire, hash, baselineLiveVector(t, db))
}

// Baseline qualification uses the existing public opening and live authority.
// Planner ordering and committed-prefix reconstruction are deliberately absent.
func TestBaselineLive_ConfirmedTombstoneReinsertion(t *testing.T) {
	ctx := context.Background()
	authority, err := NewA1SurvivorTestServer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, authority.CloseWithError()) })
	user := "baseline-" + uuid.NewString()
	token, err := authority.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "baseline.sqlite")
	config := defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
	open := func() *oversqlite.Client {
		return newReadyOnlyTestClient(t, path, authority.URL(), func(context.Context) (string, error) { return token, nil }, config,
			`CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`)
	}
	client := open()
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	id := uuid.NewString()
	db := client.Database()
	_, err = db.Exec(`INSERT INTO categories VALUES(?,'original',NULL)`, id)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM categories WHERE id=?`, id)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	key, err := json.Marshal(map[string]string{"id": id})
	require.NoError(t, err)
	var tombstone int64
	var kind string
	require.NoError(t, db.QueryRow(`SELECT server_kind,row_version FROM _sync_row_state WHERE key_json=?`, string(key)).Scan(&kind, &tombstone))
	require.Equal(t, "deleted", kind)
	require.Positive(t, tombstone)
	var remoteCount int
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT count(*) FROM business.categories WHERE id=$1`, id).Scan(&remoteCount))
	require.Zero(t, remoteCount)
	_, err = db.Exec(`INSERT INTO categories VALUES(?,'reinserted',NULL)`, id)
	require.NoError(t, err)
	var baseKind string
	var baseVersion int64
	require.NoError(t, db.QueryRow(`SELECT base_kind,base_row_version FROM _sync_dirty_rows WHERE key_json=?`, string(key)).Scan(&baseKind, &baseVersion))
	require.Equal(t, "deleted", baseKind)
	require.Equal(t, tombstone, baseVersion)
	var deletionProvenance, reinsertProvenance string
	require.NoError(t, db.QueryRow(`SELECT server_provenance FROM _sync_row_state WHERE key_json=?`, string(key)).Scan(&deletionProvenance))
	require.NoError(t, db.QueryRow(`SELECT base_provenance FROM _sync_dirty_rows WHERE key_json=?`, string(key)).Scan(&reinsertProvenance))
	require.JSONEq(t, deletionProvenance, reinsertProvenance)
	before := baselineLiveVector(t, db)
	t.Logf("reinsertion before physical close: %s", before)
	require.NoError(t, client.Close()) // ready owner closes its physical database pool
	client = open()
	db = client.Database()
	require.Equal(t, before, baselineLiveVector(t, db))
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err, "confirmed tombstone must admit public live reinsertion")
	var name string
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT name FROM business.categories WHERE id=$1`, id).Scan(&name))
	require.Equal(t, "reinserted", name)
	var version int64
	require.NoError(t, db.QueryRow(`SELECT server_kind,row_version,json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json=?`, string(key)).Scan(&kind, &version, &name))
	require.Equal(t, "live", kind)
	require.Greater(t, version, tombstone)
	require.Equal(t, "reinserted", name)
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_relationships"} {
		var count int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count)
	}
	var outboxState string
	require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle`).Scan(&outboxState))
	require.Equal(t, "none", outboxState)
	t.Logf("reinsertion settled: %s", baselineLiveVector(t, db))
}

// A complete deterministic persisted owner vector compares actual connection
// restart bytes, including provenance, both relationship views and wire/hash.
func baselineLiveVector(t *testing.T, db *sql.DB) string {
	t.Helper()
	values := map[string][]json.RawMessage{}
	tables, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND (name='categories' OR substr(name,1,6)='_sync_') ORDER BY name`)
	require.NoError(t, err)
	var names []string
	for tables.Next() {
		var name string
		require.NoError(t, tables.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, tables.Err())
	require.NoError(t, tables.Close())
	require.Contains(t, names, "_sync_upload_journal")
	for _, table := range names {
		rows, err := db.Query(`PRAGMA table_info("` + table + `")`)
		require.NoError(t, err)
		var columns []string
		for rows.Next() {
			var id, notnull, pk int
			var name, kind string
			var defaultValue any
			require.NoError(t, rows.Scan(&id, &name, &kind, &notnull, &defaultValue, &pk))
			columns = append(columns, name)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.NotEmpty(t, columns, table)
		var projections, ordering []string
		for _, col := range columns {
			q := `"` + col + `"`
			ordering = append(ordering, q)
			projections = append(projections, fmt.Sprintf(`'%s',json_object('storage',typeof(%s),'value',CASE WHEN typeof(%s)='blob' THEN lower(hex(%s)) WHEN %s IS NULL THEN NULL ELSE CAST(%s AS TEXT) END)`, col, q, q, q, q, q))
		}
		rows, err = db.Query(`SELECT json_object(` + strings.Join(projections, ",") + `) FROM "` + table + `" ORDER BY ` + strings.Join(ordering, ","))
		require.NoError(t, err)
		values[table] = []json.RawMessage{}
		for rows.Next() {
			var raw string
			require.NoError(t, rows.Scan(&raw))
			values[table] = append(values[table], json.RawMessage(raw))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	encoded, err := json.Marshal(values)
	require.NoError(t, err)
	return string(encoded)
}

type baselineProtocolRequest struct {
	Path, Method string
	Header       http.Header
	Body         []byte
}

type baselineCommittedResponseLoss struct {
	lose     atomic.Bool
	requests atomic.Int64
}

func (transport *baselineCommittedResponseLoss) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.requests.Add(1)
	response, err := http.DefaultTransport.RoundTrip(request)
	if err == nil && request.URL.Path == "/sync/push" && transport.lose.Swap(false) {
		if response.StatusCode != http.StatusOK {
			return response, nil
		}
		_, err = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if err != nil {
			return nil, err
		}
		return nil, io.ErrUnexpectedEOF
	}
	return response, err
}

func TestBaselineLive_SourceRecoverySnapshotPreservesFrozenOwners(t *testing.T) {
	ctx := context.Background()
	authority, err := NewA1SurvivorTestServer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, authority.CloseWithError()) })
	user := "baseline-recovery-" + uuid.NewString()
	token, err := authority.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	config := defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
	config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	client := newReadyOnlyTestClient(t, filepath.Join(t.TempDir(), "recovery.sqlite"), authority.URL(), func(context.Context) (string, error) { return token, nil }, config, `CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	transport := &baselineCommittedResponseLoss{}
	client.HTTP = &http.Client{Transport: transport}
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	id := uuid.NewString()
	db := client.Database()
	_, err = db.Exec(`INSERT INTO categories VALUES(?,'original',NULL)`, id)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	var aPayload, aProvenance string
	require.NoError(t, db.QueryRow(`SELECT server_payload,server_provenance FROM _sync_row_state`).Scan(&aPayload, &aProvenance))
	_, err = db.Exec(`UPDATE categories SET name='frozen' WHERE id=?`, id)
	require.NoError(t, err)
	transport.lose.Store(true)
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.False(t, transport.lose.Load())
	var base, provenance, wire, local, hash string
	require.NoError(t, db.QueryRow(`SELECT base_payload,base_provenance,wire_payload,local_payload FROM _sync_outbox_rows`).Scan(&base, &provenance, &wire, &local))
	require.JSONEq(t, aPayload, base)
	require.JSONEq(t, aProvenance, provenance)
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&hash))
	require.Len(t, hash, 64)
	var remote string
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT name FROM business.categories WHERE id=$1`, id).Scan(&remote))
	require.Equal(t, "frozen", remote)
	replacement := uuid.NewString()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO _sync_source_state(source_id) VALUES(?)`, replacement)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE _sync_attachment_state SET rebuild_required=1`)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE _sync_operation_state SET kind='source_recovery',reason='source_sequence_changed',replacement_source_id=?`, replacement)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	before := baselineLiveVector(t, db)
	requestsBefore := transport.requests.Load()
	_, err = client.Rebuild(ctx)
	var unresolved *oversqlite.UploadJournalIntegrityError
	require.ErrorAs(t, err, &unresolved)
	require.Equal(t, before, baselineLiveVector(t, db), "snapshot must preserve every frozen owner byte")
	require.Equal(t, requestsBefore, transport.requests.Load(), "unresolved F/C refuses before HTTP")
	var newBase, newProvenance, newWire, newLocal, newHash, source, kind, name string
	var version, next, checkpoint, bundleID int64
	require.NoError(t, db.QueryRow(`SELECT base_payload,base_provenance,wire_payload,local_payload,source_bundle_id FROM _sync_outbox_rows`).Scan(&newBase, &newProvenance, &newWire, &newLocal, &bundleID))
	require.Equal(t, base, newBase)
	require.Equal(t, provenance, newProvenance)
	require.Equal(t, wire, newWire)
	require.Equal(t, local, newLocal)
	require.Equal(t, int64(2), bundleID, "the original frozen operation is never rebound to replacement sequence1")
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash,source_id FROM _sync_outbox_bundle`).Scan(&newHash, &source))
	require.Equal(t, hash, newHash)
	require.NotEqual(t, replacement, source)
	settlement, err := client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, settlement.Outcome)
	for _, table := range []string{"_sync_upload_journal", "_sync_outbox_rows", "_sync_dirty_rows"} {
		var count int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count)
	}
	_, err = client.Rebuild(ctx)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT server_kind,row_version,json_extract(server_payload,'$.name.value') FROM _sync_row_state`).Scan(&kind, &version, &name))
	require.Equal(t, "live", kind)
	require.Equal(t, int64(2), version)
	require.Equal(t, "frozen", name)
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, replacement).Scan(&next))
	require.Equal(t, int64(1), next)
	require.NoError(t, db.QueryRow(`SELECT current_source_id,last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&source, &checkpoint))
	require.Equal(t, replacement, source)
	require.Equal(t, int64(2), checkpoint)
	for _, table := range []string{"_sync_dirty_rows", "_sync_relationships"} {
		var count int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count)
	}
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT name FROM business.categories WHERE id=$1`, id).Scan(&remote))
	require.Equal(t, "frozen", remote)
	_, err = db.Exec(`UPDATE categories SET name='replacement-write' WHERE id=?`, id)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT name FROM business.categories WHERE id=$1`, id).Scan(&remote))
	require.Equal(t, "replacement-write", remote)
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, replacement).Scan(&next))
	require.EqualValues(t, 2, next, "fresh replacement write consumes its own first sequence")
	t.Log("baseline-live: original F/C preserved through refusal and settled before clean source replacement; fresh replacement write converged")
	t.Log(baselineLiveVector(t, db))
}
