package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	lose atomic.Bool
}

func (transport *baselineCommittedResponseLoss) RoundTrip(request *http.Request) (*http.Response, error) {
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

// baselineRecoveryRequest is one request a source-recovery rebuild sent, with the body of a snapshot
// session request so the test can see which source it asked to replace.
type baselineRecoveryRequest struct {
	Method, Path string
	Body         []byte
}

// baselineSourceRecoveryTransport loses one committed push response and, while refuseSettlement is set,
// answers every upload operation of the old source the way a server answers a source it cannot sequence.
type baselineSourceRecoveryTransport struct {
	baselineCommittedResponseLoss
	refuseSettlement atomic.Bool
	mu               sync.Mutex
	recorded         []baselineRecoveryRequest
}

func (transport *baselineSourceRecoveryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body []byte
	if request.URL.Path == "/sync/snapshot-sessions" && request.Body != nil {
		var err error
		body, err = io.ReadAll(request.Body)
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		request = request.Clone(request.Context())
		request.Body = io.NopCloser(bytes.NewReader(body))
	}
	transport.mu.Lock()
	transport.recorded = append(transport.recorded, baselineRecoveryRequest{request.Method, request.URL.Path, body})
	transport.mu.Unlock()
	if transport.refuseSettlement.Load() && (request.URL.Path == "/sync/push" || strings.HasPrefix(request.URL.Path, "/sync/push-outcomes/")) {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		refusal := `{"error":"source_sequence_changed","message":"source sequence cannot continue"}`
		return &http.Response{
			StatusCode: http.StatusConflict, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(refusal)), ContentLength: int64(len(refusal)), Request: request,
		}, nil
	}
	return transport.baselineCommittedResponseLoss.RoundTrip(request)
}

// take returns the requests recorded since the previous call and starts a new recording.
func (transport *baselineSourceRecoveryTransport) take() []baselineRecoveryRequest {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	taken := transport.recorded
	transport.recorded = nil
	return taken
}

func baselineRequestPaths(requests []baselineRecoveryRequest) []string {
	paths := make([]string, 0, len(requests))
	for _, request := range requests {
		paths = append(paths, request.Method+" "+request.Path)
	}
	return paths
}

// baselineFrozenCommittedFixture is a client whose second upload F committed on the server while its
// response was lost, so F stays frozen locally with no outcome, and whose old source then needs recovery.
type baselineFrozenCommittedFixture struct {
	authority   *TestServer
	client      *oversqlite.Client
	db          *sql.DB
	transport   *baselineSourceRecoveryTransport
	id          string
	oldSource   string
	replacement string
	frozenHash  string
}

func newBaselineFrozenCommittedFixture(t *testing.T, ctx context.Context, beforeRecovery func(*sql.DB)) *baselineFrozenCommittedFixture {
	t.Helper()
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
	transport := &baselineSourceRecoveryTransport{}
	client.HTTP = &http.Client{Transport: transport}
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	f := &baselineFrozenCommittedFixture{authority: authority, client: client, db: client.Database(), transport: transport, id: uuid.NewString()}
	_, err = f.db.Exec(`INSERT INTO categories VALUES(?,'original',NULL)`, f.id)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE categories SET name='frozen' WHERE id=?`, f.id)
	require.NoError(t, err)
	transport.lose.Store(true)
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.False(t, transport.lose.Load())
	var bundleID int64
	require.NoError(t, f.db.QueryRow(`SELECT source_id,source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle`).Scan(&f.oldSource, &bundleID, &f.frozenHash))
	require.EqualValues(t, 2, bundleID)
	require.Len(t, f.frozenHash, 64)
	require.Equal(t, "frozen", f.remoteName(t, ctx, f.id), "F committed on the server")
	require.Equal(t, 1, f.retainedOutcomes(t, ctx, f.oldSource), "the server retains F's unacknowledged outcome")
	if beforeRecovery != nil {
		beforeRecovery(f.db)
	}
	// The client records that its source cannot continue (r0001 section 6.4 "client-recorded equivalent").
	f.replacement = uuid.NewString()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO _sync_source_state(source_id) VALUES(?)`, f.replacement)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE _sync_attachment_state SET rebuild_required=1`)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE _sync_operation_state SET kind='source_recovery',reason='source_sequence_changed',replacement_source_id=?`, f.replacement)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	transport.take()
	return f
}

func (f *baselineFrozenCommittedFixture) remoteName(t *testing.T, ctx context.Context, id string) string {
	t.Helper()
	var name string
	require.NoError(t, f.authority.Pool.QueryRow(ctx, `SELECT name FROM business.categories WHERE id=$1`, id).Scan(&name))
	return name
}

func (f *baselineFrozenCommittedFixture) retainedOutcomes(t *testing.T, ctx context.Context, source string) int {
	t.Helper()
	var count int
	require.NoError(t, f.authority.Pool.QueryRow(ctx, `SELECT count(*) FROM sync.push_outcomes WHERE source_id=$1`, source).Scan(&count))
	return count
}

func (f *baselineFrozenCommittedFixture) serverSourceState(t *testing.T, ctx context.Context, source string) (state, replacedBy string) {
	t.Helper()
	err := f.authority.Pool.QueryRow(ctx, `SELECT state,replaced_by_source_id FROM sync.source_state WHERE source_id=$1`, source).Scan(&state, &replacedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ""
	}
	require.NoError(t, err)
	return state, replacedBy
}

func (f *baselineFrozenCommittedFixture) localInt(t *testing.T, query string, args ...any) int {
	t.Helper()
	var count int
	require.NoError(t, f.db.QueryRow(query, args...).Scan(&count))
	return count
}

// requireSnapshotReplacement checks that the request asks the server to replace the old source.
func (f *baselineFrozenCommittedFixture) requireSnapshotReplacement(t *testing.T, request baselineRecoveryRequest) {
	t.Helper()
	var body struct {
		SourceReplacement struct {
			PreviousSourceID string `json:"previous_source_id"`
			NewSourceID      string `json:"new_source_id"`
			Reason           string `json:"reason"`
		} `json:"source_replacement"`
	}
	require.NoError(t, json.Unmarshal(request.Body, &body))
	require.Equal(t, f.oldSource, body.SourceReplacement.PreviousSourceID)
	require.Equal(t, f.replacement, body.SourceReplacement.NewSourceID)
	require.Equal(t, "source_sequence_changed", body.SourceReplacement.Reason)
}

// r0001 section 6.4 steps 2-4: an explicit Rebuild out of source recovery first gives the old source
// ordinary settlement of its committed frozen upload, then installs the snapshot under the replacement
// source and carries the edit made after the freeze, which the replacement then uploads.
func TestBaselineLive_SourceRecoveryRebuildSettlesFrozenUploadBeforeReplacement(t *testing.T) {
	ctx := context.Background()
	carried := uuid.NewString()
	f := newBaselineFrozenCommittedFixture(t, ctx, func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO categories VALUES(?,'after-freeze',NULL)`, carried)
		require.NoError(t, err)
	})

	report, err := f.client.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedSnapshot, report.Outcome)

	requests := f.transport.take()
	paths := baselineRequestPaths(requests)
	resolve := slices.Index(paths, "POST /sync/push-outcomes/resolve")
	ack := slices.Index(paths, "POST /sync/push-outcomes/ack")
	snapshot := slices.Index(paths, "POST /sync/snapshot-sessions")
	require.True(t, resolve >= 0 && ack > resolve && snapshot > ack, "settlement under the old source precedes the replacement snapshot: %v", paths)
	require.NotContains(t, paths, "POST /sync/push", "the frozen request is resolved, never re-sent: %v", paths)
	f.requireSnapshotReplacement(t, requests[snapshot])

	// F settled by its real committed outcome; the server released the old source only after the ack.
	require.Zero(t, f.retainedOutcomes(t, ctx, f.oldSource))
	state, replacedBy := f.serverSourceState(t, ctx, f.oldSource)
	require.Equal(t, "retired", state)
	require.Equal(t, f.replacement, replacedBy)
	require.Zero(t, f.localInt(t, `SELECT count(*) FROM _sync_upload_journal`))
	require.Zero(t, f.localInt(t, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Equal(t, 1, f.localInt(t, `SELECT count(*) FROM _sync_outbox_bundle WHERE state='none'`))
	var current, kind string
	var rebuildRequired, checkpoint int
	require.NoError(t, f.db.QueryRow(`SELECT current_source_id,rebuild_required,last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&current, &rebuildRequired, &checkpoint))
	require.Equal(t, f.replacement, current)
	require.Zero(t, rebuildRequired)
	require.Equal(t, 2, checkpoint, "the checkpoint moves to the snapshot boundary, F's commit")
	require.NoError(t, f.db.QueryRow(`SELECT kind FROM _sync_operation_state`).Scan(&kind))
	require.Equal(t, "none", kind)
	var name, serverKind, serverName string
	var version int64
	require.NoError(t, f.db.QueryRow(`SELECT name FROM categories WHERE id=?`, f.id).Scan(&name))
	require.Equal(t, "frozen", name)
	require.NoError(t, f.db.QueryRow(`SELECT server_kind,row_version,json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE json_extract(key_json,'$.id')=?`, f.id).Scan(&serverKind, &version, &serverName))
	require.Equal(t, "live", serverKind)
	require.EqualValues(t, 2, version, "the snapshot observes the row as written by F's commit, the scope's second bundle sequence")
	require.Equal(t, "frozen", serverName)

	// The edit made after the freeze is carried as ordinary dirty intent: the snapshot lacks the row and
	// the edit's base was absent, so it is an INSERT with base 0.
	var op string
	var base int64
	require.NoError(t, f.db.QueryRow(`SELECT op,base_row_version FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id')=?`, carried).Scan(&op, &base))
	require.Equal(t, "INSERT", op)
	require.Zero(t, base)
	require.Equal(t, 1, f.localInt(t, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Equal(t, 1, f.localInt(t, `SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, f.replacement))

	pushed, err := f.client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, pushed.Outcome)
	require.Equal(t, "after-freeze", f.remoteName(t, ctx, carried))
	require.Equal(t, "frozen", f.remoteName(t, ctx, f.id))
	var committedSourceBundle int64
	require.NoError(t, f.authority.Pool.QueryRow(ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state WHERE source_id=$1`, f.replacement).Scan(&committedSourceBundle))
	require.EqualValues(t, 1, committedSourceBundle, "the carried intent is the replacement source's first bundle")
	require.Zero(t, f.localInt(t, `SELECT count(*) FROM _sync_dirty_rows`))
}

// r0001 section 6.4 step 3: when settlement under the old source is refused, the server still retains
// F's committed outcome and refuses the replacement with push_outcome_pending. The rebuild fails with
// every local owner intact, and discard stays unavailable because nothing proves F has no outcome.
func TestBaselineLive_SourceReplacementRefusedWhileOldSourceHoldsOutcome(t *testing.T) {
	ctx := context.Background()
	f := newBaselineFrozenCommittedFixture(t, ctx, nil)
	before := baselineLiveVector(t, f.db)

	f.transport.refuseSettlement.Store(true)
	_, err := f.client.Rebuild(ctx)
	require.ErrorContains(t, err, "push_outcome_pending")
	requests := f.transport.take()
	paths := baselineRequestPaths(requests)
	require.Equal(t, []string{"POST /sync/push-outcomes/resolve", "POST /sync/snapshot-sessions"}, paths,
		"one refused settlement attempt, then the replacement request the server refuses")
	f.requireSnapshotReplacement(t, requests[1])
	require.Equal(t, before, baselineLiveVector(t, f.db), "a refused replacement changes no local byte")
	var source, hash string
	var bundleID int64
	require.NoError(t, f.db.QueryRow(`SELECT source_id,source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle`).Scan(&source, &bundleID, &hash))
	require.Equal(t, f.oldSource, source)
	require.EqualValues(t, 2, bundleID)
	require.Equal(t, f.frozenHash, hash)
	require.Equal(t, 1, f.retainedOutcomes(t, ctx, f.oldSource), "the server still retains F's outcome")
	state, _ := f.serverSourceState(t, ctx, f.oldSource)
	require.Equal(t, "active", state)
	state, _ = f.serverSourceState(t, ctx, f.replacement)
	require.Empty(t, state, "the refused replacement creates no server source")

	_, err = f.client.DiscardPendingIntentForRecovery(ctx)
	var unavailable *oversqlite.PendingIntentDiscardUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.Empty(t, f.transport.take(), "a refused discard sends nothing")
	require.Equal(t, before, baselineLiveVector(t, f.db), "a refused discard changes no local byte")

	f.transport.refuseSettlement.Store(false)
	report, err := f.client.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	paths = baselineRequestPaths(f.transport.take())
	ack := slices.Index(paths, "POST /sync/push-outcomes/ack")
	require.True(t, slices.Index(paths, "POST /sync/push-outcomes/resolve") >= 0 && ack > slices.Index(paths, "POST /sync/push-outcomes/resolve"), "%v", paths)
	require.True(t, slices.Index(paths, "POST /sync/snapshot-sessions") > ack, "%v", paths)
	require.NotContains(t, paths, "POST /sync/push", "the frozen request is resolved, never re-sent: %v", paths)
	require.Zero(t, f.retainedOutcomes(t, ctx, f.oldSource))
	require.Zero(t, f.localInt(t, `SELECT count(*) FROM _sync_upload_journal`))
	require.Zero(t, f.localInt(t, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Equal(t, 1, f.localInt(t, `SELECT count(*) FROM _sync_attachment_state WHERE current_source_id=?`, f.replacement))
	var name string
	require.NoError(t, f.db.QueryRow(`SELECT name FROM categories WHERE id=?`, f.id).Scan(&name))
	require.Equal(t, "frozen", name)
}
