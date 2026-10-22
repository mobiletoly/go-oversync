// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/stretchr/testify/require"
)

type outcomeFixture struct {
	*directPushFixture
	cfg    *ServiceConfig
	server *managedIntegrationPostgresServer
}

func outcomeService(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg *ServiceConfig) *SyncService {
	t.Helper()
	svc, err := newTestRuntimeService(pool, cfg, integrationTestLogger(4))
	require.NoError(t, err)
	baseline := svc.config.Readiness.Adapter
	svc.config.Readiness.Expected.WriteRejections = []ApplicationWriteRejection{{SQLState: "P0001", Category: "application_rejected"}}
	svc.config.Readiness.Adapter = readinessAdapterFunc(func(ctx context.Context, r DatabaseReadinessReader) (ReadinessEvidence, error) {
		e, err := baseline.Inspect(ctx, r)
		e.Deployment.WriteRejections = svc.config.Readiness.Expected.WriteRejections
		return e, err
	})
	require.NoError(t, svc.Bootstrap(ctx))
	t.Cleanup(func() { require.NoError(t, svc.Close(context.Background())) })
	return svc
}
func outcomeImages(t *testing.T, run func(*testing.T, context.Context, *outcomeFixture)) {
	t.Helper()
	for _, image := range []struct{ name, tag string }{{"pg16", "postgres:16"}, {"pg17_10", "postgres:17.10"}} {
		t.Run(image.name, func(t *testing.T) {
			server, err := startManagedIntegrationPostgres(image.tag)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.close()) })
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			pool := newIntegrationTestPoolForServer(t, ctx, server)
			schema := "outcome_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
			tables := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}, {Schema: schema, Table: "posts", SyncKeyColumns: []string{"id"}}}
			cfg := &ServiceConfig{AppName: "outcome-test", SchemaGenerations: testSchemaGenerationsForRegisteredTables(tables), RegisteredTables: tables, RegisteredTableCatalogRevision: 1, ReservedServerSourceIDs: []string{"server-writer"}, MaxBytesPerBundle: 16384, MaxBytesPerSnapshotRow: 16384, RetainedBundlesPerUser: 1}
			svc := outcomeService(t, ctx, pool, cfg)
			actor := Actor{UserID: "scope-" + schema, SourceID: "writer"}
			mustInitializeEmptyScope(t, ctx, svc, actor.UserID, actor.SourceID)
			f := &outcomeFixture{directPushFixture: &directPushFixture{pool: pool, svc: svc, schemaName: schema, writer: actor, reader: Actor{UserID: actor.UserID, SourceID: "reader"}}, cfg: cfg, server: server}
			resolveConnectForPush(t, ctx, svc, f.reader, false)
			var version string
			var fsync, fullPage, synchronous string
			require.NoError(t, pool.QueryRow(ctx, `SELECT version(),current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit')`).Scan(&version, &fsync, &fullPage, &synchronous))
			require.Equal(t, "on", fsync)
			require.Equal(t, "on", fullPage)
			require.Equal(t, "on", synchronous)
			digest, err := dockerOutput(ctx, "inspect", "--format", "{{.Image}}", server.containerID)
			require.NoError(t, err)
			t.Log("POSTGRES", image.tag, strings.TrimSpace(digest), version, "durability", fsync, fullPage, synchronous)
			run(t, ctx, f)
		})
	}
}
func (f *outcomeFixture) request(t *testing.T, seq int64, rows ...PushRequestRow) *OneShotPushRequest {
	t.Helper()
	hash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(f.svc), rows)
	require.NoError(t, err)
	return &OneShotPushRequest{SourceBundleID: seq, CanonicalRequestHash: hash, Rows: rows}
}
func (f *outcomeFixture) ack(t *testing.T, ctx context.Context, actor Actor, o *PushOutcome) {
	t.Helper()
	require.NoError(t, f.svc.AcknowledgePushOutcome(ctx, actor, testSchemaGenerationProofForService(f.svc), PushOutcomeAckRequest{SourceBundleID: o.SourceBundleID, CanonicalRequestHash: o.CanonicalRequestHash, OutcomeHash: o.OutcomeHash}))
}
func outcomeCode(t *testing.T, err error, code string) {
	t.Helper()
	var p *PushOutcomeError
	require.ErrorAs(t, err, &p)
	require.Equal(t, code, p.Code)
}
func (f *outcomeFixture) count(t *testing.T, ctx context.Context, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, f.pool.QueryRow(ctx, sql, args...).Scan(&n))
	return n
}
func (f *outcomeFixture) query(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	_, err := f.pool.Exec(ctx, sql, args...)
	require.NoError(t, err)
}
func (f *outcomeFixture) http(t *testing.T, ctx context.Context, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)).WithContext(ContextWithActor(ctx, f.writer))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(ProtocolVersionHeader, "v1")
	proof := testSchemaGenerationProofForService(f.svc)
	r.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
	r.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
	w := httptest.NewRecorder()
	h := NewHTTPSyncHandlers(f.svc, nil)
	switch path {
	case "/sync/push":
		h.HandlePush(w, r)
	case "/sync/push-outcomes/resolve":
		h.HandlePushOutcomeResolve(w, r)
	case "/sync/push-outcomes/ack":
		h.HandlePushOutcomeAck(w, r)
	default:
		t.Fatal(path)
	}
	return w
}

// These requests traverse the real resolve/ack handlers and must leave the
// retained bytes and progress intact until the caller sends a valid ack.
func (f *outcomeFixture) checkControlBoundaries(t *testing.T, ctx context.Context, stored []byte) {
	t.Helper()
	h := NewHTTPSyncHandlers(f.svc, nil)
	proof := testSchemaGenerationProofForService(f.svc)
	for _, endpoint := range []struct {
		name    string
		handler http.HandlerFunc
	}{{"resolve", h.HandlePushOutcomeResolve}, {"ack", h.HandlePushOutcomeAck}} {
		for _, tc := range []struct {
			name, body, encoding, protocol, media string
			status                                int
		}{
			{"missing members", `{}`, "", "v1", "application/json", 400},
			{"duplicate members", `{"source_bundle_id":1,"source_bundle_id":1}`, "", "v1", "application/json", 400},
			{"unknown member", `{"unexpected":true}`, "", "v1", "application/json", 400},
			{"trailing value", `{} {}`, "", "v1", "application/json", 400},
			{"body bound", strings.Repeat(" ", int(OutcomeControlBodyLimit)+1), "", "v1", "application/json", 413},
			{"gzip decoded bound", strings.Repeat(" ", int(OutcomeControlBodyLimit)+1), "gzip", "v1", "application/json", 413},
			{"gzip encoded bound", strings.Repeat(" ", int(OutcomeControlBodyLimit)), "gzip-uncompressed", "v1", "application/json", 413},
			{"media type", `{}`, "", "v1", "text/plain", 415},
			{"protocol", `{}`, "", "v2", "application/json", 426},
		} {
			t.Run(endpoint.name+"/"+tc.name, func(t *testing.T) {
				body := []byte(tc.body)
				encoding := tc.encoding
				if strings.HasPrefix(encoding, "gzip") {
					var zipped bytes.Buffer
					level := gzip.DefaultCompression
					if encoding == "gzip-uncompressed" {
						level = gzip.NoCompression
					}
					z, err := gzip.NewWriterLevel(&zipped, level)
					require.NoError(t, err)
					_, err = z.Write(body)
					require.NoError(t, err)
					require.NoError(t, z.Close())
					body = zipped.Bytes()
					encoding = "gzip"
				}
				r := httptest.NewRequest(http.MethodPost, "/sync/push-outcomes/"+endpoint.name, bytes.NewReader(body)).WithContext(ContextWithActor(ctx, f.writer))
				r.Header.Set(ProtocolVersionHeader, tc.protocol)
				r.Header.Set("Content-Type", tc.media)
				if encoding != "" {
					r.Header.Set("Content-Encoding", encoding)
				}
				r.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
				r.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
				w := httptest.NewRecorder()
				endpoint.handler(w, r)
				require.Equal(t, tc.status, w.Code, w.Body.String())
			})
		}
	}
	var after []byte
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT envelope FROM sync.push_outcomes WHERE source_id=$1`, f.writer.SourceID).Scan(&after))
	require.Equal(t, stored, after)
	require.Zero(t, f.count(t, ctx, `SELECT acknowledged_sequence FROM sync.push_source_progress WHERE source_id='writer'`))
}

func TestPushOutcome_AtomicCommit(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		derived := uuid.NewString()
		f.query(t, ctx, fmt.Sprintf(`CREATE TABLE %s.audit(n int); CREATE FUNCTION %s.normalize_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.name:=lower(NEW.name); RETURN NEW; END; $$; CREATE TRIGGER normalize_effect BEFORE INSERT ON %s.users FOR EACH ROW EXECUTE FUNCTION %s.normalize_effect(); CREATE FUNCTION %s.expand_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='seed' THEN INSERT INTO %s.audit VALUES(1); INSERT INTO %s.users VALUES(NEW._sync_scope_id,'%s','derived','derived@example.com'); END IF; RETURN NEW; END; $$; CREATE TRIGGER expand_effect AFTER INSERT ON %s.users FOR EACH ROW EXECUTE FUNCTION %s.expand_effect()`, f.schemaName, f.schemaName, f.schemaName, f.schemaName, f.schemaName, f.schemaName, f.schemaName, derived, f.schemaName, f.schemaName))
		req := f.request(t, 1, f.userRow(uuid.New(), "SEED"))
		result, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), req)
		require.NoError(t, err)
		require.Equal(t, "committed", result.Outcome.Status)
		require.EqualValues(t, 2, result.Outcome.Committed.RowCount)
		hash, err := HashPushOutcome(*result.Outcome, generationDescriptor(f.svc.generationViews[1].contract))
		require.NoError(t, err)
		require.Equal(t, result.Outcome.OutcomeHash, hash)
		require.EqualValues(t, 2, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
		require.EqualValues(t, 1, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_log`))
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_capture_stage`))
		replay, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), req)
		require.NoError(t, err)
		require.Equal(t, result.encoded, replay.encoded)
		invalid := f.userRow(uuid.New(), "invalid")
		invalid.Op = "INVALID"
		for _, rows := range [][]PushRequestRow{nil, {f.userRow(uuid.New(), "changed")}, {invalid}} {
			changed := *req
			changed.Rows = rows
			_, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), &changed)
			require.Error(t, err, "direct replay must validate its logical request")
		}

		require.EqualValues(t, 1, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.audit`, f.schemaName)))
		resolved, err := f.svc.ResolvePushOutcome(ctx, f.writer, testSchemaGenerationProofForService(f.svc), pushOperationRequest(req))
		require.NoError(t, err)
		require.Equal(t, result.encoded, resolved.encoded)
		groups, err := CompactGroupsForRows(generationDescriptor(f.svc.generationViews[1].contract), req.Rows)
		require.NoError(t, err)
		httpResult := f.http(t, ctx, "/sync/push", CompactPushRequest{SourceBundleID: 1, CanonicalRequestHash: req.CanonicalRequestHash, Rows: groups})
		require.Equal(t, 200, httpResult.Code, httpResult.Body.String())
		require.Equal(t, result.encoded, httpResult.Body.Bytes())
		var stored []byte
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT envelope FROM sync.push_outcomes`).Scan(&stored))
		require.Equal(t, result.encoded, stored)
		f.checkControlBoundaries(t, ctx, stored)

		_, err = f.pool.Exec(ctx, `UPDATE sync.push_outcomes SET envelope='broken'`)
		require.Error(t, err)
		stats, err := f.svc.PushOutcomeStorage(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, stats.Outstanding)
		require.EqualValues(t, len(stored), stats.PayloadBytes)
		require.Positive(t, stats.PhysicalBytes)
		require.GreaterOrEqual(t, stats.OldestAge, time.Duration(0))
		ack := f.http(t, ctx, "/sync/push-outcomes/ack", PushOutcomeAckRequest{SourceBundleID: 1, CanonicalRequestHash: result.Outcome.CanonicalRequestHash, OutcomeHash: result.Outcome.OutcomeHash})
		require.Equal(t, 204, ack.Code, ack.Body.String())
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		require.EqualValues(t, 1, f.count(t, ctx, `SELECT acknowledged_sequence FROM sync.push_source_progress WHERE source_id='writer'`))
	})
}
func TestPushOutcome_RejectionRollback(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		initial := Actor{UserID: f.writer.UserID + "-initial", SourceID: "initializer"}
		conn, err := f.svc.Connect(ctx, initial, testSchemaGenerationProofForService(f.svc), &ConnectRequest{HasLocalPendingRows: true})
		require.NoError(t, err)
		require.Equal(t, "initialize_local", conn.Resolution)
		var before string
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT row_to_json(s)::text FROM sync.scope_state s JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1`, initial.UserID).Scan(&before))
		f.query(t, ctx, fmt.Sprintf(`CREATE TABLE %s.audit(n int); CREATE FUNCTION %s.reject_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='reject' THEN INSERT INTO %s.audit VALUES(1); RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='private application details'; ELSIF NEW.name='infrastructure' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='private unexpected SQL'; END IF; RETURN NEW; END; $$; CREATE CONSTRAINT TRIGGER reject_effect AFTER INSERT ON %s.users DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %s.reject_effect()`, f.schemaName, f.schemaName, f.schemaName, f.schemaName, f.schemaName))
		// Observe the outer transaction at retained-rejection insertion: savepoint
		// rollback must already have removed every transaction-local capture setting.
		f.query(t, ctx, fmt.Sprintf(`CREATE FUNCTION %s.check_rejection_context() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF convert_from(NEW.envelope,'UTF8')::jsonb->>'status'='rejected' AND (NULLIF(current_setting('oversync.bundle_user_id',true),'') IS NOT NULL OR NULLIF(current_setting('oversync.bundle_user_pk',true),'') IS NOT NULL OR NULLIF(current_setting('oversync.bundle_source_id',true),'') IS NOT NULL OR NULLIF(current_setting('oversync.bundle_source_bundle_id',true),'') IS NOT NULL OR NULLIF(current_setting('oversync.bundle_initialization_id',true),'') IS NOT NULL) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='rejected business context leaked'; END IF; RETURN NEW; END; $$; CREATE TRIGGER check_rejection_context AFTER INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION %s.check_rejection_context()`, f.schemaName, f.schemaName))
		req := f.request(t, 1, f.userRow(uuid.New(), "reject"))
		req.InitializationID = conn.InitializationID
		result, err := f.svc.PushOneShot(ctx, initial, testSchemaGenerationProofForService(f.svc), req)
		require.NoError(t, err)
		require.Equal(t, "rejected", result.Outcome.Status)
		require.Equal(t, "application_rejected", result.Outcome.Rejection.Code)
		require.Nil(t, result.Outcome.Committed)
		require.NotContains(t, string(result.encoded), "private")
		var after string
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT row_to_json(s)::text FROM sync.scope_state s JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1`, initial.UserID).Scan(&after))
		require.Equal(t, before, after)
		require.EqualValues(t, 0, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
		require.EqualValues(t, 0, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.audit`, f.schemaName)))
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_log`))
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.row_state`))
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_capture_stage`))
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state s JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1`, initial.UserID))
		f.ack(t, ctx, initial, result.Outcome)
		unexpected := f.request(t, 2, f.userRow(uuid.New(), "infrastructure"))
		unexpected.InitializationID = req.InitializationID
		_, err = f.svc.PushOneShot(ctx, initial, testSchemaGenerationProofForService(f.svc), unexpected)
		require.Error(t, err)
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		// Rejection consumes operation progress, not committed-history progress.
		id := uuid.New()
		seed, err := f.svc.PushOneShot(ctx, f.reader, testSchemaGenerationProofForService(f.svc), f.request(t, 1, f.userRow(id, "original")))
		require.NoError(t, err)
		f.ack(t, ctx, f.reader, seed.Outcome)
		conflictReq := f.request(t, 1, f.updateUserRow(id, "stale", 999))
		conflict, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), conflictReq)
		require.NoError(t, err)
		require.Equal(t, "push_conflict", conflict.Outcome.Rejection.Code)
		require.Equal(t, "rejected", conflict.Outcome.Status)
		require.NotNil(t, conflict.Outcome.Rejection.Conflict)
		f.ack(t, ctx, f.writer, conflict.Outcome)
		next, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), f.request(t, 2, f.userRow(uuid.New(), "next")))
		require.NoError(t, err)
		require.EqualValues(t, 2, next.Outcome.SourceBundleID)
		require.EqualValues(t, 2, next.Outcome.Committed.BundleSeq)
		require.EqualValues(t, 2, f.count(t, ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state WHERE source_id='writer'`))
	})
}

func waitOutcomeCondition(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("outcome barrier did not become reachable")
}
func outcomeGate(t *testing.T, ctx context.Context, f *outcomeFixture, key int64) func() {
	t.Helper()
	conn, err := f.pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, key)
	require.NoError(t, err)
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			_, err := conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
			require.NoError(t, err)
			conn.Release()
		}
	}
	t.Cleanup(unlock)
	return unlock
}
func waitOutcomeGate(t *testing.T, ctx context.Context, f *outcomeFixture, key int64) {
	waitOutcomeCondition(t, ctx, func() bool {
		return f.count(t, ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=$1 AND NOT granted`, key) > 0
	})
}
func TestPushOutcome_ResolveSerializesWithPush(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		other := outcomeService(t, ctx, f.pool, f.cfg)
		proof := testSchemaGenerationProofForService(f.svc)
		f.query(t, ctx, `CREATE FUNCTION sync.test_outcome_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(712301); RETURN NEW; END; $$; CREATE TRIGGER test_outcome_gate AFTER INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION sync.test_outcome_gate()`)
		req := f.request(t, 1, f.userRow(uuid.New(), "race"))
		unlock := outcomeGate(t, ctx, f, 712301)
		type pushed struct {
			r   *OneShotPushResponse
			err error
		}
		push := make(chan pushed, 1)
		go func() { r, e := f.svc.PushOneShot(ctx, f.writer, proof, req); push <- pushed{r, e} }()
		waitOutcomeGate(t, ctx, f, 712301)
		type resolved struct {
			r   *RetainedPushOutcome
			err error
		}
		resolve := make(chan resolved, 1)
		go func() {
			r, e := other.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(req))
			resolve <- resolved{r, e}
		}()
		select {
		case <-resolve:
			t.Fatal("resolve crossed an uncommitted push")
		case <-time.After(30 * time.Millisecond):
		}
		unlock()
		a := <-push
		b := <-resolve
		require.NoError(t, a.err)
		require.NoError(t, b.err)
		require.Equal(t, a.r.encoded, b.r.encoded)
		require.Equal(t, "committed", b.r.Outcome.Status)
		f.ack(t, ctx, f.writer, b.r.Outcome)
		f.query(t, ctx, `DROP TRIGGER test_outcome_gate ON sync.push_outcomes; DROP FUNCTION sync.test_outcome_gate()`)
		f.query(t, ctx, fmt.Sprintf(`CREATE FUNCTION %s.rollback_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='rollback' THEN PERFORM pg_advisory_xact_lock(712302); RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='forced nonterminal rollback'; END IF; RETURN NEW; END; $$; CREATE TRIGGER rollback_gate BEFORE INSERT ON %s.users FOR EACH ROW EXECUTE FUNCTION %s.rollback_gate()`, f.schemaName, f.schemaName, f.schemaName))
		rollback := f.request(t, 2, f.userRow(uuid.New(), "rollback"))
		unlock2 := outcomeGate(t, ctx, f, 712302)
		go func() { r, e := f.svc.PushOneShot(ctx, f.writer, proof, rollback); push <- pushed{r, e} }()
		waitOutcomeGate(t, ctx, f, 712302)
		go func() {
			r, e := other.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(rollback))
			resolve <- resolved{r, e}
		}()
		unlock2()
		a = <-push
		b = <-resolve
		require.Error(t, a.err)
		require.NoError(t, b.err)
		require.Equal(t, "cancelled", b.r.Outcome.Status)
		f.ack(t, ctx, f.writer, b.r.Outcome)
		next := f.request(t, 3, f.userRow(uuid.New(), "never-applied"))
		cancelled, err := other.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(next))
		require.NoError(t, err)
		delayed, err := f.svc.PushOneShot(ctx, f.writer, proof, next)
		require.NoError(t, err)
		require.Equal(t, cancelled.encoded, delayed.encoded)
		require.EqualValues(t, 1, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
	})
}
func TestPushOutcome_AckAndSequenceFence(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		proof := testSchemaGenerationProofForService(f.svc)
		req := f.request(t, 1, f.userRow(uuid.New(), "cancelled"))
		future := pushOperationRequest(req)
		future.SourceBundleID = 2
		_, err := f.svc.ResolvePushOutcome(ctx, f.writer, proof, future)
		outcomeCode(t, err, "source_sequence_out_of_order")
		missing := PushOutcomeAckRequest{SourceBundleID: 1, CanonicalRequestHash: req.CanonicalRequestHash, OutcomeHash: strings.Repeat("a", 64)}
		outcomeCode(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, missing), "push_outcome_mismatch")
		cancelled, err := f.svc.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(req))
		require.NoError(t, err)
		mismatch := pushOperationRequest(req)
		mismatch.CanonicalRequestHash = strings.Repeat("b", 64)
		_, err = f.svc.ResolvePushOutcome(ctx, f.writer, proof, mismatch)
		outcomeCode(t, err, "push_outcome_mismatch")
		token := uuid.NewString()
		mismatch = pushOperationRequest(req)
		mismatch.InitializationID = &token
		_, err = f.svc.ResolvePushOutcome(ctx, f.writer, proof, mismatch)
		outcomeCode(t, err, "push_outcome_mismatch")
		wrong := proof
		wrong.Fingerprint = strings.Repeat("e", 64)
		_, err = f.svc.ResolvePushOutcome(ctx, f.writer, wrong, pushOperationRequest(req))
		outcomeCode(t, err, "push_outcome_mismatch")
		_, err = f.svc.PushOneShot(ctx, f.writer, proof, f.request(t, 2, f.userRow(uuid.New(), "pending")))
		outcomeCode(t, err, "push_outcome_pending")
		badAck := missing
		badAck.OutcomeHash = strings.Repeat("f", 64)
		outcomeCode(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, badAck), "push_outcome_mismatch")
		ack := PushOutcomeAckRequest{SourceBundleID: 1, CanonicalRequestHash: req.CanonicalRequestHash, OutcomeHash: cancelled.Outcome.OutcomeHash}
		require.NoError(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, ack))
		require.NoError(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, ack))
		_, err = f.svc.PushOneShot(ctx, f.writer, proof, req)
		outcomeCode(t, err, "push_outcome_acknowledged")
		_, err = f.svc.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(req))
		outcomeCode(t, err, "push_outcome_acknowledged")
		commit, err := f.svc.PushOneShot(ctx, f.writer, proof, f.request(t, 2, f.userRow(uuid.New(), "committed")))
		require.NoError(t, err)
		require.EqualValues(t, 1, commit.Outcome.Committed.BundleSeq)
		f.ack(t, ctx, f.writer, commit.Outcome)
		outcomeCode(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, ack), "push_outcome_acknowledged")
		require.EqualValues(t, 2, f.count(t, ctx, `SELECT acknowledged_sequence FROM sync.push_source_progress WHERE source_id='writer'`))
		require.EqualValues(t, 2, f.count(t, ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state WHERE source_id='writer'`))
		// Expired initialization may still resolve/cancel the original frozen tuple.
		initActor := Actor{UserID: f.writer.UserID + "-lease", SourceID: "initializer"}
		lease, err := f.svc.Connect(ctx, initActor, proof, &ConnectRequest{HasLocalPendingRows: true})
		require.NoError(t, err)
		initReq := f.request(t, 1, f.userRow(uuid.New(), "expired"))
		initReq.InitializationID = lease.InitializationID
		f.query(t, ctx, `UPDATE sync.scope_state SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE initialization_id=$1`, lease.InitializationID)
		resolved, err := f.svc.ResolvePushOutcome(ctx, initActor, proof, pushOperationRequest(initReq))
		require.NoError(t, err)
		require.Equal(t, "cancelled", resolved.Outcome.Status)
		require.Equal(t, lease.InitializationID, *resolved.Outcome.InitializationID)
	})
}
func TestPushOutcome_HistoryPruning(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		proof := testSchemaGenerationProofForService(f.svc)
		req := f.request(t, 1, f.userRow(uuid.New(), "retained"))
		first, err := f.svc.PushOneShot(ctx, f.writer, proof, req)
		require.NoError(t, err)
		for seq := int64(1); seq <= 3; seq++ {
			r, err := f.svc.PushOneShot(ctx, f.reader, proof, f.request(t, seq, f.userRow(uuid.New(), fmt.Sprintf("prune%d", seq))))
			require.NoError(t, err)
			f.ack(t, ctx, f.reader, r.Outcome)
		}
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_log WHERE source_id='writer'`))
		require.EqualValues(t, 1, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		require.NoError(t, f.svc.Close(ctx))
		_, err = dockerOutput(ctx, "restart", f.server.containerID)
		require.NoError(t, err)
		database := f.pool.Config().ConnConfig.Database
		f.pool.Close()
		port, err := dockerOutput(ctx, "port", f.server.containerID, "5432/tcp")
		require.NoError(t, err)
		f.server.host, f.server.port, err = parseDockerPublishedPort(port)
		require.NoError(t, err)
		require.NoError(t, f.server.waitUntilReady(ctx))
		f.pool, err = pgxpool.New(ctx, f.server.databaseURL(database))
		require.NoError(t, err)
		t.Cleanup(f.pool.Close)

		f.svc = outcomeService(t, ctx, f.pool, f.cfg)
		resolved, err := f.svc.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(req))
		require.NoError(t, err)
		require.Equal(t, first.encoded, resolved.encoded)
		replay, err := f.svc.PushOneShot(ctx, f.writer, proof, req)
		require.NoError(t, err)
		require.Equal(t, first.encoded, replay.encoded)
		require.EqualValues(t, 4, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
	})
}
func TestPushOutcome_LifecycleBinding(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		f.checkSourceOwnership(t, ctx)
		proof := testSchemaGenerationProofForService(f.svc)
		req := f.request(t, 1, f.userRow(uuid.New(), "pending"))
		r, err := f.svc.PushOneShot(ctx, f.writer, proof, req)
		require.NoError(t, err)
		// Existing explicit source replacement must expose the pending-outcome
		// fence through its actual HTTP boundary before creating any snapshot/state.
		replacementBody := []byte(`{"source_replacement":{"previous_source_id":"writer","new_source_id":"successor","reason":"history_pruned"}}`)
		replacementRequest := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", bytes.NewReader(replacementBody)).WithContext(ContextWithActor(ctx, f.writer))
		replacementRequest.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
		replacementRequest.Header.Set("Content-Type", "application/json")
		replacementRequest.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
		replacementRequest.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
		replacementResponse := httptest.NewRecorder()
		NewHTTPSyncHandlers(f.svc, nil).HandleCreateSnapshotSession(replacementResponse, replacementRequest)
		require.Equal(t, 409, replacementResponse.Code, replacementResponse.Body.String())
		require.Contains(t, replacementResponse.Body.String(), "push_outcome_pending")
		require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.snapshot_sessions`))
		require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.source_state WHERE source_id='successor'`))
		err = pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
			return retireSourceState(ctx, tx, f.count(t, ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1`, f.writer.UserID), f.writer.SourceID, "successor", "test")
		})
		outcomeCode(t, err, "push_outcome_pending")
		_, err = f.pool.Exec(ctx, `DELETE FROM sync.source_generation_bindings WHERE source_id='writer'`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "push_outcome_pending")
		nextCfg := *f.cfg
		nextCfg.SchemaGenerations = cloneSchemaGenerations(f.cfg.SchemaGenerations)
		nextCfg.SchemaGenerations[0].ID = 2
		retired, err := newTestRuntimeService(f.pool, &nextCfg, integrationTestLogger(4))
		require.NoError(t, err)
		require.Error(t, retired.Bootstrap(ctx))
		require.NoError(t, retired.Close(ctx))
		f.ack(t, ctx, f.writer, r.Outcome)
		f.query(t, ctx, `UPDATE sync.source_state SET state='retired',replaced_by_source_id='successor',retirement_reason='test' WHERE source_id='writer'; DELETE FROM sync.source_generation_bindings WHERE source_id='writer'`)
		f.svc = outcomeService(t, ctx, f.pool, &nextCfg)
		ack := PushOutcomeAckRequest{SourceBundleID: 1, CanonicalRequestHash: req.CanonicalRequestHash, OutcomeHash: r.Outcome.OutcomeHash}
		require.NoError(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, ack))
		// Actual HTTP duplicate ack needs no old generation view or current binding.
		raw, err := json.Marshal(ack)
		require.NoError(t, err)
		request := httptest.NewRequest("POST", "/sync/push-outcomes/ack", bytes.NewReader(raw)).WithContext(ContextWithActor(ctx, f.writer))
		request.Header.Set(ProtocolVersionHeader, "v1")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(SchemaGenerationIDHeader, "1")
		request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
		response := httptest.NewRecorder()
		NewHTTPSyncHandlers(f.svc, nil).HandlePushOutcomeAck(response, request)
		require.Equal(t, 204, response.Code, response.Body.String())
		// Synthetic administrative binding replacement after ack does not change
		// the latest acknowledgment's original proof. The old generation view is gone.
		currentProof := testSchemaGenerationProofForService(f.svc)
		f.query(t, ctx, `INSERT INTO sync.source_generation_bindings(user_pk,source_id,schema_generation_id,schema_generation_fingerprint) SELECT user_pk,$2,$3,$4 FROM sync.user_state WHERE user_id=$1`, f.writer.UserID, f.writer.SourceID, currentProof.ID, currentProof.Fingerprint)
		require.NoError(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, proof, ack))
		require.EqualValues(t, 2, f.count(t, ctx, `SELECT schema_generation_id FROM sync.source_generation_bindings WHERE source_id='writer'`))
		wrong := proof
		wrong.ID = 2
		outcomeCode(t, f.svc.AcknowledgePushOutcome(ctx, f.writer, wrong, ack), "push_outcome_mismatch")
		cross := f.writer
		cross.UserID += "-other"
		require.Error(t, f.svc.AcknowledgePushOutcome(ctx, cross, proof, ack))
		_, err = f.pool.Exec(ctx, `DELETE FROM sync.source_state WHERE source_id='writer'`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "acknowledgment fence")
		// Explicit destructive scope deletion remains an independent lifecycle operation.
		f.query(t, ctx, `DELETE FROM sync.user_state WHERE user_id=$1`, f.writer.UserID)
		require.EqualValues(t, 0, f.count(t, ctx, `SELECT count(*) FROM sync.push_source_progress`))
	})
}

func TestPushOutcome_BoundedRetention(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		proof := testSchemaGenerationProofForService(f.svc)
		other := outcomeService(t, ctx, f.pool, f.cfg)
		// Aging evidence does not make it eligible for eviction.
		f.query(t, ctx, fmt.Sprintf(`CREATE FUNCTION %s.age_outcome() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.created_at:=clock_timestamp()-interval '1000 days'; RETURN NEW; END; $$; CREATE TRIGGER age_outcome BEFORE INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION %s.age_outcome()`, f.schemaName, f.schemaName))
		req := f.request(t, 1, f.userRow(uuid.New(), "bounded"))
		result, err := f.svc.PushOneShot(ctx, f.writer, proof, req)
		require.NoError(t, err)
		_, err = f.pool.Exec(ctx, `INSERT INTO sync.push_outcomes SELECT * FROM sync.push_outcomes`)
		require.Error(t, err)
		// Both runtime instances observe the same single retained result.
		b, err := other.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(req))
		require.NoError(t, err)
		require.Equal(t, result.encoded, b.encoded)
		userPK := f.count(t, ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1`, f.writer.UserID)
		require.NoError(t, pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error { return f.svc.applyRetentionPolicyForUser(ctx, tx, userPK) }))
		require.EqualValues(t, 1, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		stats, err := f.svc.PushOutcomeStorage(ctx)
		require.NoError(t, err)
		require.Greater(t, stats.OldestAge, 999*24*time.Hour)
		require.LessOrEqual(t, stats.PayloadBytes, f.svc.config.MaxPushOutcomeDecodedBytes)
		t.Logf("OUTCOME_STORAGE before_ack count=%d payload_bytes=%d oldest_age=%s physical_bytes=%d ceiling=%d", stats.Outstanding, stats.PayloadBytes, stats.OldestAge, stats.PhysicalBytes, f.svc.config.MaxPushOutcomeDecodedBytes)
		// Configuration is persisted once, immutable, and checked before traffic.
		_, err = f.pool.Exec(ctx, `UPDATE sync.push_protocol_store SET max_outcome_bytes=max_outcome_bytes+1`)
		require.Error(t, err)
		changed := *f.cfg
		changed.MaxPushOutcomeDecodedBytes = f.svc.config.MaxPushOutcomeDecodedBytes + 1
		wrong, err := newTestRuntimeService(f.pool, &changed, integrationTestLogger(4))
		require.NoError(t, err)
		err = wrong.Bootstrap(ctx)
		require.ErrorContains(t, err, "immutable push outcome ceiling mismatch")
		require.NoError(t, wrong.Close(ctx))
		f.query(t, ctx, `DROP TRIGGER age_outcome ON sync.push_outcomes`)
		f.ack(t, ctx, f.writer, result.Outcome)
		stats, err = f.svc.PushOutcomeStorage(ctx)
		require.NoError(t, err)
		require.Zero(t, stats.Outstanding)
		require.Zero(t, stats.PayloadBytes)
		t.Logf("OUTCOME_STORAGE after_ack count=%d payload_bytes=%d oldest_age=%s physical_bytes=%d", stats.Outstanding, stats.PayloadBytes, stats.OldestAge, stats.PhysicalBytes)
		// A storage write error occurs after business/capture/source work, yet all of
		// that work rolls back with the failed retained evidence insert.
		second := f.request(t, 2, f.userRow(uuid.New(), "storage"))
		secondGroups, err := CompactGroupsForRows(generationDescriptor(f.svc.generationViews[1].contract), second.Rows)
		require.NoError(t, err)
		for _, state := range []string{"53100", "P0001"} {
			f.query(t, ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.fail_outcome() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='%s',MESSAGE='synthetic storage failure'; END; $$; CREATE TRIGGER fail_outcome AFTER INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION %s.fail_outcome()`, f.schemaName, state, f.schemaName))
			response := f.http(t, ctx, "/sync/push", CompactPushRequest{SourceBundleID: 2, CanonicalRequestHash: second.CanonicalRequestHash, Rows: secondGroups})
			require.Equal(t, 500, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "application_rejected")
			require.NotContains(t, response.Body.String(), "synthetic storage failure")
			require.EqualValues(t, 1, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
			require.EqualValues(t, 1, f.count(t, ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state WHERE source_id='writer'`))
			require.EqualValues(t, 1, f.count(t, ctx, `SELECT acknowledged_sequence FROM sync.push_source_progress WHERE source_id='writer'`))
			require.EqualValues(t, 2, f.count(t, ctx, `SELECT next_bundle_seq FROM sync.user_state WHERE user_id=$1`, f.writer.UserID))
			require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
			require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_capture_stage`))
			f.query(t, ctx, `DROP TRIGGER fail_outcome ON sync.push_outcomes`)
		}
		retry, err := f.svc.PushOneShot(ctx, f.writer, proof, second)
		require.NoError(t, err)
		f.ack(t, ctx, f.writer, retry.Outcome)
		// A tiny legal request whose server normalization expands beyond an admitted
		// complete effect is nonterminal413, and leaves no partial business or proof.
		f.query(t, ctx, fmt.Sprintf(`CREATE FUNCTION %s.expand_large() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='expanded' THEN NEW.name:=repeat('<',6000); END IF; RETURN NEW; END; $$; CREATE TRIGGER expand_large BEFORE INSERT ON %s.users FOR EACH ROW EXECUTE FUNCTION %s.expand_large()`, f.schemaName, f.schemaName, f.schemaName))
		// Canonical rows and the logical bundle fit; JSON HTML escaping alone
		// makes the final complete retained envelope exceed its immutable ceiling.
		projected := f.userRow(uuid.New(), "expanded")
		projected.Payload = []byte(fmt.Sprintf(`{"id":%q,"name":%q,"email":"expanded@example.com"}`, projected.Key["id"], strings.Repeat("<", 6000)))
		rowBytes, err := ExpandedRowDecodedBytes(projected)
		require.NoError(t, err)
		require.Less(t, rowBytes, f.svc.maxBytesPerSnapshotRow())
		logicalHash, logicalBytes, err := protocolhash.CommittedBundle([]protocolhash.BundleRow{{Schema: projected.Schema, Table: projected.Table, Key: projected.Key, Op: projected.Op, RowVersion: 3, Payload: projected.Payload}})
		require.NoError(t, err)
		require.Less(t, logicalBytes, int64(f.svc.config.MaxBytesPerBundle))
		hypothetical := newPushOutcome(f.writer, proof, PushOutcomeResolveRequest{SourceBundleID: 3, CanonicalRequestHash: strings.Repeat("b", 64)}, "committed")
		hypothetical.OutcomeHash = strings.Repeat("c", 64)
		projectedGroups, err := CompactGroupsForRows(generationDescriptor(f.svc.generationViews[1].contract), []PushRequestRow{{Schema: projected.Schema, Table: projected.Table, Key: projected.Key, Op: projected.Op, BaseRowVersion: 3, Payload: projected.Payload}})
		require.NoError(t, err)
		hypothetical.Committed = &CommittedPushEffect{BundleSeq: 3, BundleHash: logicalHash, RowCount: 1, Rows: projectedGroups}
		fullEnvelope, err := json.Marshal(hypothetical)
		require.NoError(t, err)
		require.Greater(t, int64(len(fullEnvelope)), f.svc.config.MaxPushOutcomeDecodedBytes)
		large := f.request(t, 3, f.userRow(uuid.New(), "expanded"))
		groups, err := CompactGroupsForRows(generationDescriptor(f.svc.generationViews[1].contract), large.Rows)
		require.NoError(t, err)
		response := f.http(t, ctx, "/sync/push", CompactPushRequest{SourceBundleID: 3, CanonicalRequestHash: large.CanonicalRequestHash, Rows: groups})
		require.Equal(t, 413, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "push_outcome_too_large")
		require.EqualValues(t, 2, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
		require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		require.EqualValues(t, 2, f.count(t, ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state WHERE source_id='writer'`))
		cancelled, err := f.svc.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(large))
		require.NoError(t, err)
		require.Equal(t, "cancelled", cancelled.Outcome.Status)
	})
}
func TestPushOutcome_UnrelatedScopesProgress(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		other := outcomeService(t, ctx, f.pool, f.cfg)
		proof := testSchemaGenerationProofForService(f.svc)
		userPK := f.count(t, ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1`, f.writer.UserID)
		f.query(t, ctx, fmt.Sprintf(`CREATE FUNCTION %s.scope_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.user_pk=%d THEN PERFORM pg_advisory_xact_lock(712304); END IF; RETURN NEW; END; $$; CREATE TRIGGER scope_gate AFTER INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION %s.scope_gate()`, f.schemaName, userPK, f.schemaName))
		unlock := outcomeGate(t, ctx, f, 712304)
		result := make(chan error, 1)
		req := f.request(t, 1, f.userRow(uuid.New(), "blocked"))
		go func() { _, err := f.svc.PushOneShot(ctx, f.writer, proof, req); result <- err }()
		waitOutcomeGate(t, ctx, f, 712304)
		// The existing reserved server-writer row remains a shared reservation.
		reservation, err := f.pool.Begin(ctx)
		require.NoError(t, err)
		defer reservation.Rollback(context.Background())
		_, err = reservation.Exec(ctx, `SELECT source_id FROM sync.server_source_reservations WHERE source_id='server-writer' FOR KEY SHARE`)
		require.NoError(t, err)
		free := Actor{UserID: f.writer.UserID + "-free", SourceID: "free-source"}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err = other.Connect(probe, free, proof, &ConnectRequest{})
		require.NoError(t, err)
		r, err := other.PushOneShot(probe, free, proof, f.request(t, 1, f.userRow(uuid.New(), "free")))
		require.NoError(t, err)
		require.Equal(t, "committed", r.Outcome.Status)
		// Cancellation of a waiter provides no cancellation/commit evidence.
		short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		defer stop()
		_, err = other.ResolvePushOutcome(short, f.writer, proof, pushOperationRequest(req))
		require.Error(t, err)
		unlock()
		require.NoError(t, <-result)
		resolved, err := other.ResolvePushOutcome(ctx, f.writer, proof, pushOperationRequest(req))
		require.NoError(t, err)
		require.Equal(t, "committed", resolved.Outcome.Status)
	})
}

type outcomeChildJob struct {
	Mode    string
	Config  *ServiceConfig
	Actor   Actor
	Request *OneShotPushRequest
	Ack     *PushOutcomeAckRequest
	Dir     string
}
type outcomeChildProcess struct {
	cmd    *exec.Cmd
	done   chan error
	joined bool
	output *bytes.Buffer
	job    outcomeChildJob
}

func startOutcomeChild(t *testing.T, ctx context.Context, f *outcomeFixture, mode string, req *OneShotPushRequest, ack *PushOutcomeAckRequest) *outcomeChildProcess {
	t.Helper()
	job := outcomeChildJob{Mode: mode, Config: f.cfg, Actor: f.writer, Request: req, Ack: ack, Dir: t.TempDir()}
	raw, err := json.Marshal(job)
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOutcomeProcessWorker$", "-test.count=1", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "OVERSYNC_OUTCOME_CHILD="+string(raw), "OVERSYNC_OUTCOME_DATABASE="+f.pool.Config().ConnString())
	p := &outcomeChildProcess{cmd: cmd, done: make(chan error, 1), output: &bytes.Buffer{}, job: job}
	cmd.Stdout = p.output
	cmd.Stderr = p.output
	require.NoError(t, cmd.Start())
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !p.joined {
			_ = cmd.Process.Kill()
			<-p.done
			p.joined = true
		}
	})
	waitOutcomeCondition(t, ctx, func() bool { _, err := os.Stat(job.Dir + "/ready"); return err == nil })
	return p
}
func (p *outcomeChildProcess) start(t *testing.T) {
	t.Helper()
	require.NoError(t, os.WriteFile(p.job.Dir+"/start", []byte("start"), 0600))
}
func (p *outcomeChildProcess) result(t *testing.T, ctx context.Context) []byte {
	t.Helper()
	waitOutcomeCondition(t, ctx, func() bool { _, err := os.Stat(p.job.Dir + "/result.json"); return err == nil })
	raw, err := os.ReadFile(p.job.Dir + "/result.json")
	require.NoError(t, err)
	return raw
}
func (p *outcomeChildProcess) kill(t *testing.T) {
	t.Helper()
	require.NoError(t, p.cmd.Process.Kill())
	err := <-p.done
	p.joined = true
	require.Error(t, err)
	t.Log("CHILD_REAPED", p.job.Mode, p.cmd.Process.Pid)
}
func (p *outcomeChildProcess) finish(t *testing.T) {
	t.Helper()
	err := <-p.done
	p.joined = true
	require.NoError(t, err, p.output.String())
	t.Log("CHILD_REAPED", p.job.Mode, p.cmd.Process.Pid)
}

func TestOutcomeProcessWorker(t *testing.T) {
	raw := os.Getenv("OVERSYNC_OUTCOME_CHILD")
	if raw == "" {
		t.Skip("owned subprocess entrypoint")
	}
	var job outcomeChildJob
	require.NoError(t, json.Unmarshal([]byte(raw), &job))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("OVERSYNC_OUTCOME_DATABASE"))
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	svc := outcomeService(t, ctx, pool, job.Config)
	proof := testSchemaGenerationProofForService(svc)
	require.NoError(t, os.WriteFile(job.Dir+"/ready", []byte("ready"), 0600))
	waitOutcomeCondition(t, ctx, func() bool { _, err := os.Stat(job.Dir + "/start"); return err == nil })
	var result []byte
	switch job.Mode {
	case "before_commit", "after_commit":
		r, err := svc.PushOneShot(ctx, job.Actor, proof, job.Request)
		require.NoError(t, err)
		result = r.encoded
	case "resolve", "after_cancel":
		r, err := svc.ResolvePushOutcome(ctx, job.Actor, proof, pushOperationRequest(job.Request))
		require.NoError(t, err)
		result = r.encoded
	case "ack", "after_ack":
		require.NoError(t, svc.AcknowledgePushOutcome(ctx, job.Actor, proof, *job.Ack))
		result = []byte(`{}`)
	default:
		t.Fatal(job.Mode)
	}
	// Rename publishes the complete exact response only after the API transaction
	// commits; a partial result file is never accepted as proof by the parent.
	require.NoError(t, os.WriteFile(job.Dir+"/result.tmp", result, 0600))
	require.NoError(t, os.Rename(job.Dir+"/result.tmp", job.Dir+"/result.json"))
	if strings.HasPrefix(job.Mode, "after_") {
		<-ctx.Done()
	}
}
func TestPushOutcome_CrossProcessCrash(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		// Start a worker before injecting the DB commit barrier so its normal layout
		// preflight remains mandatory and sees the unmodified production manifest.
		req := f.request(t, 1, f.userRow(uuid.New(), "before-death"))
		child := startOutcomeChild(t, ctx, f, "before_commit", req, nil)
		contender := startOutcomeChild(t, ctx, f, "resolve", req, nil)
		unlock := outcomeGate(t, ctx, f, 712305)
		f.query(t, ctx, fmt.Sprintf(`CREATE FUNCTION %s.crash_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF convert_from(NEW.envelope,'UTF8')::jsonb->>'status'='committed' THEN PERFORM pg_advisory_xact_lock(712305); END IF; RETURN NEW; END; $$; CREATE TRIGGER crash_gate AFTER INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION %s.crash_gate()`, f.schemaName, f.schemaName))
		child.start(t)
		waitOutcomeGate(t, ctx, f, 712305)
		contender.start(t)
		waitOutcomeCondition(t, ctx, func() bool {
			return f.count(t, ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_lock(hashtextextended%'`) > 0
		})
		child.kill(t)
		unlock()
		f.query(t, ctx, `DROP TRIGGER crash_gate ON sync.push_outcomes`)
		cancelledRaw := contender.result(t, ctx)
		contender.finish(t)
		f.query(t, ctx, `DROP TRIGGER IF EXISTS crash_gate ON sync.push_outcomes`)
		require.Zero(t, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
		require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_log`))
		require.EqualValues(t, 1, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_capture_stage`))
		var cancelled PushOutcome
		require.NoError(t, json.Unmarshal(cancelledRaw, &cancelled))
		require.Equal(t, "cancelled", cancelled.Status)
		delayed, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), req)
		require.NoError(t, err)
		require.Equal(t, cancelledRaw, delayed.encoded)
		f.ack(t, ctx, f.writer, &cancelled)
		req2 := f.request(t, 2, f.userRow(uuid.New(), "after-death"))
		committer := startOutcomeChild(t, ctx, f, "after_commit", req2, nil)
		committer.start(t)
		committedRaw := committer.result(t, ctx)
		committer.kill(t)
		reader2 := startOutcomeChild(t, ctx, f, "resolve", req2, nil)
		reader2.start(t)
		require.Equal(t, committedRaw, reader2.result(t, ctx))
		reader2.finish(t)
		var committed PushOutcome
		require.NoError(t, json.Unmarshal(committedRaw, &committed))
		require.Equal(t, "committed", committed.Status)
		require.EqualValues(t, 1, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
		ack := &PushOutcomeAckRequest{SourceBundleID: 2, CanonicalRequestHash: req2.CanonicalRequestHash, OutcomeHash: committed.OutcomeHash}
		acker := startOutcomeChild(t, ctx, f, "after_ack", nil, ack)
		acker.start(t)
		acker.result(t, ctx)
		acker.kill(t)
		duplicate := startOutcomeChild(t, ctx, f, "ack", nil, ack)
		duplicate.start(t)
		duplicate.result(t, ctx)
		duplicate.finish(t)
		require.Zero(t, f.count(t, ctx, `SELECT count(*) FROM sync.push_outcomes`))
		require.EqualValues(t, 2, f.count(t, ctx, `SELECT acknowledged_sequence FROM sync.push_source_progress WHERE source_id='writer'`))
		req3 := f.request(t, 3, f.userRow(uuid.New(), "cancel-death"))
		canceller := startOutcomeChild(t, ctx, f, "after_cancel", req3, nil)
		canceller.start(t)
		cancel3 := canceller.result(t, ctx)
		canceller.kill(t)
		freshReader := startOutcomeChild(t, ctx, f, "resolve", req3, nil)
		freshReader.start(t)
		require.Equal(t, cancel3, freshReader.result(t, ctx))
		freshReader.finish(t)
		delayed3, err := f.svc.PushOneShot(ctx, f.writer, testSchemaGenerationProofForService(f.svc), req3)
		require.NoError(t, err)
		require.Equal(t, cancel3, delayed3.encoded)
		require.EqualValues(t, 1, f.count(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)))
	})
}
