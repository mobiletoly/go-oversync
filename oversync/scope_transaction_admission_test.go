// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type scopeAdmissionFunc func(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error

func (fn scopeAdmissionFunc) AdmitScopeTransaction(ctx context.Context, tx DatabaseWriteTx, op ScopeTransactionOperation) error {
	return fn(ctx, tx, op)
}

type scopeAdmissionTx struct{ pgx.Tx }

func (*scopeAdmissionTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func TestScopeTransactionAdmission_RestrictedCapabilityAndSafeErrors(t *testing.T) {
	service := &SyncService{config: &ServiceConfig{}}
	// A nil policy does not touch the transaction at all.
	require.NoError(t, service.admitScopeTransaction(context.Background(), nil, ScopeTransactionOperation{}))
	op := ScopeTransactionOperation{ScopeID: "exact scope", Kind: ScopeTransactionServerWrite, SourceID: "exact source"}
	service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(ctx context.Context, tx DatabaseWriteTx, got ScopeTransactionOperation) error {
		require.Equal(t, op, got)
		_, raw := tx.(pgx.Tx)
		require.False(t, raw)
		_, err := tx.Exec(context.Background(), "SELECT 1")
		require.ErrorAs(t, err, new(*RetryableCallbackViolationError))
		_, err = tx.Exec(ctx, "COMMIT")
		require.ErrorAs(t, err, new(*RetryableCallbackViolationError))
		err = service.WithinSyncBundle(ctx, Actor{}, BundleSource{}, RetryableBundleWriteOptions{}, nil)
		require.ErrorAs(t, err, new(*RetryableCallbackViolationError))
		return fmt.Errorf("private policy detail: %w", &ScopeAccessDeniedError{})
	})
	err := service.admitScopeTransaction(context.Background(), &scopeAdmissionTx{}, op)
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	require.EqualError(t, err, "scope access denied")

	internal := errors.New("private database detail")
	service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error { return internal })
	err = service.admitScopeTransaction(context.Background(), &scopeAdmissionTx{}, op)
	require.ErrorAs(t, err, new(*ScopeAdmissionUnavailableError))
	require.ErrorIs(t, err, internal)
	require.EqualError(t, err, "scope admission unavailable")
	require.Same(t, err, sanitizePushError(err))

	handlers := NewHTTPSyncHandlers(service, nil)
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{&ScopeAccessDeniedError{}, http.StatusForbidden, "scope_access_denied"},
		{err, http.StatusServiceUnavailable, "scope_admission_unavailable"},
	} {
		for _, write := range []func(http.ResponseWriter, *http.Request, error) bool{handlers.writeServiceUnavailable, handlers.writePushTransactionFailure} {
			recorder := httptest.NewRecorder()
			require.True(t, write(recorder, httptest.NewRequest(http.MethodPost, "/sync/connect", nil), fmt.Errorf("wrapped: %w", tc.err)))
			require.Equal(t, tc.status, recorder.Code)
			require.Contains(t, recorder.Body.String(), tc.code)
			require.NotContains(t, recorder.Body.String(), "private")
		}
	}
}

type scopeAdmissionFixture struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	service *SyncService
	manager *ScopeManager
	schema  string
	proof   SchemaGenerationProof
}

func newScopeAdmissionFixture(t *testing.T) *scopeAdmissionFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	pool := newIntegrationTestPool(t, ctx)
	schema := "scope_admission_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.retired_scope (scope_id TEXT PRIMARY KEY)`, pgx.Identifier{schema}.Sanitize()))
	require.NoError(t, err)
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		AppName: "scope-admission", RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered), ReservedServerSourceIDs: []string{"permanent-delete"},
		ScopeTransactionAdmission: scopeAdmissionFunc(func(ctx context.Context, tx DatabaseWriteTx, op ScopeTransactionOperation) error {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`, "scope-admission:"+op.ScopeID); err != nil {
				return err
			}
			// A fresh statement after the lock is essential under Read Committed.
			var retired bool
			if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s.retired_scope WHERE scope_id=$1)`, pgx.Identifier{schema}.Sanitize()), op.ScopeID).Scan(&retired); err != nil {
				return err
			}
			if retired {
				return &ScopeAccessDeniedError{}
			}
			return nil
		}),
	}, integrationTestLogger(slog.LevelWarn))
	return &scopeAdmissionFixture{ctx: ctx, pool: pool, service: service, manager: NewScopeManager(service, ScopeManagerConfig{}), schema: schema, proof: testSchemaGenerationProofForService(service)}
}

func (f *scopeAdmissionFixture) write(_ string, rowID uuid.UUID) RetryableDatabaseWrite {
	return func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users (id,name,email) VALUES ($1,'Synthetic',$2)`, pgx.Identifier{f.schema}.Sanitize()), rowID, rowID.String()+"@example.invalid")
		return err
	}
}

func (f *scopeAdmissionFixture) retire(t *testing.T, scope string) {
	t.Helper()
	_, err := f.pool.Exec(f.ctx, fmt.Sprintf(`INSERT INTO %s.retired_scope VALUES ($1)`, pgx.Identifier{f.schema}.Sanitize()), scope)
	require.NoError(t, err)
}

func TestScopeTransactionAdmission_AllMutationBoundariesAndReplay(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	var observed []ScopeTransactionOperation
	policy := f.service.config.ScopeTransactionAdmission
	f.service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(ctx context.Context, tx DatabaseWriteTx, op ScopeTransactionOperation) error {
		observed = append(observed, op)
		return policy.AdmitScopeTransaction(ctx, tx, op)
	})
	actor := Actor{UserID: uuid.NewString(), SourceID: "device"}
	_, err := f.service.Connect(f.ctx, actor, f.proof, &ConnectRequest{})
	require.NoError(t, err)
	id := uuid.New()
	rows := []PushRequestRow{{Schema: f.schema, Table: "users", Key: SyncKey{"id": id.String()}, Op: OpInsert, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"Synthetic","email":"synthetic@example.invalid"}`, id.String()))}}
	push := &OneShotPushRequest{SourceBundleID: 1, CanonicalRequestHash: mustCanonicalPushRequestHash(t, f.service, rows), Rows: rows}
	_, err = f.service.PushOneShot(f.ctx, actor, f.proof, push)
	require.NoError(t, err)
	serverOpts := ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}
	serverWrite := f.write(actor.UserID, uuid.New())
	_, err = f.manager.ExecWrite(f.ctx, actor.UserID, serverOpts, serverWrite)
	require.NoError(t, err)
	bundleOpts := retryableBundleWriteOptionsForTest()
	bundleSource := BundleSource{SourceID: "wrapper", SourceBundleID: 1}
	bundleWrite := f.write(actor.UserID, uuid.New())
	require.NoError(t, f.service.WithinSyncBundle(f.ctx, actor, bundleSource, bundleOpts, bundleWrite))
	snapshot, err := f.service.CreateSnapshotSession(f.ctx, actor, f.proof)
	require.NoError(t, err)
	f.retire(t, actor.UserID)
	require.Equal(t, []ScopeTransactionOperation{
		{ScopeID: actor.UserID, Kind: ScopeTransactionConnect, SourceID: actor.SourceID},
		{ScopeID: actor.UserID, Kind: ScopeTransactionClientPush, SourceID: actor.SourceID},
		{ScopeID: actor.UserID, Kind: ScopeTransactionServerWrite, SourceID: "server"},
		{ScopeID: actor.UserID, Kind: ScopeTransactionClientPush, SourceID: "wrapper"},
		{ScopeID: actor.UserID, Kind: ScopeTransactionSnapshotCreate, SourceID: actor.SourceID},
	}, observed)

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"connect", func() error { _, err := f.service.Connect(f.ctx, actor, f.proof, &ConnectRequest{}); return err }},
		{"client committed receipt", func() error { _, err := f.service.PushOneShot(f.ctx, actor, f.proof, push); return err }},
		{"server committed receipt", func() error { _, err := f.manager.ExecWrite(f.ctx, actor.UserID, serverOpts, serverWrite); return err }},
		{"wrapper committed receipt", func() error { return f.service.WithinSyncBundle(f.ctx, actor, bundleSource, bundleOpts, bundleWrite) }},
		{"snapshot creation", func() error { _, err := f.service.CreateSnapshotSession(f.ctx, actor, f.proof); return err }},
		{"snapshot replacement", func() error {
			_, err := f.service.CreateSnapshotSessionWithRequest(f.ctx, actor, f.proof, &SnapshotSessionCreateRequest{SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: actor.SourceID, NewSourceID: "replacement", Reason: "history_pruned"}})
			return err
		}},
		{"snapshot retirement", func() error { return f.service.DeleteSnapshotSession(f.ctx, actor, f.proof, snapshot.SnapshotID) }},
	} {
		t.Run(tc.name, func(t *testing.T) { require.ErrorAs(t, tc.run(), new(*ScopeAccessDeniedError)) })
	}
	var liveSnapshots, users int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.snapshot_sessions WHERE expires_at > now()`).Scan(&liveSnapshots))
	require.Equal(t, 1, liveSnapshots)
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, pgx.Identifier{f.schema}.Sanitize())).Scan(&users))
	require.Equal(t, 3, users)

	fresh := uuid.NewString()
	f.retire(t, fresh)
	_, err = f.service.Connect(f.ctx, Actor{UserID: fresh, SourceID: "device"}, f.proof, &ConnectRequest{})
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	_, err = f.manager.ExecWrite(f.ctx, fresh, ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}, f.write(fresh, uuid.New()))
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	err = f.service.WithinSyncBundle(f.ctx, Actor{UserID: fresh}, BundleSource{SourceID: "wrapper", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), f.write(fresh, uuid.New()))
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	err = withOriginTransaction(f.ctx, f.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		_, err := f.service.persistAdoptionBaseline(f.ctx, tx, fresh, []adoptionBusinessRow{{}}, nil)
		return err
	})
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	adoptionOrigin := observed[len(observed)-1]
	require.Equal(t, fresh, adoptionOrigin.ScopeID)
	require.Equal(t, ScopeTransactionServerWrite, adoptionOrigin.Kind)
	require.True(t, strings.HasPrefix(adoptionOrigin.SourceID, adoptionSourcePrefix))
	_, err = uuid.Parse(strings.TrimPrefix(adoptionOrigin.SourceID, adoptionSourcePrefix))
	require.NoError(t, err)
	var metadata int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.user_state WHERE user_id=$1`, fresh).Scan(&metadata))
	require.Zero(t, metadata)

	// SDK-derived origin cannot be elevated by either client entry point.
	_, err = f.service.Connect(f.ctx, Actor{UserID: fresh, SourceID: "permanent-delete"}, f.proof, &ConnectRequest{})
	require.Error(t, err)
	require.NotErrorAs(t, err, new(*ScopeAccessDeniedError))
	err = f.service.WithinSyncBundle(f.ctx, Actor{UserID: fresh}, BundleSource{SourceID: "permanent-delete", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), f.write(fresh, uuid.New()))
	require.Error(t, err)
	require.NotErrorAs(t, err, new(*ScopeAccessDeniedError))
}

func TestScopeTransactionAdmission_HTTPEnvelopes(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	actor := Actor{UserID: uuid.NewString(), SourceID: "device"}
	_, err := f.service.Connect(f.ctx, actor, f.proof, &ConnectRequest{})
	require.NoError(t, err)
	id := uuid.NewString()
	rows := []PushRequestRow{{Schema: f.schema, Table: "users", Key: SyncKey{"id": id}, Op: OpInsert, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"Synthetic","email":"synthetic@example.invalid"}`, id))}}
	contract, ok := f.service.schemaGenerationRegistry.Generation(f.proof.ID)
	require.True(t, ok)
	groups, err := CompactGroupsForRows(generationDescriptor(contract), rows)
	require.NoError(t, err)
	pushBody, err := json.Marshal(CompactPushRequest{SourceBundleID: 1, CanonicalRequestHash: mustCanonicalPushRequestHash(t, f.service, rows), Rows: groups})
	require.NoError(t, err)
	handlers := NewHTTPSyncHandlers(f.service, nil)
	private := "private dependency detail"
	for _, policy := range []struct {
		name          string
		err           error
		status        int
		code, message string
	}{
		{"denied", &ScopeAccessDeniedError{}, http.StatusForbidden, "scope_access_denied", "Scope access denied"},
		{"plain_dependency", errors.New(private), http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
		{"source_generation", &SourceGenerationError{Code: SourceGenerationConflict, Message: private}, http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
		{"schema_generation", &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMismatch, Message: private}, http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
		{"application_rejection", &ApplicationWriteRejectedError{Category: "private_dependency_category"}, http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
		{"projection", &GenerationProjectionError{Code: BundleProjectionTooLarge, Message: private}, http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
		{"push_limit", &GenerationProjectionError{Code: BundleLimitExceeded, Operation: "push", Message: private}, http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
		{"replay", fmt.Errorf("%s: %w", private, outcomeError("push_outcome_mismatch")), http.StatusServiceUnavailable, "scope_admission_unavailable", "Scope admission unavailable; retry later"},
	} {
		t.Run(policy.name, func(t *testing.T) {
			f.service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error { return policy.err })
			if policy.status == http.StatusServiceUnavailable {
				// Trusted callers and SQL retry inspection retain the exact cause.
				_, err := f.service.Connect(f.ctx, actor, f.proof, &ConnectRequest{})
				require.ErrorAs(t, err, new(*ScopeAdmissionUnavailableError))
				require.ErrorIs(t, err, policy.err)
			}
			for _, endpoint := range []struct {
				name, method, path string
				body               []byte
				handler            http.HandlerFunc
			}{
				{"connect", http.MethodPost, "/sync/connect", []byte(`{"has_local_pending_rows":false}`), handlers.HandleConnect},
				{"push", http.MethodPost, "/sync/push", pushBody, handlers.HandlePush},
				{"create", http.MethodPost, "/sync/snapshot-sessions", nil, handlers.HandleCreateSnapshotSession},
				{"replace", http.MethodPost, "/sync/snapshot-sessions", []byte(`{"source_replacement":{"previous_source_id":"device","new_source_id":"replacement","reason":"history_pruned"}}`), handlers.HandleCreateSnapshotSession},
				{"retire", http.MethodDelete, "/sync/snapshot-sessions/id", nil, handlers.HandleDeleteSnapshotSession},
			} {
				t.Run(endpoint.name, func(t *testing.T) {
					request := httptest.NewRequest(endpoint.method, endpoint.path, bytes.NewReader(endpoint.body))
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
					request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(f.proof.ID))
					request.Header.Set(SchemaGenerationFingerprintHeader, f.proof.Fingerprint)
					request.SetPathValue("snapshot_id", "id")
					request = request.WithContext(ContextWithActor(request.Context(), actor))
					recorder := httptest.NewRecorder()
					endpoint.handler(recorder, request)
					require.Equal(t, policy.status, recorder.Code, recorder.Body.String())
					require.JSONEq(t, fmt.Sprintf(`{"error":%q,"message":%q}`, policy.code, policy.message), recorder.Body.String())
					require.NotContains(t, recorder.Body.String(), "private")
				})
			}
		})
	}
	f.service.config.ScopeTransactionAdmission = nil
	request := httptest.NewRequest(http.MethodPost, "/sync/connect", strings.NewReader(`{"has_local_pending_rows":false}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(f.proof.ID))
	request.Header.Set(SchemaGenerationFingerprintHeader, f.proof.Fingerprint)
	request = request.WithContext(ContextWithActor(request.Context(), actor))
	recorder := httptest.NewRecorder()
	handlers.HandleConnect(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"resolution":"remote_authoritative"}`, recorder.Body.String())
}

func TestScopeTransactionAdmission_OrdinaryDomainErrorsRetainBehavior(t *testing.T) {
	handlers := NewHTTPSyncHandlers(&SyncService{}, nil)
	for _, ordinary := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"source_generation", &SourceGenerationError{Code: SourceGenerationConflict, Message: "public conflict"}, http.StatusConflict, string(SourceGenerationConflict)},
		{"unretained_application_error", &ApplicationWriteRejectedError{Category: "public_rejection"}, http.StatusInternalServerError, "push_failed"},
		{"projection", &GenerationProjectionError{Code: BundleProjectionTooLarge, Message: "public projection"}, http.StatusUnprocessableEntity, string(BundleProjectionTooLarge)},
		{"replay", outcomeError("push_outcome_mismatch"), http.StatusConflict, "push_outcome_mismatch"},
	} {
		t.Run(ordinary.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handlers.writeCompactPushError(recorder, httptest.NewRequest(http.MethodPost, "/sync/push", nil), "device", ordinary.err)
			require.Equal(t, ordinary.status, recorder.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			require.Equal(t, ordinary.code, body["error"])
		})
	}
}

func TestScopeTransactionAdmission_RecheckedOnTransactionRetry(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	attempts, callbacks := 0, 0
	f.service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error {
		attempts++
		if attempts == 1 {
			return &pgconn.PgError{Code: "40001", Message: "synthetic rolled-back admission"}
		}
		return &ScopeAccessDeniedError{}
	})
	scope := uuid.NewString()
	_, err := f.manager.ExecWrite(f.ctx, scope, ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}, func(context.Context, DatabaseWriteTx) error { callbacks++; return nil })
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	require.Equal(t, 2, attempts)
	require.Zero(t, callbacks)
	var metadata int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.user_state WHERE user_id=$1`, scope).Scan(&metadata))
	require.Zero(t, metadata)
}

// waitForScopeAdmissionBlock observes PostgreSQL's wait graph, so scheduling
// delays cannot establish either side of the retirement/write ordering proof.
func waitForScopeAdmissionBlock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blocker int32) {
	t.Helper()
	require.Eventually(t, func() bool {
		var blocked bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked)
		return err == nil && blocked
	}, 3*time.Second, 5*time.Millisecond)
}

func TestScopeTransactionAdmission_RetirementWinsBeforeWaitingWrite(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	scope := uuid.NewString()
	retirement, err := f.pool.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer retirement.Rollback(context.Background())
	_, err = retirement.Exec(f.ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "scope-admission:"+scope)
	require.NoError(t, err)
	_, err = retirement.Exec(f.ctx, fmt.Sprintf(`INSERT INTO %s.retired_scope VALUES($1)`, pgx.Identifier{f.schema}.Sanitize()), scope)
	require.NoError(t, err)
	var retirementPID int32
	require.NoError(t, retirement.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&retirementPID))
	result := make(chan error, 1)
	go func() {
		_, err := f.manager.ExecWrite(f.ctx, scope, ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}, f.write(scope, uuid.New()))
		result <- err
	}()
	waitForScopeAdmissionBlock(t, f.ctx, f.pool, retirementPID)
	require.NoError(t, retirement.Commit(f.ctx))
	select {
	case err := <-result:
		require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	var metadata, users int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.user_state WHERE user_id=$1`, scope).Scan(&metadata))
	require.Zero(t, metadata)
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE _sync_scope_id=$1`, pgx.Identifier{f.schema}.Sanitize()), scope).Scan(&users))
	require.Zero(t, users)
}

func TestScopeTransactionAdmission_AdmittedWriteFinishesBeforeRetirement(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	scope := uuid.NewString()
	writeEntered := make(chan int32, 1)
	releaseWrite := make(chan struct{})
	defer func() {
		select {
		case <-releaseWrite:
		default:
			close(releaseWrite)
		}
	}()
	writeResult := make(chan error, 1)
	rowID := uuid.New()
	go func() {
		_, err := f.manager.ExecWrite(f.ctx, scope, ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}, func(ctx context.Context, tx DatabaseWriteTx) error {
			var pid int32
			if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				return err
			}
			writeEntered <- pid
			select {
			case <-releaseWrite:
			case <-ctx.Done():
				return ctx.Err()
			}
			return f.write(scope, rowID)(ctx, tx)
		})
		writeResult <- err
	}()
	var writePID int32
	select {
	case writePID = <-writeEntered:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	retirement, err := f.pool.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer retirement.Rollback(context.Background())
	retirementLocked := make(chan error, 1)
	go func() {
		_, err := retirement.Exec(f.ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "scope-admission:"+scope)
		retirementLocked <- err
	}()
	waitForScopeAdmissionBlock(t, f.ctx, f.pool, writePID)
	close(releaseWrite)
	select {
	case err := <-writeResult:
		require.NoError(t, err)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	select {
	case err := <-retirementLocked:
		require.NoError(t, err)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	var rows int
	require.NoError(t, retirement.QueryRow(f.ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE _sync_scope_id=$1`, pgx.Identifier{f.schema}.Sanitize()), scope).Scan(&rows))
	require.Equal(t, 1, rows)
	_, err = retirement.Exec(f.ctx, fmt.Sprintf(`INSERT INTO %s.retired_scope VALUES($1)`, pgx.Identifier{f.schema}.Sanitize()), scope)
	require.NoError(t, err)
	require.NoError(t, retirement.Commit(f.ctx))
	_, err = f.manager.ExecWrite(f.ctx, scope, ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}, f.write(scope, uuid.New()))
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
}

func TestScopeTransactionAdmission_RequireExistingScopeNeverInitializes(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	scope := uuid.NewString()
	callback := false
	opts := ScopeWriteOptions{WriterID: "permanent-delete", RequireExistingScope: true, RetryableWriteOptions: retryableWriteOptionsForTest()}
	_, err := f.manager.ExecWrite(f.ctx, scope, opts, func(context.Context, DatabaseWriteTx) error { callback = true; return nil })
	require.ErrorAs(t, err, new(*ScopeUninitializedError))
	require.False(t, callback)
	var metadata int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.user_state WHERE user_id=$1`, scope).Scan(&metadata))
	require.Zero(t, metadata)
	actor := Actor{UserID: scope, SourceID: "device"}
	_, err = f.service.Connect(f.ctx, actor, f.proof, &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	_, err = f.manager.ExecWrite(f.ctx, scope, opts, f.write(scope, uuid.New()))
	require.ErrorAs(t, err, new(*ScopeUninitializedError))
	_, err = f.service.Connect(f.ctx, actor, f.proof, &ConnectRequest{})
	require.NoError(t, err)
	result, err := f.manager.ExecWrite(f.ctx, scope, opts, f.write(scope, uuid.New()))
	require.NoError(t, err)
	require.False(t, result.AutoInitialized)
}

func TestScopeTransactionAdmission_FailureRollsBackPolicySQL(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	scope := uuid.NewString()
	f.service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(ctx context.Context, tx DatabaseWriteTx, op ScopeTransactionOperation) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.retired_scope VALUES($1)`, pgx.Identifier{f.schema}.Sanitize()), op.ScopeID)
		if err != nil {
			return err
		}
		return &ScopeAccessDeniedError{}
	})
	_, err := f.service.Connect(f.ctx, Actor{UserID: scope, SourceID: "device"}, f.proof, &ConnectRequest{})
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	var retired, metadata int
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT count(*) FROM %s.retired_scope WHERE scope_id=$1`, pgx.Identifier{f.schema}.Sanitize()), scope).Scan(&retired))
	require.Zero(t, retired)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.user_state WHERE user_id=$1`, scope).Scan(&metadata))
	require.Zero(t, metadata)
}

func TestScopeTransactionAdmission_RechecksAfterRolledBackWrite(t *testing.T) {
	f := newScopeAdmissionFixture(t)
	_, err := f.pool.Exec(f.ctx, fmt.Sprintf(`CREATE FUNCTION %s.force_retry() RETURNS void LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001', MESSAGE='synthetic rollback'; END $$`, pgx.Identifier{f.schema}.Sanitize()))
	require.NoError(t, err)
	admissions, callbacks := 0, 0
	f.service.config.ScopeTransactionAdmission = scopeAdmissionFunc(func(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error {
		admissions++
		if admissions > 1 {
			return &ScopeAccessDeniedError{}
		}
		return nil
	})
	scope := uuid.NewString()
	_, err = f.manager.ExecWrite(f.ctx, scope, ScopeWriteOptions{WriterID: "server", RetryableWriteOptions: retryableWriteOptionsForTest()}, func(ctx context.Context, tx DatabaseWriteTx) error {
		callbacks++
		if err := f.write(scope, uuid.New())(ctx, tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`SELECT %s.force_retry()`, pgx.Identifier{f.schema}.Sanitize()))
		return err
	})
	require.ErrorAs(t, err, new(*ScopeAccessDeniedError))
	require.Equal(t, 2, admissions)
	require.Equal(t, 1, callbacks)
	var users, metadata int
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE _sync_scope_id=$1`, pgx.Identifier{f.schema}.Sanitize()), scope).Scan(&users))
	require.Zero(t, users)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.user_state WHERE user_id=$1`, scope).Scan(&metadata))
	require.Zero(t, metadata)
}
