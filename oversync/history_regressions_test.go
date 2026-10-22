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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/stretchr/testify/require"
)

type observedProjectionResponseWriter struct {
	*httptest.ResponseRecorder
	maxWrite int
}

type cancelBlockingProjectionWriter struct {
	header  http.Header
	started chan struct{}
	ctx     context.Context
	once    sync.Once
}

func (w *cancelBlockingProjectionWriter) Header() http.Header { return w.header }
func (w *cancelBlockingProjectionWriter) WriteHeader(int)     {}
func (w *cancelBlockingProjectionWriter) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.ctx.Done()
	return 0, w.ctx.Err()
}

type failingProjectionResponseWriter struct{ header http.Header }

func (w *failingProjectionResponseWriter) Header() http.Header { return w.header }
func (w *failingProjectionResponseWriter) WriteHeader(int)     {}
func (w *failingProjectionResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("forced projection response write failure")
}

func (w *observedProjectionResponseWriter) Write(p []byte) (int, error) {
	if len(p) > w.maxWrite {
		w.maxWrite = len(p)
	}
	return w.ResponseRecorder.Write(p)
}

func TestDirectPush_DeferredConstraintEffectsFinishBeforeCaptureAndCoverage(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "deferred_effects_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	schemaID := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s.users ADD COLUMN hidden_amount NUMERIC NOT NULL DEFAULT 1;
		ALTER TABLE %s.users ADD CONSTRAINT hidden_secret_check CHECK (name <> 'SECRET_FAIL');
		CREATE TABLE %s.audit_events (
			_sync_scope_id TEXT NOT NULL, id UUID NOT NULL, detail TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		);
		CREATE FUNCTION %s.finish_deferred_user() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.name = 'Bob' THEN
				UPDATE %s.users SET name = 'BOB'
				WHERE _sync_scope_id = NEW._sync_scope_id AND id = NEW.id;
				INSERT INTO %s.audit_events VALUES (NEW._sync_scope_id, NEW.id, 'normalized');
			ELSIF NEW.name = 'DROP' THEN
				DELETE FROM %s.users
				WHERE _sync_scope_id = NEW._sync_scope_id AND id = NEW.id;
				INSERT INTO %s.audit_events VALUES (NEW._sync_scope_id, NEW.id, 'must-rollback');
			END IF;
			RETURN NULL;
		END $$;
		CREATE FUNCTION %s.inject_hidden_codec_value() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.name = 'CODEC' THEN NEW.hidden_amount := 'NaN'; END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER inject_hidden_codec_value BEFORE INSERT ON %s.users
		FOR EACH ROW EXECUTE FUNCTION %s.inject_hidden_codec_value();
		CREATE CONSTRAINT TRIGGER finish_deferred_user
		AFTER INSERT ON %s.users DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION %s.finish_deferred_user();
	`, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID))
	require.NoError(t, err)

	registered := []RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schema, Table: "audit_events", SyncKeyColumns: []string{"id"}},
	}
	visibleUsers := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{
		{Name: "email", WireType: GenerationWireTypeText},
		{Name: "id", WireType: GenerationWireTypeUUID},
		{Name: "name", WireType: GenerationWireTypeText},
	}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{visibleUsers}}},
		AppName:           "deferred-effect-capture",
	}, integrationTestLogger(slog.LevelWarn))
	actor := Actor{UserID: "deferred-user", SourceID: "device"}
	proof := proofForGeneration(t, service, 1)
	connected, err := service.Connect(ctx, actor, proof, &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", connected.Resolution)

	normalizedID := uuid.NewString()
	committed, _, err := pushRowsWithGeneration(t, ctx, service, actor, proof, 1, []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": normalizedID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"email":"bob@example.com","id":%q,"name":"Bob"}`, normalizedID)),
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), committed.RowCount)
	pull, err := service.ProcessPull(ctx, actor, proof, 0, 10, 0)
	require.NoError(t, err)
	require.JSONEq(t, fmt.Sprintf(`{"email":"bob@example.com","id":%q,"name":"BOB"}`, normalizedID), string(pull.Bundles[0].Rows[0].Payload))

	acknowledgeRetainedOutcomeForTest(t, ctx, service, actor, proof)

	droppedID := uuid.NewString()
	_, _, err = pushRowsWithGeneration(t, ctx, service, actor, proof, 2, []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": droppedID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"email":"drop@example.com","id":%q,"name":"DROP"}`, droppedID)),
	}})
	var coverageErr *PushEffectCoverageError
	require.ErrorAs(t, err, &coverageErr)
	var users, audits int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE _sync_scope_id=$1 AND id=$2`, schemaID), actor.UserID, droppedID).Scan(&users))
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.audit_events WHERE _sync_scope_id=$1 AND id=$2`, schemaID), actor.UserID, droppedID).Scan(&audits))
	require.Zero(t, users)
	require.Zero(t, audits)

	secretID := uuid.NewString()
	_, _, err = pushRowsWithGeneration(t, ctx, service, actor, proof, 2, []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": secretID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"email":"secret@example.com","id":%q,"name":"SECRET_FAIL"}`, secretID)),
	}})
	var safeErr *PushFailedError
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &safeErr)
	require.False(t, errors.As(err, &pgErr))
	require.NotContains(t, err.Error(), "hidden_secret_check")
	require.NotContains(t, err.Error(), "SECRET_FAIL")

	codecID := uuid.NewString()
	_, _, err = pushRowsWithGeneration(t, ctx, service, actor, proof, 2, []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": codecID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"email":"codec@example.com","id":%q,"name":"CODEC"}`, codecID)),
	}})
	safeErr = nil
	require.ErrorAs(t, err, &safeErr)
	require.NotContains(t, err.Error(), "hidden_amount")
	require.NotContains(t, err.Error(), "numeric")
	require.NotContains(t, err.Error(), "NaN")

	t.Run("WithinSyncBundle", func(t *testing.T) {
		trustedActor := Actor{UserID: "trusted-within-user", SourceID: "trusted-within"}
		mustInitializeEmptyScope(t, ctx, service, trustedActor.UserID, "reader")
		rowID := uuid.NewString()
		err := service.WithinSyncBundle(ctx, trustedActor, BundleSource{SourceID: trustedActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users(id,name,email) VALUES($1,'Bob','trusted@example.com')`, schemaID), rowID)
			return err
		})
		require.NoError(t, err)
		assertDeferredTrustedEffectsCaptured(t, ctx, pool, schemaID, trustedActor.UserID, rowID)
	})

	t.Run("ScopeManager", func(t *testing.T) {
		scopeID := "trusted-manager-user"
		rowID := uuid.NewString()
		manager := NewScopeManager(service, ScopeManagerConfig{})
		result, err := manager.ExecWrite(ctx, scopeID, ScopeWriteOptions{
			WriterID: "trusted-manager", RetryableWriteOptions: retryableWriteOptionsForTest(),
			RequiredEffectTables: []RegisteredEffectTable{{Schema: schema, Table: "users"}},
		}, func(ctx context.Context, tx DatabaseWriteTx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users(id,name,email) VALUES($1,'Bob','manager@example.com')`, schemaID), rowID)
			return err
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), result.Bundle.RowCount)
		assertDeferredTrustedEffectsCaptured(t, ctx, pool, schemaID, scopeID, rowID)
	})
}

func assertDeferredTrustedEffectsCaptured(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaID, userID, rowID string) {
	t.Helper()
	var name string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE _sync_scope_id=$1 AND id=$2`, schemaID), userID, rowID).Scan(&name))
	require.Equal(t, "BOB", name)
	var auditCount, canonicalRows int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.audit_events WHERE _sync_scope_id=$1 AND id=$2 AND detail='normalized'`, schemaID), userID, rowID).Scan(&auditCount))
	require.Equal(t, int64(1), auditCount)
	require.NoError(t, pool.QueryRow(ctx, `SELECT row_count FROM sync.bundle_log WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1) ORDER BY bundle_seq DESC LIMIT 1`, userID).Scan(&canonicalRows))
	require.Equal(t, int64(2), canonicalRows)
}

func TestDirectPush_AcquisitionFailuresAreSanitizedAtDirectGoBoundary(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	rows := []PushRequestRow{fixture.userRow(uuid.New(), "Acquisition")}

	for _, tc := range []struct {
		name      string
		configure func()
		expected  error
	}{
		{
			name: "service shutting down",
			configure: func() {
				fixture.svc.lifecycle = serviceLifecycleShuttingDown
				fixture.svc.bootstrapReadiness = bootstrapReadinessReady
			},
			expected: errServiceShuttingDown,
		},
		{
			name: "service not ready",
			configure: func() {
				fixture.svc.lifecycle = serviceLifecycleRunning
				fixture.svc.bootstrapReadiness = bootstrapReadinessNotReady
			},
			expected: errServiceNotReady,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture.svc.mu.Lock()
			tc.configure()
			fixture.svc.mu.Unlock()
			_, err := fixture.push(t, ctx, fixture.writer, 1, rows)
			require.ErrorIs(t, err, tc.expected)
			require.Equal(t, tc.expected, err, "sanitization must preserve the exact safe lifecycle sentinel without its wrapper")

			recorder := httptest.NewRecorder()
			handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
			require.True(t, handlers.writePushTransactionFailure(recorder, nil, err))
			require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
			require.Equal(t, "service_not_ready", decodeErrorResponse(t, recorder).Error)

			fixture.svc.mu.Lock()
			fixture.svc.lifecycle = serviceLifecycleRunning
			fixture.svc.bootstrapReadiness = bootstrapReadinessReady
			fixture.svc.mu.Unlock()
			require.Zero(t, fixture.businessUserCount(t, ctx))
		})
	}

	for _, tc := range []struct {
		name  string
		hooks *userUploadConnTestHooks
	}{
		{
			name: "pool acquisition",
			hooks: &userUploadConnTestHooks{beforeAcquire: func(context.Context) error {
				return fmt.Errorf("authentication exposed hidden_pool_role: %w", &pgconn.PgError{Code: "28000", Message: "hidden authentication failure", Detail: "hidden_pool_role"})
			}},
		},
		{
			name: "advisory lock acquisition",
			hooks: &userUploadConnTestHooks{beforeAdvisoryLock: func(context.Context, *pgxpool.Conn) error {
				return fmt.Errorf("lock exposed hidden_lock_constraint: %w", &pgconn.PgError{Code: "XX000", Message: "hidden lock failure", ConstraintName: "hidden_lock_constraint"})
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture.svc.uploadConnHooks = tc.hooks
			_, err := fixture.push(t, ctx, fixture.writer, 1, rows)
			var safeErr *PushFailedError
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &safeErr)
			require.False(t, errors.As(err, &pgErr))
			require.Equal(t, "push failed", err.Error())
			require.Zero(t, fixture.businessUserCount(t, ctx))
		})
	}

	fixture.svc.uploadConnHooks = nil
	committed, err := fixture.push(t, ctx, fixture.writer, 1, rows)
	require.NoError(t, err)
	require.Equal(t, int64(1), committed.Bundle.RowCount)
}

func TestBootstrap_PopulatedAdoptionRejectsCanonicalBundleLimitsAtomically(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		rowCount             int
		keyLen               int
		valueLen             int
		maxRows              int
		maxBytes             int
		expectKeySuppression bool
	}{
		{name: "row ceiling plus one", rowCount: 2, valueLen: 8, maxRows: 1, maxBytes: 1 << 20},
		{name: "database payload exceeds query ceiling", rowCount: 1, valueLen: 2048, maxRows: 100, maxBytes: 512},
		{name: "database text key exceeds query ceiling", rowCount: 1, keyLen: 2048, valueLen: 8, maxRows: 100, maxBytes: 512, expectKeySuppression: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schema := "adopt_limit_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			schemaID := pgx.Identifier{schema}.Sanitize()
			tableID := pgx.Identifier{schema, "items"}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s; CREATE TABLE %s (_sync_scope_id TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (_sync_scope_id,id))`, schemaID, tableID))
			require.NoError(t, err)
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
			for i := range tc.rowCount {
				key := fmt.Sprintf("id-%d", i)
				if tc.keyLen > 0 {
					key = strings.Repeat("k", tc.keyLen)
				}
				_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES ($1,$2,$3)`, tableID), "adopted", key, strings.Repeat("x", tc.valueLen))
				require.NoError(t, err)
			}
			registered := []RegisteredTable{{Schema: schema, Table: "items", SyncKeyColumns: []string{"id"}}}
			service, err := newTestRuntimeService(pool, &ServiceConfig{
				RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
				SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
				MaxRowsPerBundle:  tc.maxRows, MaxBytesPerBundle: tc.maxBytes,
				MaxBytesPerPullResponse: int64(max(tc.maxBytes, 1<<20)), AppName: "adoption-limit",
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = service.Close(context.Background()) })
			canonicalized := 0
			keySuppressed := false
			if tc.name != "row ceiling plus one" {
				service.adoptionHooks = &adoptionTestHooks{
					onCanonicalized: func(int) { canonicalized++ },
					onReaderCreated: func(_ int, reader *adoptionBusinessScopeReader) {
						reader.afterBoundedRowScan = func(keyValid bool, keyBytes int64, _ bool, _ int64) {
							if !keyValid && keyBytes > int64(tc.maxBytes) {
								keySuppressed = true
							}
						}
					},
				}
			}
			err = service.Bootstrap(ctx)
			var projectionErr *GenerationProjectionError
			require.ErrorAs(t, err, &projectionErr)
			require.Equal(t, BundleLimitExceeded, projectionErr.Code)
			if tc.name != "row ceiling plus one" {
				require.Zero(t, canonicalized, "the query must suppress an oversized payload before Go decoding and canonicalization")
			}
			if tc.expectKeySuppression {
				require.True(t, keySuppressed, "the query must suppress an oversized text key before Go string allocation")
			}
			var businessRows int
			require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, tableID)).Scan(&businessRows))
			require.Equal(t, tc.rowCount, businessRows)
			var syncSchemaExists bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
			require.False(t, syncSchemaExists)
		})
	}
}

func TestBootstrap_PopulatedAdoptionCanonicalByteBoundary(t *testing.T) {
	for _, delta := range []int64{0, -1} {
		name := "exact ceiling"
		if delta < 0 {
			name = "ceiling plus one"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schema := "adopt_exact_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			schemaID := pgx.Identifier{schema}.Sanitize()
			tableID := pgx.Identifier{schema, "items"}.Sanitize()
			require.NoError(t, func() error {
				_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s; CREATE TABLE %s (_sync_scope_id TEXT NOT NULL,id TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(_sync_scope_id,id)); INSERT INTO %s VALUES('adopted','id-0','boundary')`, schemaID, tableID, tableID))
				return err
			}())
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
			payload, err := jcs.Marshal(map[string]any{"id": "id-0", "value": "boundary"})
			require.NoError(t, err)
			hasher := protocolhash.NewCommittedBundleHasher()
			require.NoError(t, hasher.Add(protocolhash.BundleRow{Schema: schema, Table: "items", Key: map[string]any{"id": "id-0"}, Op: OpInsert, RowVersion: 1, Payload: payload}))
			_, exactBytes, err := hasher.Sum()
			require.NoError(t, err)
			limit := int(exactBytes + delta)
			registered := []RegisteredTable{{Schema: schema, Table: "items", SyncKeyColumns: []string{"id"}}}
			service, err := newTestRuntimeService(pool, &ServiceConfig{
				RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
				SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
				MaxRowsPerBundle:  100, MaxBytesPerBundle: limit, MaxBytesPerPullResponse: 1 << 20,
				AppName: "adoption-exact-boundary",
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = service.Close(context.Background()) })
			err = service.Bootstrap(ctx)
			if delta == 0 {
				require.NoError(t, err)
				var storedBytes int64
				require.NoError(t, pool.QueryRow(ctx, `SELECT byte_count FROM sync.bundle_log`).Scan(&storedBytes))
				require.Equal(t, exactBytes, storedBytes)
				return
			}
			var projectionErr *GenerationProjectionError
			require.ErrorAs(t, err, &projectionErr)
			require.Equal(t, BundleLimitExceeded, projectionErr.Code)
		})
	}
}

func TestCapturedAndRetainedBundles_EnforceAggregateBoundsAndIntegrityFamilies(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "bundle_bounds_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered),
		MaxRowsPerBundle:  100, MaxBytesPerBundle: 900, MaxBytesPerPullResponse: 4096,
		AppName: "bundle-bounds",
	}, integrationTestLogger(slog.LevelWarn))
	actor := Actor{UserID: "bounded-user", SourceID: "server"}
	mustInitializeEmptyScope(t, ctx, service, actor.UserID, "reader")
	tableID := pgx.Identifier{schema, "users"}.Sanitize()
	err := service.WithinSyncBundle(ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		for range 2 {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id,name,email) VALUES ($1,$2,$3)`, tableID), uuid.New(), strings.Repeat("n", 400), "e@example.com"); err != nil {
				return err
			}
		}
		return nil
	})
	var projectionErr *GenerationProjectionError
	require.ErrorAs(t, err, &projectionErr)
	require.Equal(t, BundleLimitExceeded, projectionErr.Code)
	var businessRows int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE _sync_scope_id=$1`, tableID), actor.UserID).Scan(&businessRows))
	require.Zero(t, businessRows)

	service.config.MaxBytesPerBundle = 1 << 20
	err = service.WithinSyncBundle(ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id,name,email) VALUES ($1,$2,$3)`, tableID), uuid.New(), "ok", "ok@example.com")
		return err
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE sync.bundle_rows SET row_ordinal=2 WHERE bundle_seq=1`)
	require.NoError(t, err)
	_, err = service.ProcessPull(ctx, Actor{UserID: actor.UserID, SourceID: "reader"}, testSchemaGenerationProofForService(service), 0, 10, 0)
	projectionErr = nil
	require.ErrorAs(t, err, &projectionErr)
	require.Equal(t, GenerationProjectionIntegrityError, projectionErr.Code)
}

func TestGenerationProjectionIntegrityCorruption_UsesProductionHTTPErrorFamily(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "integrity_http_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	tableIdent := pgx.Identifier{schema, "users"}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN score DOUBLE PRECISION NOT NULL DEFAULT 1`, tableIdent))
	require.NoError(t, err)
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "integrity-http",
	}, integrationTestLogger(slog.LevelWarn))
	reader := Actor{UserID: "integrity-user", SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, service, reader.UserID, reader.SourceID)
	writer := Actor{UserID: reader.UserID, SourceID: "writer"}
	rowID := uuid.New()
	require.NoError(t, service.WithinSyncBundle(ctx, writer, BundleSource{SourceID: writer.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(id,name,email) VALUES($1,'Valid','valid@example.com')`, tableIdent), rowID)
		return err
	}))
	proof := testSchemaGenerationProofForService(service)
	handlers := NewHTTPSyncHandlers(service, nil)
	assertIntegrityResponse := func(t *testing.T, method, target string, handle func(http.ResponseWriter, *http.Request)) {
		t.Helper()
		request := httptest.NewRequest(method, target, nil)
		request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
		request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
		request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
		request = request.WithContext(ContextWithActor(request.Context(), reader))
		recorder := httptest.NewRecorder()
		handle(recorder, request)
		require.Equal(t, http.StatusInternalServerError, recorder.Code)
		require.Contains(t, recorder.Body.String(), `"error":"generation_projection_integrity_error"`)
	}

	var sourceID string
	var tableID int32
	var keyBytes []byte
	var opCode int16
	require.NoError(t, pool.QueryRow(ctx, `SELECT source_id FROM sync.bundle_log WHERE bundle_seq=1`).Scan(&sourceID))
	require.NoError(t, pool.QueryRow(ctx, `SELECT table_id,key_bytes,op_code FROM sync.bundle_rows WHERE bundle_seq=1`).Scan(&tableID, &keyBytes, &opCode))
	require.NoError(t, func() error {
		_, err := pool.Exec(ctx, `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_table_id_fkey, DROP CONSTRAINT bundle_rows_op_code_chk, DROP CONSTRAINT bundle_rows_payload_by_op_chk`)
		return err
	}())
	for _, testCase := range []struct {
		name    string
		mutate  string
		value   any
		restore any
	}{
		{name: "invalid source id", mutate: `UPDATE sync.bundle_log SET source_id=$1 WHERE bundle_seq=1`, value: "bad source", restore: sourceID},
		{name: "unknown table id", mutate: `UPDATE sync.bundle_rows SET table_id=$1 WHERE bundle_seq=1`, value: int32(999999), restore: tableID},
		{name: "malformed key", mutate: `UPDATE sync.bundle_rows SET key_bytes=$1 WHERE bundle_seq=1`, value: []byte{0}, restore: keyBytes},
		{name: "invalid operation", mutate: `UPDATE sync.bundle_rows SET op_code=$1 WHERE bundle_seq=1`, value: int16(99), restore: opCode},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, func() error { _, err := pool.Exec(ctx, testCase.mutate, testCase.value); return err }())
			assertIntegrityResponse(t, http.MethodGet, "/sync/pull?after_bundle_seq=0", handlers.HandlePull)
			require.NoError(t, func() error { _, err := pool.Exec(ctx, testCase.mutate, testCase.restore); return err }())
		})
	}

	userPK, err := lookupUserPK(ctx, pool, reader.UserID)
	require.NoError(t, err)
	fakeID := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO sync.row_state(user_pk,table_id,key_bytes,bundle_seq,deleted) VALUES($1,$2,$3,1,FALSE)`, userPK, tableID, fakeID[:])
	require.NoError(t, err)
	assertIntegrityResponse(t, http.MethodPost, "/sync/snapshot-sessions", handlers.HandleCreateSnapshotSession)
	require.NoError(t, func() error {
		_, err := pool.Exec(ctx, `DELETE FROM sync.row_state WHERE user_pk=$1 AND table_id=$2 AND key_bytes=$3`, userPK, tableID, fakeID[:])
		return err
	}())

	_, err = pool.Exec(ctx, `UPDATE sync.row_state SET key_bytes=$1 WHERE user_pk=$2 AND table_id=$3 AND key_bytes=$4`, []byte{0}, userPK, tableID, keyBytes)
	require.NoError(t, err)
	assertIntegrityResponse(t, http.MethodPost, "/sync/snapshot-sessions", handlers.HandleCreateSnapshotSession)
	_, err = pool.Exec(ctx, `UPDATE sync.row_state SET key_bytes=$1 WHERE user_pk=$2 AND table_id=$3 AND key_bytes=$4`, keyBytes, userPK, tableID, []byte{0})
	require.NoError(t, err)

	for _, trigger := range []string{registeredTableCaptureTriggerName, registeredTableOwnerGuardTrigger} {
		_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER %s`, tableIdent, pgx.Identifier{trigger}.Sanitize()))
		require.NoError(t, err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET score='Infinity'::float8 WHERE _sync_scope_id=$1 AND id=$2`, tableIdent), reader.UserID, rowID)
	require.NoError(t, err)
	for _, trigger := range []string{registeredTableCaptureTriggerName, registeredTableOwnerGuardTrigger} {
		_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER %s`, tableIdent, pgx.Identifier{trigger}.Sanitize()))
		require.NoError(t, err)
	}
	conflictActor := Actor{UserID: reader.UserID, SourceID: "conflict-client"}
	_, err = service.Connect(ctx, conflictActor, proof, &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	conflictRows := []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": rowID.String()}, Op: OpUpdate, BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(`{"email":"stale@example.com","id":%q,"name":"Stale","score":"2"}`, rowID.String())),
	}}
	requestHash, err := computeGenerationPinnedPushRequestHash(proof, conflictRows)
	require.NoError(t, err)
	contract, ok := service.schemaGenerationRegistry.Generation(proof.ID)
	require.True(t, ok)
	groups, err := CompactGroupsForRows(generationDescriptor(contract), conflictRows)
	require.NoError(t, err)
	pushBody, err := json.Marshal(CompactPushRequest{SourceBundleID: 1, CanonicalRequestHash: requestHash, Rows: groups})
	require.NoError(t, err)
	commitRequest := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(pushBody))
	commitRequest.Header.Set("Content-Type", "application/json")
	commitRequest.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	commitRequest.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
	commitRequest.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
	commitRequest = commitRequest.WithContext(ContextWithActor(commitRequest.Context(), conflictActor))
	commitRecorder := httptest.NewRecorder()
	handlers.HandlePush(commitRecorder, commitRequest)
	require.Equal(t, http.StatusInternalServerError, commitRecorder.Code)
	require.Contains(t, commitRecorder.Body.String(), `"error":"generation_projection_integrity_error"`)
}

func TestHistoryProjectionResourceBound_TwoPopulationScalesKeepFixedHighWater(t *testing.T) {
	type result struct {
		metrics  historyProjectionRuntimeMetricsSnapshot
		maxWrite int
	}
	run := func(population int) result {
		ctx := context.Background()
		pool := newIntegrationTestPool(t, ctx)
		schema := "projection_bound_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
		schemaID := pgx.Identifier{schema}.Sanitize()
		tableID := pgx.Identifier{schema, "items"}.Sanitize()
		_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s; CREATE TABLE %s (_sync_scope_id TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (_sync_scope_id,id))`, schemaID, tableID))
		require.NoError(t, err)
		t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
		registered := []RegisteredTable{{Schema: schema, Table: "items", SyncKeyColumns: []string{"id"}}}
		service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
			RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
			SchemaGenerations: []SchemaGeneration{{
				ID: 1,
				Tables: []GenerationTable{{
					Schema: schema, Table: "items",
					Columns: []GenerationColumn{
						{Name: "id", WireType: GenerationWireTypeText},
						{Name: "value", WireType: GenerationWireTypeText},
					},
				}},
			}},
			AppName:          "production-projection-bound",
			MaxRowsPerBundle: 64, MaxBytesPerBundle: 8 << 10, MaxBytesPerPullResponse: 16 << 10,
			MaxConcurrentHistoryProjections: 1,
		}, integrationTestLogger(slog.LevelWarn))
		actor := Actor{UserID: fmt.Sprintf("population-%d", population), SourceID: "reader"}
		mustInitializeEmptyScope(t, ctx, service, actor.UserID, actor.SourceID)
		var userPK int64
		var registeredTableID int32
		require.NoError(t, pool.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1`, actor.UserID).Scan(&userPK))
		require.NoError(t, pool.QueryRow(ctx, `SELECT table_id FROM sync.table_catalog WHERE schema_name=$1 AND table_name='items'`, schema).Scan(&registeredTableID))

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		for bundleSeq := int64(1); bundleSeq <= int64(population); bundleSeq++ {
			bundleRows := make([]BundleRow, 8)
			storageKeys := make([][]byte, 8)
			for rowIndex := range bundleRows {
				id := fmt.Sprintf("item-%06d-%02d", bundleSeq, rowIndex)
				payload, marshalErr := jcs.Marshal(map[string]any{"id": id, "value": strings.Repeat("v", 256)})
				require.NoError(t, marshalErr)
				bundleRows[rowIndex] = BundleRow{Schema: schema, Table: "items", Key: SyncKey{"id": id}, Op: OpInsert, RowVersion: bundleSeq, Payload: payload}
				storageKeys[rowIndex] = []byte(id)
			}
			hash, byteCount, hashErr := computeCommittedBundleHash(bundleRows)
			require.NoError(t, hashErr)
			_, err = tx.Exec(ctx, `INSERT INTO sync.bundle_log (user_pk,bundle_seq,source_id,source_bundle_id,row_count,byte_count,bundle_hash,canonical_request_hash,committed_at) VALUES ($1,$2,'seed',$2,$3,$4,$5,'',now())`, userPK, bundleSeq, len(bundleRows), byteCount, hash)
			require.NoError(t, err)
			for rowIndex := range bundleRows {
				_, err = tx.Exec(ctx, `INSERT INTO sync.bundle_rows (user_pk,bundle_seq,row_ordinal,table_id,key_bytes,op_code,payload_wire) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)`, userPK, bundleSeq, rowIndex+1, registeredTableID, storageKeys[rowIndex], opCodeInsert, []byte(bundleRows[rowIndex].Payload))
				require.NoError(t, err)
			}
		}
		_, err = tx.Exec(ctx, `UPDATE sync.user_state SET next_bundle_seq=$2 WHERE user_pk=$1`, userPK, population+1)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		service.historyProjectionMetrics = historyProjectionRuntimeMetrics{}
		handlers := NewHTTPSyncHandlers(service, integrationTestLogger(slog.LevelWarn))
		proof := proofForGeneration(t, service, 1)
		var after int64
		maxWrite := 0
		for after < int64(population) {
			request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/sync/pull?after_bundle_seq=%d&max_bundles=8&target_bundle_seq=%d", after, population), nil)
			request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
			request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
			request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
			request = request.WithContext(ContextWithActor(request.Context(), actor))
			writer := &observedProjectionResponseWriter{ResponseRecorder: httptest.NewRecorder()}
			handlers.HandlePull(writer, request)
			require.Equal(t, http.StatusOK, writer.Code, writer.Body.String())
			maxWrite = max(maxWrite, writer.maxWrite)
			var response CompactPullResponse
			require.NoError(t, json.NewDecoder(bytes.NewReader(writer.Body.Bytes())).Decode(&response))
			require.NotEmpty(t, response.Bundles)
			after = response.Bundles[len(response.Bundles)-1].BundleSeq
		}
		metrics := service.historyProjectionRuntimeMetricsSnapshot()
		require.Equal(t, int64(0), metrics.Active)
		require.Equal(t, int64(1), metrics.ActiveHighWater)
		t.Logf("population=%d owned_buffers_high=%d owned_bytes_high=%d retained_rows_high=%d retained_logical_bytes_high=%d permit_high=%d max_encoder_write=%d canonical_rows_total=%d",
			population, metrics.OwnedBuffersHigh, metrics.OwnedBytesHigh, metrics.RetainedRowsHigh, metrics.RetainedBytesHigh, metrics.ActiveHighWater, maxWrite, metrics.CanonicalRows)
		return result{metrics: metrics, maxWrite: maxWrite}
	}

	small := run(16)
	large := run(512)
	require.Equal(t, small.metrics.OwnedBuffersHigh, large.metrics.OwnedBuffersHigh)
	require.Equal(t, small.metrics.OwnedBytesHigh, large.metrics.OwnedBytesHigh)
	require.Equal(t, small.metrics.RetainedRowsHigh, large.metrics.RetainedRowsHigh)
	require.LessOrEqual(t, small.metrics.RetainedBytesHigh, int64(32<<10))
	require.LessOrEqual(t, large.metrics.RetainedBytesHigh, int64(32<<10))
	require.Less(t, small.maxWrite, 20<<10)
	require.Less(t, large.maxWrite, 20<<10)
	require.Greater(t, large.metrics.CanonicalRows, small.metrics.CanonicalRows)
	require.Less(t, large.metrics.OwnedBytesHigh, int64(64<<10))
	require.Less(t, large.maxWrite, 20<<10)
}

func TestHistoryProjectionHTTPLease_RemainsTrackedThroughResponseLifetime(t *testing.T) {
	newFixture := func(t *testing.T) (*SyncService, *HTTPSyncHandlers, Actor, SchemaGenerationProof) {
		t.Helper()
		ctx := context.Background()
		pool := newIntegrationTestPool(t, ctx)
		schema := "projection_lease_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
		require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
		t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
		registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
		service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
			SchemaGenerations:               testSchemaGenerationsForRegisteredTables(registered),
			RegisteredTables:                registered,
			RegisteredTableCatalogRevision:  1,
			MaxConcurrentHistoryProjections: 1,
		}, integrationTestLogger(slog.LevelWarn))
		reader := Actor{UserID: "lease-user", SourceID: "reader"}
		writer := Actor{UserID: reader.UserID, SourceID: "writer"}
		mustPushUserBundle(t, ctx, service, writer, schema, 1, uuid.New(), "Lease")
		mustInitializeEmptyScope(t, ctx, service, reader.UserID, reader.SourceID)
		return service, NewHTTPSyncHandlers(service, integrationTestLogger(slog.LevelWarn)), reader, testSchemaGenerationProofForService(service)
	}
	requestFor := func(ctx context.Context, actor Actor, proof SchemaGenerationProof) *http.Request {
		request := httptest.NewRequest(http.MethodGet, "/sync/pull?after_bundle_seq=0", nil).WithContext(ctx)
		request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
		request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
		request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
		return request.WithContext(ContextWithActor(request.Context(), actor))
	}

	t.Run("shutdown waits for blocked response write", func(t *testing.T) {
		service, handlers, actor, proof := newFixture(t)
		release := make(chan struct{})
		writer := newBlockingSnapshotResponseWriter(release)
		handlerDone := make(chan struct{})
		go func() {
			defer close(handlerDone)
			handlers.HandlePull(writer, requestFor(context.Background(), actor, proof))
		}()
		waitForSnapshotWriter(t, writer)
		require.Equal(t, int64(1), service.historyProjectionRuntimeMetricsSnapshot().Active)

		closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := service.Close(closeCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		close(release)
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("pull handler did not release after writer unblocked")
		}
		require.NoError(t, service.Close(context.Background()))
		require.Zero(t, service.historyProjectionRuntimeMetricsSnapshot().Active)
	})

	t.Run("request cancellation releases response lease", func(t *testing.T) {
		service, handlers, actor, proof := newFixture(t)
		requestCtx, cancel := context.WithCancel(context.Background())
		writer := &cancelBlockingProjectionWriter{header: make(http.Header), started: make(chan struct{}), ctx: requestCtx}
		handlerDone := make(chan struct{})
		go func() {
			defer close(handlerDone)
			handlers.HandlePull(writer, requestFor(requestCtx, actor, proof))
		}()
		select {
		case <-writer.started:
		case <-time.After(5 * time.Second):
			t.Fatal("pull writer did not block")
		}
		cancel()
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled pull handler did not return")
		}
		require.Zero(t, service.historyProjectionRuntimeMetricsSnapshot().Active)
		require.NoError(t, service.Close(context.Background()))
	})

	t.Run("write failure releases response lease", func(t *testing.T) {
		service, handlers, actor, proof := newFixture(t)
		writer := &failingProjectionResponseWriter{header: make(http.Header)}
		handlers.HandlePull(writer, requestFor(context.Background(), actor, proof))
		require.Zero(t, service.historyProjectionRuntimeMetricsSnapshot().Active)
		require.NoError(t, service.Close(context.Background()))
	})
}

func TestPushHTTPLease_RemainsTrackedThroughResponseLifetime(t *testing.T) {
	type fixture struct {
		service  *SyncService
		handlers *HTTPSyncHandlers
		actor    Actor
		proof    SchemaGenerationProof
		body     []byte
	}
	newFixture := func(t *testing.T) fixture {
		t.Helper()
		ctx := context.Background()
		pool := newIntegrationTestPool(t, ctx)
		schema := "push_chunk_lease_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
		require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
		t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
		registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
		service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
			SchemaGenerations:               testSchemaGenerationsForRegisteredTables(registered),
			RegisteredTables:                registered,
			RegisteredTableCatalogRevision:  1,
			MaxConcurrentHistoryProjections: 1,
		}, integrationTestLogger(slog.LevelWarn))
		actor := Actor{UserID: "push-chunk-lease-user", SourceID: "device"}
		proof := testSchemaGenerationProofForService(service)
		rowID := uuid.NewString()
		rows := []PushRequestRow{{
			Schema: schema, Table: "users", Key: SyncKey{"id": rowID}, Op: OpInsert,
			Payload: json.RawMessage(fmt.Sprintf(`{"email":"lease@example.com","id":"%s","name":"Lease"}`, rowID)),
		}}
		initializationID := resolveConnectForPush(t, ctx, service, actor, true)
		contract, ok := service.schemaGenerationRegistry.Generation(proof.ID)
		require.True(t, ok)
		groups, err := CompactGroupsForRows(generationDescriptor(contract), rows)
		require.NoError(t, err)
		body, err := json.Marshal(CompactPushRequest{
			SourceBundleID: 1, CanonicalRequestHash: mustCanonicalPushRequestHash(t, service, rows),
			InitializationID: initializationID, Rows: groups,
		})
		require.NoError(t, err)
		return fixture{service: service, handlers: NewHTTPSyncHandlers(service, integrationTestLogger(slog.LevelWarn)), actor: actor, proof: proof, body: body}
	}
	requestFor := func(ctx context.Context, f fixture) *http.Request {
		request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(f.body)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
		request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(f.proof.ID))
		request.Header.Set(SchemaGenerationFingerprintHeader, f.proof.Fingerprint)
		return request.WithContext(ContextWithActor(request.Context(), f.actor))
	}

	t.Run("shutdown waits for blocked response write", func(t *testing.T) {
		f := newFixture(t)
		release := make(chan struct{})
		writer := newBlockingSnapshotResponseWriter(release)
		handlerDone := make(chan struct{})
		go func() {
			defer close(handlerDone)
			f.handlers.HandlePush(writer, requestFor(context.Background(), f))
		}()
		waitForSnapshotWriter(t, writer)
		require.Equal(t, int64(1), f.service.historyProjectionRuntimeMetricsSnapshot().Active)

		closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, f.service.Close(closeCtx), context.DeadlineExceeded)
		close(release)
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("push chunk handler did not release after writer unblocked")
		}
		require.NoError(t, f.service.Close(context.Background()))
		require.Zero(t, f.service.historyProjectionRuntimeMetricsSnapshot().Active)
	})

	t.Run("request cancellation releases response lease", func(t *testing.T) {
		f := newFixture(t)
		requestCtx, cancel := context.WithCancel(context.Background())
		writer := &cancelBlockingProjectionWriter{header: make(http.Header), started: make(chan struct{}), ctx: requestCtx}
		handlerDone := make(chan struct{})
		go func() {
			defer close(handlerDone)
			f.handlers.HandlePush(writer, requestFor(requestCtx, f))
		}()
		select {
		case <-writer.started:
		case <-time.After(5 * time.Second):
			t.Fatal("push chunk writer did not block")
		}
		cancel()
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled push chunk handler did not return")
		}
		require.Zero(t, f.service.historyProjectionRuntimeMetricsSnapshot().Active)
		require.NoError(t, f.service.Close(context.Background()))
	})

	t.Run("write failure releases response lease", func(t *testing.T) {
		f := newFixture(t)
		writer := &failingProjectionResponseWriter{header: make(http.Header)}
		f.handlers.HandlePush(writer, requestFor(context.Background(), f))
		require.Zero(t, f.service.historyProjectionRuntimeMetricsSnapshot().Active)
		require.NoError(t, f.service.Close(context.Background()))
	})
}

func TestPushConflict_OversizedVisibleRowIsSuppressedBeforeGoMaterialization(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "conflict_bound_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	largeConfig := &ServiceConfig{
		SchemaGenerations:               testSchemaGenerationsForRegisteredTables(registered),
		RegisteredTables:                registered,
		RegisteredTableCatalogRevision:  1,
		MaxRowsPerBundle:                8,
		MaxBytesPerBundle:               64 << 10,
		MaxBytesPerPullResponse:         64 << 10,
		MaxConcurrentHistoryProjections: 1,
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, largeConfig, integrationTestLogger(slog.LevelWarn))
	userID := "conflict-user"
	mustInitializeEmptyScope(t, ctx, service, userID, "initializing-reader")
	rowID := uuid.New()
	largeName := strings.Repeat("v", 12<<10)
	err := service.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users (id,name,email) VALUES ($1,$2,$3)`, pgx.Identifier{schemaName}.Sanitize()), rowID, largeName, "large@example.com")
		return execErr
	})
	require.NoError(t, err)
	require.NoError(t, service.Close(ctx))

	smallConfig := *largeConfig
	smallConfig.MaxBytesPerBundle = 4096
	smallConfig.MaxBytesPerPullResponse = 4096
	smallConfig.MaxPushOutcomeDecodedBytes = 73728 // preserve immutable store ceiling across a projection-limit restart
	restarted := newBootstrappedIntegrationService(t, ctx, pool, &smallConfig, integrationTestLogger(slog.LevelWarn))
	var observation conflictRowScanObservation
	restarted.uploadConnHooks = &userUploadConnTestHooks{afterConflictRowScan: func(actual conflictRowScanObservation) {
		observation = actual
	}}
	_, err = pushRowsOneShot(t, ctx, restarted, Actor{UserID: userID, SourceID: "device"}, 1, []PushRequestRow{{
		Schema: schemaName, Table: "users", Key: SyncKey{"id": rowID.String()}, Op: OpUpdate, BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(`{"email":"small@example.com","id":"%s","name":"small"}`, rowID)),
	}})
	var limitErr *PushOutcomeError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "push_outcome_too_large", limitErr.Code)
	require.Greater(t, observation.ReportedPayloadByteCount, int64(smallConfig.MaxBytesPerBundle))
	require.False(t, observation.PayloadMaterialized, "oversized authoritative data must be suppressed before the driver scans it")
	require.Zero(t, observation.MaterializedPayloadBytes)
	require.Zero(t, restarted.historyProjectionRuntimeMetricsSnapshot().Active)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sync.bundle_log AS bundle
		JOIN sync.user_state AS owner ON owner.user_pk = bundle.user_pk
		WHERE owner.user_id = $1
	`, userID).Scan(&bundleCount))
	require.Equal(t, 1, bundleCount, "the rejected stale push must not commit another bundle")
}
