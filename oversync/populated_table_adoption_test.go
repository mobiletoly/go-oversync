package oversync

import (
	"bytes"
	"context"
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
	"github.com/stretchr/testify/require"
)

func TestBootstrap_PopulatedAdoptionReadiness(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "adopt_ready_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "populated-adoption-readiness",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	actor := Actor{UserID: "readiness-user", SourceID: "reader"}

	_, err = svc.Connect(ctx, actor, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)
	status, err := svc.GetStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, "unhealthy", status.Status)
	require.False(t, status.AcceptingOperations)

	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))
	req := httptest.NewRequest(http.MethodPost, "/sync/connect", bytes.NewBufferString(`{"has_local_pending_rows":false}`))
	req.Header.Set("Content-Type", "application/json")
	setTestSchemaGenerationHeaders(req, svc)
	req = req.WithContext(ContextWithActor(req.Context(), actor))
	rec := httptest.NewRecorder()
	handlers.HandleConnect(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), `"service_not_ready"`)

	require.NoError(t, svc.Bootstrap(ctx))
	status, err = svc.GetStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.AcceptingOperations)
	connect, err := svc.Connect(ctx, actor, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", connect.Resolution)
}

func TestBootstrap_FreshPopulatedAdoptionPreservesExactDurableState(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()

	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, tableIdent))
	require.NoError(t, err)

	userID := "existing-user-" + suffix
	rowID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name)
		VALUES ($1, $2, $3)
	`, tableIdent), userID, rowID, "Existing")
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "populated-table-adoption",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	require.NoError(t, svc.Bootstrap(ctx))
	var businessScope, businessName string
	var businessID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT _sync_scope_id, id, name FROM %s`, tableIdent)).Scan(&businessScope, &businessID, &businessName))
	require.Equal(t, userID, businessScope)
	require.Equal(t, rowID, businessID)
	require.Equal(t, "Existing", businessName)
	var snapshotSessions, captureRows int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM sync.snapshot_sessions),
			(SELECT count(*) FROM sync.bundle_capture_stage)
	`).Scan(&snapshotSessions, &captureRows))
	require.Zero(t, snapshotSessions)
	require.Zero(t, captureRows)

	actor := Actor{UserID: userID, SourceID: "reader"}
	connect, err := svc.Connect(ctx, actor, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)

	pull, err := svc.ProcessPull(ctx, actor, testSchemaGenerationProofForService(svc), 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), pull.StableBundleSeq)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.True(t, strings.HasPrefix(pull.Bundles[0].SourceID, adoptionSourcePrefix))
	require.Equal(t, int64(1), pull.Bundles[0].SourceBundleID)
	require.Equal(t, int64(1), pull.Bundles[0].Rows[0].RowVersion)
	require.Equal(t, rowID.String(), pull.Bundles[0].Rows[0].Key["id"])
	require.JSONEq(t, fmt.Sprintf(`{"id":"%s","name":"Existing"}`, rowID), string(pull.Bundles[0].Rows[0].Payload))
	expectedRequestHash, err := computeCanonicalPushRequestHash([]PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        pull.Bundles[0].Rows[0].Payload,
	}})
	require.NoError(t, err)
	require.Empty(t, pull.Bundles[0].CanonicalRequestHash)
	expectedBundleHash, expectedByteCount, err := computeCommittedBundleHash(pull.Bundles[0].Rows)
	require.NoError(t, err)
	require.Equal(t, renderBundleHash(expectedBundleHash), pull.Bundles[0].BundleHash)
	var storedRowCount, storedByteCount, storedSourceBundleID int64
	var storedHash []byte
	var storedSourceID, storedRequestHash string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT bundle.row_count, bundle.byte_count, bundle.bundle_hash,
			bundle.source_id, bundle.source_bundle_id, bundle.canonical_request_hash
		FROM sync.bundle_log AS bundle
		JOIN sync.user_state AS users ON users.user_pk = bundle.user_pk
		WHERE users.user_id = $1 AND bundle.bundle_seq = 1
	`, userID).Scan(&storedRowCount, &storedByteCount, &storedHash, &storedSourceID, &storedSourceBundleID, &storedRequestHash))
	require.Equal(t, int64(1), storedRowCount)
	require.Equal(t, expectedByteCount, storedByteCount)
	require.Equal(t, expectedBundleHash, storedHash)
	require.Equal(t, pull.Bundles[0].SourceID, storedSourceID)
	require.Equal(t, int64(1), storedSourceBundleID)
	require.Equal(t, expectedRequestHash, storedRequestHash)

	session, err := svc.CreateSnapshotSession(ctx, actor, testSchemaGenerationProofForService(svc))
	require.NoError(t, err)
	require.Equal(t, int64(1), session.SnapshotBundleSeq)
	require.Equal(t, int64(1), session.RowCount)

	chunk, err := svc.GetSnapshotChunk(ctx, actor, testSchemaGenerationProofForService(svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, int64(1), chunk.Rows[0].RowVersion)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
	require.JSONEq(t, fmt.Sprintf(`{"id":"%s","name":"Existing"}`, rowID), string(chunk.Rows[0].Payload))

	var (
		nextBundleSeq       int64
		retainedBundleFloor int64
		scopeStateCode      int16
		initializedAt       time.Time
		initializedBy       string
		initializerSource   *string
		initializationID    *uuid.UUID
		leaseExpiresAt      *time.Time
		rowStateCount       int64
		sourceStateCount    int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT users.next_bundle_seq, users.retained_bundle_floor, scope.state_code,
			scope.initialized_at, scope.initialized_by_source_id,
			scope.initializer_source_id, scope.initialization_id, scope.lease_expires_at
		FROM sync.user_state AS users
		JOIN sync.scope_state AS scope ON scope.user_pk = users.user_pk
		WHERE users.user_id = $1
	`, userID).Scan(
		&nextBundleSeq, &retainedBundleFloor, &scopeStateCode,
		&initializedAt, &initializedBy, &initializerSource, &initializationID, &leaseExpiresAt,
	))
	require.Equal(t, int64(2), nextBundleSeq)
	require.Zero(t, retainedBundleFloor)
	require.Equal(t, scopeStateCodeInitialized, scopeStateCode)
	require.False(t, initializedAt.IsZero())
	require.Equal(t, storedSourceID, initializedBy)
	require.Nil(t, initializerSource)
	require.Nil(t, initializationID)
	require.Nil(t, leaseExpiresAt)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.row_state AS state
		JOIN sync.user_state AS users ON users.user_pk = state.user_pk
		WHERE users.user_id = $1 AND state.bundle_seq = 1 AND NOT state.deleted
	`, userID).Scan(&rowStateCount))
	require.Equal(t, int64(1), rowStateCount)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.source_state AS source
		JOIN sync.user_state AS users ON users.user_pk = source.user_pk
		WHERE users.user_id = $1
		  AND source.source_id LIKE 'oversync-adoption:%'
		  AND source.state = 'active'
		  AND source.max_committed_source_bundle_id = 1
	`, userID).Scan(&sourceStateCount))
	require.Equal(t, int64(1), sourceStateCount)
}

func TestBootstrap_CloseWaitsForActiveBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_close_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	blocker, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer blocker.Release()
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, syncBootstrapLockKey)
	require.NoError(t, err)
	locked := true
	defer func() {
		if locked {
			_, _ = blocker.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey)
		}
	}()

	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "bootstrap-close-drain",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	bootstrapResult := make(chan error, 1)
	go func() { bootstrapResult <- svc.Bootstrap(ctx) }()
	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return svc.bootstrapReadiness == bootstrapReadinessBootstrapping
	}, 5*time.Second, 10*time.Millisecond)

	closeResult := make(chan error, 1)
	go func() { closeResult <- svc.Close(ctx) }()
	var earlyCloseErr error
	earlyClose := false
	select {
	case earlyCloseErr = <-closeResult:
		earlyClose = true
	case <-time.After(150 * time.Millisecond):
	}

	var unlocked bool
	require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey).Scan(&unlocked))
	require.True(t, unlocked)
	locked = false
	require.NoError(t, <-bootstrapResult)
	if !earlyClose {
		earlyCloseErr = <-closeResult
	}
	require.False(t, earlyClose, "Close returned while bootstrap could still mutate the database")
	require.NoError(t, earlyCloseErr)
	lifecycle, inFlight, accepting := svc.lifecycleSnapshot()
	require.Equal(t, serviceLifecycleClosed, lifecycle)
	require.Zero(t, inFlight)
	require.False(t, accepting)
}

func TestBootstrap_PopulatedAdoptionConcurrentInstances(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_parallel_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "parallel-owner-" + suffix
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New())
	require.NoError(t, err)
	config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "parallel-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 1}
	services := make([]*SyncService, 2)
	for i := range services {
		services[i], err = newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		t.Cleanup(func() { _ = services[i].Close(context.Background()) })
	}
	start := make(chan struct{})
	errs := make(chan error, len(services))
	var wg sync.WaitGroup
	for _, svc := range services {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- svc.Bootstrap(ctx)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var bundleCount, sourceCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.source_state`).Scan(&sourceCount))
	require.Equal(t, int64(1), bundleCount)
	require.Equal(t, int64(1), sourceCount)
}

func TestBootstrap_PopulatedAdoptionConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_lock_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "lock-owner-" + suffix
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()

	writerTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	writerDone := false
	t.Cleanup(func() {
		if !writerDone {
			_ = writerTx.Rollback(context.Background())
		}
	})
	_, err = writerTx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Before Lock', 'before@example.com')
	`, tableIdent), userID, uuid.New())
	require.NoError(t, err)

	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "locking-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 1}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	bootstrapResult := make(chan error, 1)
	go func() { bootstrapResult <- svc.Bootstrap(ctx) }()

	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return svc.bootstrapReadiness == bootstrapReadinessBootstrapping
	}, 5*time.Second, 10*time.Millisecond)
	_, err = svc.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)

	require.NoError(t, writerTx.Commit(ctx))
	writerDone = true
	require.NoError(t, <-bootstrapResult)
	connect, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)
	var rowStateCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rowStateCount))
	require.Equal(t, int64(1), rowStateCount)

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Direct', 'direct@example.com')
	`, tableIdent), userID, uuid.New())
	require.Error(t, err, "a writer waking after adoption must encounter the owner guard")

	managedID := uuid.New()
	bundle, err := pushRowsOneShot(t, ctx, svc, Actor{UserID: userID, SourceID: "managed-writer"}, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": managedID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        []byte(fmt.Sprintf(`{"id":"%s","name":"Managed","email":"managed@example.com"}`, managedID)),
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), bundle.BundleSeq, "the first post-adoption managed write must be contiguous")
}

func TestBootstrap_PopulatedAdoptionRollbackAndRetry(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_retry_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "retry-owner-" + suffix
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New())
	require.NoError(t, err)

	eventName := "reject_adoption_trigger_" + suffix
	functionName := "reject_adoption_trigger_fn_" + suffix
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION public.%s()
		RETURNS event_trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic adoption trigger failure'; END;
		$$;
		CREATE EVENT TRIGGER %s ON ddl_command_start
		WHEN TAG IN ('CREATE TRIGGER') EXECUTE FUNCTION public.%s();
	`, pgx.Identifier{functionName}.Sanitize(), pgx.Identifier{eventName}.Sanitize(), pgx.Identifier{functionName}.Sanitize()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP EVENT TRIGGER IF EXISTS "+pgx.Identifier{eventName}.Sanitize())
		_, _ = pool.Exec(context.Background(), "DROP FUNCTION IF EXISTS public."+pgx.Identifier{functionName}.Sanitize()+"()")
	})

	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "retry-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 1}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	err = svc.Bootstrap(ctx)
	require.ErrorContains(t, err, "synthetic adoption trigger failure")
	var syncSchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
	require.False(t, syncSchemaExists, "failed adoption must roll back schema and state")
	_, connectErr := svc.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.True(t, errors.Is(connectErr, errServiceNotReady))

	_, err = pool.Exec(ctx, "DROP EVENT TRIGGER "+pgx.Identifier{eventName}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(ctx))
	var bundleCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleCount))
	require.Equal(t, int64(1), bundleCount)
}
