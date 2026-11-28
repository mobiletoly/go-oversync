//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type auditMultiServiceFixture struct {
	controlPool  *pgxpool.Pool
	servicePools [2]*pgxpool.Pool
	services     [2]*SyncService
	schemaName   string
	suffix       string
}

func newAuditMultiServiceFixture(
	t *testing.T,
	ctx context.Context,
	scenario string,
	maxServiceConns int32,
	uploadLockTimeout time.Duration,
) *auditMultiServiceFixture {
	t.Helper()

	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	fixture := &auditMultiServiceFixture{
		schemaName: "audit_concurrency_" + strings.ToLower(scenario) + "_" + suffix,
		suffix:     suffix,
	}

	fixture.controlPool = newAuditConfiguredPool(t, ctx, databaseURL, "audit-control-"+scenario+"-"+suffix, 8)
	for i := range fixture.servicePools {
		fixture.servicePools[i] = newAuditConfiguredPool(
			t,
			ctx,
			databaseURL,
			fmt.Sprintf("audit-service-%d-%s-%s", i+1, scenario, suffix),
			maxServiceConns,
		)
	}

	require.NoError(t, resetTestSyncSchema(ctx, fixture.controlPool))
	require.NoError(t, resetTestBusinessSchema(ctx, fixture.controlPool, fixture.schemaName))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, fixture.controlPool, fixture.schemaName); err != nil {
			t.Errorf("drop audit concurrency schema: %v", err)
		}
		if managed {
			return
		}
		if err := resetTestSyncSchema(cleanupCtx, fixture.controlPool); err != nil {
			t.Errorf("reset caller-managed audit sync schema: %v", err)
		}
	})

	for i := range fixture.services {
		config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
			{Schema: fixture.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		}), AppName: fmt.Sprintf("audit-concurrency-service-%d", i+1),
			UploadLockTimeout: uploadLockTimeout,
			RegisteredTables: []RegisteredTable{
				{Schema: fixture.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			},
			RegisteredTableCatalogRevision: 1,
		}
		service, err := newTestRuntimeService(fixture.servicePools[i], config, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		require.NoError(t, service.Bootstrap(ctx))
		fixture.services[i] = service
		t.Cleanup(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := service.Close(closeCtx); err != nil {
				t.Errorf("close audit concurrency service: %v", err)
			}
		})
	}

	return fixture
}

func newAuditConfiguredPool(
	t *testing.T,
	ctx context.Context,
	databaseURL string,
	applicationName string,
	maxConns int32,
) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	config.MaxConns = maxConns
	config.MinConns = 0
	config.ConnConfig.RuntimeParams["application_name"] = applicationName

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))
	return pool
}

func (f *auditMultiServiceFixture) actor(scopeID, sourceID string) Actor {
	return Actor{UserID: scopeID + "-" + f.suffix, SourceID: sourceID}
}

func (f *auditMultiServiceFixture) userRow(t *testing.T, id uuid.UUID, name string) PushRequestRow {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"id":    id.String(),
		"name":  name,
		"email": strings.ToLower(strings.ReplaceAll(name, " ", ".")) + "@example.com",
	})
	require.NoError(t, err)
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        payload,
	}
}

func (f *auditMultiServiceFixture) directPushRequest(
	t *testing.T,
	ctx context.Context,
	service *SyncService,
	actor Actor,
	row PushRequestRow,
) *OneShotPushRequest {
	t.Helper()

	initializationID := resolveConnectForPush(t, ctx, service, actor, true)
	return &OneShotPushRequest{
		SourceBundleID:       1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, service, []PushRequestRow{row}),
		InitializationID:     initializationID,
		Rows:                 []PushRequestRow{row},
	}
}

func (f *auditMultiServiceFixture) requireBusinessRowCount(
	t *testing.T,
	ctx context.Context,
	scopeID string,
	want int64,
) {
	t.Helper()

	var count int64
	tableIdent := pgx.Identifier{f.schemaName, "users"}.Sanitize()
	require.NoError(t, f.controlPool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE _sync_scope_id = $1`, tableIdent),
		scopeID,
	).Scan(&count))
	require.Equal(t, want, count)
}

func requireAuditCommittedSourceTuple(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	actor Actor,
	sourceBundleID int64,
	wantHash string,
) {
	t.Helper()

	var bundleCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log AS bundle
		JOIN sync.user_state AS users ON users.user_pk = bundle.user_pk
		WHERE users.user_id = $1
		  AND bundle.source_id = $2
		  AND bundle.source_bundle_id = $3
	`, actor.UserID, actor.SourceID, sourceBundleID).Scan(&bundleCount))
	require.Equal(t, int64(1), bundleCount)

	var bundleSeq int64
	var bundleHashBytes []byte
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT bundle.bundle_seq, bundle.bundle_hash
		FROM sync.bundle_log AS bundle
		JOIN sync.user_state AS users ON users.user_pk = bundle.user_pk
		WHERE users.user_id = $1
		  AND bundle.source_id = $2
		  AND bundle.source_bundle_id = $3
	`, actor.UserID, actor.SourceID, sourceBundleID).Scan(&bundleSeq, &bundleHashBytes))
	require.Positive(t, bundleSeq)
	if wantHash != "" {
		require.Equal(t, wantHash, renderBundleHash(bundleHashBytes))
	}

	var sourceCount, watermark int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(MAX(source.max_committed_source_bundle_id), 0)
		FROM sync.source_state AS source
		JOIN sync.user_state AS users ON users.user_pk = source.user_pk
		WHERE users.user_id = $1
		  AND source.source_id = $2
	`, actor.UserID, actor.SourceID).Scan(&sourceCount, &watermark))
	require.Equal(t, int64(1), sourceCount)
	require.Equal(t, sourceBundleID, watermark)
}

type auditHeldScopeLock struct {
	conn     *pgxpool.Conn
	scopeID  string
	released bool
}

func holdAuditScopeLock(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	scopeID string,
) *auditHeldScopeLock {
	t.Helper()

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, scopeID)
	require.NoError(t, err)

	held := &auditHeldScopeLock{conn: conn, scopeID: scopeID}
	t.Cleanup(func() {
		if held.released {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = held.conn.Exec(cleanupCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, held.scopeID)
		held.conn.Release()
		held.released = true
	})
	return held
}

func (l *auditHeldScopeLock) release(t *testing.T, ctx context.Context) {
	t.Helper()
	if l.released {
		return
	}
	_, err := l.conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, l.scopeID)
	require.NoError(t, err)
	l.conn.Release()
	l.released = true
}

func waitForAuditAdvisoryLockWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		var count int64
		err := pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND NOT granted
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		`).Scan(&count)
		return err == nil && count == want
	}, 5*time.Second, 10*time.Millisecond, "expected %d advisory-lock waiters", want)
}

type auditCommitResult struct {
	index    int
	response *Bundle
	err      error
}

func receiveAuditCommitResults(t *testing.T, results <-chan auditCommitResult, count int) []auditCommitResult {
	t.Helper()

	collected := make([]auditCommitResult, 0, count)
	for range count {
		select {
		case result := <-results:
			collected = append(collected, result)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for concurrent commit")
		}
	}
	return collected
}

func TestAuditConcurrency_TwoServicesSerializeSameScopePushes(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "same_scope", 2, 0)
	scopeID := "same-scope-" + fixture.suffix
	mustInitializeEmptyScope(t, ctx, fixture.services[0], scopeID, "initializer")

	actors := []Actor{
		{UserID: scopeID, SourceID: "writer-a"},
		{UserID: scopeID, SourceID: "writer-b"},
	}
	requests := []*OneShotPushRequest{
		fixture.directPushRequest(t, ctx, fixture.services[0], actors[0], fixture.userRow(t, uuid.New(), "Same A")),
		fixture.directPushRequest(t, ctx, fixture.services[1], actors[1], fixture.userRow(t, uuid.New(), "Same B")),
	}

	held := holdAuditScopeLock(t, ctx, fixture.controlPool, scopeID)
	start := make(chan struct{})
	results := make(chan auditCommitResult, len(requests))
	for i := range requests {
		go func(index int) {
			<-start
			response, err := fixture.services[index].PushOneShot(ctx, actors[index], testSchemaGenerationProofForService(fixture.services[index]), requests[index])
			var bundle *Bundle
			if response != nil {
				bundle = response.Bundle
			}
			results <- auditCommitResult{index: index, response: bundle, err: err}
		}(i)
	}
	close(start)
	waitForAuditAdvisoryLockWaiters(t, ctx, fixture.controlPool, 2)
	held.release(t, ctx)

	commits := receiveAuditCommitResults(t, results, len(requests))
	bundleSeqs := make([]int64, 0, len(commits))
	for _, commit := range commits {
		require.NoError(t, commit.err)
		require.NotNil(t, commit.response)
		bundleSeqs = append(bundleSeqs, commit.response.BundleSeq)
		requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actors[commit.index], 1, commit.response.BundleHash)
	}
	require.ElementsMatch(t, []int64{1, 2}, bundleSeqs)
	fixture.requireBusinessRowCount(t, ctx, scopeID, 2)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
}

func TestAuditConcurrency_TwoServicesProgressDifferentScopes(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "different_scopes", 2, 0)
	actors := []Actor{
		fixture.actor("left", "writer-left"),
		fixture.actor("right", "writer-right"),
	}
	for i := range actors {
		mustInitializeEmptyScope(t, ctx, fixture.services[i], actors[i].UserID, actors[i].SourceID)
	}
	requests := []*OneShotPushRequest{
		fixture.directPushRequest(t, ctx, fixture.services[0], actors[0], fixture.userRow(t, uuid.New(), "Left")),
		fixture.directPushRequest(t, ctx, fixture.services[1], actors[1], fixture.userRow(t, uuid.New(), "Right")),
	}
	held := []*auditHeldScopeLock{
		holdAuditScopeLock(t, ctx, fixture.controlPool, actors[0].UserID),
		holdAuditScopeLock(t, ctx, fixture.controlPool, actors[1].UserID),
	}

	start := make(chan struct{})
	results := make(chan auditCommitResult, len(requests))
	for i := range requests {
		go func(index int) {
			<-start
			response, err := fixture.services[index].PushOneShot(ctx, actors[index], testSchemaGenerationProofForService(fixture.services[index]), requests[index])
			var bundle *Bundle
			if response != nil {
				bundle = response.Bundle
			}
			results <- auditCommitResult{index: index, response: bundle, err: err}
		}(i)
	}
	close(start)
	waitForAuditAdvisoryLockWaiters(t, ctx, fixture.controlPool, 2)
	for _, lock := range held {
		lock.release(t, ctx)
	}

	commits := receiveAuditCommitResults(t, results, len(requests))
	for _, commit := range commits {
		require.NoError(t, commit.err)
		require.NotNil(t, commit.response)
		require.Equal(t, int64(1), commit.response.BundleSeq)
		fixture.requireBusinessRowCount(t, ctx, actors[commit.index].UserID, 1)
		requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actors[commit.index], 1, commit.response.BundleHash)
	}
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
}

func TestAuditConcurrency_BlockedScopeAllowsIndependentProgressCancellationAndRetry(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "blocked_progress", 2, 0)
	blockedActor := fixture.actor("blocked", "writer-blocked")
	independentActor := fixture.actor("independent", "writer-independent")
	mustInitializeEmptyScope(t, ctx, fixture.services[0], blockedActor.UserID, blockedActor.SourceID)
	mustInitializeEmptyScope(t, ctx, fixture.services[0], independentActor.UserID, independentActor.SourceID)

	blockedRequest := fixture.directPushRequest(t, ctx, fixture.services[0], blockedActor, fixture.userRow(t, uuid.New(), "Blocked"))
	held := holdAuditScopeLock(t, ctx, fixture.controlPool, blockedActor.UserID)
	waitCtx, cancelWait := context.WithCancel(ctx)
	waitResult := make(chan error, 1)
	go func() {
		_, err := fixture.services[0].PushOneShot(waitCtx, blockedActor, testSchemaGenerationProofForService(fixture.services[0]), blockedRequest)
		waitResult <- err
	}()
	waitForAuditAdvisoryLockWaiters(t, ctx, fixture.controlPool, 1)

	independentCtx, cancelIndependent := context.WithTimeout(ctx, 5*time.Second)
	independentBundle, err := pushRowsOneShot(
		t,
		independentCtx,
		fixture.services[0],
		independentActor,
		1,
		[]PushRequestRow{fixture.userRow(t, uuid.New(), "Independent")},
	)
	cancelIndependent()
	require.NoError(t, err)
	require.NotNil(t, independentBundle)
	requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, independentActor, 1, independentBundle.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)

	cancelWait()
	select {
	case waitErr := <-waitResult:
		require.ErrorIs(t, waitErr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled blocked-scope operation did not return")
	}
	require.Eventually(t, func() bool {
		return fixture.servicePools[0].Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "cancelled blocked-scope operation leaked a pool slot")
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)

	held.release(t, ctx)
	retriedBundle, err := pushRowsOneShot(
		t,
		ctx,
		fixture.services[0],
		blockedActor,
		1,
		[]PushRequestRow{fixture.userRow(t, uuid.New(), "Retried")},
	)
	require.NoError(t, err)
	require.NotNil(t, retriedBundle)
	fixture.requireBusinessRowCount(t, ctx, blockedActor.UserID, 1)
	fixture.requireBusinessRowCount(t, ctx, independentActor.UserID, 1)
	requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, blockedActor, 1, retriedBundle.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
}

func TestAuditConcurrencyContract_BlockedScopeWaitersDoNotStarveIndependentScope(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "starvation_contract", 2, 0)
	blockedActor := fixture.actor("starved-blocked", "writer-blocked")
	independentActor := fixture.actor("starved-independent", "writer-independent")
	mustInitializeEmptyScope(t, ctx, fixture.services[0], blockedActor.UserID, blockedActor.SourceID)
	mustInitializeEmptyScope(t, ctx, fixture.services[0], independentActor.UserID, independentActor.SourceID)

	blockedRequest := fixture.directPushRequest(t, ctx, fixture.services[0], blockedActor, fixture.userRow(t, uuid.New(), "Starved"))
	held := holdAuditScopeLock(t, ctx, fixture.controlPool, blockedActor.UserID)
	waitCtx, cancelWaiters := context.WithCancel(ctx)
	waitResults := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := fixture.services[0].PushOneShot(waitCtx, blockedActor, testSchemaGenerationProofForService(fixture.services[0]), blockedRequest)
			waitResults <- err
		}()
	}
	waitForAuditAdvisoryLockWaiters(t, ctx, fixture.controlPool, 1)
	require.Eventually(t, func() bool {
		return fixture.servicePools[0].Stat().AcquiredConns() == 1
	}, 5*time.Second, 10*time.Millisecond, "same-scope waiters must share one pool slot while blocked")

	independentCtx, cancelIndependent := context.WithTimeout(ctx, 300*time.Millisecond)
	independentResponse, independentErr := pushRowsOneShot(t, independentCtx, fixture.services[0], independentActor, 1, []PushRequestRow{fixture.userRow(t, uuid.New(), "Independent")})

	cancelIndependent()

	cancelWaiters()
	for range 2 {
		select {
		case waitErr := <-waitResults:
			require.Error(t, waitErr)
		case <-time.After(5 * time.Second):
			t.Fatal("blocked-scope waiter did not return after cancellation")
		}
	}
	held.release(t, ctx)
	require.Eventually(t, func() bool {
		return fixture.servicePools[0].Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "blocked-scope waiters leaked pool slots")
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)

	t.Logf("independent scope result while same-scope waiters shared one pool slot: response=%v error=%v", independentResponse, independentErr)
	require.NoError(t, independentErr, "waiters for one blocked scope must not exhaust the pool or starve an independent scope")
	require.NotNil(t, independentResponse)
}
