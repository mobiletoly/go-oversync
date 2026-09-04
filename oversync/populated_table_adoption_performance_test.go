package oversync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	adoptionQueryShapeBatchSize         = 32
	auditAdoptionQueryShapeRowsPerScope = 300
)

type adoptionQueryTracer struct {
	mu             sync.Mutex
	queries        []string
	batches        [][]string
	businessSchema string
	cancel         context.CancelFunc
	cancelContains string
	canceled       bool
}

type adoptionQueryCounts struct {
	total                int
	business             int
	scopeEnvelope        int
	currentRow           int
	currentRowStatements int
	history              int
	historyStatements    int
	persistence          int
	catalogDefinition    int
	explicitDataLocks    int
	dml                  int
	ddl                  int
	unknown              []string
}

type cancelAfterErrChecks struct {
	context.Context
	checks   int
	cancelAt int
}

func loadFastAttachDataFingerprints(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
	tableCount int,
) map[string]string {
	t.Helper()
	relations := make([]string, 0, tableCount+15)
	for tableIndex := range tableCount {
		relations = append(relations, pgx.Identifier{schemaName, fmt.Sprintf("table_%02d", tableIndex)}.Sanitize())
	}
	for _, tableName := range []string{
		"bundle_capture_stage", "bundle_log", "bundle_rows", "meta",
		"row_state", "scope_state", "scope_write_receipts", "server_source_reservations", "snapshot_session_rows",
		"snapshot_sessions", "source_state", "table_catalog", "user_state",
	} {
		relations = append(relations, pgx.Identifier{"sync", tableName}.Sanitize())
	}
	fingerprints := make(map[string]string, len(relations))
	for _, relation := range relations {
		var fingerprint string
		require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*)::text || ':' || COALESCE(
				md5(string_agg(md5(to_jsonb(snapshot)::text), '' ORDER BY to_jsonb(snapshot)::text)),
				md5('')
			)
			FROM %s AS snapshot
		`, relation)).Scan(&fingerprint))
		fingerprints[relation] = fingerprint
	}
	return fingerprints
}

func (c *cancelAfterErrChecks) Err() error {
	c.checks++
	if c.checks >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

func (t *adoptionQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if isSyncMaintenanceContext(ctx) {
		return ctx
	}
	t.mu.Lock()
	t.queries = append(t.queries, data.SQL)
	shouldCancel := !t.canceled && t.cancel != nil && strings.Contains(data.SQL, t.cancelContains)
	if shouldCancel {
		t.canceled = true
	}
	t.mu.Unlock()
	if shouldCancel {
		t.cancel()
	}
	return ctx
}

func (*adoptionQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *adoptionQueryTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if isSyncMaintenanceContext(ctx) {
		return ctx
	}
	queries := make([]string, len(data.Batch.QueuedQueries))
	containsCancel := false
	for i, query := range data.Batch.QueuedQueries {
		queries[i] = query.SQL
		if strings.Contains(query.SQL, t.cancelContains) {
			containsCancel = true
		}
	}
	t.mu.Lock()
	t.batches = append(t.batches, queries)
	shouldCancel := !t.canceled && t.cancel != nil && containsCancel
	if shouldCancel {
		t.canceled = true
	}
	t.mu.Unlock()
	if shouldCancel {
		t.cancel()
	}
	return ctx
}

func (*adoptionQueryTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (*adoptionQueryTracer) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

func (t *adoptionQueryTracer) reset() {
	t.mu.Lock()
	t.queries = nil
	t.batches = nil
	t.mu.Unlock()
}

func (t *adoptionQueryTracer) counts() adoptionQueryCounts {
	t.mu.Lock()
	defer t.mu.Unlock()
	var counts adoptionQueryCounts
	for _, query := range t.queries {
		normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
		counts.total++
		classified := false
		if normalized == "begin isolation level read committed read write not deferrable" ||
			normalized == "commit" || normalized == "rollback" ||
			strings.Contains(normalized, "pg_advisory_xact_lock") ||
			normalized == "select pg_try_advisory_xact_lock_shared($1::int,$2::int)" ||
			normalized == "select pg_catalog.current_setting('session_replication_role')='origin'" {
			counts.catalogDefinition++
			classified = true
		}
		if strings.HasPrefix(normalized, "lock table ") {
			counts.explicitDataLocks++
			classified = true
		}
		if strings.HasPrefix(normalized, "insert ") || strings.HasPrefix(normalized, "update ") ||
			strings.HasPrefix(normalized, "delete ") || strings.HasPrefix(normalized, "merge ") {
			counts.dml++
			classified = true
		}
		if strings.HasPrefix(normalized, "create ") || strings.HasPrefix(normalized, "alter ") ||
			strings.HasPrefix(normalized, "drop ") || strings.HasPrefix(normalized, "truncate ") ||
			strings.HasPrefix(normalized, "comment ") || strings.HasPrefix(normalized, "grant ") ||
			strings.HasPrefix(normalized, "revoke ") {
			counts.ddl++
			classified = true
		}
		if strings.Contains(normalized, "oversync:adoption-business-scan") ||
			(strings.Contains(normalized, "to_jsonb(src) - '_sync_scope_id'") && strings.Contains(normalized, "where src.")) ||
			(t.businessSchema != "" && strings.Contains(normalized, "from \""+strings.ToLower(t.businessSchema)+"\".")) {
			counts.business++
			classified = true
		}
		if strings.Contains(normalized, "oversync:adoption-scope-state") ||
			strings.Contains(normalized, "from sync.user_state where user_id = $1 for update") ||
			strings.Contains(normalized, "select state_code from sync.scope_state where user_pk = $1 for update") ||
			strings.Contains(normalized, "(select count(*) from sync.row_state where user_pk = $1)") {
			counts.scopeEnvelope++
			classified = true
		}
		if strings.Contains(normalized, "oversync:adoption-current-state-batch") ||
			strings.Contains(normalized, "from sync.row_state where user_pk = $1 order by table_id, key_bytes") {
			counts.currentRow++
			counts.currentRowStatements++
			classified = true
		}
		if strings.Contains(normalized, "oversync:adoption-history-batch") ||
			strings.Contains(normalized, "from sync.bundle_log as bundle left join sync.bundle_rows as rows") {
			counts.history++
			counts.historyStatements++
			classified = true
		}
		if strings.Contains(normalized, "insert into sync.bundle_log") ||
			strings.Contains(normalized, "insert into sync.row_state") ||
			strings.Contains(normalized, "insert into sync.source_state") ||
			strings.Contains(normalized, "update sync.user_state") ||
			strings.Contains(normalized, "update sync.scope_state") {
			counts.persistence++
			classified = true
		}
		if strings.Contains(normalized, "pg_catalog") || strings.Contains(normalized, "from pg_") ||
			strings.Contains(normalized, "join pg_") || strings.Contains(normalized, "information_schema") ||
			strings.Contains(normalized, "to_regclass(") || strings.Contains(normalized, "from sync.meta") ||
			strings.Contains(normalized, "from sync.table_catalog") ||
			strings.Contains(normalized, "from sync.server_source_reservations") {
			counts.catalogDefinition++
			classified = true
		}
		if !classified {
			counts.unknown = append(counts.unknown, normalized)
		}
	}
	for _, batch := range t.batches {
		counts.total++
		currentStatements := 0
		historyStatements := 0
		for _, query := range batch {
			normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
			if strings.Contains(normalized, "oversync:adoption-current-state-batch") {
				currentStatements++
			}
			if strings.Contains(normalized, "oversync:adoption-history-batch") {
				historyStatements++
			}
		}
		if currentStatements > 0 {
			counts.currentRow++
			counts.currentRowStatements += currentStatements
		}
		if historyStatements > 0 {
			counts.history++
			counts.historyStatements += historyStatements
		}
	}
	return counts
}

func (t *adoptionQueryTracer) normalizedStatementFamilies() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	families := make([]string, 0, len(t.queries)+len(t.batches))
	for _, query := range t.queries {
		families = append(families, strings.ToLower(strings.Join(strings.Fields(query), " ")))
	}
	for _, batch := range t.batches {
		statements := make([]string, len(batch))
		for i, query := range batch {
			statements[i] = strings.ToLower(strings.Join(strings.Fields(query), " "))
		}
		families = append(families, "batch: "+strings.Join(statements, " | "))
	}
	return families
}

func TestBootstrap_ExistingLayoutAttachesWithoutDataQueriesLocksOrMutation(t *testing.T) {
	t.Parallel()
	const tableCount = 8

	ctx := context.Background()
	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	tracer := &adoptionQueryTracer{}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	poolConfig.ConnConfig.Tracer = tracer
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "oversync-adoption-query-shape"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if !managed {
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed query-shape database: %v", err)
			}
		}
		pool.Close()
	})
	require.NoError(t, resetTestSyncSchema(ctx, pool))

	var expectedAttachStatementFamilies []string
	for _, scopeCount := range []int{0, 1, 100, 500} {
		t.Run(fmt.Sprintf("scopes_%d", scopeCount), func(t *testing.T) {
			require.NoError(t, resetTestSyncSchema(ctx, pool))
			suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			schemaName := "adopt_query_shape_" + suffix
			schemaIdent := pgx.Identifier{schemaName}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
			require.NoError(t, err)
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

			registeredTables := make([]RegisteredTable, tableCount)
			for tableIndex := range tableCount {
				tableName := fmt.Sprintf("table_%02d", tableIndex)
				tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
				_, err = pool.Exec(ctx, fmt.Sprintf(`
					CREATE TABLE %s (
						_sync_scope_id TEXT NOT NULL,
						id UUID NOT NULL,
						payload TEXT NOT NULL,
						PRIMARY KEY (_sync_scope_id, id)
					)
				`, tableIdent))
				require.NoError(t, err)
				registeredTables[tableIndex] = RegisteredTable{
					Schema:         schemaName,
					Table:          tableName,
					SyncKeyColumns: []string{"id"},
				}
			}

			// The audit-tagged correctness case uses 150,000 rows. The separate
			// full-scale capacity benchmark retains the 350,000-row reference.
			if scopeCount == 500 && oversyncAuditBuild {
				for tableIndex := range tableCount {
					tableName := fmt.Sprintf("table_%02d", tableIndex)
					tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
					rowsPerScope := auditAdoptionQueryShapeRowsPerScope / tableCount
					if tableIndex < auditAdoptionQueryShapeRowsPerScope%tableCount {
						rowsPerScope++
					}
					_, err = pool.Exec(ctx, fmt.Sprintf(`
						INSERT INTO %s (_sync_scope_id, id, payload)
						SELECT
							format('query-shape-scope-%%s', lpad(scope_index::text, 3, '0')),
							md5($1 || '/' || scope_index::text || '/' || row_index::text || '/%d')::uuid,
							format('payload-%%s-%%s-%02d', scope_index, row_index)
						FROM generate_series(0, 499) AS scopes(scope_index)
						CROSS JOIN generate_series(0, %d) AS rows(row_index)
					`, tableIdent, tableIndex, tableIndex, rowsPerScope-1), suffix)
					require.NoError(t, err)
				}
			} else if scopeCount > 0 {
				var batch pgx.Batch
				for scopeIndex := range scopeCount {
					userID := fmt.Sprintf("query-shape-scope-%03d", scopeIndex)
					for tableIndex := range tableCount {
						tableName := fmt.Sprintf("table_%02d", tableIndex)
						tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
						rowID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%d/%d", suffix, scopeIndex, tableIndex)))
						batch.Queue(
							fmt.Sprintf(`INSERT INTO %s (_sync_scope_id, id, payload) VALUES ($1, $2, $3)`, tableIdent),
							userID,
							rowID,
							fmt.Sprintf("payload-%03d-%02d", scopeIndex, tableIndex),
						)
					}
				}
				results := pool.SendBatch(ctx, &batch)
				for range scopeCount * tableCount {
					_, err = results.Exec()
					require.NoError(t, err)
				}
				require.NoError(t, results.Close())
			}

			config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registeredTables), AppName: "populated-adoption-query-shape",
				RegisteredTables:               registeredTables,
				RegisteredTableCatalogRevision: 1,
			}
			tracer.reset()
			first, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			require.NoError(t, first.Bootstrap(ctx))
			require.NoError(t, first.Close(ctx))

			wantBusinessQueries := (scopeCount + adoptionQueryShapeBatchSize - 1) / adoptionQueryShapeBatchSize
			pristineCounts := tracer.counts()
			require.Equal(t, wantBusinessQueries, pristineCounts.business, "pristine business payload queries must follow the bounded scope-batch formula")
			wantScopeEnvelopeQueries := 0
			if scopeCount > 0 {
				wantScopeEnvelopeQueries = 6
			}
			require.Equal(t, wantScopeEnvelopeQueries, pristineCounts.scopeEnvelope, "pristine scope-envelope queries must remain fixed")
			require.Equal(t, scopeCount, pristineCounts.currentRow, "every adopted scope must receive exact post-persistence current-row validation")
			require.Equal(t, scopeCount, pristineCounts.history, "every adopted scope must receive exact post-persistence retained-history validation")
			require.Equal(t, scopeCount, pristineCounts.currentRowStatements)
			require.Equal(t, scopeCount, pristineCounts.historyStatements)
			before := loadFastAttachDataFingerprints(t, ctx, pool, schemaName, tableCount)

			tracer.reset()
			attachMetrics := &collectedStageMetrics{}
			config.StageMetrics = attachMetrics
			second, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = second.Close(context.Background()) })
			spoolDir := t.TempDir()
			second.adoptionSpoolDir = spoolDir
			canonicalizations := 0
			bundleHashes := 0
			spoolFiles := 0
			var spoolBytes int64
			second.adoptionHooks = &adoptionTestHooks{
				onCanonicalized:       func(int) { canonicalizations++ },
				onCommittedBundleHash: func() { bundleHashes++ },
				onSpoolCreated:        func(int, string) { spoolFiles++ },
				onSpoolRemoved:        func(_ int, _ string, bytes int64, _ error) { spoolBytes += bytes },
			}
			tracer.businessSchema = schemaName
			require.NoError(t, second.Bootstrap(ctx))

			restartCounts := tracer.counts()
			statementFamilies := tracer.normalizedStatementFamilies()
			require.Contains(t, statementFamilies, "select pg_try_advisory_xact_lock_shared($1::int,$2::int)", "readiness adds one fixed authority-lock query")
			require.Contains(t, statementFamilies, "select pg_catalog.current_setting('session_replication_role')='origin'", "one fixed actual-session attestation")
			// The catalog revision peek selects read-only attachment versus locked append.
			require.Equal(t, 39, restartCounts.total, "existing-layout attachment SQL count must be scale-independent, including readiness admission and catalog revision selection")
			if expectedAttachStatementFamilies == nil {
				expectedAttachStatementFamilies = append([]string(nil), statementFamilies...)
			} else {
				require.Equal(t, expectedAttachStatementFamilies, statementFamilies, "existing-layout attachment SQL families and ordering must be scale-independent")
			}
			require.Zero(t, restartCounts.business, "existing-layout attachment must not query business rows")
			require.Zero(t, restartCounts.scopeEnvelope, "existing-layout attachment must not query operational scope state")
			require.Zero(t, restartCounts.currentRow, "existing-layout attachment must not query current row state")
			require.Zero(t, restartCounts.history, "existing-layout attachment must not query retained history")
			require.Zero(t, restartCounts.persistence, "existing-layout attachment must not mutate managed state")
			require.Zero(t, canonicalizations, "existing-layout attachment must not canonicalize payloads")
			require.Zero(t, bundleHashes, "existing-layout attachment must not hash retained bundles")
			require.Zero(t, spoolFiles, "existing-layout attachment must not create adoption spools")
			require.Zero(t, spoolBytes, "existing-layout attachment must not write adoption spool bytes")
			require.Zero(t, restartCounts.explicitDataLocks, "existing-layout attachment must not take explicit data locks")
			require.Zero(t, restartCounts.dml, "existing-layout attachment must not execute DML")
			require.Zero(t, restartCounts.ddl, "existing-layout attachment must not execute DDL")
			require.NotZero(t, restartCounts.catalogDefinition, "existing-layout attachment must perform definition validation")
			require.Empty(t, restartCounts.unknown, "every existing-layout Bootstrap SQL statement must be classified")
			stageRecords := attachMetrics.recordsForOperation("bootstrap")
			require.Len(t, stageRecords, 1, "existing attachment must emit only managed-layout validation")
			require.Len(t, stageRecords["managed_layout_validation"], 1)
			require.Equal(t, before, loadFastAttachDataFingerprints(t, ctx, pool, schemaName, tableCount), "existing attachment must preserve every business and managed row value")
			entries, err := os.ReadDir(spoolDir)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestAdoptionBusinessScopeReader_CancellationStopsSpoolReplay(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_cancel_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	userIDs := []string{"cancel-a-" + suffix, "cancel-b-" + suffix}
	for index, userID := range userIDs {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}

	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-replay-cancellation",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	replayCtx, cancel := context.WithCancel(ctx)
	tx, err := pool.Begin(replayCtx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	reader, err := newAdoptionBusinessScopeReader(
		replayCtx,
		tx,
		service.sortedAdoptionTableInfos(),
		service.columnTypesByTable,
		userIDs,
		func(schemaName, tableName string, payload []byte) ([]byte, error) {
			return service.canonicalizeWirePayload(schemaName, tableName, payload)
		},
		service.config.MaxBytesPerBundle,
	)
	require.NoError(t, err)
	var consumed []string
	err = reader.forEach(func(userID string, _ []adoptionBusinessRow) error {
		consumed = append(consumed, userID)
		if len(consumed) == 1 {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, userIDs[:1], consumed, "cancellation must prevent the next scope callback")
}

func TestAdoptionBusinessScopeReader_CancellationDuringSpoolReplay(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_replay_cancel_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "replay-cancel-" + suffix
	for index := range 3 {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-during-replay-cancellation",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	reader, err := newAdoptionBusinessScopeReader(ctx, tx, service.sortedAdoptionTableInfos(), service.columnTypesByTable, []string{userID}, func(schemaName, tableName string, payload []byte) ([]byte, error) {
		return service.canonicalizeWirePayload(schemaName, tableName, payload)
	}, service.config.MaxBytesPerBundle)
	require.NoError(t, err)
	reader.ctx = &cancelAfterErrChecks{Context: ctx, cancelAt: 4}
	consumeCalled := false
	err = reader.forEach(func(_ string, _ []adoptionBusinessRow) error {
		consumeCalled = true
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, consumeCalled, "cancellation after decoding a replay row must stop before the scope callback")
}

func TestAdoptionBusinessScopeReader_PostgresOrderingMatchesEncodedKeys(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_key_order_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	for _, ddl := range []string{
		fmt.Sprintf(`CREATE TABLE %s (_sync_scope_id TEXT NOT NULL, id UUID NOT NULL, value TEXT NOT NULL, PRIMARY KEY (_sync_scope_id, id))`, pgx.Identifier{schemaName, "uuid_rows"}.Sanitize()),
		fmt.Sprintf(`CREATE TABLE %s (_sync_scope_id TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (_sync_scope_id, id))`, pgx.Identifier{schemaName, "text_rows"}.Sanitize()),
	} {
		_, err = pool.Exec(ctx, ddl)
		require.NoError(t, err)
	}
	userID := "key-order-" + suffix
	uuidKeys := []string{
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"00000000-0000-0000-0000-000000000001",
		"7fffffff-ffff-ffff-ffff-ffffffffffff",
	}
	textKeys := []string{"éclair", "zebra", "Ångström", "東京", "apple"}
	for _, key := range uuidKeys {
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, pgx.Identifier{schemaName, "uuid_rows"}.Sanitize()), userID, key, key)
		require.NoError(t, err)
	}
	for _, key := range textKeys {
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, pgx.Identifier{schemaName, "text_rows"}.Sanitize()), userID, key, key)
		require.NoError(t, err)
	}

	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTablesWithWireTypes([]RegisteredTable{
		{Schema: schemaName, Table: "uuid_rows", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "text_rows", SyncKeyColumns: []string{"id"}},
	}, map[string]GenerationWireType{Key(schemaName, "text_rows") + ".id": GenerationWireTypeText}), AppName: "adoption-key-ordering",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "uuid_rows", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "text_rows", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	reader, err := newAdoptionBusinessScopeReader(ctx, tx, service.sortedAdoptionTableInfos(), service.columnTypesByTable, []string{userID}, func(schemaName, tableName string, payload []byte) ([]byte, error) {
		return service.canonicalizeWirePayload(schemaName, tableName, payload)
	}, service.config.MaxBytesPerBundle)
	require.NoError(t, err)
	var actual []adoptionBusinessRow
	require.NoError(t, reader.forEach(func(_ string, rows []adoptionBusinessRow) error {
		actual = append(actual, rows...)
		return nil
	}))

	expected := append([]adoptionBusinessRow(nil), actual...)
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].tableInfo.tableID != expected[j].tableInfo.tableID {
			return expected[i].tableInfo.tableID < expected[j].tableInfo.tableID
		}
		return bytes.Compare(expected[i].keyBytes, expected[j].keyBytes) < 0
	})
	require.Equal(t, expected, actual)
}

func TestAdoptionBusinessScopeReader_CanonicalizesEachRowOncePerAttempt(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_canonical_once_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "canonical-once-" + suffix
	for index := range 3 {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-canonicalization-count",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	for attempt := 1; attempt <= 2; attempt++ {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		canonicalizationCount := 0
		reader, err := newAdoptionBusinessScopeReader(ctx, tx, service.sortedAdoptionTableInfos(), service.columnTypesByTable, []string{userID}, func(schema, table string, payload []byte) ([]byte, error) {
			canonicalizationCount++
			return service.canonicalizeWirePayload(schema, table, payload)
		}, service.config.MaxBytesPerBundle)
		require.NoError(t, err)
		observedRows := 0
		require.NoError(t, reader.forEach(func(_ string, rows []adoptionBusinessRow) error {
			observedRows += len(rows)
			return nil
		}))
		require.NoError(t, tx.Rollback(ctx))
		require.Equal(t, observedRows, canonicalizationCount, "attempt %d must canonicalize each observed business row exactly once", attempt)
		require.Equal(t, 3, observedRows)
	}
}

type readFailingAdoptionSpool struct {
	adoptionSpoolFile
	failReads bool
}

func (s *readFailingAdoptionSpool) Read(p []byte) (int, error) {
	if s.failReads {
		return 0, errors.New("injected adoption spool read failure")
	}
	return s.adoptionSpoolFile.Read(p)
}

func (s *readFailingAdoptionSpool) Seek(offset int64, whence int) (int64, error) {
	position, err := s.adoptionSpoolFile.Seek(offset, whence)
	if err == nil && offset == 0 && whence == 0 {
		s.failReads = true
	}
	return position, err
}

func TestAdoptionBusinessScopeReader_RemovesOwnedSpoolOnEveryExit(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_spool_cleanup_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "spool-cleanup-" + suffix
	for index := range 4 {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-spool-cleanup",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	newReader := func(t *testing.T, readerCtx context.Context, dir string) (*adoptionBusinessScopeReader, pgx.Tx) {
		t.Helper()
		tx, err := pool.Begin(readerCtx)
		require.NoError(t, err)
		reader, err := newAdoptionBusinessScopeReader(readerCtx, tx, service.sortedAdoptionTableInfos(), service.columnTypesByTable, []string{userID}, func(schemaName, tableName string, payload []byte) ([]byte, error) {
			return service.canonicalizeWirePayload(schemaName, tableName, payload)
		}, service.config.MaxBytesPerBundle)
		require.NoError(t, err)
		reader.spoolOwner = newAdoptionSpoolOwner(dir)
		return reader, tx
	}
	requireEmpty := func(t *testing.T, dir string) {
		t.Helper()
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries, "adoption spool directory must have no residual files")
	}

	t.Run("success", func(t *testing.T) {
		dir := t.TempDir()
		reader, tx := newReader(t, ctx, dir)
		require.NoError(t, reader.forEach(func(string, []adoptionBusinessRow) error { return nil }))
		require.NoError(t, tx.Rollback(ctx))
		requireEmpty(t, dir)
	})

	t.Run("validation rejection", func(t *testing.T) {
		dir := t.TempDir()
		reader, tx := newReader(t, ctx, dir)
		injected := errors.New("injected validation rejection")
		require.ErrorIs(t, reader.forEach(func(string, []adoptionBusinessRow) error { return injected }), injected)
		require.NoError(t, tx.Rollback(ctx))
		requireEmpty(t, dir)
	})

	t.Run("cancellation before replay", func(t *testing.T) {
		dir := t.TempDir()
		readerCtx, cancel := context.WithCancel(ctx)
		reader, tx := newReader(t, readerCtx, dir)
		reader.afterBusinessRowsRead = func() error {
			cancel()
			return nil
		}
		require.ErrorIs(t, reader.forEach(func(string, []adoptionBusinessRow) error { return nil }), context.Canceled)
		require.NoError(t, tx.Rollback(context.Background()))
		requireEmpty(t, dir)
	})

	t.Run("decode error", func(t *testing.T) {
		dir := t.TempDir()
		reader, tx := newReader(t, ctx, dir)
		owner := newAdoptionSpoolOwner(dir)
		create := owner.create
		owner.create = func() (adoptionSpoolFile, error) {
			spool, err := create()
			if err != nil {
				return nil, err
			}
			return &readFailingAdoptionSpool{adoptionSpoolFile: spool}, nil
		}
		reader.spoolOwner = owner
		err := reader.forEach(func(string, []adoptionBusinessRow) error { return nil })
		require.ErrorContains(t, err, "injected adoption spool read failure")
		require.NoError(t, tx.Rollback(ctx))
		requireEmpty(t, dir)
	})

	t.Run("panic", func(t *testing.T) {
		dir := t.TempDir()
		reader, tx := newReader(t, ctx, dir)
		func() {
			defer func() { require.Equal(t, "injected panic", recover()) }()
			_ = reader.forEach(func(string, []adoptionBusinessRow) error { panic("injected panic") })
		}()
		require.NoError(t, tx.Rollback(ctx))
		requireEmpty(t, dir)
	})
}

func TestBootstrap_PopulatedAdoptionRetriesWithFreshReaderAndSpool(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_actual_retry_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "retry-owner-" + suffix
	for index := range 3 {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-actual-retry",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	spoolDir := t.TempDir()
	service.adoptionSpoolDir = spoolDir
	var readers []*adoptionBusinessScopeReader
	canonicalizedByAttempt := map[int]int{}
	service.adoptionHooks = &adoptionTestHooks{
		onReaderCreated: func(_ int, reader *adoptionBusinessScopeReader) {
			readers = append(readers, reader)
		},
		onCanonicalized: func(attempt int) {
			canonicalizedByAttempt[attempt]++
		},
		afterFreshBusinessRowsRead: func(ctx context.Context, tx pgx.Tx, attempt int) error {
			var userCount, rowCount, bundleCount int64
			require.NoError(t, tx.QueryRow(ctx, `
				SELECT
					(SELECT count(*) FROM sync.user_state),
					(SELECT count(*) FROM sync.row_state),
					(SELECT count(*) FROM sync.bundle_log)
			`).Scan(&userCount, &rowCount, &bundleCount))
			require.Zero(t, userCount, "a retry attempt must not see failed-attempt user state")
			require.Zero(t, rowCount, "a retry attempt must not see failed-attempt row state")
			require.Zero(t, bundleCount, "a retry attempt must not see failed-attempt history")
			if attempt == 1 {
				return &pgconn.PgError{Code: "40001", Message: "injected serialization failure after business rows"}
			}
			return nil
		},
	}

	require.NoError(t, service.Bootstrap(ctx))
	require.Len(t, readers, 2)
	require.NotSame(t, readers[0], readers[1], "the retry must construct a fresh business reader")
	require.Equal(t, map[int]int{1: 3, 2: 3}, canonicalizedByAttempt, "each attempt must canonicalize each observed row once")
	entries, err := os.ReadDir(spoolDir)
	require.NoError(t, err)
	require.Empty(t, entries, "failed and successful attempts must remove their owned spools")

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	states, err := loadAdoptionScopeStates(ctx, tx, []string{userID})
	require.NoError(t, err)
	require.NoError(t, validatePersistedAdoptionState(adoptedScopeSummary{userID: userID, rowCount: 3}, states[userID]))
}

func TestBootstrap_PopulatedAdoptionStreamingRetainedHistoryCancellation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_stream_cancel_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "stream-cancel-owner-" + suffix
	for index := range 3 {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}
	config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-streaming-retained-cancellation",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	states, err := loadAdoptionScopeStates(ctx, tx, []string{userID})
	require.NoError(t, err)
	state := states[userID]
	currentRows, err := service.loadAdoptionCurrentRowState(ctx, tx, userID, state, state.nextBundleSeq-1, nil)
	require.NoError(t, err)
	require.Len(t, currentRows, 3)

	retainedCtx, cancel := context.WithCancel(ctx)
	progressCalls := 0
	_, err = service.validateRetainedAdoptionBundles(retainedCtx, tx, userID, state, currentRows, func() {
		progressCalls++
		if progressCalls == len(currentRows)+2 {
			cancel()
		}
	})
	require.ErrorIs(t, err, context.Canceled, "cancellation after a decoded retained row must stop validation")
	require.NoError(t, tx.Rollback(context.Background()))

	retry, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = retry.Close(context.Background()) })
	require.NoError(t, retry.Bootstrap(ctx), "a clean retry must attach the unchanged durable state")
}

func TestBootstrap_PopulatedAdoptionProgressAdvancesInsideEveryLongStage(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_progress_loops_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "progress-owner-" + suffix
	for index := range 32 {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New(), fmt.Sprintf("User %d", index), fmt.Sprintf("user%d@example.com", index))
		require.NoError(t, err)
	}
	var logs bytes.Buffer
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-loop-progress",
		LogStageTimings: true,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	service.bootstrapProgressNow = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	service.bootstrapProgressInterval = 5 * time.Second

	require.NoError(t, service.Bootstrap(ctx))
	output := logs.String()
	require.Contains(t, output, `"stage":"business_row_scan_canonicalization"`)
	require.Contains(t, output, `"stage":"pristine_baseline_persistence"`)
	require.Contains(t, output, `"stage":"coherent_state_history_validation"`)
	require.NotContains(t, output, userID)
}

func requireAdoptionStageError(t *testing.T, recorder *collectedStageMetrics, expected string) {
	t.Helper()
	records := recorder.recordsForOperation("bootstrap")
	for _, stage := range []string{
		"business_row_scan_canonicalization",
		"coherent_state_history_validation",
		"pristine_baseline_persistence",
	} {
		stageRecords := records[stage]
		if stage == expected {
			require.Lenf(t, stageRecords, 1, "expected exactly one %s stage record", stage)
			require.Truef(t, stageRecords[0].Error, "expected %s to carry the stage error", stage)
			continue
		}
		for _, record := range stageRecords {
			require.Falsef(t, record.Error, "unexpected error attribution for %s", stage)
		}
	}
}

func TestBootstrap_PopulatedAdoptionAttributesBusinessScanError(t *testing.T) {
	runBootstrapCancellationStageAttributionTest(
		t,
		"business",
		"oversync:adoption-business-scan",
		"business_row_scan_canonicalization",
	)
}

func TestBootstrap_PopulatedAdoptionAttributesCanonicalizationError(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName, config := prepareStageAttributionFixture(t, ctx, pool, "canonicalization")
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	recorder := &collectedStageMetrics{}
	config.StageMetrics = recorder
	service, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	injected := errors.New("injected canonicalization failure")
	service.adoptionHooks = &adoptionTestHooks{
		afterCanonicalize: func(int) error { return injected },
	}

	err = service.Bootstrap(ctx)
	require.ErrorContains(t, err, injected.Error())
	var adoptionErr *PopulatedTableAdoptionError
	require.ErrorAs(t, err, &adoptionErr)
	require.Equal(t, "baseline_unrepresentable", adoptionErr.Reason)
	requireAdoptionStageError(t, recorder, "business_row_scan_canonicalization")
}

func TestBootstrap_PopulatedAdoptionAttributesPersistenceError(t *testing.T) {
	runBootstrapCancellationStageAttributionTest(
		t,
		"persistence",
		"INSERT INTO sync.bundle_log",
		"pristine_baseline_persistence",
	)
}

func runBootstrapCancellationStageAttributionTest(t *testing.T, fixtureSuffix, cancelContains, expectedStage string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tracer := &adoptionQueryTracer{cancel: cancel, cancelContains: cancelContains}
	pool := newTracedAdoptionTestPool(t, ctx, tracer)
	schemaName, config := prepareStageAttributionFixture(t, ctx, pool, fixtureSuffix)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	recorder := &collectedStageMetrics{}
	config.StageMetrics = recorder
	service, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.ErrorIs(t, service.Bootstrap(ctx), context.Canceled)
	requireAdoptionStageError(t, recorder, expectedStage)
}

func newTracedAdoptionTestPool(t *testing.T, ctx context.Context, tracer *adoptionQueryTracer) *pgxpool.Pool {
	t.Helper()
	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	config, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if !managed {
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed traced adoption database: %v", err)
			}
		}
		pool.Close()
	})
	require.NoError(t, resetTestSyncSchema(ctx, pool))
	return pool
}

func prepareStageAttributionFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) (string, *ServiceConfig) {
	t.Helper()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_stage_" + label + "_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), "stage-owner-"+suffix, uuid.New())
	require.NoError(t, err)
	return schemaName, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "adoption-stage-attribution-" + label,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}
}
