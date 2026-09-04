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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func snapshotRowIdentity(row SnapshotRow) string {
	return fmt.Sprintf("%s.%s:%s", row.Schema, row.Table, row.Key["id"])
}

func collectSnapshotChunkRows(
	t *testing.T,
	ctx context.Context,
	svc *SyncService,
	actor Actor,
	snapshotID string,
	maxRows int,
) ([]SnapshotRow, int64) {
	t.Helper()

	afterRowOrdinal := int64(0)
	seen := make([]SnapshotRow, 0)
	stableSnapshotBundleSeq := int64(0)
	for {
		chunk, err := svc.GetSnapshotChunk(ctx, actor, testSchemaGenerationProofForService(svc), snapshotID, afterRowOrdinal, maxRows, defaultBytesPerSnapshotChunk)
		require.NoError(t, err)
		if stableSnapshotBundleSeq == 0 {
			stableSnapshotBundleSeq = chunk.SnapshotBundleSeq
		} else {
			require.Equal(t, stableSnapshotBundleSeq, chunk.SnapshotBundleSeq)
		}
		seen = append(seen, chunk.Rows...)
		if !chunk.HasMore {
			require.Equal(t, afterRowOrdinal+int64(len(chunk.Rows)), chunk.NextRowOrdinal)
			break
		}
		require.Greater(t, chunk.NextRowOrdinal, afterRowOrdinal)
		afterRowOrdinal = chunk.NextRowOrdinal
	}

	return seen, stableSnapshotBundleSeq
}

type snapshotSessionFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	svc        *SyncService
	suffix     string
	schemaName string
	userID     string
	writer     Actor
	reader     Actor
}

type snapshotSessionFixtureOptions struct {
	defaultRowsPerSnapshotChunk        int
	maxRowsPerSnapshotChunk            int
	maxRowsPerSnapshotSession          int64
	maxBytesPerSnapshotSession         int64
	snapshotCleanupInterval            time.Duration
	snapshotCleanupBatchRows           int
	snapshotCleanupBatchSessions       int
	snapshotCleanupMaxBatches          int
	snapshotCleanupBatchTimeout        time.Duration
	snapshotMaterializationBatchRows   int
	snapshotMaterializationBatchBytes  int64
	maxBytesPerSnapshotRow             int64
	maxConcurrentSnapshotBuilds        int
	maxConcurrentSnapshotChunkRequests int
	registeredTables                   []RegisteredTable
	schemaGenerations                  func(string) []SchemaGeneration
}

func newSnapshotSessionFixture(t *testing.T, scenario string, opts snapshotSessionFixtureOptions) *snapshotSessionFixture {
	t.Helper()

	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	scenarioID := "snapshot-" + strings.ReplaceAll(scenario, "_", "-")
	schemaName := "snapshot_" + scenario + "_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	userID := scenarioID + "-user-" + suffix
	registeredTables := opts.registeredTables
	if len(registeredTables) == 0 {
		registeredTables = []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	} else {
		registeredTables = append([]RegisteredTable(nil), registeredTables...)
		for i := range registeredTables {
			if registeredTables[i].Schema == "" {
				registeredTables[i].Schema = schemaName
			}
		}
	}
	var schemaGenerations []SchemaGeneration
	if opts.schemaGenerations != nil {
		schemaGenerations = opts.schemaGenerations(schemaName)
	}
	if len(schemaGenerations) == 0 {
		schemaGenerations = testSchemaGenerationsForRegisteredTables(registeredTables)
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: schemaGenerations, AppName: scenarioID + "-test",
		DefaultRowsPerSnapshotChunk:        opts.defaultRowsPerSnapshotChunk,
		MaxRowsPerSnapshotChunk:            opts.maxRowsPerSnapshotChunk,
		MaxRowsPerSnapshotSession:          opts.maxRowsPerSnapshotSession,
		MaxBytesPerSnapshotSession:         opts.maxBytesPerSnapshotSession,
		SnapshotCleanupInterval:            opts.snapshotCleanupInterval,
		SnapshotCleanupBatchRows:           opts.snapshotCleanupBatchRows,
		SnapshotCleanupBatchSessions:       opts.snapshotCleanupBatchSessions,
		SnapshotCleanupMaxBatchesPerRun:    opts.snapshotCleanupMaxBatches,
		SnapshotCleanupBatchTimeout:        opts.snapshotCleanupBatchTimeout,
		SnapshotMaterializationBatchRows:   opts.snapshotMaterializationBatchRows,
		SnapshotMaterializationBatchBytes:  opts.snapshotMaterializationBatchBytes,
		MaxBytesPerSnapshotRow:             opts.maxBytesPerSnapshotRow,
		MaxConcurrentSnapshotBuilds:        opts.maxConcurrentSnapshotBuilds,
		MaxConcurrentSnapshotChunkRequests: opts.maxConcurrentSnapshotChunkRequests,
		RegisteredTables:                   registeredTables,
		RegisteredTableCatalogRevision:     1,
	}, logger)
	mustInitializeEmptyScope(t, ctx, svc, userID, "reader")

	return &snapshotSessionFixture{
		ctx:        ctx,
		pool:       pool,
		svc:        svc,
		suffix:     suffix,
		schemaName: schemaName,
		userID:     userID,
		writer:     Actor{UserID: userID, SourceID: "writer"},
		reader:     Actor{UserID: userID, SourceID: "reader"},
	}
}

func (f *snapshotSessionFixture) actor(sourceID string) Actor {
	return Actor{UserID: f.userID, SourceID: sourceID}
}

func (f *snapshotSessionFixture) pushUser(t *testing.T, sourceBundleID int64, rowID uuid.UUID, name string) *Bundle {
	t.Helper()
	return f.pushUserAs(t, f.writer, sourceBundleID, rowID, name)
}

func (f *snapshotSessionFixture) pushUserAs(t *testing.T, actor Actor, sourceBundleID int64, rowID uuid.UUID, name string) *Bundle {
	t.Helper()
	return mustPushUserBundle(t, f.ctx, f.svc, actor, f.schemaName, sourceBundleID, rowID, name)
}

func (f *snapshotSessionFixture) createSessionWithUser(t *testing.T, rowID uuid.UUID, name string) *SnapshotSession {
	t.Helper()
	f.pushUser(t, 1, rowID, name)

	session, err := f.svc.CreateSnapshotSession(f.ctx, f.reader, testSchemaGenerationProofForService(f.svc))
	require.NoError(t, err)
	return session
}

func (f *snapshotSessionFixture) countRows(t *testing.T, query string, args ...any) int {
	t.Helper()

	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, query, args...).Scan(&count))
	return count
}

func (f *snapshotSessionFixture) snapshotSessionCountForUser(t *testing.T) int {
	t.Helper()

	return f.countRows(t, `
		SELECT COUNT(*)
		FROM sync.snapshot_sessions
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, f.userID)
}

func (f *snapshotSessionFixture) snapshotSessionRowCountForUser(t *testing.T) int {
	t.Helper()

	return f.countRows(t, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows ssr
		JOIN sync.snapshot_sessions ss ON ss.snapshot_id = ssr.snapshot_id
		WHERE ss.user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, f.userID)
}

func (f *snapshotSessionFixture) snapshotSessionCount(t *testing.T, snapshotID string) int {
	t.Helper()

	return f.countRows(t, `
		SELECT COUNT(*)
		FROM sync.snapshot_sessions
		WHERE snapshot_id = $1::uuid
	`, snapshotID)
}

func (f *snapshotSessionFixture) snapshotSessionRowCount(t *testing.T, snapshotID string) int {
	t.Helper()

	return f.countRows(t, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows
		WHERE snapshot_id = $1::uuid
	`, snapshotID)
}

func TestSnapshotSessions_CreateAndFetchChunksAtFrozenBundleSeq(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "chunks", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 2,
		maxRowsPerSnapshotChunk:     2,
	})
	row1 := uuid.New()
	row2 := uuid.New()
	row3 := uuid.New()

	resp1 := fixture.pushUser(t, 1, row1, "Alpha")
	resp2 := fixture.pushUser(t, 2, row2, "Bravo")
	resp3 := fixture.pushUser(t, 3, row3, "Charlie")

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, resp3.BundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(3), session.RowCount)

	chunk1, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 2, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotID, chunk1.SnapshotID)
	require.Equal(t, session.SnapshotBundleSeq, chunk1.SnapshotBundleSeq)
	require.Len(t, chunk1.Rows, 2)
	require.NotContains(t, string(chunk1.Rows[0].Payload), `"_sync_scope_id"`)
	require.True(t, chunk1.HasMore)
	require.Equal(t, int64(2), chunk1.NextRowOrdinal)

	resp4 := fixture.pushUser(t, 4, uuid.New(), "Delta")
	require.Equal(t, int64(4), resp4.BundleSeq)

	chunk2, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, chunk1.NextRowOrdinal, 2, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotID, chunk2.SnapshotID)
	require.Equal(t, session.SnapshotBundleSeq, chunk2.SnapshotBundleSeq)
	require.Len(t, chunk2.Rows, 1)
	require.False(t, chunk2.HasMore)
	require.Equal(t, int64(3), chunk2.NextRowOrdinal)

	seenIDs := []string{
		chunk1.Rows[0].Key["id"].(string),
		chunk1.Rows[1].Key["id"].(string),
		chunk2.Rows[0].Key["id"].(string),
	}
	require.ElementsMatch(t, []string{row1.String(), row2.String(), row3.String()}, seenIDs)
	require.NotContains(t, seenIDs, resp4.Rows[0].Key["id"].(string))

	require.Equal(t, 3, fixture.snapshotSessionRowCount(t, session.SnapshotID))

	require.Equal(t, int64(1), resp1.BundleSeq)
	require.Equal(t, int64(2), resp2.BundleSeq)
}

func TestSnapshotSessions_OneThousandRowsAcrossMaterializationAndChunkBoundaries(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "thousand", snapshotSessionFixtureOptions{
		snapshotMaterializationBatchRows: 128,
		defaultRowsPerSnapshotChunk:      137,
		maxRowsPerSnapshotChunk:          137,
	})
	pushRows := make([]PushRequestRow, 1_000)
	for i := range pushRows {
		rowID := uuid.New()
		pushRows[i] = PushRequestRow{
			Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": rowID.String()}, Op: OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"User %d","email":"user-%d@example.com"}`, rowID, i, i)),
		}
	}
	_, err := pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, 1, pushRows)
	require.NoError(t, err)
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, int64(1_000), session.RowCount)
	rows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session.SnapshotID, 137)
	require.Len(t, rows, 1_000)
	var previous []byte
	for i := range rows {
		key, ok := rows[i].Key["id"].(string)
		require.True(t, ok)
		parsed, parseErr := uuid.Parse(key)
		require.NoError(t, parseErr)
		if previous != nil {
			require.Less(t, bytes.Compare(previous, parsed[:]), 0)
		}
		previous = append(previous[:0], parsed[:]...)
	}
	metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	require.LessOrEqual(t, metrics.MaterializationBatchRowsHighWater, int64(128))
	require.LessOrEqual(t, metrics.ChunkRowsHighWater, int64(137))
}

func TestSnapshotSessions_MultiTableSnapshotUsesDeterministicTableOrder(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "multi_table", snapshotSessionFixtureOptions{
		registeredTables: []RegisteredTable{
			{Table: "users", SyncKeyColumns: []string{"id"}},
			{Table: "files", SyncKeyColumns: []string{"id"}},
		},
	})
	userID, fileID := uuid.New(), uuid.New()
	_, err := pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		{
			Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": userID.String()}, Op: OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"User","email":"multi@example.com"}`, userID)),
		},
		{
			Schema: fixture.schemaName, Table: "files", Key: SyncKey{"id": fileID.String()}, Op: OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"File","data":"AQID"}`, fileID)),
		},
	})
	require.NoError(t, err)
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	rows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session.SnapshotID, 1)
	require.Len(t, rows, 2)
	require.Equal(t, []string{"files", "users"}, []string{rows[0].Table, rows[1].Table})
}

func TestSnapshotSessions_ReadCommittedFenceHasNoSnapshotPullGap(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "visibility_fence", snapshotSessionFixtureOptions{})
	beforeID, afterID := uuid.New(), uuid.New()
	before := fixture.pushUser(t, 1, beforeID, "Before")
	reached := make(chan struct{})
	release := make(chan struct{})
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotCommit: func(context.Context) error { close(reached); <-release; return nil }}
	type result struct {
		session *SnapshotSession
		err     error
	}
	created := make(chan result, 1)
	go func() {
		session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		created <- result{session, err}
	}()
	<-reached
	after := fixture.pushUser(t, 2, afterID, "After")
	close(release)
	resultValue := <-created
	require.NoError(t, resultValue.err)
	require.Equal(t, before.BundleSeq, resultValue.session.SnapshotBundleSeq)
	fixture.svc.snapshotHooks = nil
	rows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, resultValue.session.SnapshotID, 10)
	require.Len(t, rows, 1)
	require.Equal(t, beforeID.String(), rows[0].Key["id"])
	pull, err := fixture.svc.ProcessPull(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), resultValue.session.SnapshotBundleSeq, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Equal(t, after.BundleSeq, pull.Bundles[0].BundleSeq)
	require.Equal(t, afterID.String(), pull.Bundles[0].Rows[0].Key["id"])
}

func TestSnapshotSessions_CreateUsesRequiredMutationTransactionModes(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "mutation_modes", snapshotSessionFixtureOptions{})
	fixture.pushUser(t, 1, uuid.New(), "Before")

	_, err := fixture.pool.Exec(fixture.ctx, `
		CREATE OR REPLACE FUNCTION sync.assert_snapshot_mutation_modes()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $function$
		BEGIN
			IF current_setting('transaction_isolation') <> 'read committed'
			   OR current_setting('transaction_read_only') <> 'off'
			   OR current_setting('transaction_deferrable') <> 'off' THEN
				RAISE EXCEPTION 'unexpected snapshot transaction modes: isolation=%, read_only=%, deferrable=%',
					current_setting('transaction_isolation'),
					current_setting('transaction_read_only'),
					current_setting('transaction_deferrable');
			END IF;
			RETURN NEW;
		END;
		$function$;
		CREATE TRIGGER assert_snapshot_mutation_modes
		BEFORE INSERT ON sync.snapshot_sessions
		FOR EACH ROW EXECUTE FUNCTION sync.assert_snapshot_mutation_modes();
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = fixture.pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS assert_snapshot_mutation_modes ON sync.snapshot_sessions`)
		_, _ = fixture.pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS sync.assert_snapshot_mutation_modes()`)
	})

	_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
}

func TestSnapshotSessions_OneChunkStillUsesSessionStorage(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "one_chunk", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 1000,
		maxRowsPerSnapshotChunk:     5000,
	})
	rowID := uuid.New()

	resp := fixture.pushUser(t, 1, rowID, "Solo")
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, resp.BundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(1), session.RowCount)

	require.Equal(t, 1, fixture.snapshotSessionRowCount(t, session.SnapshotID))

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 1000, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.False(t, chunk.HasMore)
	require.Equal(t, int64(1), chunk.NextRowOrdinal)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestSnapshotSessions_NoGapsOrDuplicatesAcrossChunkFetches(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "gapless", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 2,
		maxRowsPerSnapshotChunk:     2,
	})
	insertedIDs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		rowID := uuid.New()
		insertedIDs = append(insertedIDs, rowID.String())
		fixture.pushUser(t, int64(i+1), rowID, fmt.Sprintf("User%d", i))
	}

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	rows, stableBundleSeq := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session.SnapshotID, 2)
	require.Equal(t, session.SnapshotBundleSeq, stableBundleSeq)
	require.Len(t, rows, 5)

	seenIDs := make([]string, 0, len(rows))
	seenSet := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		id := row.Key["id"].(string)
		seenIDs = append(seenIDs, id)
		seenSet[id] = struct{}{}
	}
	require.Len(t, seenSet, 5)
	require.ElementsMatch(t, insertedIDs, seenIDs)

	deterministicChunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 2, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, deterministicChunk.Rows, 2)
	require.Equal(t, snapshotRowIdentity(rows[0]), snapshotRowIdentity(deterministicChunk.Rows[0]))
	require.Equal(t, snapshotRowIdentity(rows[1]), snapshotRowIdentity(deterministicChunk.Rows[1]))
}

func TestSnapshotSessions_ActiveSessionRemainsReadableAfterHistoryPrune(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "prune_session", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 10,
		maxRowsPerSnapshotChunk:     10,
	})

	row1 := uuid.New()
	row2 := uuid.New()
	row3 := uuid.New()
	fixture.pushUser(t, 1, row1, "One")
	fixture.pushUser(t, 2, row2, "Two")
	bundle3 := fixture.pushUser(t, 3, row3, "Three")

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, bundle3.BundleSeq, session.SnapshotBundleSeq)

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)

	var sourceStateCountBefore int
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.source_state WHERE user_pk = $1`, userPK).Scan(&sourceStateCountBefore))
	require.Greater(t, sourceStateCountBefore, 0)

	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = $2
		WHERE user_pk = $1
	`, userPK, session.SnapshotBundleSeq)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		DELETE FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq <= $2
	`, userPK, session.SnapshotBundleSeq)
	require.NoError(t, err)

	var sourceStateCountAfter int
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.source_state WHERE user_pk = $1`, userPK).Scan(&sourceStateCountAfter))
	require.Equal(t, sourceStateCountBefore, sourceStateCountAfter)

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotBundleSeq, chunk.SnapshotBundleSeq)
	require.Len(t, chunk.Rows, 3)
}

func TestSnapshotSessions_RepeatedSessionCreationUsesDeterministicRowOrdering(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "ordering", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 10,
		maxRowsPerSnapshotChunk:     10,
	})

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	fixture.pushUser(t, 1, ids[2], "Zulu")
	fixture.pushUser(t, 2, ids[0], "Alpha")
	fixture.pushUser(t, 3, ids[1], "Mike")

	session1, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	rows1, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session1.SnapshotID, 10)

	session2, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	rows2, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session2.SnapshotID, 10)

	require.Len(t, rows1, len(rows2))
	for i := range rows1 {
		require.Equal(t, snapshotRowIdentity(rows1[i]), snapshotRowIdentity(rows2[i]))
		require.Equal(t, rows1[i].RowVersion, rows2[i].RowVersion)
		require.JSONEq(t, string(rows1[i].Payload), string(rows2[i].Payload))
	}
}

func TestSnapshotSessions_TextKeysUseCanonicalByteOrderAcrossPages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "snapshot_text_key_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			doc_id TEXT NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, doc_id)
		)
	`, schemaIdent))
	require.NoError(t, err)

	tables := []RegisteredTable{
		{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"doc_id"}},
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, tables, map[string]GenerationWireType{
		Key(schemaName, "docs") + ".doc_id": GenerationWireTypeText,
	}), AppName: "snapshot-text-key-test",
		SnapshotMaterializationBatchRows: 2,
		DefaultRowsPerSnapshotChunk:      2,
		MaxRowsPerSnapshotChunk:          2,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"doc_id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	userID := "snapshot-text-key-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	for index, key := range []string{"ä", "z", "A", " a"} {
		payload, marshalErr := json.Marshal(map[string]string{"doc_id": key, "body": "Body " + key})
		require.NoError(t, marshalErr)
		_, err = pushRowsOneShot(t, ctx, svc, writer, int64(index+1), []PushRequestRow{{
			Schema: schemaName, Table: "docs", Key: SyncKey{"doc_id": key}, Op: OpInsert,
			BaseRowVersion: 0, Payload: payload,
		}})
		require.NoError(t, err)
	}

	reader := Actor{UserID: userID, SourceID: "reader"}
	_, err = svc.Connect(ctx, reader, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.NoError(t, err)
	session, err := svc.CreateSnapshotSession(ctx, reader, testSchemaGenerationProofForService(svc))
	require.NoError(t, err)
	require.Equal(t, int64(4), session.RowCount)
	rows, _ := collectSnapshotChunkRows(t, ctx, svc, reader, session.SnapshotID, 2)
	require.Len(t, rows, 4)
	actualKeys := make([]string, len(rows))
	for i := range rows {
		key, ok := rows[i].Key["doc_id"].(string)
		require.True(t, ok)
		actualKeys[i] = key
	}
	require.Equal(t, []string{" a", "A", "z", "ä"}, actualKeys)

	keyRows, err := pool.Query(ctx, `SELECT key_bytes FROM sync.snapshot_session_rows WHERE snapshot_id=$1::uuid ORDER BY row_ordinal`, session.SnapshotID)
	require.NoError(t, err)
	defer keyRows.Close()
	var previous []byte
	for keyRows.Next() {
		var current []byte
		require.NoError(t, keyRows.Scan(&current))
		if previous != nil {
			require.Less(t, bytes.Compare(previous, current), 0)
		}
		previous = append(previous[:0], current...)
	}
	require.NoError(t, keyRows.Err())
}

func TestSnapshotSessions_DeleteInvalidatesFurtherChunkFetches(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "delete", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	require.NoError(t, fixture.svc.DeleteSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID))

	_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	var notFoundErr *SnapshotSessionNotFoundError
	var expiredErr *SnapshotSessionExpiredError
	require.True(t, errors.As(err, &notFoundErr) || errors.As(err, &expiredErr), "retired snapshot must be unreadable: %v", err)
	require.Eventually(t, func() bool { return fixture.snapshotSessionRowCount(t, session.SnapshotID) == 0 }, 5*time.Second, 10*time.Millisecond)
}

func TestSnapshotSessions_InvalidCursorRejected(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "invalid_cursor", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")

	_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, -1, 10, defaultBytesPerSnapshotChunk)
	var invalidErr *SnapshotChunkInvalidError
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "after_row_ordinal")

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 0, defaultBytesPerSnapshotChunk)
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "max_rows")
}

func TestSnapshotSessions_WrongUserRejected(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "wrong_user", snapshotSessionFixtureOptions{})
	writer := fixture.writer
	readerA := Actor{UserID: writer.UserID, SourceID: "reader-a"}
	readerB := Actor{UserID: "snapshot-wrong-user-b-" + fixture.suffix, SourceID: "reader-b"}
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")
	_, err := fixture.svc.Connect(fixture.ctx, readerA, testSchemaGenerationProofForService(fixture.svc), &ConnectRequest{})
	require.NoError(t, err)
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, readerB.UserID, readerB.SourceID)

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, readerA, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, readerB, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	var forbiddenErr *SnapshotSessionForbiddenError
	require.ErrorAs(t, err, &forbiddenErr)

	err = fixture.svc.DeleteSnapshotSession(fixture.ctx, readerB, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID)
	require.ErrorAs(t, err, &forbiddenErr)
}

func TestSnapshotSessions_ExpiredSessionRejected(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "expired", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_sessions
		SET expires_at = now() - interval '1 second'
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	var expiredErr *SnapshotSessionExpiredError
	require.ErrorAs(t, err, &expiredErr)
}

func TestSnapshotSessions_ChunkPayloadPreservesCurrentAfterImage(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "after_image", snapshotSessionFixtureOptions{})
	actor := fixture.writer
	row1 := uuid.New()
	row2 := uuid.New()

	fixture.pushUserAs(t, actor, 1, row1, "Alpha")
	fixture.pushUserAs(t, actor, 2, row2, "Gamma")
	require.NoError(t, fixture.svc.WithinSyncBundle(fixture.ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 3}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.users WHERE id = $1`, fixture.schemaName), row2)
		return err
	}))

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	rows, stableBundleSeq := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session.SnapshotID, 10)
	require.Equal(t, int64(3), stableBundleSeq)
	require.Len(t, rows, 1)
	require.Equal(t, fixture.schemaName, rows[0].Schema)
	require.Equal(t, "users", rows[0].Table)
	require.Equal(t, int64(1), rows[0].RowVersion)
	require.Equal(t, row1.String(), rows[0].Key["id"])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rows[0].Payload, &payload))
	require.Equal(t, "Alpha", payload["name"])
}

func TestSnapshotSessions_CreateEmptySnapshot(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "empty", snapshotSessionFixtureOptions{})
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.reader.UserID, fixture.reader.SourceID)
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Zero(t, session.RowCount)
	require.Zero(t, session.ByteCount)

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Empty(t, chunk.Rows)
	require.False(t, chunk.HasMore)
	require.Zero(t, chunk.NextRowOrdinal)
}

func TestSnapshotSessions_RotatedCreateRetiresPreviousAndReservesReplacement(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rotate", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-old"
	newSourceID := "writer-new"
	writer := fixture.actor(oldSourceID)
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")

	session, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, session.SnapshotID)

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)

	var (
		oldState                string
		oldMaxCommittedBundleID int64
		oldReplacedBySourceID   string
		oldRetirementReason     string
		newState                string
		newMaxCommittedBundleID int64
		newReplacedBySourceID   string
		newRetirementReason     string
	)
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, oldSourceID).Scan(&oldState, &oldMaxCommittedBundleID, &oldReplacedBySourceID, &oldRetirementReason))
	require.Equal(t, sourceStateRetired, oldState)
	require.Equal(t, int64(1), oldMaxCommittedBundleID)
	require.Equal(t, newSourceID, oldReplacedBySourceID)
	require.Equal(t, "history_pruned", oldRetirementReason)

	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&newState, &newMaxCommittedBundleID, &newReplacedBySourceID, &newRetirementReason))
	require.Equal(t, sourceStateReserved, newState)
	require.Zero(t, newMaxCommittedBundleID)
	require.Empty(t, newReplacedBySourceID)
	require.Empty(t, newRetirementReason)
}

func TestSnapshotSessions_RotatedCreateForNeverCommittedSourceCreatesRetiredZeroFloor(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rotate_empty", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-empty-old"
	newSourceID := "writer-empty-new"
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, oldSourceID)

	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, fixture.actor(oldSourceID), testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "source_sequence_changed",
		},
	})

	require.NoError(t, err)

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)

	var oldState string
	var oldMaxCommittedBundleID int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, oldSourceID).Scan(&oldState, &oldMaxCommittedBundleID))
	require.Equal(t, sourceStateRetired, oldState)
	require.Zero(t, oldMaxCommittedBundleID)

	var newState string
	var newMaxCommittedBundleID int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&newState, &newMaxCommittedBundleID))
	require.Equal(t, sourceStateReserved, newState)
	require.Zero(t, newMaxCommittedBundleID)
}

func TestSnapshotSessions_RepeatedEquivalentRotationIsIdempotentForSourceState(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rotate_repeat", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-repeat-old"
	newSourceID := "writer-repeat-new"
	writer := fixture.actor(oldSourceID)
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")

	req := &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	}
	session1, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, testSchemaGenerationProofForService(fixture.svc), req)
	require.NoError(t, err)
	session2, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, testSchemaGenerationProofForService(fixture.svc), req)
	require.NoError(t, err)
	require.NotEqual(t, session1.SnapshotID, session2.SnapshotID)

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)

	var sourceStateCount int
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*)
		FROM sync.source_state
		WHERE user_pk = $1
	`, userPK).Scan(&sourceStateCount))
	// The fixture's reader binding plus the permanent old/new replacement witnesses remain.
	require.Equal(t, 3, sourceStateCount)

	var oldState, oldReplacedBySourceID string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state, replaced_by_source_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, oldSourceID).Scan(&oldState, &oldReplacedBySourceID))
	require.Equal(t, sourceStateRetired, oldState)
	require.Equal(t, newSourceID, oldReplacedBySourceID)

	var newState string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&newState))
	require.Equal(t, sourceStateReserved, newState)
}

func TestSnapshotSessions_ReplacementDoesNotObserveUnsubmittedPushRows(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rot_concurrent", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-concurrent-old"
	newSourceID := "writer-concurrent-new"
	oldActor := fixture.actor(oldSourceID)
	row1, row2 := uuid.New(), uuid.New()
	fixture.pushUserAs(t, oldActor, 1, row1, "Before")

	stagedRows := []PushRequestRow{{
		Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": row2.String()}, Op: OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Concurrent","email":"concurrent@example.com"}`, row2)),
	}}
	requestHash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(fixture.svc), stagedRows)
	require.NoError(t, err)
	replacement, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldSourceID, NewSourceID: newSourceID, Reason: "history_pruned"},
	})
	require.NoError(t, err)
	require.NotNil(t, replacement)
	_, err = fixture.svc.PushOneShot(fixture.ctx, oldActor, testSchemaGenerationProofForService(fixture.svc), &OneShotPushRequest{
		SourceBundleID: 2, CanonicalRequestHash: requestHash, Rows: stagedRows,
	})
	var retiredErr *SourceRetiredError
	require.ErrorAs(t, err, &retiredErr)
	rows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.actor(newSourceID), replacement.SnapshotID, 10)
	require.Len(t, rows, 1)
}

func TestSnapshotSessions_LostReplacementResponseRetriesSameIdentity(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rotate_lost_response", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-lost-old"
	newSourceID := "writer-lost-new"
	oldActor := fixture.actor(oldSourceID)
	rowID := uuid.New()
	fixture.pushUserAs(t, oldActor, 1, rowID, "Acknowledged")
	req := &SnapshotSessionCreateRequest{SourceReplacement: &SnapshotSourceReplacement{
		PreviousSourceID: oldSourceID, NewSourceID: newSourceID, Reason: "history_pruned",
	}}
	lostResponse := errors.New("simulated lost replacement response")
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotCommit: func(context.Context) error { return lostResponse }}
	first, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, testSchemaGenerationProofForService(fixture.svc), req)
	require.ErrorIs(t, err, lostResponse)
	require.Nil(t, first)
	require.Equal(t, 1, fixture.snapshotSessionCountForUser(t))

	fixture.svc.snapshotHooks = nil
	retry, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, testSchemaGenerationProofForService(fixture.svc), req)
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, 2, fixture.snapshotSessionCountForUser(t))
	rows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.actor(newSourceID), retry.SnapshotID, 10)
	require.Len(t, rows, 1)
	require.Equal(t, rowID.String(), rows[0].Key["id"])

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)
	var oldState, replacedBy, newState string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT state,replaced_by_source_id FROM sync.source_state WHERE user_pk=$1 AND source_id=$2`, userPK, oldSourceID).Scan(&oldState, &replacedBy))
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT state FROM sync.source_state WHERE user_pk=$1 AND source_id=$2`, userPK, newSourceID).Scan(&newState))
	require.Equal(t, sourceStateRetired, oldState)
	require.Equal(t, newSourceID, replacedBy)
	require.Equal(t, sourceStateReserved, newState)
}

func TestSnapshotSessions_RetiredSourceIsRejectedByEveryOrdinaryOperation(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "retired_ops", snapshotSessionFixtureOptions{})
	proof := testSchemaGenerationProofForService(fixture.svc)
	oldActor := fixture.actor("retired-old")
	newSourceID := "retired-new"
	fixture.pushUserAs(t, oldActor, 1, uuid.New(), "Before retirement")
	oldSnapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, oldActor, proof)
	require.NoError(t, err)
	_, err = fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, proof, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldActor.SourceID, NewSourceID: newSourceID, Reason: "history_pruned"},
	})
	require.NoError(t, err)

	assertRetired := func(name string, call func() error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			var retired *SourceRetiredError
			require.ErrorAs(t, call(), &retired)
			require.Equal(t, oldActor.SourceID, retired.SourceID)
			require.Equal(t, newSourceID, retired.ReplacedBySourceID)
		})
	}
	assertRetired("connect", func() error {
		_, err := fixture.svc.Connect(fixture.ctx, oldActor, proof, &ConnectRequest{})
		return err
	})
	assertRetired("pull", func() error { _, err := fixture.svc.ProcessPull(fixture.ctx, oldActor, proof, 0, 1, 0); return err })
	assertRetired("push", func() error {
		rowID := uuid.NewString()
		rows := []PushRequestRow{{
			Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": rowID}, Op: OpInsert,
			Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"retired","email":"retired@example.com"}`, rowID)),
		}}
		requestHash, err := computeGenerationPinnedPushRequestHash(proof, rows)
		if err != nil {
			return err
		}
		_, err = fixture.svc.PushOneShot(fixture.ctx, oldActor, proof, &OneShotPushRequest{SourceBundleID: 2, CanonicalRequestHash: requestHash, Rows: rows})
		return err
	})
	assertRetired("snapshot create", func() error { _, err := fixture.svc.CreateSnapshotSession(fixture.ctx, oldActor, proof); return err })
	assertRetired("snapshot get", func() error {
		_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, oldActor, proof, oldSnapshot.SnapshotID, 0, 1, 1)
		return err
	})
	assertRetired("snapshot delete", func() error {
		return fixture.svc.DeleteSnapshotSession(fixture.ctx, oldActor, proof, oldSnapshot.SnapshotID)
	})
	assertRetired("watch", func() error {
		_, err := fixture.svc.SubscribeBundleChanges(fixture.ctx, oldActor, proof, 0)
		return err
	})

	discovery, err := fixture.svc.GetGeneration(fixture.ctx, oldActor, proof.ID)
	require.NoError(t, err)
	require.Equal(t, canonicalSchemaGenerationID(proof.ID), discovery.SchemaGeneration.ID)
}

func TestSnapshotSessions_SourceReplacementCanUpgradeGenerationAtomically(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "replacement_upgrade", snapshotSessionFixtureOptions{
		schemaGenerations: func(schema string) []SchemaGeneration {
			table := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{
				{Name: "email", WireType: GenerationWireTypeText},
				{Name: "id", WireType: GenerationWireTypeUUID},
				{Name: "name", WireType: GenerationWireTypeText},
			}}
			return []SchemaGeneration{{ID: 1, Tables: []GenerationTable{table}}, {ID: 2, Tables: []GenerationTable{table}}}
		},
	})
	proof1 := proofForGeneration(t, fixture.svc, 1)
	proof2 := proofForGeneration(t, fixture.svc, 2)
	oldActor := fixture.actor("upgrade-old")
	newActor := fixture.actor("upgrade-new")
	_, err := fixture.svc.Connect(fixture.ctx, oldActor, proof1, &ConnectRequest{})
	require.NoError(t, err)

	snapshot, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, proof2, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldActor.SourceID, NewSourceID: newActor.SourceID, Reason: "schema_generation_upgrade"},
	})
	require.NoError(t, err)

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)
	var generationID int32
	var fingerprint string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT schema_generation_id,schema_generation_fingerprint FROM sync.source_generation_bindings WHERE user_pk=$1 AND source_id=$2`, userPK, newActor.SourceID).Scan(&generationID, &fingerprint))
	require.Equal(t, int32(proof2.ID), generationID)
	require.Equal(t, proof2.Fingerprint, fingerprint)
	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, newActor, proof2, snapshot.SnapshotID, 0, 1, 1)
	require.NoError(t, err)
	_, err = fixture.svc.ProcessPull(fixture.ctx, oldActor, proof1, 0, 1, 0)
	var retired *SourceRetiredError
	require.ErrorAs(t, err, &retired)

	retry, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, proof2, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldActor.SourceID, NewSourceID: newActor.SourceID, Reason: "schema_generation_upgrade"},
	})
	require.NoError(t, err)
	require.NotNil(t, retry)
}

func TestSnapshotSessions_DeleteSerializesWithSourceReplacement(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "delete_replace_race", snapshotSessionFixtureOptions{})
	proof := testSchemaGenerationProofForService(fixture.svc)
	oldActor := fixture.actor("delete-race-old")
	newActor := fixture.actor("delete-race-new")
	_, err := fixture.svc.Connect(fixture.ctx, oldActor, proof, &ConnectRequest{})
	require.NoError(t, err)
	snapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, oldActor, proof)
	require.NoError(t, err)

	reached := make(chan struct{})
	release := make(chan struct{})
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterDeleteSessionRead: func(context.Context) error {
		close(reached)
		<-release
		return nil
	}}
	deleteResult := make(chan error, 1)
	go func() {
		deleteResult <- fixture.svc.DeleteSnapshotSession(fixture.ctx, oldActor, proof, snapshot.SnapshotID)
	}()
	<-reached

	replacementResult := make(chan error, 1)
	go func() {
		_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, proof, &SnapshotSessionCreateRequest{SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldActor.SourceID,
			NewSourceID:      newActor.SourceID,
			Reason:           "history_pruned",
		}})
		replacementResult <- err
	}()
	select {
	case err := <-replacementResult:
		t.Fatalf("source replacement completed before delete released its source lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-deleteResult)
	require.NoError(t, <-replacementResult)
	fixture.svc.snapshotHooks = nil

	_, err = fixture.svc.ProcessPull(fixture.ctx, oldActor, proof, 0, 1, 0)
	var retired *SourceRetiredError
	require.ErrorAs(t, err, &retired)
}

func TestSnapshotSessions_CorruptStoredGenerationPrecedesBindingChecks(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "corrupt_gen", snapshotSessionFixtureOptions{})
	proof := testSchemaGenerationProofForService(fixture.svc)
	_, err := fixture.svc.Connect(fixture.ctx, fixture.reader, proof, &ConnectRequest{})
	require.NoError(t, err)
	snapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, proof)
	require.NoError(t, err)
	for _, statement := range []string{
		`ALTER TABLE sync.snapshot_sessions DROP CONSTRAINT snapshot_sessions_source_generation_fk`,
		`ALTER TABLE sync.snapshot_sessions DROP CONSTRAINT snapshot_sessions_generation_fingerprint_chk`,
	} {
		_, err = fixture.pool.Exec(fixture.ctx, statement)
		require.NoError(t, err)
	}
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET schema_generation_fingerprint='bad' WHERE snapshot_id=$1::uuid`, snapshot.SnapshotID)
	require.NoError(t, err)

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, snapshot.SnapshotID, 0, 1, 1)
	var corrupt *SessionGenerationError
	require.ErrorAs(t, err, &corrupt)
	require.Equal(t, SessionGenerationStateCorrupt, corrupt.Code)
	require.Equal(t, "snapshot", corrupt.SessionKind)
	require.Equal(t, "schema_generation_fingerprint", corrupt.Field)
}

func TestSnapshotSessions_RotatedCreateRejectsConflictingReplacementTargets(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rotate_conflict", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-conflict-old"
	firstNewSourceID := "writer-conflict-new-a"
	secondNewSourceID := "writer-conflict-new-b"
	otherOldSourceID := "writer-conflict-old-b"
	writer := fixture.actor(oldSourceID)
	otherWriter := fixture.actor(otherOldSourceID)
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")
	fixture.pushUserAs(t, otherWriter, 1, uuid.New(), "Bravo")

	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      firstNewSourceID,
			Reason:           "history_pruned",
		},
	})

	require.NoError(t, err)

	_, err = fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      secondNewSourceID,
			Reason:           "history_pruned",
		},
	})

	var retiredErr *SourceRetiredError
	require.ErrorAs(t, err, &retiredErr)
	require.Equal(t, oldSourceID, retiredErr.SourceID)
	require.Equal(t, firstNewSourceID, retiredErr.ReplacedBySourceID)

	_, err = fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, otherWriter, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: otherOldSourceID,
			NewSourceID:      firstNewSourceID,
			Reason:           "history_pruned",
		},
	})

	var replacementErr *SourceReplacementInvalidError
	require.ErrorAs(t, err, &replacementErr)
	require.Equal(t, "source replacement is invalid", replacementErr.Error())
	require.NotContains(t, replacementErr.Error(), firstNewSourceID)
}

func TestSnapshotSessions_FirstCommitUnderReservedReplacementActivatesSource(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rotate_activate", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-activate-old"
	newSourceID := "writer-activate-new"
	oldWriter := fixture.actor(oldSourceID)
	newWriter := fixture.actor(newSourceID)
	fixture.pushUserAs(t, oldWriter, 1, uuid.New(), "Alpha")

	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldWriter, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	})

	require.NoError(t, err)

	fixture.pushUserAs(t, newWriter, 1, uuid.New(), "Bravo")

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)

	var state string
	var maxCommittedSourceBundleID int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&state, &maxCommittedSourceBundleID))
	require.Equal(t, sourceStateActive, state)
	require.Equal(t, int64(1), maxCommittedSourceBundleID)
}

func TestSnapshotSessions_GetChunkDoesNotIssueSnapshotSessionUpdate(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "no_update", snapshotSessionFixtureOptions{})
	rowID := uuid.New()

	session := fixture.createSessionWithUser(t, rowID, "Alpha")

	conn, err := fixture.pool.Acquire(fixture.ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_snapshot_session_updates ON sync.snapshot_sessions`)
		conn.Release()
	})

	_, err = conn.Exec(fixture.ctx, `
		CREATE OR REPLACE FUNCTION pg_temp.reject_snapshot_session_updates()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'unexpected snapshot session update';
		END;
		$$
	`)
	require.NoError(t, err)
	_, err = conn.Exec(fixture.ctx, `
		CREATE TRIGGER reject_snapshot_session_updates
		BEFORE UPDATE ON sync.snapshot_sessions
		FOR EACH ROW
		EXECUTE FUNCTION pg_temp.reject_snapshot_session_updates()
	`)
	require.NoError(t, err)

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestSnapshotSessions_CreateRejectsConfiguredLimitsAndRollsBack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		scenario             string
		maxRows              int64
		maxBytes             int64
		rowNames             []string
		dimension            string
		expectedActual       int64
		actualGreaterThan    int64
		expectRowRollbackGap bool
	}{
		{
			name:                 "row limit",
			scenario:             "row_cap",
			maxRows:              1,
			rowNames:             []string{"Alpha", "Bravo"},
			dimension:            "row_count",
			expectedActual:       2,
			expectRowRollbackGap: true,
		},
		{
			name:              "byte limit",
			scenario:          "byte_cap",
			maxBytes:          1,
			rowNames:          []string{"Alpha"},
			dimension:         "logical_byte_count",
			actualGreaterThan: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSnapshotSessionFixture(t, tc.scenario, snapshotSessionFixtureOptions{
				maxRowsPerSnapshotSession:  tc.maxRows,
				maxBytesPerSnapshotSession: tc.maxBytes,
			})
			for i, name := range tc.rowNames {
				fixture.pushUser(t, int64(i+1), uuid.New(), name)
			}

			_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			var limitErr *SnapshotSessionLimitExceededError
			require.ErrorAs(t, err, &limitErr)
			require.Equal(t, tc.dimension, limitErr.Dimension)
			if tc.expectedActual > 0 {
				require.Equal(t, tc.expectedActual, limitErr.Actual)
			} else {
				require.Greater(t, limitErr.Actual, tc.actualGreaterThan)
			}
			require.Equal(t, int64(1), limitErr.Limit)

			require.Zero(t, fixture.snapshotSessionCountForUser(t))
			if tc.expectRowRollbackGap {
				require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
			}
		})
	}
}

func TestSnapshotSessions_IntegrityMismatchRollsBackWithoutVisibleSession(t *testing.T) {
	t.Parallel()
	t.Run("live row missing row state", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "missing_state", snapshotSessionFixtureOptions{})
		fixture.pushUser(t, 1, uuid.New(), "Alpha")
		userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
		require.NoError(t, err)
		_, err = fixture.pool.Exec(fixture.ctx, `DELETE FROM sync.row_state WHERE user_pk=$1`, userPK)
		require.NoError(t, err)
		_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.ErrorContains(t, err, "integrity mismatch")
		require.Zero(t, fixture.snapshotSessionCountForUser(t))
		require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
	})
	t.Run("row state missing live row", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "missing_business", snapshotSessionFixtureOptions{})
		rowID := uuid.New()
		fixture.pushUser(t, 1, rowID, "Alpha")
		table := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
		for _, trigger := range []string{registeredTableCaptureTriggerName, registeredTableOwnerGuardTrigger} {
			_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER %s`, table, pgx.Identifier{trigger}.Sanitize()))
			require.NoError(t, err)
		}
		_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`DELETE FROM %s WHERE id=$1`, table), rowID)
		require.NoError(t, err)
		for _, trigger := range []string{registeredTableCaptureTriggerName, registeredTableOwnerGuardTrigger} {
			_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER %s`, table, pgx.Identifier{trigger}.Sanitize()))
			require.NoError(t, err)
		}
		_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.ErrorContains(t, err, "integrity mismatch")
		require.Zero(t, fixture.snapshotSessionCountForUser(t))
		require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
	})
	t.Run("live row has deleted state", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "deleted_state", snapshotSessionFixtureOptions{})
		fixture.pushUser(t, 1, uuid.New(), "Alpha")
		userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
		require.NoError(t, err)
		_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.row_state SET deleted=TRUE WHERE user_pk=$1`, userPK)
		require.NoError(t, err)
		_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.ErrorContains(t, err, "integrity mismatch")
		require.Zero(t, fixture.snapshotSessionCountForUser(t))
		require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
	})
	t.Run("row version beyond checkpoint rolls back source replacement", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "future_row_version", snapshotSessionFixtureOptions{})
		oldSourceID := "future-version-old"
		newSourceID := "future-version-new"
		oldActor := fixture.actor(oldSourceID)
		fixture.pushUserAs(t, oldActor, 1, uuid.New(), "Alpha")
		userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
		require.NoError(t, err)
		_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.row_state SET bundle_seq=2 WHERE user_pk=$1`, userPK)
		require.NoError(t, err)

		_, err = fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{
			SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldSourceID, NewSourceID: newSourceID, Reason: "history_pruned"},
		})
		var integrity *GenerationProjectionError
		require.ErrorAs(t, err, &integrity)
		require.Equal(t, GenerationProjectionIntegrityError, integrity.Code)
		require.Zero(t, fixture.snapshotSessionCountForUser(t))
		require.Zero(t, fixture.snapshotSessionRowCountForUser(t))

		var oldState, replacedBy string
		require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
			SELECT state, replaced_by_source_id FROM sync.source_state
			WHERE user_pk=$1 AND source_id=$2
		`, userPK, oldSourceID).Scan(&oldState, &replacedBy))
		require.Equal(t, sourceStateActive, oldState)
		require.Empty(t, replacedBy)
		var newCount int
		require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
			SELECT COUNT(*) FROM sync.source_state WHERE user_pk=$1 AND source_id=$2
		`, userPK, newSourceID).Scan(&newCount))
		require.Zero(t, newCount)
	})
}

func TestSnapshotSessions_DuplicateLogicalKeysAreRejectedByManagedIdentity(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "duplicate_identity", snapshotSessionFixtureOptions{})
	rowID := uuid.New()
	fixture.pushUser(t, 1, rowID, "Alpha")
	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)
	tableID, err := fixture.svc.tableIDForTable(fixture.schemaName, "users")
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		INSERT INTO sync.row_state(user_pk,table_id,key_bytes,bundle_seq,deleted,payload_wire)
		VALUES($1,$2,$3,1,FALSE,'{}'::jsonb)
	`, userPK, tableID, append([]byte(nil), rowID[:]...))
	require.Error(t, err)

	tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		INSERT INTO %s(_sync_scope_id,id,name,email)
		VALUES($1,$2,'Duplicate','duplicate@example.com')
	`, tableIdent), fixture.userID, rowID)
	require.Error(t, err)

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, int64(1), session.RowCount)
}

func TestSnapshotSessions_RowWireLimitRejectsAndRollsBack(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "row_wire_limit", snapshotSessionFixtureOptions{})
	fixture.pushUser(t, 1, uuid.New(), strings.Repeat("x", 512))
	fixture.svc.config.MaxBytesPerSnapshotRow = 128
	fixture.svc.config.SnapshotMaterializationBatchBytes = 128
	_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	var limit *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "row_decoded_body_bytes", limit.Dimension)
	require.Greater(t, limit.Actual, limit.Limit)
	require.Zero(t, fixture.snapshotSessionCountForUser(t))
	require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
}

func TestSnapshotSessions_ExpandedKeyPlusPayloadRowLimitRollsBack(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "expanded_row_limit", snapshotSessionFixtureOptions{})
	bundle := fixture.pushUser(t, 1, uuid.New(), "Expanded")
	require.Len(t, bundle.Rows, 1)
	row := bundle.Rows[0]
	expandedBytes, err := ExpandedRowDecodedBytes(PushRequestRow{
		Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
		BaseRowVersion: row.RowVersion, Payload: row.Payload,
	})
	require.NoError(t, err)
	require.Greater(t, expandedBytes, int64(len(row.Payload)))
	fixture.svc.config.MaxBytesPerSnapshotRow = expandedBytes - 1

	_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	var limit *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "row_decoded_body_bytes", limit.Dimension)
	require.Equal(t, expandedBytes, limit.Actual)
	require.Equal(t, expandedBytes-1, limit.Limit)
	require.Zero(t, fixture.snapshotSessionCountForUser(t))
	require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
}

func TestSnapshotSessions_OversizedRowsAreSuppressedBeforeMaterialization(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "row_preflight", snapshotSessionFixtureOptions{})
	fixture.pushUser(t, 1, uuid.New(), strings.Repeat("x", 512))
	fixture.svc.config.MaxBytesPerSnapshotRow = 128
	fixture.svc.config.SnapshotMaterializationBatchBytes = 128
	var observation snapshotPayloadScanObservation
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotMaterializedRowScan: func(actual snapshotPayloadScanObservation) {
		observation = actual
	}}
	_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	var limit *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limit)
	require.Greater(t, observation.ReportedPayloadByteCount, fixture.svc.maxBytesPerSnapshotRow())
	require.False(t, observation.PayloadMaterialized)
	require.Zero(t, observation.MaterializedPayloadBytes)
	require.Zero(t, fixture.snapshotSessionCountForUser(t))
}

func TestSnapshotSessions_OversizedRetainedRowsAreSuppressedBeforeChunkDecode(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "chunk_preflight", snapshotSessionFixtureOptions{})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	fixture.svc.config.MaxBytesPerSnapshotRow = 128
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_session_rows
		SET payload_wire = $2::json, wire_byte_count = 100
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID, fmt.Sprintf(`{"id":"x","name":"%s"}`, strings.Repeat("x", 512)))
	require.NoError(t, err)
	var observation snapshotPayloadScanObservation
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotChunkRowScan: func(actual snapshotPayloadScanObservation) {
		observation = actual
	}}
	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, 1024)
	var integrity *GenerationProjectionError
	require.ErrorAs(t, err, &integrity)
	require.Equal(t, GenerationProjectionIntegrityError, integrity.Code)
	require.Greater(t, observation.ReportedPayloadByteCount, fixture.svc.maxBytesPerSnapshotRow())
	require.False(t, observation.PayloadMaterialized)
	require.Zero(t, observation.MaterializedPayloadBytes)
}

func TestSnapshotSessions_OversizedRetainedKeysAreSuppressedBeforeChunkDecode(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "chunk_key_preflight", snapshotSessionFixtureOptions{})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	fixture.svc.config.MaxBytesPerSnapshotRow = 128
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_session_rows
		SET key_bytes = decode(repeat('61', 512), 'hex'), wire_byte_count = 100
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)
	var observation snapshotPayloadScanObservation
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotChunkRowScan: func(actual snapshotPayloadScanObservation) {
		observation = actual
	}}
	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, 1024)
	var integrity *GenerationProjectionError
	require.ErrorAs(t, err, &integrity)
	require.Equal(t, GenerationProjectionIntegrityError, integrity.Code)
	require.Greater(t, observation.ReportedKeyByteCount, fixture.svc.maxBytesPerSnapshotRow())
	require.False(t, observation.KeyMaterialized)
	require.Zero(t, observation.MaterializedKeyBytes)
}

func TestSnapshotSessions_RowDecodedLimitRejectsOversizePayload(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "rowdecodedlimit", snapshotSessionFixtureOptions{})
	fixture.pushUser(t, 1, uuid.New(), strings.Repeat("x", 512))
	fixture.svc.config.MaxBytesPerSnapshotRow = 128
	fixture.svc.config.SnapshotMaterializationBatchBytes = 128
	_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	var limit *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "row_decoded_body_bytes", limit.Dimension)
	require.Greater(t, limit.Actual, limit.Limit)
	require.Zero(t, fixture.snapshotSessionCountForUser(t))
	require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
}

func TestSnapshotSessions_DatabaseWriteAndCommitFailuresRollBack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		scenario   string
		triggerSQL string
	}{
		{
			name:     "batch insert",
			scenario: "failbatch",
			triggerSQL: `
				CREATE TRIGGER reject_snapshot_row_insert
				BEFORE INSERT ON sync.snapshot_session_rows
				FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_snapshot_write()
			`,
		},
		{
			name:     "deferred commit",
			scenario: "failcommit",
			triggerSQL: `
				CREATE CONSTRAINT TRIGGER reject_snapshot_session_commit
				AFTER INSERT ON sync.snapshot_sessions
				DEFERRABLE INITIALLY DEFERRED
				FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_snapshot_write()
			`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSnapshotSessionFixture(t, tc.scenario, snapshotSessionFixtureOptions{})
			fixture.pushUser(t, 1, uuid.New(), "Alpha")
			_, err := fixture.pool.Exec(fixture.ctx, `
				CREATE OR REPLACE FUNCTION pg_temp.reject_snapshot_write()
				RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected snapshot write failure';
				END;
				$$
			`)
			require.NoError(t, err)
			_, err = fixture.pool.Exec(fixture.ctx, tc.triggerSQL)
			require.NoError(t, err)

			_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			require.ErrorContains(t, err, "injected snapshot write failure")
			require.Zero(t, fixture.snapshotSessionCountForUser(t))
			require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
		})
	}
}

func TestSnapshotSessions_CancellationAtBoundedStagesRollsBackEarlierPages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		install func(*snapshotTestHooks)
	}{
		{name: "row read", install: func(h *snapshotTestHooks) {
			calls := 0
			h.afterSnapshotRowRead = func(context.Context) error {
				calls++
				if calls == 2 {
					return context.Canceled
				}
				return nil
			}
		}},
		{name: "canonicalization", install: func(h *snapshotTestHooks) {
			calls := 0
			h.beforeSnapshotCanonicalize = func(context.Context) error {
				calls++
				if calls == 2 {
					return context.Canceled
				}
				return nil
			}
		}},
		{name: "batch copy", install: func(h *snapshotTestHooks) {
			calls := 0
			h.beforeSnapshotCopy = func(context.Context) error {
				calls++
				if calls == 2 {
					return context.Canceled
				}
				return nil
			}
		}},
		{name: "final metadata", install: func(h *snapshotTestHooks) {
			h.beforeSnapshotFinalize = func(context.Context) error { return context.Canceled }
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scenario := "cancel_" + strings.ReplaceAll(tc.name, " ", "_")
			if tc.name == "canonicalization" {
				scenario = "cancel_canonical"
			}
			fixture := newSnapshotSessionFixture(t, scenario, snapshotSessionFixtureOptions{snapshotMaterializationBatchRows: 1})
			fixture.pushUser(t, 1, uuid.New(), "Alpha")
			fixture.pushUser(t, 2, uuid.New(), "Bravo")
			hooks := &snapshotTestHooks{}
			tc.install(hooks)
			fixture.svc.snapshotHooks = hooks
			_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, fixture.snapshotSessionCountForUser(t))
			require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
		})
	}
}

func TestSnapshotSessions_CancelledRotationLeavesPreviousSourceActive(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "cancel_rotation", snapshotSessionFixtureOptions{})
	oldSource := "writer-old"
	oldActor := fixture.actor(oldSource)
	fixture.pushUserAs(t, oldActor, 1, uuid.New(), "Alpha")
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotFence: func(context.Context) error { return context.Canceled }}
	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, testSchemaGenerationProofForService(fixture.svc), &SnapshotSessionCreateRequest{SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldSource, NewSourceID: "writer-new", Reason: "history_pruned"}})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, fixture.snapshotSessionCountForUser(t))
	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)
	var state, replaced string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT state,replaced_by_source_id FROM sync.source_state WHERE user_pk=$1 AND source_id=$2`, userPK, oldSource).Scan(&state, &replaced))
	require.Equal(t, sourceStateActive, state)
	require.Empty(t, replaced)
	var replacementCount int
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM sync.source_state WHERE user_pk=$1 AND source_id='writer-new'`, userPK).Scan(&replacementCount))
	require.Zero(t, replacementCount)
}

func TestSnapshotSessions_CreateSucceedsUnderConfiguredLimits(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "under_cap", snapshotSessionFixtureOptions{
		maxRowsPerSnapshotSession:  10,
		maxBytesPerSnapshotSession: 1 << 20,
	})
	rowID := uuid.New()

	session := fixture.createSessionWithUser(t, rowID, "Alpha")
	require.Equal(t, int64(1), session.RowCount)
	require.Greater(t, session.ByteCount, int64(0))
}

func TestSnapshotSessions_ExactWireByteCountsAndByteBoundedChunks(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "wire_bytes", snapshotSessionFixtureOptions{})
	fixture.pushUser(t, 1, uuid.New(), "Alpha")
	fixture.pushUser(t, 2, uuid.New(), "Bravo")
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)

	rows, err := fixture.pool.Query(fixture.ctx, `
		SELECT wire_byte_count FROM sync.snapshot_session_rows
		WHERE snapshot_id=$1::uuid ORDER BY row_ordinal
	`, session.SnapshotID)
	require.NoError(t, err)
	var wireCounts []int64
	for rows.Next() {
		var count int64
		require.NoError(t, rows.Scan(&count))
		wireCounts = append(wireCounts, count)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Len(t, wireCounts, 2)
	require.Equal(t, wireCounts[0]+wireCounts[1], session.ByteCount)

	proof := testSchemaGenerationProofForService(fixture.svc)
	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 0, 10, 1<<20)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 2)
	require.False(t, chunk.HasMore)
	require.Equal(t, wireCounts[0]+wireCounts[1], chunk.ByteCount)
	contract, ok := fixture.svc.schemaGenerationRegistry.Generation(proof.ID)
	require.True(t, ok)
	groups, logicalBytes, err := CompactGroupsForSnapshotRowsAt(generationDescriptor(contract), chunk.Rows[:1], 1)
	require.NoError(t, err)
	require.Equal(t, wireCounts[0], logicalBytes)
	encoded, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: 1, LogicalByteCount: logicalBytes, HasMore: true})
	require.NoError(t, err)
	require.Greater(t, int64(len(encoded)), wireCounts[0])

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 0, 10, int64(len(encoded))-1)
	var tooSmall *SnapshotChunkTooSmallError
	require.ErrorAs(t, err, &tooSmall)
	require.Equal(t, int64(len(encoded)), tooSmall.RequiredDecodedBodyBytes)

	finalGroups, finalLogicalBytes, err := CompactGroupsForSnapshotRowsAt(generationDescriptor(contract), chunk.Rows[1:], 2)
	require.NoError(t, err)
	finalEncoded, err := json.Marshal(CompactSnapshotChunkResponse{Rows: finalGroups, NextRowOrdinal: 2, LogicalByteCount: finalLogicalBytes, HasMore: false})
	require.NoError(t, err)
	finalChunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 1, 10, int64(len(finalEncoded)))
	require.NoError(t, err)
	require.Len(t, finalChunk.Rows, 1)
	require.False(t, finalChunk.HasMore)
	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 1, 10, int64(len(finalEncoded))-1)
	require.ErrorAs(t, err, &tooSmall)
	require.Equal(t, int64(len(finalEncoded)), tooSmall.RequiredDecodedBodyBytes)
	metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	require.Equal(t, wireCounts[0]+wireCounts[1], metrics.ChunkBytesHighWater)
	require.Equal(t, wireCounts[0]+wireCounts[1], metrics.ChunkRetainedBytesHighWater)
}

func TestSnapshotConfig_DefaultsRelationshipsAndCapacityAreFailClosed(t *testing.T) {
	t.Parallel()
	service, err := newTestRuntimeService(nil, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(runtimeServiceTestRegisteredTables()), RegisteredTables: runtimeServiceTestRegisteredTables(), RegisteredTableCatalogRevision: 1}, nil)
	require.NoError(t, err)
	require.Equal(t, defaultSnapshotMaterializationBatchRows, service.config.SnapshotMaterializationBatchRows)
	require.Equal(t, defaultSnapshotMaterializationBatchBytes, service.config.SnapshotMaterializationBatchBytes)
	require.Equal(t, defaultMaxConcurrentSnapshotBuilds, cap(service.snapshotBuildPermits))
	require.Equal(t, defaultMaxConcurrentSnapshotChunkRequests, cap(service.snapshotChunkPermits))

	invalid := []ServiceConfig{
		{SnapshotMaterializationBatchBytes: 1, MaxBytesPerSnapshotRow: 2},
		{DefaultBytesPerSnapshotChunk: 2, MaxBytesPerSnapshotChunk: 1, MaxBytesPerSnapshotRow: 1, SnapshotMaterializationBatchBytes: 2},
		{MaxBytesPerSnapshotRow: 2, MaxBytesPerSnapshotChunk: 1, SnapshotMaterializationBatchBytes: 2},
		{MaxBytesPerBundle: 4096, MaxBytesPerPullResponse: 8192, MaxBytesPerSnapshotRow: 4096, MaxBytesPerSnapshotChunk: 4096, SnapshotMaterializationBatchBytes: 8192},
		{MaxBytesPerBundle: 4096, MaxBytesPerPullResponse: 4096, MaxBytesPerSnapshotRow: 4096, MaxBytesPerSnapshotChunk: 8192, SnapshotMaterializationBatchBytes: 8192},
		{MaxBytesPerBundle: 4095, MaxBytesPerPullResponse: 8192, MaxBytesPerSnapshotRow: 4096, MaxBytesPerSnapshotChunk: 8192, SnapshotMaterializationBatchBytes: 8192},
		{MaxConcurrentSnapshotBuilds: -1},
		{MaxRowsPerSnapshotSession: -1},
	}
	for i := range invalid {
		invalid[i].RegisteredTables = runtimeServiceTestRegisteredTables()
		invalid[i].RegisteredTableCatalogRevision = 1
		_, err := newTestRuntimeService(nil, &invalid[i], nil)
		require.Error(t, err, "case %d", i)
	}

	for range cap(service.snapshotBuildPermits) {
		service.snapshotBuildPermits <- struct{}{}
	}
	require.False(t, service.tryAcquireSnapshotBuild(), "a full build semaphore must fail closed")
	for range cap(service.snapshotChunkPermits) {
		service.snapshotChunkPermits <- struct{}{}
	}
	require.False(t, service.tryAcquireSnapshotChunk(), "a full chunk semaphore must fail closed")
}

func TestSnapshotCapacity_ConcurrentLimitsAndCancellationReleasePermits(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "concurrent_capacity", snapshotSessionFixtureOptions{
		maxConcurrentSnapshotBuilds:        2,
		maxConcurrentSnapshotChunkRequests: 4,
	})
	service := fixture.svc
	actor := fixture.reader
	session, err := service.CreateSnapshotSession(fixture.ctx, actor, testSchemaGenerationProofForService(service))
	require.NoError(t, err)
	buildStarted := make(chan struct{}, 2)
	chunkStarted := make(chan struct{}, 4)
	service.snapshotHooks = &snapshotTestHooks{
		afterBuildPermit: func(ctx context.Context) error { buildStarted <- struct{}{}; <-ctx.Done(); return ctx.Err() },
		afterChunkPermit: func(ctx context.Context) error { chunkStarted <- struct{}{}; <-ctx.Done(); return ctx.Err() },
	}

	buildCancel := make([]context.CancelFunc, 2)
	buildDone := make(chan error, 2)
	for i := 0; i < 2; i++ {
		var ctx context.Context
		ctx, buildCancel[i] = context.WithCancel(context.Background())
		go func(ctx context.Context) {
			_, err := service.CreateSnapshotSession(ctx, actor, testSchemaGenerationProofForService(service))
			buildDone <- err
		}(ctx)
	}
	for i := 0; i < 2; i++ {
		<-buildStarted
	}
	_, err = service.CreateSnapshotSession(context.Background(), actor, testSchemaGenerationProofForService(service))
	var capacity *SnapshotCapacityError
	require.ErrorAs(t, err, &capacity)
	for _, cancel := range buildCancel {
		cancel()
	}
	for i := 0; i < 2; i++ {
		require.ErrorIs(t, <-buildDone, context.Canceled)
	}
	require.Equal(t, int64(0), service.snapshotRuntimeMetricsSnapshot().ActiveBuilds)
	require.Equal(t, int64(2), service.snapshotRuntimeMetricsSnapshot().BuildHighWater)

	chunkCancel := make([]context.CancelFunc, 4)
	chunkDone := make(chan error, 4)
	for i := 0; i < 4; i++ {
		var ctx context.Context
		ctx, chunkCancel[i] = context.WithCancel(context.Background())
		go func(ctx context.Context) {
			_, err := service.GetSnapshotChunk(ctx, actor, testSchemaGenerationProofForService(service), session.SnapshotID, 0, 1, 1)
			chunkDone <- err
		}(ctx)
	}
	for i := 0; i < 4; i++ {
		<-chunkStarted
	}
	_, err = service.GetSnapshotChunk(context.Background(), actor, testSchemaGenerationProofForService(service), session.SnapshotID, 0, 1, 1)
	require.ErrorAs(t, err, &capacity)
	for _, cancel := range chunkCancel {
		cancel()
	}
	for i := 0; i < 4; i++ {
		require.ErrorIs(t, <-chunkDone, context.Canceled)
	}
	require.Equal(t, int64(0), service.snapshotRuntimeMetricsSnapshot().ActiveChunks)
	require.Equal(t, int64(4), service.snapshotRuntimeMetricsSnapshot().ChunkHighWater)
}

func TestSnapshotChunk_CancellationReleasesPermitAndRetryPreservesOrdinals(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "chunk_capacity_retry", snapshotSessionFixtureOptions{})
	for i, name := range []string{"Alpha", "Bravo", "Charlie"} {
		fixture.pushUser(t, int64(i+1), uuid.New(), name)
	}
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)

	started := make(chan struct{})
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterChunkPermit: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := fixture.svc.GetSnapshotChunk(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 1, defaultBytesPerSnapshotChunk)
		done <- err
	}()
	<-started
	for i := 0; i < defaultMaxConcurrentSnapshotChunkRequests-1; i++ {
		fixture.svc.snapshotChunkPermits <- struct{}{}
	}
	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 1, defaultBytesPerSnapshotChunk)
	var capacity *SnapshotCapacityError
	require.ErrorAs(t, err, &capacity)
	for i := 0; i < defaultMaxConcurrentSnapshotChunkRequests-1; i++ {
		<-fixture.svc.snapshotChunkPermits
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, int64(0), fixture.svc.snapshotRuntimeMetricsSnapshot().ActiveChunks)

	fixture.svc.snapshotHooks = nil
	var ordinals []int64
	for after := int64(0); ; {
		chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, after, 1, defaultBytesPerSnapshotChunk)
		require.NoError(t, err)
		for range chunk.Rows {
			after++
			ordinals = append(ordinals, after)
		}
		if !chunk.HasMore {
			break
		}
	}
	require.Equal(t, []int64{1, 2, 3}, ordinals)
}

func TestSnapshotHTTP_ByteAndCapacityErrorsAreStructured(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "http_errors", snapshotSessionFixtureOptions{})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))

	request := httptest.NewRequest(http.MethodGet, "/sync/snapshot-sessions/"+session.SnapshotID+"?max_rows=10&max_bytes=1", nil)
	setTestSchemaGenerationHeaders(request, fixture.svc)
	request.SetPathValue("snapshot_id", session.SnapshotID)
	request = request.WithContext(ContextWithActor(request.Context(), fixture.reader))
	recorder := httptest.NewRecorder()
	handlers.HandleGetSnapshotChunk(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var response ErrorResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "snapshot_chunk_too_small", response.Error)
	require.Greater(t, response.RequiredDecodedBodyBytes, int64(1))

	for i := 0; i < cap(fixture.svc.snapshotBuildPermits); i++ {
		fixture.svc.snapshotBuildPermits <- struct{}{}
	}
	request = httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", nil)
	setTestSchemaGenerationHeaders(request, fixture.svc)
	request = request.WithContext(ContextWithActor(request.Context(), fixture.reader))
	recorder = httptest.NewRecorder()
	handlers.HandleCreateSnapshotSession(recorder, request)
	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Equal(t, "1", recorder.Header().Get("Retry-After"))
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "snapshot_build_capacity", response.Error)
	for i := 0; i < cap(fixture.svc.snapshotBuildPermits); i++ {
		<-fixture.svc.snapshotBuildPermits
	}

	for i := 0; i < cap(fixture.svc.snapshotChunkPermits); i++ {
		fixture.svc.snapshotChunkPermits <- struct{}{}
	}
	request = httptest.NewRequest(http.MethodGet, "/sync/snapshot-sessions/"+session.SnapshotID, nil)
	setTestSchemaGenerationHeaders(request, fixture.svc)
	request.SetPathValue("snapshot_id", session.SnapshotID)
	request = request.WithContext(ContextWithActor(request.Context(), fixture.reader))
	recorder = httptest.NewRecorder()
	handlers.HandleGetSnapshotChunk(recorder, request)
	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Equal(t, "1", recorder.Header().Get("Retry-After"))
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "snapshot_chunk_capacity", response.Error)
	for i := 0; i < cap(fixture.svc.snapshotChunkPermits); i++ {
		<-fixture.svc.snapshotChunkPermits
	}
}

func TestSnapshotSessions_CleanupExpiredSessionsRemovesRows(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "cleanup", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	fixture.svc.stopSnapshotCleanupWorker()
	require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_sessions
		SET expires_at = now() - interval '1 second'
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)
	result, err := fixture.svc.cleanupSnapshotBatch(fixture.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.deletedRows)
	require.Equal(t, int64(1), result.deletedSessions)

	require.Zero(t, fixture.snapshotSessionCount(t, session.SnapshotID))
	require.Zero(t, fixture.snapshotSessionRowCount(t, session.SnapshotID))
}

func TestSnapshotCleanup_BatchesAreBoundedAndPreserveUnexpiredAndAuthoritativeState(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "cleanup_bounds", snapshotSessionFixtureOptions{
		snapshotCleanupInterval: time.Hour, snapshotCleanupBatchRows: 2, snapshotCleanupBatchSessions: 2, snapshotCleanupMaxBatches: 1,
	})
	fixture.pushUser(t, 1, uuid.New(), "Alpha")
	var expired []string
	for i := 0; i < 3; i++ {
		session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.NoError(t, err)
		expired = append(expired, session.SnapshotID)
	}
	unexpired, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second' WHERE snapshot_id=ANY($1::uuid[])`, expired)
	require.NoError(t, err)
	var businessRows, rowStateRows, bundleRows int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, fmt.Sprintf(`SELECT (SELECT count(*) FROM %s.users),(SELECT count(*) FROM sync.row_state),(SELECT count(*) FROM sync.bundle_rows)`, pgx.Identifier{fixture.schemaName}.Sanitize())).Scan(&businessRows, &rowStateRows, &bundleRows))

	result, err := fixture.svc.cleanupSnapshotBatch(fixture.ctx)
	require.NoError(t, err)
	require.LessOrEqual(t, result.candidateSessions, int64(2))
	require.LessOrEqual(t, result.deletedRows, int64(2))
	require.LessOrEqual(t, result.deletedSessions, int64(2))
	require.Equal(t, 1, fixture.countRows(t, `SELECT count(*) FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid`, unexpired.SnapshotID))
	var afterBusiness, afterState, afterBundles int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, fmt.Sprintf(`SELECT (SELECT count(*) FROM %s.users),(SELECT count(*) FROM sync.row_state),(SELECT count(*) FROM sync.bundle_rows)`, pgx.Identifier{fixture.schemaName}.Sanitize())).Scan(&afterBusiness, &afterState, &afterBundles))
	require.Equal(t, []int64{businessRows, rowStateRows, bundleRows}, []int64{afterBusiness, afterState, afterBundles})
}

func TestSnapshotCleanup_PeriodicPostCreateAndPostBootstrapTriggers(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "cleanup_triggers", snapshotSessionFixtureOptions{snapshotCleanupInterval: 20 * time.Millisecond})
	first := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	_, err := fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second' WHERE snapshot_id=$1::uuid`, first.SnapshotID)
	require.NoError(t, err)
	second, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return fixture.snapshotSessionCount(t, first.SnapshotID) == 0 }, 5*time.Second, 10*time.Millisecond)
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second' WHERE snapshot_id=$1::uuid`, second.SnapshotID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return fixture.snapshotSessionCount(t, second.SnapshotID) == 0 }, 5*time.Second, 10*time.Millisecond)

	third, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second' WHERE snapshot_id=$1::uuid`, third.SnapshotID)
	require.NoError(t, err)
	require.NoError(t, fixture.svc.Close(fixture.ctx))
	restarted, err := newTestRuntimeService(fixture.pool, fixture.svc.config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	fixture.svc = restarted
	require.NoError(t, restarted.Bootstrap(fixture.ctx))
	require.Eventually(t, func() bool { return fixture.snapshotSessionCount(t, third.SnapshotID) == 0 }, 5*time.Second, 10*time.Millisecond)
}

func TestSnapshotCleanup_TwoInstancesUseSkipLocked(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "cleanup_instances", snapshotSessionFixtureOptions{snapshotCleanupInterval: time.Hour, snapshotCleanupBatchRows: 2, snapshotCleanupBatchSessions: 2})
	fixture.pushUser(t, 1, uuid.New(), "Alpha")
	for i := 0; i < 4; i++ {
		_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.NoError(t, err)
	}
	second, err := newTestRuntimeService(fixture.pool, fixture.svc.config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(fixture.ctx))
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second'`)
	require.NoError(t, err)
	results := make(chan error, 2)
	go func() { _, err := fixture.svc.cleanupSnapshotBatch(fixture.ctx); results <- err }()
	go func() { _, err := second.cleanupSnapshotBatch(fixture.ctx); results <- err }()
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	metrics1, metrics2 := fixture.svc.snapshotRuntimeMetricsSnapshot(), second.snapshotRuntimeMetricsSnapshot()
	require.LessOrEqual(t, metrics1.CleanupCandidateSessionsHighWater, int64(2))
	require.LessOrEqual(t, metrics2.CleanupCandidateSessionsHighWater, int64(2))
}

func TestSnapshotCleanup_SkippedLockedSessionIsDeletedOnRetry(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "cleanup_retry", snapshotSessionFixtureOptions{
		snapshotCleanupInterval: time.Hour,
	})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	fixture.svc.stopSnapshotCleanupWorker()
	require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
	_, err := fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second' WHERE snapshot_id=$1::uuid`, session.SnapshotID)
	require.NoError(t, err)
	tx, err := fixture.pool.Begin(fixture.ctx)
	require.NoError(t, err)
	_, err = tx.Exec(fixture.ctx, `SELECT 1 FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid FOR UPDATE`, session.SnapshotID)
	require.NoError(t, err)
	before := fixture.svc.snapshotRuntimeMetricsSnapshot()
	fixture.svc.runSnapshotCleanup(fixture.ctx, "test_locked_session")
	afterSkipped := fixture.svc.snapshotRuntimeMetricsSnapshot()
	require.Equal(t, before.CleanupRuns+1, afterSkipped.CleanupRuns)
	require.Equal(t, int64(0), afterSkipped.ActiveCleanupRuns)
	require.Equal(t, 1, fixture.snapshotSessionCount(t, session.SnapshotID))
	require.NoError(t, tx.Rollback(fixture.ctx))
	fixture.svc.runSnapshotCleanup(fixture.ctx, "test_retry")
	require.Equal(t, 0, fixture.snapshotSessionCount(t, session.SnapshotID))
	require.Equal(t, 0, fixture.snapshotSessionRowCount(t, session.SnapshotID))
}

func TestSnapshotChunk_InFlightRepeatableReadFinishesAcrossRetirement(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "chunk_retirement", snapshotSessionFixtureOptions{snapshotCleanupInterval: time.Hour})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	reached := make(chan struct{})
	release := make(chan struct{})
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterChunkSessionRead: func(context.Context) error { close(reached); <-release; return nil }}
	type result struct {
		chunk *SnapshotChunkResponse
		err   error
	}
	done := make(chan result, 1)
	go func() {
		chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
		done <- result{chunk, err}
	}()
	<-reached
	require.NoError(t, fixture.svc.DeleteSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID))
	close(release)
	inFlight := <-done
	require.NoError(t, inFlight.err)
	require.Len(t, inFlight.chunk.Rows, 1)
	fixture.svc.snapshotHooks = nil
	_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	var expired *SnapshotSessionExpiredError
	var missing *SnapshotSessionNotFoundError
	require.True(t, errors.As(err, &expired) || errors.As(err, &missing), "later read must observe retirement: %v", err)
}

func TestSnapshotSessions_CreateQueryPlanUsesRowStateSnapshotIndex(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "plan", snapshotSessionFixtureOptions{})
	for i := 0; i < 8; i++ {
		fixture.pushUser(t, int64(i+1), uuid.New(), fmt.Sprintf("User%d", i))
	}

	var planLines []string
	err := pgx.BeginTxFunc(fixture.ctx, fixture.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(fixture.ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			return err
		}
		if _, err := tx.Exec(fixture.ctx, `SET LOCAL enable_hashjoin = off`); err != nil {
			return err
		}
		if _, err := tx.Exec(fixture.ctx, `SET LOCAL enable_mergejoin = off`); err != nil {
			return err
		}

		rows, err := tx.Query(fixture.ctx, `
			EXPLAIN (COSTS OFF)
			SELECT table_id, key_bytes, bundle_seq
			FROM sync.row_state
			WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
			  AND deleted = FALSE
			ORDER BY table_id, key_bytes
		`, fixture.userID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return err
			}
			planLines = append(planLines, line)
		}
		return rows.Err()
	})
	require.NoError(t, err)
	require.NotEmpty(t, planLines)

	planText := strings.Join(planLines, "\n")
	require.Contains(t, planText, "rs_user_live_snapshot_idx")
	require.True(t, slices.ContainsFunc(planLines, func(line string) bool {
		return strings.Contains(line, "Index") && strings.Contains(line, "rs_user_live_snapshot_idx")
	}))
}
