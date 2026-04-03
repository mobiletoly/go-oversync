package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
		chunk, err := svc.GetSnapshotChunk(ctx, actor, snapshotID, afterRowOrdinal, maxRows)
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

func TestSnapshotSessions_CreateAndFetchChunksAtFrozenBundleSeq(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_chunks_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:   1,
		AppName:                     "snapshot-chunks-test",
		DefaultRowsPerSnapshotChunk: 2,
		MaxRowsPerSnapshotChunk:     2,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-chunks-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	row1 := uuid.New()
	row2 := uuid.New()
	row3 := uuid.New()

	resp1 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "Alpha")
	resp2 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Bravo")
	resp3 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 3, row3, "Charlie")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, resp3.BundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(3), session.RowCount)

	chunk1, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 2)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotID, chunk1.SnapshotID)
	require.Equal(t, session.SnapshotBundleSeq, chunk1.SnapshotBundleSeq)
	require.Len(t, chunk1.Rows, 2)
	require.NotContains(t, string(chunk1.Rows[0].Payload), `"_sync_scope_id"`)
	require.True(t, chunk1.HasMore)
	require.Equal(t, int64(2), chunk1.NextRowOrdinal)

	resp4 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 4, uuid.New(), "Delta")
	require.Equal(t, int64(4), resp4.BundleSeq)

	chunk2, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, chunk1.NextRowOrdinal, 2)
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

	var storedRowCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID).Scan(&storedRowCount))
	require.Equal(t, 3, storedRowCount)

	require.Equal(t, int64(1), resp1.BundleSeq)
	require.Equal(t, int64(2), resp2.BundleSeq)
}

func TestSnapshotSessions_OneChunkStillUsesSessionStorage(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_one_chunk_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:   1,
		AppName:                     "snapshot-one-chunk-test",
		DefaultRowsPerSnapshotChunk: 1000,
		MaxRowsPerSnapshotChunk:     5000,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-one-chunk-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	rowID := uuid.New()

	resp := mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, rowID, "Solo")
	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, resp.BundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(1), session.RowCount)

	var storedRowCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID).Scan(&storedRowCount))
	require.Equal(t, 1, storedRowCount)

	chunk, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 1000)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.False(t, chunk.HasMore)
	require.Equal(t, int64(1), chunk.NextRowOrdinal)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestSnapshotSessions_NoGapsOrDuplicatesAcrossChunkFetches(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_gapless_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:   1,
		AppName:                     "snapshot-gapless-test",
		DefaultRowsPerSnapshotChunk: 2,
		MaxRowsPerSnapshotChunk:     2,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-gapless-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	insertedIDs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		rowID := uuid.New()
		insertedIDs = append(insertedIDs, rowID.String())
		mustPushUserBundle(t, ctx, svc, writer, schemaName, int64(i+1), rowID, fmt.Sprintf("User%d", i))
	}

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	rows, stableBundleSeq := collectSnapshotChunkRows(t, ctx, svc, reader, session.SnapshotID, 2)
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

	deterministicChunk, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 2)
	require.NoError(t, err)
	require.Len(t, deterministicChunk.Rows, 2)
	require.Equal(t, snapshotRowIdentity(rows[0]), snapshotRowIdentity(deterministicChunk.Rows[0]))
	require.Equal(t, snapshotRowIdentity(rows[1]), snapshotRowIdentity(deterministicChunk.Rows[1]))
}

func TestSnapshotSessions_ActiveSessionRemainsReadableAfterHistoryPrune(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_prune_session_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:   1,
		AppName:                     "snapshot-prune-session-test",
		DefaultRowsPerSnapshotChunk: 10,
		MaxRowsPerSnapshotChunk:     10,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-prune-session-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}

	row1 := uuid.New()
	row2 := uuid.New()
	row3 := uuid.New()
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "One")
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Two")
	bundle3 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 3, row3, "Three")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, bundle3.BundleSeq, session.SnapshotBundleSeq)

	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)

	var sourceStateCountBefore int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.source_state WHERE user_pk = $1`, userPK).Scan(&sourceStateCountBefore))
	require.Greater(t, sourceStateCountBefore, 0)

	_, err = pool.Exec(ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = $2
		WHERE user_pk = $1
	`, userPK, session.SnapshotBundleSeq)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		DELETE FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq <= $2
	`, userPK, session.SnapshotBundleSeq)
	require.NoError(t, err)

	var sourceStateCountAfter int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.source_state WHERE user_pk = $1`, userPK).Scan(&sourceStateCountAfter))
	require.Equal(t, sourceStateCountBefore, sourceStateCountAfter)

	chunk, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotBundleSeq, chunk.SnapshotBundleSeq)
	require.Len(t, chunk.Rows, 3)
}

func TestSnapshotSessions_RepeatedSessionCreationUsesDeterministicRowOrdering(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_ordering_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:   1,
		AppName:                     "snapshot-ordering-test",
		DefaultRowsPerSnapshotChunk: 10,
		MaxRowsPerSnapshotChunk:     10,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-ordering-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, ids[2], "Zulu")
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, ids[0], "Alpha")
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 3, ids[1], "Mike")

	session1, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	rows1, _ := collectSnapshotChunkRows(t, ctx, svc, reader, session1.SnapshotID, 10)

	session2, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	rows2, _ := collectSnapshotChunkRows(t, ctx, svc, reader, session2.SnapshotID, 10)

	require.Len(t, rows1, len(rows2))
	for i := range rows1 {
		require.Equal(t, snapshotRowIdentity(rows1[i]), snapshotRowIdentity(rows2[i]))
		require.Equal(t, rows1[i].RowVersion, rows2[i].RowVersion)
		require.JSONEq(t, string(rows1[i].Payload), string(rows2[i].Payload))
	}
}

func TestSnapshotSessions_DeleteInvalidatesFurtherChunkFetches(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_delete_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-delete-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-delete-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.NoError(t, svc.DeleteSnapshotSession(ctx, reader, session.SnapshotID))

	var storedRowCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID).Scan(&storedRowCount))
	require.Zero(t, storedRowCount)

	_, err = svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 10)
	var notFoundErr *SnapshotSessionNotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func TestSnapshotSessions_InvalidCursorRejected(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_invalid_cursor_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-invalid-cursor-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-invalid-cursor-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)

	_, err = svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, -1, 10)
	var invalidErr *SnapshotChunkInvalidError
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "after_row_ordinal")

	_, err = svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 0)
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "max_rows")
}

func TestSnapshotSessions_WrongUserRejected(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_wrong_user_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-wrong-user-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	writer := Actor{UserID: "snapshot-wrong-user-a-" + suffix, SourceID: "writer"}
	readerA := Actor{UserID: writer.UserID, SourceID: "reader-a"}
	readerB := Actor{UserID: "snapshot-wrong-user-b-" + suffix, SourceID: "reader-b"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, readerA)
	require.NoError(t, err)

	_, err = svc.GetSnapshotChunk(ctx, readerB, session.SnapshotID, 0, 10)
	var forbiddenErr *SnapshotSessionForbiddenError
	require.ErrorAs(t, err, &forbiddenErr)

	err = svc.DeleteSnapshotSession(ctx, readerB, session.SnapshotID)
	require.ErrorAs(t, err, &forbiddenErr)
}

func TestSnapshotSessions_ExpiredSessionRejected(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_expired_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-expired-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-expired-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE sync.snapshot_sessions
		SET expires_at = now() - interval '1 second'
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)

	_, err = svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 10)
	var expiredErr *SnapshotSessionExpiredError
	require.ErrorAs(t, err, &expiredErr)
}

func TestSnapshotSessions_ChunkPayloadPreservesCurrentAfterImage(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_after_image_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-after-image-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-after-image-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	row1 := uuid.New()
	row2 := uuid.New()

	mustPushUserBundle(t, ctx, svc, actor, schemaName, 1, row1, "Alpha")
	mustPushUserBundle(t, ctx, svc, actor, schemaName, 2, row2, "Gamma")
	require.NoError(t, svc.WithinSyncBundle(ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 3}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.users WHERE id = $1`, schemaName), row2)
		return err
	}))

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	rows, stableBundleSeq := collectSnapshotChunkRows(t, ctx, svc, reader, session.SnapshotID, 10)
	require.Equal(t, int64(3), stableBundleSeq)
	require.Len(t, rows, 1)
	require.Equal(t, schemaName, rows[0].Schema)
	require.Equal(t, "users", rows[0].Table)
	require.Equal(t, int64(1), rows[0].RowVersion)
	require.Equal(t, row1.String(), rows[0].Key["id"])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rows[0].Payload, &payload))
	require.Equal(t, "Alpha", payload["name"])
}

func TestSnapshotSessions_CreateEmptySnapshot(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_empty_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-empty-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	reader := Actor{UserID: "snapshot-empty-user-" + suffix, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)
	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.Zero(t, session.RowCount)
	require.Zero(t, session.ByteCount)

	chunk, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Empty(t, chunk.Rows)
	require.False(t, chunk.HasMore)
	require.Zero(t, chunk.NextRowOrdinal)
}

func TestSnapshotSessions_RotatedCreateRetiresPreviousAndReservesReplacement(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_rotate_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-rotate-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-rotate-user-" + suffix
	oldSourceID := "writer-old"
	newSourceID := "writer-new"
	writer := Actor{UserID: userID, SourceID: oldSourceID}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	session, err := svc.CreateSnapshotSessionWithRequest(ctx, writer, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, session.SnapshotID)

	userPK, err := lookupUserPK(ctx, pool, userID)
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
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, oldSourceID).Scan(&oldState, &oldMaxCommittedBundleID, &oldReplacedBySourceID, &oldRetirementReason))
	require.Equal(t, sourceStateRetired, oldState)
	require.Equal(t, int64(1), oldMaxCommittedBundleID)
	require.Equal(t, newSourceID, oldReplacedBySourceID)
	require.Equal(t, "history_pruned", oldRetirementReason)

	require.NoError(t, pool.QueryRow(ctx, `
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
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_rotate_empty_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-rotate-empty-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-rotate-empty-user-" + suffix
	oldSourceID := "writer-empty-old"
	newSourceID := "writer-empty-new"
	mustInitializeEmptyScope(t, ctx, svc, userID, oldSourceID)

	_, err := svc.CreateSnapshotSessionWithRequest(ctx, Actor{UserID: userID, SourceID: oldSourceID}, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "source_sequence_changed",
		},
	})
	require.NoError(t, err)

	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)

	var oldState string
	var oldMaxCommittedBundleID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, oldSourceID).Scan(&oldState, &oldMaxCommittedBundleID))
	require.Equal(t, sourceStateRetired, oldState)
	require.Zero(t, oldMaxCommittedBundleID)

	var newState string
	var newMaxCommittedBundleID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&newState, &newMaxCommittedBundleID))
	require.Equal(t, sourceStateReserved, newState)
	require.Zero(t, newMaxCommittedBundleID)
}

func TestSnapshotSessions_RepeatedEquivalentRotationIsIdempotentForSourceState(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_rotate_repeat_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-rotate-repeat-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-rotate-repeat-user-" + suffix
	oldSourceID := "writer-repeat-old"
	newSourceID := "writer-repeat-new"
	writer := Actor{UserID: userID, SourceID: oldSourceID}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	req := &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	}
	session1, err := svc.CreateSnapshotSessionWithRequest(ctx, writer, req)
	require.NoError(t, err)
	session2, err := svc.CreateSnapshotSessionWithRequest(ctx, writer, req)
	require.NoError(t, err)
	require.NotEqual(t, session1.SnapshotID, session2.SnapshotID)

	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)

	var sourceStateCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.source_state
		WHERE user_pk = $1
	`, userPK).Scan(&sourceStateCount))
	require.Equal(t, 2, sourceStateCount)

	var oldState, oldReplacedBySourceID string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, replaced_by_source_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, oldSourceID).Scan(&oldState, &oldReplacedBySourceID))
	require.Equal(t, sourceStateRetired, oldState)
	require.Equal(t, newSourceID, oldReplacedBySourceID)

	var newState string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&newState))
	require.Equal(t, sourceStateReserved, newState)
}

func TestSnapshotSessions_RotatedCreateRejectsConflictingReplacementTargets(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_rotate_conflict_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-rotate-conflict-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-rotate-conflict-user-" + suffix
	oldSourceID := "writer-conflict-old"
	firstNewSourceID := "writer-conflict-new-a"
	secondNewSourceID := "writer-conflict-new-b"
	otherOldSourceID := "writer-conflict-old-b"
	writer := Actor{UserID: userID, SourceID: oldSourceID}
	otherWriter := Actor{UserID: userID, SourceID: otherOldSourceID}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")
	mustPushUserBundle(t, ctx, svc, otherWriter, schemaName, 1, uuid.New(), "Bravo")

	_, err := svc.CreateSnapshotSessionWithRequest(ctx, writer, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      firstNewSourceID,
			Reason:           "history_pruned",
		},
	})
	require.NoError(t, err)

	_, err = svc.CreateSnapshotSessionWithRequest(ctx, writer, &SnapshotSessionCreateRequest{
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

	_, err = svc.CreateSnapshotSessionWithRequest(ctx, otherWriter, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: otherOldSourceID,
			NewSourceID:      firstNewSourceID,
			Reason:           "history_pruned",
		},
	})
	var replacementErr *SourceReplacementInvalidError
	require.ErrorAs(t, err, &replacementErr)
	require.Contains(t, replacementErr.Error(), firstNewSourceID)
}

func TestSnapshotSessions_FirstCommitUnderReservedReplacementActivatesSource(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_rotate_activate_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-rotate-activate-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-rotate-activate-user-" + suffix
	oldSourceID := "writer-activate-old"
	newSourceID := "writer-activate-new"
	oldWriter := Actor{UserID: userID, SourceID: oldSourceID}
	newWriter := Actor{UserID: userID, SourceID: newSourceID}
	mustPushUserBundle(t, ctx, svc, oldWriter, schemaName, 1, uuid.New(), "Alpha")

	_, err := svc.CreateSnapshotSessionWithRequest(ctx, oldWriter, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	})
	require.NoError(t, err)

	mustPushUserBundle(t, ctx, svc, newWriter, schemaName, 1, uuid.New(), "Bravo")

	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)

	var state string
	var maxCommittedSourceBundleID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, newSourceID).Scan(&state, &maxCommittedSourceBundleID))
	require.Equal(t, sourceStateActive, state)
	require.Equal(t, int64(1), maxCommittedSourceBundleID)
}

func TestSnapshotSessions_GetChunkDoesNotIssueSnapshotSessionUpdate(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_no_update_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-no-update-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-no-update-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	rowID := uuid.New()
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, rowID, "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
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
	_, err = pool.Exec(ctx, `
		CREATE TRIGGER reject_snapshot_session_updates
		BEFORE UPDATE ON sync.snapshot_sessions
		FOR EACH ROW
		EXECUTE FUNCTION pg_temp.reject_snapshot_session_updates()
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_snapshot_session_updates ON sync.snapshot_sessions`)
	})

	chunk, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestSnapshotSessions_CreateRejectsRowLimitAndRollsBack(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_row_cap_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:  1,
		AppName:                    "snapshot-row-cap-test",
		MaxRowsPerSnapshotSession:  1,
		MaxBytesPerSnapshotSession: 0,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-row-cap-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, uuid.New(), "Bravo")

	_, err := svc.CreateSnapshotSession(ctx, reader)
	var limitErr *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "row_count", limitErr.Dimension)
	require.Equal(t, int64(2), limitErr.Actual)
	require.Equal(t, int64(1), limitErr.Limit)

	var sessionCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_sessions
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&sessionCount))
	require.Zero(t, sessionCount)

	var rowCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows ssr
		JOIN sync.snapshot_sessions ss ON ss.snapshot_id = ssr.snapshot_id
		WHERE ss.user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&rowCount))
	require.Zero(t, rowCount)
}

func TestSnapshotSessions_CreateRejectsByteLimitAndRollsBack(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_byte_cap_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:  1,
		AppName:                    "snapshot-byte-cap-test",
		MaxRowsPerSnapshotSession:  0,
		MaxBytesPerSnapshotSession: 1,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-byte-cap-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	_, err := svc.CreateSnapshotSession(ctx, reader)
	var limitErr *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "byte_count", limitErr.Dimension)
	require.Greater(t, limitErr.Actual, int64(1))
	require.Equal(t, int64(1), limitErr.Limit)

	var sessionCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_sessions
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&sessionCount))
	require.Zero(t, sessionCount)
}

func TestSnapshotSessions_CreateSucceedsUnderConfiguredLimits(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_under_cap_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:  1,
		AppName:                    "snapshot-under-cap-test",
		MaxRowsPerSnapshotSession:  10,
		MaxBytesPerSnapshotSession: 1 << 20,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-under-cap-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	rowID := uuid.New()
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, rowID, "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, int64(1), session.RowCount)
	require.Greater(t, session.ByteCount, int64(0))
}

func TestSnapshotSessions_CleanupExpiredSessionsRemovesRows(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_cleanup_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-cleanup-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-cleanup-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, uuid.New(), "Alpha")

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE sync.snapshot_sessions
		SET expires_at = now() - interval '1 second'
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)
	require.NoError(t, cleanupExpiredSnapshotSessionsQuerier(ctx, pool))

	var sessionCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_sessions
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID).Scan(&sessionCount))
	require.Zero(t, sessionCount)

	var rowCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.snapshot_session_rows
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID).Scan(&rowCount))
	require.Zero(t, rowCount)
}

func TestSnapshotSessions_CreateQueryPlanUsesRowStateSnapshotIndex(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_plan_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-plan-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-plan-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	for i := 0; i < 8; i++ {
		mustPushUserBundle(t, ctx, svc, writer, schemaName, int64(i+1), uuid.New(), fmt.Sprintf("User%d", i))
	}

	var planLines []string
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SET LOCAL enable_hashjoin = off`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SET LOCAL enable_mergejoin = off`); err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			EXPLAIN (COSTS OFF)
			SELECT table_id, key_bytes, bundle_seq
			FROM sync.row_state
			WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
			  AND deleted = FALSE
			ORDER BY table_id, key_bytes
		`, userID)
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
