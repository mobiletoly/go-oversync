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
	defaultRowsPerSnapshotChunk int
	maxRowsPerSnapshotChunk     int
	maxRowsPerSnapshotSession   int64
	maxBytesPerSnapshotSession  int64
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
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion:   1,
		AppName:                     scenarioID + "-test",
		DefaultRowsPerSnapshotChunk: opts.defaultRowsPerSnapshotChunk,
		MaxRowsPerSnapshotChunk:     opts.maxRowsPerSnapshotChunk,
		MaxRowsPerSnapshotSession:   opts.maxRowsPerSnapshotSession,
		MaxBytesPerSnapshotSession:  opts.maxBytesPerSnapshotSession,
		RegisteredTables:            []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
	}, logger)

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

	session, err := f.svc.CreateSnapshotSession(f.ctx, f.reader)
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

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	require.Equal(t, resp3.BundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(3), session.RowCount)

	chunk1, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 2)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotID, chunk1.SnapshotID)
	require.Equal(t, session.SnapshotBundleSeq, chunk1.SnapshotBundleSeq)
	require.Len(t, chunk1.Rows, 2)
	require.NotContains(t, string(chunk1.Rows[0].Payload), `"_sync_scope_id"`)
	require.True(t, chunk1.HasMore)
	require.Equal(t, int64(2), chunk1.NextRowOrdinal)

	resp4 := fixture.pushUser(t, 4, uuid.New(), "Delta")
	require.Equal(t, int64(4), resp4.BundleSeq)

	chunk2, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, chunk1.NextRowOrdinal, 2)
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

func TestSnapshotSessions_OneChunkStillUsesSessionStorage(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "one_chunk", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 1000,
		maxRowsPerSnapshotChunk:     5000,
	})
	rowID := uuid.New()

	resp := fixture.pushUser(t, 1, rowID, "Solo")
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	require.Equal(t, resp.BundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(1), session.RowCount)

	require.Equal(t, 1, fixture.snapshotSessionRowCount(t, session.SnapshotID))

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 1000)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.False(t, chunk.HasMore)
	require.Equal(t, int64(1), chunk.NextRowOrdinal)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestSnapshotSessions_NoGapsOrDuplicatesAcrossChunkFetches(t *testing.T) {
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

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
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

	deterministicChunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 2)
	require.NoError(t, err)
	require.Len(t, deterministicChunk.Rows, 2)
	require.Equal(t, snapshotRowIdentity(rows[0]), snapshotRowIdentity(deterministicChunk.Rows[0]))
	require.Equal(t, snapshotRowIdentity(rows[1]), snapshotRowIdentity(deterministicChunk.Rows[1]))
}

func TestSnapshotSessions_ActiveSessionRemainsReadableAfterHistoryPrune(t *testing.T) {
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

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
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

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Equal(t, session.SnapshotBundleSeq, chunk.SnapshotBundleSeq)
	require.Len(t, chunk.Rows, 3)
}

func TestSnapshotSessions_RepeatedSessionCreationUsesDeterministicRowOrdering(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "ordering", snapshotSessionFixtureOptions{
		defaultRowsPerSnapshotChunk: 10,
		maxRowsPerSnapshotChunk:     10,
	})

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	fixture.pushUser(t, 1, ids[2], "Zulu")
	fixture.pushUser(t, 2, ids[0], "Alpha")
	fixture.pushUser(t, 3, ids[1], "Mike")

	session1, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	rows1, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session1.SnapshotID, 10)

	session2, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	rows2, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, fixture.reader, session2.SnapshotID, 10)

	require.Len(t, rows1, len(rows2))
	for i := range rows1 {
		require.Equal(t, snapshotRowIdentity(rows1[i]), snapshotRowIdentity(rows2[i]))
		require.Equal(t, rows1[i].RowVersion, rows2[i].RowVersion)
		require.JSONEq(t, string(rows1[i].Payload), string(rows2[i].Payload))
	}
}

func TestSnapshotSessions_DeleteInvalidatesFurtherChunkFetches(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "delete", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	require.NoError(t, fixture.svc.DeleteSnapshotSession(fixture.ctx, fixture.reader, session.SnapshotID))

	require.Zero(t, fixture.snapshotSessionRowCount(t, session.SnapshotID))

	_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 10)
	var notFoundErr *SnapshotSessionNotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func TestSnapshotSessions_InvalidCursorRejected(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "invalid_cursor", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")

	_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, -1, 10)
	var invalidErr *SnapshotChunkInvalidError
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "after_row_ordinal")

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 0)
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "max_rows")
}

func TestSnapshotSessions_WrongUserRejected(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "wrong_user", snapshotSessionFixtureOptions{})
	writer := fixture.writer
	readerA := Actor{UserID: writer.UserID, SourceID: "reader-a"}
	readerB := Actor{UserID: "snapshot-wrong-user-b-" + fixture.suffix, SourceID: "reader-b"}
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, readerA)
	require.NoError(t, err)

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, readerB, session.SnapshotID, 0, 10)
	var forbiddenErr *SnapshotSessionForbiddenError
	require.ErrorAs(t, err, &forbiddenErr)

	err = fixture.svc.DeleteSnapshotSession(fixture.ctx, readerB, session.SnapshotID)
	require.ErrorAs(t, err, &forbiddenErr)
}

func TestSnapshotSessions_ExpiredSessionRejected(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "expired", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_sessions
		SET expires_at = now() - interval '1 second'
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)

	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 10)
	var expiredErr *SnapshotSessionExpiredError
	require.ErrorAs(t, err, &expiredErr)
}

func TestSnapshotSessions_ChunkPayloadPreservesCurrentAfterImage(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "after_image", snapshotSessionFixtureOptions{})
	actor := fixture.writer
	row1 := uuid.New()
	row2 := uuid.New()

	fixture.pushUserAs(t, actor, 1, row1, "Alpha")
	fixture.pushUserAs(t, actor, 2, row2, "Gamma")
	require.NoError(t, fixture.svc.WithinSyncBundle(fixture.ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 3}, func(tx pgx.Tx) error {
		_, err := tx.Exec(fixture.ctx, fmt.Sprintf(`DELETE FROM %s.users WHERE id = $1`, fixture.schemaName), row2)
		return err
	}))

	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
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
	fixture := newSnapshotSessionFixture(t, "empty", snapshotSessionFixtureOptions{})
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.reader.UserID, fixture.reader.SourceID)
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	require.Zero(t, session.RowCount)
	require.Zero(t, session.ByteCount)

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Empty(t, chunk.Rows)
	require.False(t, chunk.HasMore)
	require.Zero(t, chunk.NextRowOrdinal)
}

func TestSnapshotSessions_RotatedCreateRetiresPreviousAndReservesReplacement(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "rotate", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-old"
	newSourceID := "writer-new"
	writer := fixture.actor(oldSourceID)
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")

	session, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, &SnapshotSessionCreateRequest{
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
	fixture := newSnapshotSessionFixture(t, "rotate_empty", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-empty-old"
	newSourceID := "writer-empty-new"
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, oldSourceID)

	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, fixture.actor(oldSourceID), &SnapshotSessionCreateRequest{
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
	session1, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, req)
	require.NoError(t, err)
	session2, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, req)
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
	require.Equal(t, 2, sourceStateCount)

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

func TestSnapshotSessions_RotatedCreateRejectsConflictingReplacementTargets(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "rotate_conflict", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-conflict-old"
	firstNewSourceID := "writer-conflict-new-a"
	secondNewSourceID := "writer-conflict-new-b"
	otherOldSourceID := "writer-conflict-old-b"
	writer := fixture.actor(oldSourceID)
	otherWriter := fixture.actor(otherOldSourceID)
	fixture.pushUserAs(t, writer, 1, uuid.New(), "Alpha")
	fixture.pushUserAs(t, otherWriter, 1, uuid.New(), "Bravo")

	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      firstNewSourceID,
			Reason:           "history_pruned",
		},
	})
	require.NoError(t, err)

	_, err = fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, writer, &SnapshotSessionCreateRequest{
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

	_, err = fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, otherWriter, &SnapshotSessionCreateRequest{
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
	fixture := newSnapshotSessionFixture(t, "rotate_activate", snapshotSessionFixtureOptions{})
	oldSourceID := "writer-activate-old"
	newSourceID := "writer-activate-new"
	oldWriter := fixture.actor(oldSourceID)
	newWriter := fixture.actor(newSourceID)
	fixture.pushUserAs(t, oldWriter, 1, uuid.New(), "Alpha")

	_, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldWriter, &SnapshotSessionCreateRequest{
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
	fixture := newSnapshotSessionFixture(t, "no_update", snapshotSessionFixtureOptions{})
	rowID := uuid.New()

	session := fixture.createSessionWithUser(t, rowID, "Alpha")

	_, err := fixture.pool.Exec(fixture.ctx, `
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
	_, err = fixture.pool.Exec(fixture.ctx, `
		CREATE TRIGGER reject_snapshot_session_updates
		BEFORE UPDATE ON sync.snapshot_sessions
		FOR EACH ROW
		EXECUTE FUNCTION pg_temp.reject_snapshot_session_updates()
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = fixture.pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_snapshot_session_updates ON sync.snapshot_sessions`)
	})

	chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestSnapshotSessions_CreateRejectsConfiguredLimitsAndRollsBack(t *testing.T) {
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
			dimension:         "byte_count",
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

			_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
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

func TestSnapshotSessions_CreateSucceedsUnderConfiguredLimits(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "under_cap", snapshotSessionFixtureOptions{
		maxRowsPerSnapshotSession:  10,
		maxBytesPerSnapshotSession: 1 << 20,
	})
	rowID := uuid.New()

	session := fixture.createSessionWithUser(t, rowID, "Alpha")
	require.Equal(t, int64(1), session.RowCount)
	require.Greater(t, session.ByteCount, int64(0))
}

func TestSnapshotSessions_CleanupExpiredSessionsRemovesRows(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "cleanup", snapshotSessionFixtureOptions{})

	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_sessions
		SET expires_at = now() - interval '1 second'
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)
	require.NoError(t, cleanupExpiredSnapshotSessionsQuerier(fixture.ctx, fixture.pool))

	require.Zero(t, fixture.snapshotSessionCount(t, session.SnapshotID))
	require.Zero(t, fixture.snapshotSessionRowCount(t, session.SnapshotID))
}

func TestSnapshotSessions_CreateQueryPlanUsesRowStateSnapshotIndex(t *testing.T) {
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
