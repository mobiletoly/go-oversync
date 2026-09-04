package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// directPushFixture owns one initialized scope. Its helpers deliberately form
// complete one-shot requests; they do not model upload continuation or any
// persisted client-side staging state.
type directPushFixture struct {
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	writer     Actor
	reader     Actor
}

type directPushFixtureOptions struct {
	maxRowsPerBundle        int
	maxBytesPerBundle       int
	maxBytesPerPullResponse int64
	maxBytesPerSnapshotRow  int64
}

func newDirectPushFixture(t *testing.T, ctx context.Context, opts directPushFixtureOptions) *directPushFixture {
	t.Helper()

	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "direct_push_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	userID := "direct-push-user-" + suffix
	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations:              testSchemaGenerationsForRegisteredTables(registered),
		AppName:                        "direct-push-test",
		MaxRowsPerBundle:               opts.maxRowsPerBundle,
		MaxBytesPerBundle:              opts.maxBytesPerBundle,
		MaxBytesPerPullResponse:        opts.maxBytesPerPullResponse,
		MaxBytesPerSnapshotRow:         opts.maxBytesPerSnapshotRow,
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	mustInitializeEmptyScope(t, ctx, svc, userID, "writer")
	resolveConnectForPush(t, ctx, svc, Actor{UserID: userID, SourceID: "reader"}, false)

	return &directPushFixture{
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		writer:     Actor{UserID: userID, SourceID: "writer"},
		reader:     Actor{UserID: userID, SourceID: "reader"},
	}
}

func (f *directPushFixture) userRow(id uuid.UUID, name string) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"id":"%s","name":"%s","email":"%s@example.com"}`,
			id, name, strings.ToLower(name),
		)),
	}
}

func (f *directPushFixture) updateUserRow(id uuid.UUID, name string, baseRowVersion int64) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpUpdate,
		BaseRowVersion: baseRowVersion,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"id":"%s","name":"%s","email":"%s@example.com"}`,
			id, name, strings.ToLower(name),
		)),
	}
}

func (f *directPushFixture) deleteUserRow(id uuid.UUID, baseRowVersion int64) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpDelete,
		BaseRowVersion: baseRowVersion,
	}
}

func (f *directPushFixture) push(t *testing.T, ctx context.Context, actor Actor, sourceBundleID int64, rows []PushRequestRow) (*OneShotPushResponse, error) {
	t.Helper()

	hash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(f.svc), rows)
	if err != nil {
		return nil, err
	}
	return f.svc.PushOneShot(ctx, actor, testSchemaGenerationProofForService(f.svc), &OneShotPushRequest{
		SourceBundleID:       sourceBundleID,
		CanonicalRequestHash: hash,
		Rows:                 rows,
	})
}

func (f *directPushFixture) businessUserCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	var count int
	require.NoError(t, f.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users`, f.schemaName)).Scan(&count))
	return count
}

func (f *directPushFixture) durablePushState(t *testing.T, ctx context.Context, actor Actor) string {
	t.Helper()

	var state string
	query := fmt.Sprintf(`
		SELECT jsonb_build_object(
			'user_state', to_jsonb(us),
			'scope_state', to_jsonb(sc),
			'source_state', to_jsonb(ss),
			'bundle_count', (SELECT count(*) FROM sync.bundle_log AS bl WHERE bl.user_pk = us.user_pk),
			'bundle_row_count', (SELECT count(*) FROM sync.bundle_rows AS br WHERE br.user_pk = us.user_pk),
			'row_state_count', (SELECT count(*) FROM sync.row_state AS rs WHERE rs.user_pk = us.user_pk),
			'business_row_count', (SELECT count(*) FROM %s.users AS bu WHERE bu._sync_scope_id = us.user_id)
		)::text
		FROM sync.user_state AS us
		JOIN sync.scope_state AS sc ON sc.user_pk = us.user_pk
		JOIN sync.source_state AS ss ON ss.user_pk = us.user_pk AND ss.source_id = $2
		WHERE us.user_id = $1
	`, pgx.Identifier{f.schemaName}.Sanitize())
	require.NoError(t, f.pool.QueryRow(ctx, query, actor.UserID, actor.SourceID).Scan(&state))
	return state
}

func TestDirectPush_CompleteRequestPreservesOrderAndBusinessState(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	first := uuid.New()
	second := uuid.New()

	committed, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{f.userRow(second, "Second"), f.userRow(first, "First")})
	require.NoError(t, err)
	require.Len(t, committed.Bundle.Rows, 2)
	require.Equal(t, second.String(), committed.Bundle.Rows[0].Key["id"])
	require.Equal(t, first.String(), committed.Bundle.Rows[1].Key["id"])
	require.Equal(t, 2, f.businessUserCount(t, ctx))

	updated, err := f.push(t, ctx, f.writer, 2, []PushRequestRow{f.updateUserRow(first, "First Updated", committed.Bundle.Rows[1].RowVersion), f.deleteUserRow(second, committed.Bundle.Rows[0].RowVersion)})
	require.NoError(t, err)
	require.Len(t, updated.Bundle.Rows, 2)
	require.Equal(t, OpUpdate, updated.Bundle.Rows[0].Op)
	require.Equal(t, OpDelete, updated.Bundle.Rows[1].Op)
	require.Equal(t, 1, f.businessUserCount(t, ctx))
}

func TestDirectPush_ExactReplayReturnsSameBundleAndMismatchDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	row := f.userRow(uuid.New(), "Replay")

	first, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{row})
	require.NoError(t, err)
	replay, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{row})
	require.NoError(t, err)
	require.Equal(t, first.Bundle.BundleSeq, replay.Bundle.BundleSeq)
	require.Equal(t, first.Bundle.BundleHash, replay.Bundle.BundleHash)

	mismatched := f.userRow(uuid.New(), "Mismatch")
	_, err = f.push(t, ctx, f.writer, 1, []PushRequestRow{mismatched})
	var mismatch *PushReplayMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, 1, f.businessUserCount(t, ctx))
}

func TestDirectPush_InitializationReplaySurvivesLostSuccessAndRestartWithoutMutation(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	actor := Actor{UserID: "initialization-replay-" + uuid.NewString(), SourceID: "initializer"}

	connected, err := f.svc.Connect(ctx, actor, testSchemaGenerationProofForService(f.svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", connected.Resolution)
	require.NotEmpty(t, connected.InitializationID)

	rows := []PushRequestRow{f.userRow(uuid.New(), "Initialization Replay")}
	requestHash := mustCanonicalPushRequestHash(t, f.svc, rows)
	req := &OneShotPushRequest{
		SourceBundleID:       1,
		InitializationID:     connected.InitializationID,
		CanonicalRequestHash: requestHash,
		Rows:                 rows,
	}
	committed, err := f.svc.PushOneShot(ctx, actor, testSchemaGenerationProofForService(f.svc), req)
	require.NoError(t, err)
	stateAfterCommit := f.durablePushState(t, ctx, actor)

	require.NoError(t, f.svc.Close(ctx))
	registered := []RegisteredTable{{Schema: f.schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	restarted := newBootstrappedIntegrationService(t, ctx, f.pool, &ServiceConfig{
		SchemaGenerations:              testSchemaGenerationsForRegisteredTables(registered),
		AppName:                        "direct-push-restart-test",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	replayed, err := restarted.PushOneShot(ctx, actor, testSchemaGenerationProofForService(restarted), req)
	require.NoError(t, err)
	require.Equal(t, committed.Bundle.BundleSeq, replayed.Bundle.BundleSeq)
	require.Equal(t, committed.Bundle.BundleHash, replayed.Bundle.BundleHash)
	require.Equal(t, stateAfterCommit, f.durablePushState(t, ctx, actor), "exact initialization replay must not refresh lifecycle state or write rows")
}

func TestDirectPush_RetiredSourceReplayPrecedesLifecycleAndMismatchDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	rows := []PushRequestRow{f.userRow(uuid.New(), "Retired Replay")}
	committed, err := f.push(t, ctx, f.writer, 1, rows)
	require.NoError(t, err)

	_, err = f.pool.Exec(ctx, `
		UPDATE sync.source_state AS ss
		SET state = 'retired', replaced_by_source_id = 'replacement', retirement_reason = 'test retirement'
		FROM sync.user_state AS us
		WHERE ss.user_pk = us.user_pk AND us.user_id = $1 AND ss.source_id = $2
	`, f.writer.UserID, f.writer.SourceID)
	require.NoError(t, err)
	stateBeforeReplay := f.durablePushState(t, ctx, f.writer)

	replayed, err := f.push(t, ctx, f.writer, 1, rows)
	require.NoError(t, err)
	require.Equal(t, committed.Bundle.BundleSeq, replayed.Bundle.BundleSeq)
	require.Equal(t, committed.Bundle.BundleHash, replayed.Bundle.BundleHash)
	require.Equal(t, stateBeforeReplay, f.durablePushState(t, ctx, f.writer))

	_, err = f.push(t, ctx, f.writer, 1, []PushRequestRow{f.userRow(uuid.New(), "Retired Mismatch")})
	var mismatch *PushReplayMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, stateBeforeReplay, f.durablePushState(t, ctx, f.writer), "replay mismatch must not enter retired-source lifecycle or mutate durable state")
}

func TestDirectPush_PrunedCommittedTupleReturnsHistoryPrunedWithoutMutation(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	rows := []PushRequestRow{f.userRow(uuid.New(), "Pruned Replay")}
	committed, err := f.push(t, ctx, f.writer, 1, rows)
	require.NoError(t, err)

	_, err = f.pool.Exec(ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = $2
		WHERE user_id = $1
	`, f.writer.UserID, committed.Bundle.BundleSeq)
	require.NoError(t, err)
	stateBeforeReplay := f.durablePushState(t, ctx, f.writer)

	_, err = f.push(t, ctx, f.writer, 1, rows)
	var pruned *SourceTupleHistoryPrunedError
	require.ErrorAs(t, err, &pruned)
	require.Equal(t, int64(1), pruned.MaxCommittedSourceBundleIDHint)
	require.Equal(t, stateBeforeReplay, f.durablePushState(t, ctx, f.writer))
}

func TestDirectPush_IndivisibleFuturePullLimitIsExactAndRollsBack(t *testing.T) {
	for _, delta := range []int64{0, -1} {
		name := "exact limit"
		if delta < 0 {
			name = "limit minus one"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
			rows := []PushRequestRow{
				f.userRow(uuid.New(), strings.Repeat("A", 700)),
				f.userRow(uuid.New(), strings.Repeat("B", 700)),
			}
			projectedRows := make([]BundleRow, 0, len(rows))
			for _, row := range rows {
				projectedRows = append(projectedRows, BundleRow{
					Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
					RowVersion: 1, Payload: row.Payload,
				})
			}
			projected := Bundle{
				BundleSeq: 1, SourceID: f.writer.SourceID, SourceBundleID: 1,
				BundleHash: strings.Repeat("a", 64), Rows: projectedRows,
			}
			exact, err := compactPullResponseDecodedBytes(generationDescriptor(f.svc.generationViews[1].contract), 1, []Bundle{projected}, false)
			require.NoError(t, err)
			f.svc.config.MaxBytesPerPullResponse = exact + delta

			_, err = f.push(t, ctx, f.writer, 1, rows)
			if delta == 0 {
				require.NoError(t, err)
				require.Equal(t, 2, f.businessUserCount(t, ctx))
				return
			}
			var projection *GenerationProjectionError
			require.ErrorAs(t, err, &projection)
			require.Equal(t, BundleProjectionTooLarge, projection.Code)
			require.Zero(t, f.businessUserCount(t, ctx))
			var bundles int
			require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log`).Scan(&bundles))
			require.Zero(t, bundles)
		})
	}
}

func TestServerOriginatedBundle_RejectsOversizedVisibleRowBeforePublication(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	actor := Actor{UserID: f.writer.UserID, SourceID: "server-writer"}
	resolveConnectForPush(t, ctx, f.svc, actor, false)
	table := pgx.Identifier{f.schemaName, "users"}.Sanitize()
	rowID := uuid.New()
	oversizedName := strings.Repeat("N", int(f.svc.maxBytesPerSnapshotRow()))
	err := f.svc.WithinSyncBundle(ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id,name,email) VALUES ($1,$2,$3)`, table), rowID, oversizedName, "oversized@example.com")
		return execErr
	})
	var projection *GenerationProjectionError
	require.ErrorAs(t, err, &projection)
	require.Equal(t, BundleProjectionTooLarge, projection.Code)
	require.Zero(t, f.businessUserCount(t, ctx))
	var bundles int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log`).Scan(&bundles))
	require.Zero(t, bundles)
}

func TestDirectPush_ConflictRollsBackTheWholeRequest(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	existing := f.userRow(uuid.New(), "Existing")
	_, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{existing})
	require.NoError(t, err)

	newRow := f.userRow(uuid.New(), "Must Roll Back")
	conflictingWriter := Actor{UserID: f.writer.UserID, SourceID: "conflicting-writer"}
	resolveConnectForPush(t, ctx, f.svc, conflictingWriter, false)
	_, err = f.push(t, ctx, conflictingWriter, 1, []PushRequestRow{newRow, existing})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	require.NotNil(t, conflict.Conflict)
	require.Equal(t, 1, f.businessUserCount(t, ctx))
}

func TestDirectPush_RejectsInvalidCompleteRequestsWithoutPersistence(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	row := f.userRow(uuid.New(), "Duplicate")

	_, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{row, row})
	var validation *PushValidationError
	require.ErrorAs(t, err, &validation)
	require.Zero(t, f.businessUserCount(t, ctx))

	var relation *string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT to_regclass('sync.push_sessions')::text`).Scan(&relation))
	require.Nil(t, relation)
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT to_regclass('sync.push_session_rows')::text`).Scan(&relation))
	require.Nil(t, relation)
}

func TestDirectPush_EnforcesRequestBoundsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{maxRowsPerBundle: 1})
	_, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{f.userRow(uuid.New(), "One"), f.userRow(uuid.New(), "Two")})
	var limit *GenerationProjectionError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, BundleLimitExceeded, limit.Code)
	require.Equal(t, 0, f.businessUserCount(t, ctx))
}

func TestDirectPush_RejectsHiddenOwnerColumn(t *testing.T) {
	ctx := context.Background()
	f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	row := f.userRow(uuid.New(), "Hidden")
	row.Payload = json.RawMessage(fmt.Sprintf(`{"_sync_scope_id":"other","id":%q,"name":"Hidden","email":"hidden@example.com"}`, row.Key["id"]))

	_, err := f.push(t, ctx, f.writer, 1, []PushRequestRow{row})
	var validation *PushValidationError
	require.ErrorAs(t, err, &validation)
	require.Equal(t, 0, f.businessUserCount(t, ctx))
}
