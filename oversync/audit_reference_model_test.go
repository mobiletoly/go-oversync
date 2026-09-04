//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const auditReferenceModelSeed int64 = 20260710

type auditReferenceRow struct {
	id      uuid.UUID
	name    string
	email   string
	version int64
	live    bool
}

type auditReferenceScope struct {
	actor              Actor
	nextSourceBundleID int64
	latestBundleSeq    int64
	nextRowOrdinal     int
	rows               map[uuid.UUID]auditReferenceRow
}

type auditReferenceModel struct {
	scopes map[string]*auditReferenceScope
}

func newAuditReferenceModel(userCount int) *auditReferenceModel {
	model := &auditReferenceModel{scopes: make(map[string]*auditReferenceScope, userCount)}
	for index := 0; index < userCount; index++ {
		actor := Actor{
			UserID:   fmt.Sprintf("audit-model-user-%d", index),
			SourceID: fmt.Sprintf("audit-model-source-%d", index),
		}
		model.scopes[actor.UserID] = &auditReferenceScope{
			actor:              actor,
			nextSourceBundleID: 1,
			rows:               make(map[uuid.UUID]auditReferenceRow),
		}
	}
	return model
}

func (m *auditReferenceModel) orderedScopes() []*auditReferenceScope {
	userIDs := make([]string, 0, len(m.scopes))
	for userID := range m.scopes {
		userIDs = append(userIDs, userID)
	}
	sort.Strings(userIDs)

	scopes := make([]*auditReferenceScope, 0, len(userIDs))
	for _, userID := range userIDs {
		scopes = append(scopes, m.scopes[userID])
	}
	return scopes
}

func (s *auditReferenceScope) nextOperation(rng *rand.Rand, schemaName string) PushRequestRow {
	liveIDs := s.rowIDs(true)
	deletedIDs := s.rowIDs(false)

	op := OpInsert
	if len(liveIDs) > 0 {
		switch rng.Intn(4) {
		case 0, 1:
			op = OpInsert
		case 2:
			op = OpUpdate
		case 3:
			op = OpDelete
		}
	}

	var (
		id             uuid.UUID
		baseRowVersion int64
	)
	if op == OpInsert {
		if len(deletedIDs) > 0 && rng.Intn(3) == 0 {
			id = deletedIDs[rng.Intn(len(deletedIDs))]
			baseRowVersion = s.rows[id].version
		} else {
			s.nextRowOrdinal++
			id = auditReferenceUUID(s.actor.UserID, s.nextRowOrdinal)
		}
	} else {
		id = liveIDs[rng.Intn(len(liveIDs))]
		baseRowVersion = s.rows[id].version
	}

	row := PushRequestRow{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             op,
		BaseRowVersion: baseRowVersion,
	}
	if op != OpDelete {
		name := fmt.Sprintf("Model-%s-%d-%d", s.actor.UserID, s.nextSourceBundleID, s.nextRowOrdinal)
		email := strings.ToLower(name) + "@example.com"
		row.Payload = json.RawMessage(fmt.Sprintf(
			`{"id":%q,"name":%q,"email":%q}`,
			id,
			name,
			email,
		))
	}
	return row
}

func (s *auditReferenceScope) rowIDs(live bool) []uuid.UUID {
	ids := make([]uuid.UUID, 0)
	for id, row := range s.rows {
		if row.live == live {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	return ids
}

func (s *auditReferenceScope) applyCommittedRow(t *testing.T, request PushRequestRow, bundle *Bundle) {
	t.Helper()
	require.Len(t, bundle.Rows, 1)
	require.Equal(t, s.latestBundleSeq+1, bundle.BundleSeq)
	require.Equal(t, s.nextSourceBundleID, bundle.SourceBundleID)
	require.Equal(t, s.actor.SourceID, bundle.SourceID)

	id := uuid.MustParse(request.Key["id"].(string))
	state := s.rows[id]
	state.id = id
	state.version = bundle.BundleSeq
	if request.Op == OpDelete {
		state.live = false
		state.name = ""
		state.email = ""
	} else {
		var payload struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		}
		require.NoError(t, json.Unmarshal(request.Payload, &payload))
		state.live = true
		state.name = payload.Name
		state.email = payload.Email
	}
	s.rows[id] = state
	s.latestBundleSeq = bundle.BundleSeq
	s.nextSourceBundleID++
}

func auditReferenceUUID(userID string, ordinal int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s:%d", userID, ordinal)))
}

func TestAuditReferenceModel_SeededMultiScopeSequence(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "audit_reference_model"
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "audit-reference-model",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	model := newAuditReferenceModel(3)
	for _, scope := range model.orderedScopes() {
		mustInitializeEmptyScope(t, ctx, svc, scope.actor.UserID, scope.actor.SourceID)
	}

	rng := rand.New(rand.NewSource(auditReferenceModelSeed))
	const operationCount = 30
	for operationIndex := 0; operationIndex < operationCount; operationIndex++ {
		scopes := model.orderedScopes()
		scope := scopes[rng.Intn(len(scopes))]
		request := scope.nextOperation(rng, schemaName)

		bundle, err := pushRowsOneShot(
			t,
			ctx,
			svc,
			scope.actor,
			scope.nextSourceBundleID,
			[]PushRequestRow{request},
		)
		require.NoError(t, err, "seed=%d operation=%d user=%s", auditReferenceModelSeed, operationIndex, scope.actor.UserID)
		scope.applyCommittedRow(t, request, bundle)

		assertAuditReferenceModel(t, ctx, pool, svc, schemaName, model)
		requireAuditMetadataIntegrity(t, ctx, pool)
	}
}

func assertAuditReferenceModel(
	t *testing.T,
	ctx context.Context,
	pool interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	svc *SyncService,
	schemaName string,
	model *auditReferenceModel,
) {
	t.Helper()

	assertAuditReferenceBusinessRows(t, ctx, pool, schemaName, model)
	assertAuditReferenceRowState(t, ctx, pool, svc, schemaName, model)
	for _, scope := range model.orderedScopes() {
		assertAuditReferencePull(t, ctx, svc, scope)
		assertAuditReferenceSnapshot(t, ctx, svc, scope)
	}
}

func assertAuditReferenceBusinessRows(
	t *testing.T,
	ctx context.Context,
	pool interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	schemaName string,
	model *auditReferenceModel,
) {
	t.Helper()

	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT _sync_scope_id, id, name, email
		FROM %s.users
		ORDER BY _sync_scope_id, id
	`, pgx.Identifier{schemaName}.Sanitize()))
	require.NoError(t, err)
	defer rows.Close()

	actual := make(map[string]auditReferenceRow)
	for rows.Next() {
		var (
			userID string
			row    auditReferenceRow
		)
		require.NoError(t, rows.Scan(&userID, &row.id, &row.name, &row.email))
		row.live = true
		actual[userID+":"+row.id.String()] = row
	}
	require.NoError(t, rows.Err())

	expected := make(map[string]auditReferenceRow)
	for _, scope := range model.orderedScopes() {
		for id, row := range scope.rows {
			if row.live {
				expected[scope.actor.UserID+":"+id.String()] = row
			}
		}
	}
	for key, expectedRow := range expected {
		actualRow, ok := actual[key]
		require.True(t, ok, "missing business row %s", key)
		require.Equal(t, expectedRow.id, actualRow.id, key)
		require.Equal(t, expectedRow.name, actualRow.name, key)
		require.Equal(t, expectedRow.email, actualRow.email, key)
	}
	require.Len(t, actual, len(expected))
}

func assertAuditReferenceRowState(
	t *testing.T,
	ctx context.Context,
	pool interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	svc *SyncService,
	schemaName string,
	model *auditReferenceModel,
) {
	t.Helper()

	info, err := svc.syncKeyInfoForTable(schemaName, "users")
	require.NoError(t, err)
	rows, err := pool.Query(ctx, `
		SELECT users.user_id, state.key_bytes, state.bundle_seq, state.deleted
		FROM sync.row_state AS state
		JOIN sync.user_state AS users ON users.user_pk = state.user_pk
		WHERE state.table_id = $1
		ORDER BY users.user_id, state.key_bytes
	`, info.tableID)
	require.NoError(t, err)
	defer rows.Close()

	seen := make(map[string]struct{})
	for rows.Next() {
		var (
			userID    string
			keyBytes  []byte
			bundleSeq int64
			deleted   bool
		)
		require.NoError(t, rows.Scan(&userID, &keyBytes, &bundleSeq, &deleted))
		id, err := uuid.FromBytes(keyBytes)
		require.NoError(t, err)
		state, ok := model.scopes[userID].rows[id]
		require.True(t, ok, "unexpected row_state for %s:%s", userID, id)
		require.Equal(t, state.version, bundleSeq, "%s:%s", userID, id)
		require.Equal(t, !state.live, deleted, "%s:%s", userID, id)
		seen[userID+":"+id.String()] = struct{}{}
	}
	require.NoError(t, rows.Err())

	expectedCount := 0
	for _, scope := range model.orderedScopes() {
		expectedCount += len(scope.rows)
	}
	require.Len(t, seen, expectedCount)
}

func assertAuditReferencePull(t *testing.T, ctx context.Context, svc *SyncService, scope *auditReferenceScope) {
	t.Helper()

	response, err := svc.ProcessPull(ctx, scope.actor, testSchemaGenerationProofForService(svc), 0, 100, 0)
	require.NoError(t, err)
	require.Equal(t, scope.latestBundleSeq, response.StableBundleSeq)
	require.Len(t, response.Bundles, int(scope.latestBundleSeq))
	require.False(t, response.HasMore)
	for index, bundle := range response.Bundles {
		expectedSeq := int64(index + 1)
		require.Equal(t, expectedSeq, bundle.BundleSeq)
		require.Equal(t, expectedSeq, bundle.SourceBundleID)
		require.Equal(t, scope.actor.SourceID, bundle.SourceID)
		require.Len(t, bundle.Rows, 1)
		require.Equal(t, expectedSeq, bundle.Rows[0].RowVersion)
	}
}

func assertAuditReferenceSnapshot(t *testing.T, ctx context.Context, svc *SyncService, scope *auditReferenceScope) {
	t.Helper()

	session, err := svc.CreateSnapshotSession(ctx, scope.actor, testSchemaGenerationProofForService(svc))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.DeleteSnapshotSession(context.Background(), scope.actor, testSchemaGenerationProofForService(svc), session.SnapshotID)
	})

	rows, snapshotBundleSeq := collectSnapshotChunkRows(t, ctx, svc, scope.actor, session.SnapshotID, 7)
	require.Equal(t, scope.latestBundleSeq, snapshotBundleSeq)
	expectedLiveCount := 0
	seen := make(map[uuid.UUID]struct{})
	for _, row := range rows {
		id := uuid.MustParse(row.Key["id"].(string))
		expected, ok := scope.rows[id]
		require.True(t, ok, "snapshot contains unknown row %s for %s", id, scope.actor.UserID)
		require.True(t, expected.live, "snapshot contains tombstone %s for %s", id, scope.actor.UserID)
		require.Equal(t, expected.version, row.RowVersion)

		var payload struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		}
		require.NoError(t, json.Unmarshal(row.Payload, &payload))
		require.Equal(t, expected.name, payload.Name)
		require.Equal(t, expected.email, payload.Email)
		seen[id] = struct{}{}
	}
	for _, row := range scope.rows {
		if row.live {
			expectedLiveCount++
		}
	}
	require.Len(t, rows, expectedLiveCount)
	require.Len(t, seen, expectedLiveCount)
	require.NoError(t, svc.DeleteSnapshotSession(ctx, scope.actor, testSchemaGenerationProofForService(svc), session.SnapshotID))
}
