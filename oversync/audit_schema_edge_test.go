//go:build oversync_audit

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

type auditSchemaEdgeHarness struct {
	ctx         context.Context
	pool        *pgxpool.Pool
	logger      *slog.Logger
	schemaName  string
	schemaIdent string
}

func newAuditSchemaEdgeHarness(t *testing.T, scenario string) *auditSchemaEdgeHarness {
	t.Helper()

	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_edge_" + strings.ReplaceAll(scenario, "_", "") + "_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	_, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	return &auditSchemaEdgeHarness{
		ctx:         ctx,
		pool:        pool,
		logger:      integrationTestLogger(slog.LevelWarn),
		schemaName:  schemaName,
		schemaIdent: pgx.Identifier{schemaName}.Sanitize(),
	}
}

func newAuditSchemaEdgeHarnessWithExactSchema(t *testing.T, schemaName string) *auditSchemaEdgeHarness {
	t.Helper()

	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	_, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	return &auditSchemaEdgeHarness{
		ctx:         ctx,
		pool:        pool,
		logger:      integrationTestLogger(slog.LevelWarn),
		schemaName:  schemaName,
		schemaIdent: pgx.Identifier{schemaName}.Sanitize(),
	}
}

func (h *auditSchemaEdgeHarness) execf(t *testing.T, query string, args ...any) {
	t.Helper()

	_, err := h.pool.Exec(h.ctx, fmt.Sprintf(query, args...))
	require.NoError(t, err)
}

func (h *auditSchemaEdgeHarness) newService(
	t *testing.T,
	scenario string,
	tableName string,
	keyColumn string,
) (*SyncService, error) {
	t.Helper()

	registered := []RegisteredTable{{
		Schema:         h.schemaName,
		Table:          tableName,
		SyncKeyColumns: []string{keyColumn},
	}}
	svc, err := newTestRuntimeService(h.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, h.ctx, h.pool, registered, nil), AppName: "audit-schema-edge-" + scenario,
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, h.logger)
	if svc != nil {
		t.Cleanup(func() { _ = svc.Close(context.Background()) })
	}
	return svc, err
}

func requireAuditSchemaEdgeRejectedAtomically(t *testing.T, h *auditSchemaEdgeHarness, bootstrapErr error) {
	t.Helper()

	require.Error(t, bootstrapErr)
	requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)
}

func auditSchemaEdgePayload(t *testing.T, fields map[string]any) json.RawMessage {
	t.Helper()

	payload, err := json.Marshal(fields)
	require.NoError(t, err)
	return payload
}

func auditSchemaEdgePushRow(
	t *testing.T,
	h *auditSchemaEdgeHarness,
	svc *SyncService,
	actor Actor,
	sourceBundleID int64,
	tableName string,
	keyColumn string,
	keyValue string,
	op string,
	baseRowVersion int64,
	payload map[string]any,
) (*Bundle, error) {
	t.Helper()

	row := PushRequestRow{
		Schema:         h.schemaName,
		Table:          tableName,
		Key:            SyncKey{keyColumn: keyValue},
		Op:             op,
		BaseRowVersion: baseRowVersion,
	}
	if payload != nil {
		row.Payload = auditSchemaEdgePayload(t, payload)
	}
	return pushRowsOneShot(t, h.ctx, svc, actor, sourceBundleID, []PushRequestRow{row})
}

func auditSchemaEdgePayloadObject(t *testing.T, payload json.RawMessage) map[string]any {
	t.Helper()

	var object map[string]any
	require.NoError(t, json.Unmarshal(payload, &object))
	require.NotNil(t, object)
	return object
}

func requireAuditSchemaEdgePullAndSnapshot(
	t *testing.T,
	h *auditSchemaEdgeHarness,
	svc *SyncService,
	reader Actor,
	tableName string,
	keyColumn string,
	keyValue string,
	wantBundleSeq int64,
	wantPayload map[string]any,
) {
	t.Helper()
	mustInitializeEmptyScope(t, h.ctx, svc, reader.UserID, reader.SourceID)

	pull, err := svc.ProcessPull(h.ctx, reader, testSchemaGenerationProofForService(svc), 0, 100, 0)
	require.NoError(t, err)
	require.Equal(t, wantBundleSeq, pull.StableBundleSeq)
	require.NotEmpty(t, pull.Bundles)
	lastBundle := pull.Bundles[len(pull.Bundles)-1]
	require.Equal(t, wantBundleSeq, lastBundle.BundleSeq)
	require.Len(t, lastBundle.Rows, 1)
	require.Equal(t, h.schemaName, lastBundle.Rows[0].Schema)
	require.Equal(t, tableName, lastBundle.Rows[0].Table)
	require.Equal(t, keyValue, lastBundle.Rows[0].Key[keyColumn])
	require.Equal(t, wantPayload, auditSchemaEdgePayloadObject(t, lastBundle.Rows[0].Payload))

	session, err := svc.CreateSnapshotSession(h.ctx, reader, testSchemaGenerationProofForService(svc))
	require.NoError(t, err)
	require.Equal(t, wantBundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(1), session.RowCount)
	rows, snapshotBundleSeq := collectSnapshotChunkRows(t, h.ctx, svc, reader, session.SnapshotID, 1)
	require.Equal(t, wantBundleSeq, snapshotBundleSeq)
	require.Len(t, rows, 1)
	require.Equal(t, h.schemaName, rows[0].Schema)
	require.Equal(t, tableName, rows[0].Table)
	require.Equal(t, keyValue, rows[0].Key[keyColumn])
	require.Equal(t, wantBundleSeq, rows[0].RowVersion)
	require.Equal(t, wantPayload, auditSchemaEdgePayloadObject(t, rows[0].Payload))

	requireAuditMetadataIntegrity(t, h.ctx, h.pool)
}

func TestAuditSchemaEdge_GeneratedColumnRoundTripsOrBootstrapRejectsAtomically(t *testing.T) {
	h := newAuditSchemaEdgeHarness(t, "generated")
	h.execf(t, `
		CREATE TABLE %s.records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			normalized_body TEXT GENERATED ALWAYS AS (upper(body)) STORED,
			status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'ready')),
			optional_note TEXT,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)

	svc, err := h.newService(t, "generated", "records", "id")
	require.NoError(t, err)
	if err := svc.Bootstrap(h.ctx); err != nil {
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr, "generated columns may be unsupported, but bootstrap must reject them explicitly")
		requireAuditSchemaEdgeRejectedAtomically(t, h, err)
		return
	}

	rowID := uuid.NewString()
	writer := Actor{UserID: "generated-owner-" + rowID, SourceID: "writer"}
	reader := Actor{UserID: writer.UserID, SourceID: "reader"}
	insertBundle, err := auditSchemaEdgePushRow(t, h, svc, writer, 1, "records", "id", rowID, OpInsert, 0, map[string]any{
		"id":   rowID,
		"body": "alpha",
	})
	require.NoError(t, err)
	require.NotNil(t, insertBundle)
	require.Len(t, insertBundle.Rows, 1)
	insertAfterImage := auditSchemaEdgePayloadObject(t, insertBundle.Rows[0].Payload)
	require.Equal(t, "ALPHA", insertAfterImage["normalized_body"])
	require.Equal(t, "draft", insertAfterImage["status"])
	require.Contains(t, insertAfterImage, "optional_note")
	require.Nil(t, insertAfterImage["optional_note"])
	requireAuditSchemaEdgePullAndSnapshot(
		t, h, svc, reader, "records", "id", rowID, insertBundle.BundleSeq, insertAfterImage,
	)

	// A supported table shape must be able to accept its own authoritative after-image
	// on the next client update. The generated value itself must be filtered or safely
	// recomputed; merely accepting bootstrap and then emitting an unpushable payload is
	// partial support.
	updatePayload := insertAfterImage
	updatePayload["body"] = "beta"
	updateBundle, updateErr := auditSchemaEdgePushRow(
		t, h, svc, writer, 2, "records", "id", rowID, OpUpdate, insertBundle.BundleSeq, updatePayload,
	)
	if updateErr != nil {
		var persistedBody, persistedNormalized string
		require.NoError(t, h.pool.QueryRow(h.ctx, fmt.Sprintf(`
			SELECT body, normalized_body
			FROM %s.records
			WHERE _sync_scope_id = $1 AND id = $2::uuid
		`, h.schemaIdent), writer.UserID, rowID).Scan(&persistedBody, &persistedNormalized))
		require.Equal(t, "alpha", persistedBody, "a rejected generated-column update must roll back business state")
		require.Equal(t, "ALPHA", persistedNormalized)
		requireAuditMetadataIntegrity(t, h.ctx, h.pool)
		t.Fatalf("bootstrap accepted a generated column but its emitted after-image could not round-trip through push: %v", updateErr)
	}
	require.NotNil(t, updateBundle)
	require.Len(t, updateBundle.Rows, 1)
	updateAfterImage := auditSchemaEdgePayloadObject(t, updateBundle.Rows[0].Payload)
	require.Equal(t, "beta", updateAfterImage["body"])
	require.Equal(t, "BETA", updateAfterImage["normalized_body"])

	replayed, err := auditSchemaEdgePushRow(
		t, h, svc, writer, 2, "records", "id", rowID, OpUpdate, insertBundle.BundleSeq, updatePayload,
	)
	require.NoError(t, err)
	require.Equal(t, updateBundle.BundleSeq, replayed.BundleSeq)
	require.Equal(t, updateBundle.BundleHash, replayed.BundleHash)
	requireAuditSchemaEdgePullAndSnapshot(
		t, h, svc, reader, "records", "id", rowID, updateBundle.BundleSeq, updateAfterImage,
	)
}

func TestAuditSchemaEdge_PartitionedRegisteredTableIsCoherentOrRejectedAtomically(t *testing.T) {
	h := newAuditSchemaEdgeHarness(t, "partitioned")
	h.execf(t, `
		CREATE TABLE %s.records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		) PARTITION BY HASH (_sync_scope_id)`, h.schemaIdent)
	h.execf(t, `
		CREATE TABLE %s.records_p0 PARTITION OF %s.records
		FOR VALUES WITH (MODULUS 2, REMAINDER 0)`, h.schemaIdent, h.schemaIdent)
	h.execf(t, `
		CREATE TABLE %s.records_p1 PARTITION OF %s.records
		FOR VALUES WITH (MODULUS 2, REMAINDER 1)`, h.schemaIdent, h.schemaIdent)

	svc, err := h.newService(t, "partitioned", "records", "id")
	require.NoError(t, err)
	if err := svc.Bootstrap(h.ctx); err != nil {
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr, "partitioned tables may be unsupported, but bootstrap must reject them explicitly")
		requireAuditSchemaEdgeRejectedAtomically(t, h, err)
		return
	}

	rowID := uuid.NewString()
	writer := Actor{UserID: "partition-owner-" + rowID, SourceID: "writer"}
	reader := Actor{UserID: writer.UserID, SourceID: "reader"}
	insertBundle, err := auditSchemaEdgePushRow(t, h, svc, writer, 1, "records", "id", rowID, OpInsert, 0, map[string]any{
		"id":   rowID,
		"body": "alpha",
	})
	require.NoError(t, err)
	require.NotNil(t, insertBundle)

	updatePayload := auditSchemaEdgePayloadObject(t, insertBundle.Rows[0].Payload)
	updatePayload["body"] = "beta"
	updateBundle, err := auditSchemaEdgePushRow(
		t, h, svc, writer, 2, "records", "id", rowID, OpUpdate, insertBundle.BundleSeq, updatePayload,
	)
	require.NoError(t, err)
	require.NotNil(t, updateBundle)
	require.Len(t, updateBundle.Rows, 1)
	updateAfterImage := auditSchemaEdgePayloadObject(t, updateBundle.Rows[0].Payload)
	require.Equal(t, "beta", updateAfterImage["body"])

	var physicalTable string
	require.NoError(t, h.pool.QueryRow(h.ctx, fmt.Sprintf(`
		SELECT tableoid::regclass::text
		FROM %s.records
		WHERE _sync_scope_id = $1 AND id = $2::uuid
	`, h.schemaIdent), writer.UserID, rowID).Scan(&physicalTable))
	require.Contains(t, physicalTable, "records_p", "the row must be routed to a physical partition")

	replayed, err := auditSchemaEdgePushRow(
		t, h, svc, writer, 2, "records", "id", rowID, OpUpdate, insertBundle.BundleSeq, updatePayload,
	)
	require.NoError(t, err)
	require.Equal(t, updateBundle.BundleSeq, replayed.BundleSeq)
	require.Equal(t, updateBundle.BundleHash, replayed.BundleHash)
	requireAuditSchemaEdgePullAndSnapshot(
		t, h, svc, reader, "records", "id", rowID, updateBundle.BundleSeq, updateAfterImage,
	)
}

func TestAuditSchemaEdge_UnloggedRegisteredTableIsRejectedBeforeBootstrapMutation(t *testing.T) {
	h := newAuditSchemaEdgeHarness(t, "unlogged")
	h.execf(t, `
		CREATE UNLOGGED TABLE %s.records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)

	svc, err := h.newService(t, "unlogged", "records", "id")
	require.NoError(t, err)
	bootstrapErr := svc.Bootstrap(h.ctx)
	if bootstrapErr == nil {
		var persistence string
		require.NoError(t, h.pool.QueryRow(h.ctx, `
			SELECT c.relpersistence::text
			FROM pg_class AS c
			JOIN pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = 'records'
		`, h.schemaName).Scan(&persistence))
		t.Fatalf("bootstrap accepted registered table with relpersistence=%q; acknowledged authoritative rows require crash-durable PostgreSQL storage", persistence)
	}
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, bootstrapErr, &schemaErr)
	require.Contains(t, bootstrapErr.Error(), h.schemaName+".records")
	require.Contains(t, strings.ToLower(bootstrapErr.Error()), "unlogged")
	require.Contains(t, bootstrapErr.Error(), `relpersistence="u"`)
	require.Contains(t, bootstrapErr.Error(), "permanent logged PostgreSQL storage")
	requireAuditSchemaEdgeRejectedAtomically(t, h, bootstrapErr)
}

func TestAuditSchemaEdge_ReservedIdentifiersRoundTrip(t *testing.T) {
	h := newAuditSchemaEdgeHarnessWithExactSchema(t, "group")
	tableName := "order"
	keyColumn := "select"
	tableIdent := pgx.Identifier{h.schemaName, tableName}.Sanitize()
	h.execf(t, `
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			"select" TEXT NOT NULL,
			"from" TEXT NOT NULL,
			"where" JSONB,
			PRIMARY KEY (_sync_scope_id, "select")
		)`, tableIdent)

	svc, err := h.newService(t, "reserved-identifiers", tableName, keyColumn)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(h.ctx))
	keyValue := "Key / 01"
	writer := Actor{UserID: "quoted identifier owner", SourceID: "writer"}
	reader := Actor{UserID: writer.UserID, SourceID: "reader"}
	wantPayload := map[string]any{
		"select": keyValue,
		"from":   "origin",
		"where":  `{"nested":"value"}`,
	}
	bundle, err := auditSchemaEdgePushRow(
		t, h, svc, writer, 1, tableName, keyColumn, keyValue, OpInsert, 0, wantPayload,
	)
	require.NoError(t, err)
	require.NotNil(t, bundle)
	require.Len(t, bundle.Rows, 1)
	require.Equal(t, wantPayload, auditSchemaEdgePayloadObject(t, bundle.Rows[0].Payload))
	requireAuditSchemaEdgePullAndSnapshot(
		t, h, svc, reader, tableName, keyColumn, keyValue, bundle.BundleSeq, wantPayload,
	)
}

func TestAuditSchemaEdge_QuotedMixedCaseIdentifiersRejectWithoutPartialBootstrap(t *testing.T) {
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	h := newAuditSchemaEdgeHarnessWithExactSchema(t, "Audit Edge "+suffix)
	tableName := "Order Items"
	keyColumn := "Item ID"
	tableIdent := pgx.Identifier{h.schemaName, tableName}.Sanitize()
	h.execf(t, `
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			"Item ID" UUID NOT NULL,
			"Display Name" TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, "Item ID")
		)`, tableIdent)

	svc, bootstrapErr := h.newService(t, "quoted-mixed-case", tableName, keyColumn)
	if bootstrapErr == nil {
		bootstrapErr = svc.Bootstrap(h.ctx)
	}
	requireAuditSchemaEdgeRejectedAtomically(t, h, bootstrapErr)
}

func TestAuditSchemaEdge_ViewRegistrationRejectsWithoutPartialBootstrap(t *testing.T) {
	h := newAuditSchemaEdgeHarness(t, "view")
	h.execf(t, `
		CREATE TABLE %s.base_records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)
	h.execf(t, `CREATE VIEW %s.records AS SELECT * FROM %s.base_records`, h.schemaIdent, h.schemaIdent)

	svc, bootstrapErr := h.newService(t, "view", "records", "id")
	if bootstrapErr == nil {
		bootstrapErr = svc.Bootstrap(h.ctx)
	}
	var configErr *SchemaGenerationConfigError
	require.ErrorAs(t, bootstrapErr, &configErr)
	require.Contains(t, strings.ToLower(bootstrapErr.Error()), "sync_key")
	requireAuditSchemaEdgeRejectedAtomically(t, h, bootstrapErr)
}
