package oversync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/stretchr/testify/require"
)

func mustPushFileBundle(
	t *testing.T,
	ctx context.Context,
	svc *SyncService,
	actor Actor,
	schemaName string,
	sourceBundleID int64,
	rowID uuid.UUID,
	fileName string,
	data []byte,
) *Bundle {
	t.Helper()

	bundle, err := pushRowsOneShot(t, ctx, svc, actor, sourceBundleID, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "files",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"id":"%s","name":"%s","data":"%s"}`,
			rowID,
			fileName,
			base64.StdEncoding.EncodeToString(data),
		)),
	}})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func requirePayloadFieldBase64(t *testing.T, payload json.RawMessage, field string, expected []byte) {
	t.Helper()

	var payloadObject map[string]any
	require.NoError(t, json.Unmarshal(payload, &payloadObject))

	rawValue, ok := payloadObject[field]
	require.True(t, ok)

	encodedValue, ok := rawValue.(string)
	require.True(t, ok)
	require.Equal(t, base64.StdEncoding.EncodeToString(expected), encodedValue)
}

func TestDatabaseWirePayloadExpression_BatchesWideJSONColumns(t *testing.T) {
	for _, test := range []struct {
		columnCount int
		wantCalls   int
	}{
		{columnCount: 50, wantCalls: 1},
		{columnCount: 51, wantCalls: 2},
		{columnCount: 101, wantCalls: 3},
	} {
		t.Run(fmt.Sprintf("columns_%d", test.columnCount), func(t *testing.T) {
			columnTypes := make(map[string]string, test.columnCount)
			for index := 0; index < test.columnCount; index++ {
				columnType := "json"
				if index%2 == 1 {
					columnType = "jsonb"
				}
				columnTypes[fmt.Sprintf("j%03d", index)] = columnType
			}

			expression := databaseWirePayloadExpression("src", columnTypes)
			require.Equal(t, test.wantCalls, strings.Count(expression, "jsonb_build_object("))
			require.Contains(t, expression, "to_jsonb(src)")
			require.Less(t, strings.Index(expression, "'j000'"), strings.Index(expression, fmt.Sprintf("'j%03d'", test.columnCount-1)))
		})
	}
}

func TestPhysicalDatabasePayloadByteLimit_UsesCodecDerivedCeilings(t *testing.T) {
	const logicalLimit int64 = 4096

	ordinary, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, map[string]string{"data": "bytea", "name": "text"})
	require.NoError(t, err)
	require.Equal(t, logicalLimit*maxPostgresOrdinaryPayloadBytesPerLogicalByte, ordinary)

	floatColumns := map[string]string{"id": "uuid"}
	for index := 0; index < 20; index++ {
		floatColumns[fmt.Sprintf("f%02d", index)] = "float8"
	}
	floatLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, floatColumns)
	require.NoError(t, err)
	require.GreaterOrEqual(t, floatLimit, logicalLimit*maxPostgresOrdinaryPayloadBytesPerLogicalByte+20*maxPostgresBinary64ExpandedNumberBytes)

	jsonbLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, map[string]string{"document": "jsonb"})
	require.NoError(t, err)
	require.Equal(t, maxPostgresPayloadDatumBytes, jsonbLimit)

	jsonLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, map[string]string{"document": "json"})
	require.NoError(t, err)
	require.Equal(t, maxPostgresPayloadDatumBytes, jsonLimit)

	capped, err := physicalDatabasePayloadByteLimitForColumnTypes(64<<20, map[string]string{"document": "jsonb"})
	require.NoError(t, err)
	require.Equal(t, maxPostgresPayloadDatumBytes, capped)

	_, err = physicalDatabasePayloadByteLimitForColumnTypes(0, map[string]string{"document": "jsonb"})
	require.Error(t, err)

	storedWire, err := physicalStoredWirePayloadByteLimit(logicalLimit)
	require.NoError(t, err)
	require.Equal(t, logicalLimit*maxPostgresOrdinaryPayloadBytesPerLogicalByte, storedWire)
	_, err = physicalStoredWirePayloadByteLimit(0)
	require.Error(t, err)
}

func TestBinaryWire_ScalarFloatExpansionDoesNotConsumeLogicalBundleBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "float_limit_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))

	columns := make([]string, 20)
	columnTypes := map[string]string{"id": "uuid", "_sync_scope_id": "text"}
	for index := range columns {
		columns[index] = fmt.Sprintf("f%02d", index)
		columnTypes[columns[index]] = "float8"
	}
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.metrics (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			%s,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent, strings.Join(func() []string {
		definitions := make([]string, len(columns))
		for index, column := range columns {
			definitions[index] = column + " DOUBLE PRECISION NOT NULL"
		}
		return definitions
	}(), ",\n")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	const logicalLimit = 1024
	insertColumns := append([]string{"_sync_scope_id", "id"}, columns...)
	values := make([]string, 0, len(insertColumns))
	values = append(values, "$1", "$2")
	for range columns {
		values = append(values, "'1e-100'::float8")
	}
	adoptedID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.metrics (%s) VALUES (%s)`, schemaIdent, strings.Join(insertColumns, ","), strings.Join(values, ",")), "adopt", adoptedID)
	require.NoError(t, err)

	payloadExpression := databaseWirePayloadExpression("src", columnTypes)
	var physicalBytes int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT octet_length((%s)::text) FROM %s.metrics AS src WHERE id=$1`, payloadExpression, schemaIdent), adoptedID).Scan(&physicalBytes))
	require.Greater(t, physicalBytes, int64(logicalLimit*maxPostgresOrdinaryPayloadBytesPerLogicalByte), "fixture must exceed the old ordinary-scalar physical ceiling")
	physicalLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, columnTypes)
	require.NoError(t, err)
	require.LessOrEqual(t, physicalBytes, physicalLimit)

	registered := []RegisteredTable{{Schema: schemaName, Table: "metrics", SyncKeyColumns: []string{"id"}}}
	config := &ServiceConfig{
		SchemaGenerations:               testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
		RegisteredTables:                registered,
		RegisteredTableCatalogRevision:  1,
		MaxRowsPerBundle:                8,
		MaxBytesPerBundle:               logicalLimit,
		MaxBytesPerPullResponse:         logicalLimit + maxCompactSingleRowEnvelopeBytes,
		MaxBytesPerSnapshotRow:          logicalLimit,
		MaxConcurrentHistoryProjections: 1,
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	adopted := loadCommittedBundleForUser(t, ctx, service, "adopt", 1)
	require.LessOrEqual(t, adopted.ByteCount, int64(logicalLimit))

	payloadObject := map[string]any{"id": uuid.NewString()}
	for _, column := range columns {
		payloadObject[column] = "1e-100"
	}
	payload, err := json.Marshal(payloadObject)
	require.NoError(t, err)
	actor := Actor{UserID: "push", SourceID: "device"}
	pushID := payloadObject["id"].(string)
	committed, err := pushRowsOneShot(t, ctx, service, actor, 1, []PushRequestRow{{
		Schema: schemaName, Table: "metrics", Key: SyncKey{"id": pushID}, Op: OpInsert, Payload: payload,
	}})
	require.NoError(t, err)
	require.LessOrEqual(t, committed.ByteCount, int64(logicalLimit))

	reader := Actor{UserID: actor.UserID, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, service, reader.UserID, reader.SourceID)
	session, err := service.CreateSnapshotSession(ctx, reader, testSchemaGenerationProofForService(service))
	require.NoError(t, err)
	chunk, err := service.GetSnapshotChunk(ctx, reader, testSchemaGenerationProofForService(service), session.SnapshotID, 0, 10, logicalLimit)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)

	conflictPayload := append([]byte(nil), payload...)
	var conflictObject map[string]any
	require.NoError(t, json.Unmarshal(conflictPayload, &conflictObject))
	conflictObject["f00"] = "2e-100"
	conflictPayload, err = json.Marshal(conflictObject)
	require.NoError(t, err)
	_, err = pushRowsOneShot(t, ctx, service, Actor{UserID: actor.UserID, SourceID: "conflict-device"}, 1, []PushRequestRow{{
		Schema: schemaName, Table: "metrics", Key: SyncKey{"id": pushID}, Op: OpUpdate, BaseRowVersion: 0, Payload: conflictPayload,
	}})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	require.NotEmpty(t, conflict.Conflict.ServerRow)

	trustedActor := Actor{UserID: "trusted"}
	mustInitializeEmptyScope(t, ctx, service, trustedActor.UserID, "server")
	trustedID := uuid.New()
	err = service.WithinSyncBundle(ctx, trustedActor, BundleSource{SourceID: "server", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		args := []any{trustedActor.UserID, trustedID}
		returnErr := error(nil)
		_, returnErr = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.metrics (%s) VALUES (%s)`, schemaIdent, strings.Join(insertColumns, ","), strings.Join(values, ",")), args...)
		return returnErr
	})
	require.NoError(t, err)
	trusted := loadCommittedBundleForUser(t, ctx, service, trustedActor.UserID, 1)
	require.LessOrEqual(t, trusted.ByteCount, int64(logicalLimit))
}

func TestRetainedWirePayloadFormattingDoesNotConsumeLogicalLimits(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "retained_wire_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	columns := make([]string, 0, 300)
	for idx := range 300 {
		columns = append(columns, fmt.Sprintf("c%03d TEXT NOT NULL DEFAULT ''", idx))
	}
	tableIdent := pgx.Identifier{schemaName, "wide_rows"}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			%s,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, tableIdent, strings.Join(columns, ",\n")))
	require.NoError(t, err)

	const logicalLimit = 3500
	registered := []RegisteredTable{{Schema: schemaName, Table: "wide_rows", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), RegisteredTables: registered,
		RegisteredTableCatalogRevision: 1, MaxRowsPerBundle: 8, MaxBytesPerBundle: logicalLimit,
		MaxBytesPerPullResponse: logicalLimit + maxCompactSingleRowEnvelopeBytes, MaxConcurrentHistoryProjections: 1,
		MaxBytesPerSnapshotRow: logicalLimit,
	}, integrationTestLogger(slog.LevelWarn))
	proof := testSchemaGenerationProofForService(service)
	userID := "retained-wire-user-" + suffix
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, service, reader.UserID, reader.SourceID)
	rowID := uuid.New()
	err = service.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id) VALUES ($1)`, tableIdent), rowID)
		return execErr
	})
	require.NoError(t, err)

	var retainedPhysicalBytes, retainedLogicalBytes int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT octet_length(rows.payload_wire::text), bundle.byte_count
		FROM sync.bundle_rows AS rows
		JOIN sync.bundle_log AS bundle USING (user_pk, bundle_seq)
		JOIN sync.user_state AS owner USING (user_pk)
		WHERE owner.user_id=$1 AND bundle.bundle_seq=1
	`, userID).Scan(&retainedPhysicalBytes, &retainedLogicalBytes))
	require.Greater(t, retainedPhysicalBytes, int64(logicalLimit), "PostgreSQL formatting must exceed the logical ceiling for this regression")
	require.LessOrEqual(t, retainedLogicalBytes, int64(logicalLimit))

	pull, err := service.ProcessPull(ctx, reader, proof, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.LessOrEqual(t, pull.Bundles[0].ByteCount, int64(logicalLimit))

	snapshot, err := service.CreateSnapshotSession(ctx, reader, proof)
	require.NoError(t, err)
	require.Equal(t, int64(1), snapshot.RowCount)
	var snapshotPhysicalBytes, snapshotWireBytes int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT octet_length(payload_wire::text), wire_byte_count
		FROM sync.snapshot_session_rows
		WHERE snapshot_id=$1::uuid
	`, snapshot.SnapshotID).Scan(&snapshotPhysicalBytes, &snapshotWireBytes))
	require.Greater(t, snapshotPhysicalBytes, int64(logicalLimit))
	require.LessOrEqual(t, snapshotWireBytes, int64(logicalLimit))
	chunk, err := service.GetSnapshotChunk(ctx, reader, proof, snapshot.SnapshotID, 0, 10, 4096)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestBinaryWire_ProcessPushAcceptedBundleCanonicalizesByteaPayload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_binary_wire_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), AppName: "push-binary-wire-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "push-binary-wire-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "device-a"}
	fileID := uuid.New()
	fileData := []byte("accepted push binary payload")

	bundle := mustPushFileBundle(t, ctx, svc, actor, schemaName, 1, fileID, "accepted.bin", fileData)
	require.Len(t, bundle.Rows, 1)
	require.Equal(t, "files", bundle.Rows[0].Table)
	requirePayloadFieldBase64(t, bundle.Rows[0].Payload, "data", fileData)
	require.NotContains(t, string(bundle.Rows[0].Payload), `"_sync_scope_id"`)

	var persistedData []byte
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT data
		FROM %s.files
		WHERE id = $1
	`, schemaName), fileID).Scan(&persistedData))
	require.Equal(t, fileData, persistedData)

	userPK, _, _ := mustCompactStorageIdentity(t, ctx, svc, userID, schemaName, "files", fileID.String())
	var storedData string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT payload_wire->>'data'
		FROM sync.bundle_rows
		WHERE user_pk = $1 AND bundle_seq = $2
	`, userPK, bundle.BundleSeq).Scan(&storedData))
	require.Equal(t, base64.StdEncoding.EncodeToString(fileData), storedData)
}

func TestBinaryWire_ProcessPullCanonicalizesByteaPayload(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_binary_wire_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-binary-wire-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "pull-binary-wire-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	fileID := uuid.New()
	fileData := []byte("pull binary payload")

	bundle := mustPushFileBundle(t, ctx, svc, writer, schemaName, 1, fileID, "pull.bin", fileData)
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)

	page, err := svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, bundle.BundleSeq, page.StableBundleSeq)
	require.Len(t, page.Bundles, 1)
	require.Len(t, page.Bundles[0].Rows, 1)
	require.Equal(t, "files", page.Bundles[0].Rows[0].Table)
	requirePayloadFieldBase64(t, page.Bundles[0].Rows[0].Payload, "data", fileData)
}

func TestBinaryWire_SnapshotChunkCanonicalizesByteaPayload(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_binary_wire_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), AppName: "snapshot-binary-wire-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "snapshot-binary-wire-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	fileID := uuid.New()
	fileData := []byte("snapshot binary payload")

	bundle := mustPushFileBundle(t, ctx, svc, writer, schemaName, 1, fileID, "snapshot.bin", fileData)
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)

	session, err := svc.CreateSnapshotSession(ctx, reader, testSchemaGenerationProofForService(svc))
	require.NoError(t, err)
	require.Equal(t, bundle.BundleSeq, session.SnapshotBundleSeq)

	chunk, err := svc.GetSnapshotChunk(ctx, reader, testSchemaGenerationProofForService(svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, "files", chunk.Rows[0].Table)
	requirePayloadFieldBase64(t, chunk.Rows[0].Payload, "data", fileData)
}

func TestBinaryWire_PhysicalExpansionDoesNotConsumeLogicalBundleBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "binary_limit_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	const logicalLimit = 4096
	fileData := make([]byte, 2304)
	for idx := range fileData {
		fileData[idx] = byte(idx)
	}
	adoptedID := uuid.New()
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.files (_sync_scope_id,id,name,data) VALUES ($1,$2,$3,$4)`, pgx.Identifier{schemaName}.Sanitize()), "adopt", adoptedID, "a", fileData)
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}}}
	config := &ServiceConfig{
		SchemaGenerations:               testSchemaGenerationsForRegisteredTables(registered),
		RegisteredTables:                registered,
		RegisteredTableCatalogRevision:  1,
		MaxRowsPerBundle:                8,
		MaxBytesPerBundle:               logicalLimit,
		MaxBytesPerPullResponse:         logicalLimit + maxCompactSingleRowEnvelopeBytes,
		MaxConcurrentHistoryProjections: 1,
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))

	adopted := loadCommittedBundleForUser(t, ctx, service, "adopt", 1)
	require.LessOrEqual(t, adopted.ByteCount, int64(logicalLimit))
	requirePayloadFieldBase64(t, adopted.Rows[0].Payload, "data", fileData)

	payloadExpression := databaseWirePayloadExpression("src", service.columnTypesForTable(schemaName, "files"))
	var physicalBytes int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT octet_length((%s)::text) FROM %s.files AS src WHERE id=$1`, payloadExpression, pgx.Identifier{schemaName}.Sanitize()), adoptedID).Scan(&physicalBytes))
	require.Greater(t, physicalBytes, int64(logicalLimit), "fixture must exercise PostgreSQL hex expansion beyond the logical ceiling")
	physicalLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, service.columnTypesForTable(schemaName, "files"))
	require.NoError(t, err)
	require.LessOrEqual(t, physicalBytes, physicalLimit)

	actor := Actor{UserID: "push", SourceID: "device"}
	pushID := uuid.New()
	pushRow := PushRequestRow{
		Schema: schemaName, Table: "files", Key: SyncKey{"id": pushID.String()}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"p","data":"%s"}`, pushID, base64.StdEncoding.EncodeToString(fileData))),
	}
	proof := testSchemaGenerationProofForService(service)
	requestHash, err := computeGenerationPinnedPushRequestHash(proof, []PushRequestRow{pushRow})
	require.NoError(t, err)
	_, requestBytes, err := protocolhash.GenerationPinnedPushRequest(canonicalSchemaGenerationID(proof.ID), proof.Fingerprint, []protocolhash.PushRow{{
		Schema: pushRow.Schema, Table: pushRow.Table, Key: pushRow.Key, Op: pushRow.Op, BaseRowVersion: pushRow.BaseRowVersion, Payload: pushRow.Payload,
	}})
	require.NoError(t, err)
	require.LessOrEqual(t, requestBytes, int64(logicalLimit))
	initializationID := resolveConnectForPush(t, ctx, service, actor, true)
	committed, err := service.PushOneShot(ctx, actor, proof, &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, InitializationID: initializationID, Rows: []PushRequestRow{pushRow},
	})
	require.NoError(t, err)
	require.NoError(t, service.Close(ctx))

	restarted := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	committed, err = restarted.PushOneShot(ctx, actor, testSchemaGenerationProofForService(restarted), &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, InitializationID: initializationID, Rows: []PushRequestRow{pushRow},
	})
	require.NoError(t, err)
	require.LessOrEqual(t, committed.Bundle.ByteCount, int64(logicalLimit))

	trustedActor := Actor{UserID: "trusted"}
	mustInitializeEmptyScope(t, ctx, restarted, trustedActor.UserID, "server")
	trustedID := uuid.New()
	err = restarted.WithinSyncBundle(ctx, trustedActor, BundleSource{SourceID: "server", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.files (id,name,data) VALUES ($1,$2,$3)`, pgx.Identifier{schemaName}.Sanitize()), trustedID, "t", fileData)
		return execErr
	})
	require.NoError(t, err)
	trusted := loadCommittedBundleForUser(t, ctx, restarted, trustedActor.UserID, 1)
	require.LessOrEqual(t, trusted.ByteCount, int64(logicalLimit))
	requirePayloadFieldBase64(t, trusted.Rows[0].Payload, "data", fileData)
}

func TestBinaryWire_JSONBExpansionDoesNotConsumeLogicalBundleBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "jsonb_limit_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.items (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			document JSONB NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	const logicalLimit = 4096
	innerJSON := "[" + strings.TrimSuffix(strings.Repeat("1e-100,", 100), ",") + "]"
	require.Len(t, innerJSON, 701)
	rowID := uuid.New()
	physicalExpression := databaseWirePayloadExpression("src", map[string]string{"document": "jsonb", "id": "uuid"})
	var physicalBytes int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		WITH src AS (
			SELECT 'measure'::text AS _sync_scope_id, $1::uuid AS id, $2::jsonb AS document
		)
		SELECT octet_length((%s)::text) FROM src
	`, physicalExpression), rowID, innerJSON).Scan(&physicalBytes))
	require.Greater(t, physicalBytes, int64(logicalLimit*2), "fixture must exceed the former physical multiplier")
	var worstBinary64PhysicalBytes int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT octet_length(to_jsonb('-5e-324'::numeric)::text)`).Scan(&worstBinary64PhysicalBytes))
	require.LessOrEqual(t, worstBinary64PhysicalBytes, maxPostgresBinary64ExpandedNumberBytes)
	physicalLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit, map[string]string{"document": "jsonb", "id": "uuid"})
	require.NoError(t, err)
	require.LessOrEqual(t, physicalBytes, physicalLimit)

	registered := []RegisteredTable{{Schema: schemaName, Table: "items", SyncKeyColumns: []string{"id"}}}
	config := &ServiceConfig{
		SchemaGenerations:               testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
		RegisteredTables:                registered,
		RegisteredTableCatalogRevision:  1,
		MaxRowsPerBundle:                8,
		MaxBytesPerBundle:               logicalLimit,
		MaxBytesPerPullResponse:         logicalLimit + maxCompactSingleRowEnvelopeBytes,
		MaxConcurrentHistoryProjections: 1,
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	proof := testSchemaGenerationProofForService(service)
	payload := json.RawMessage(fmt.Sprintf(`{"document":%q,"id":"%s"}`, innerJSON, rowID))
	pushRow := PushRequestRow{Schema: schemaName, Table: "items", Key: SyncKey{"id": rowID.String()}, Op: OpInsert, Payload: payload}
	requestHash, err := computeGenerationPinnedPushRequestHash(proof, []PushRequestRow{pushRow})
	require.NoError(t, err)
	_, requestBytes, err := protocolhash.GenerationPinnedPushRequest(canonicalSchemaGenerationID(proof.ID), proof.Fingerprint, []protocolhash.PushRow{{
		Schema: pushRow.Schema, Table: pushRow.Table, Key: pushRow.Key, Op: pushRow.Op, Payload: pushRow.Payload,
	}})
	require.NoError(t, err)
	require.LessOrEqual(t, requestBytes, int64(logicalLimit))

	actor := Actor{UserID: "push", SourceID: "device"}
	initializationID := resolveConnectForPush(t, ctx, service, actor, true)
	committed, err := service.PushOneShot(ctx, actor, proof, &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, InitializationID: initializationID, Rows: []PushRequestRow{pushRow},
	})
	require.NoError(t, err)
	require.NoError(t, service.Close(ctx))

	restarted := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	committed, err = restarted.PushOneShot(ctx, actor, testSchemaGenerationProofForService(restarted), &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, InitializationID: initializationID, Rows: []PushRequestRow{pushRow},
	})
	require.NoError(t, err)
	require.LessOrEqual(t, committed.Bundle.ByteCount, int64(logicalLimit))
	committedBundle := loadCommittedBundleForUser(t, ctx, restarted, actor.UserID, committed.Bundle.BundleSeq)
	require.JSONEq(t, fmt.Sprintf(`{"document":%q,"id":"%s"}`, innerJSON, rowID), string(committedBundle.Rows[0].Payload))
	_, err = pushRowsOneShot(t, ctx, restarted, Actor{UserID: actor.UserID, SourceID: "conflict-device"}, 1, []PushRequestRow{{
		Schema: schemaName, Table: "items", Key: SyncKey{"id": rowID.String()}, Op: OpUpdate, BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(`{"document":"[]","id":"%s"}`, rowID)),
	}})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	require.NotNil(t, conflict.Conflict)
	require.JSONEq(t, fmt.Sprintf(`{"document":%q,"id":"%s"}`, innerJSON, rowID), string(conflict.Conflict.ServerRow))

	trustedActor := Actor{UserID: "trusted"}
	mustInitializeEmptyScope(t, ctx, restarted, trustedActor.UserID, "server")
	trustedID := uuid.New()
	err = restarted.WithinSyncBundle(ctx, trustedActor, BundleSource{SourceID: "server", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.items (id,document) VALUES ($1,$2::jsonb)`, schemaIdent), trustedID, innerJSON)
		return execErr
	})
	require.NoError(t, err)
	trusted := loadCommittedBundleForUser(t, ctx, restarted, trustedActor.UserID, 1)
	require.LessOrEqual(t, trusted.ByteCount, int64(logicalLimit))
	require.JSONEq(t, fmt.Sprintf(`{"document":%q,"id":"%s"}`, innerJSON, trustedID), string(trusted.Rows[0].Payload))
	require.NoError(t, restarted.Close(ctx))

	require.NoError(t, resetTestSyncSchema(ctx, pool))
	adoptedID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.items (_sync_scope_id,id,document) VALUES ('adopted',$1,$2::jsonb)`, schemaIdent), adoptedID, innerJSON)
	require.NoError(t, err)
	adoptedService := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	adopted := loadCommittedBundleForUser(t, ctx, adoptedService, "adopted", 1)
	require.LessOrEqual(t, adopted.ByteCount, int64(logicalLimit))
	require.JSONEq(t, fmt.Sprintf(`{"document":%q,"id":"%s"}`, innerJSON, adoptedID), string(adopted.Rows[0].Payload))
}

func TestBinaryWire_JSONBRetainedScaleDoesNotConsumeLogicalBundleBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "jsonb_scale_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.items (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			document JSONB NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		);
		CREATE FUNCTION %s.force_retained_scale() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			NEW.document := ('[' || array_to_string(array_fill('1.' || repeat('0', 16000), ARRAY[16]), ',') || ']')::jsonb;
			RETURN NEW;
		END $$;
		CREATE TRIGGER force_retained_scale BEFORE INSERT ON %s.items
		FOR EACH ROW EXECUTE FUNCTION %s.force_retained_scale()
	`, schemaIdent, schemaIdent, schemaIdent, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	const logicalLimit = 512
	canonicalInner := "[" + strings.TrimSuffix(strings.Repeat("1,", 16), ",") + "]"
	require.Len(t, canonicalInner, 33)
	rowID := uuid.New()
	physicalExpression := databaseWirePayloadExpression("src", map[string]string{"document": "jsonb", "id": "uuid"})
	var physicalBytes int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		WITH src AS (
			SELECT 'measure'::text AS _sync_scope_id, $1::uuid AS id,
				('[' || array_to_string(array_fill('1.' || repeat('0', 16000), ARRAY[16]), ',') || ']')::jsonb AS document
		)
		SELECT octet_length((%s)::text) FROM src
	`, physicalExpression), rowID).Scan(&physicalBytes))
	require.Greater(t, physicalBytes, int64(logicalLimit*maxPostgresPayloadBytesPerLogicalByte))

	registered := []RegisteredTable{{Schema: schemaName, Table: "items", SyncKeyColumns: []string{"id"}}}
	config := &ServiceConfig{
		SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), RegisteredTables: registered,
		RegisteredTableCatalogRevision: 1, MaxRowsPerBundle: 8, MaxBytesPerBundle: logicalLimit,
		MaxBytesPerPullResponse: 2048, MaxConcurrentHistoryProjections: 1,
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	proof := testSchemaGenerationProofForService(service)
	payload := json.RawMessage(fmt.Sprintf(`{"document":%q,"id":"%s"}`, canonicalInner, rowID))
	pushRow := PushRequestRow{Schema: schemaName, Table: "items", Key: SyncKey{"id": rowID.String()}, Op: OpInsert, Payload: payload}
	requestHash, err := computeGenerationPinnedPushRequestHash(proof, []PushRequestRow{pushRow})
	require.NoError(t, err)
	_, requestBytes, err := protocolhash.GenerationPinnedPushRequest(canonicalSchemaGenerationID(proof.ID), proof.Fingerprint, []protocolhash.PushRow{{
		Schema: pushRow.Schema, Table: pushRow.Table, Key: pushRow.Key, Op: pushRow.Op, Payload: pushRow.Payload,
	}})
	require.NoError(t, err)
	require.LessOrEqual(t, requestBytes, int64(logicalLimit))
	actor := Actor{UserID: "push", SourceID: "device"}
	initializationID := resolveConnectForPush(t, ctx, service, actor, true)
	committed, err := service.PushOneShot(ctx, actor, proof, &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, InitializationID: initializationID, Rows: []PushRequestRow{pushRow},
	})
	require.NoError(t, err)
	require.NoError(t, service.Close(ctx))

	restarted := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	committed, err = restarted.PushOneShot(ctx, actor, testSchemaGenerationProofForService(restarted), &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, InitializationID: initializationID, Rows: []PushRequestRow{pushRow},
	})
	require.NoError(t, err)
	require.LessOrEqual(t, committed.Bundle.ByteCount, int64(logicalLimit))
	committedBundle := loadCommittedBundleForUser(t, ctx, restarted, actor.UserID, committed.Bundle.BundleSeq)
	require.JSONEq(t, fmt.Sprintf(`{"document":%q,"id":"%s"}`, canonicalInner, rowID), string(committedBundle.Rows[0].Payload))
	snapshot, err := restarted.CreateSnapshotSession(ctx, actor, testSchemaGenerationProofForService(restarted))
	require.NoError(t, err)
	require.Equal(t, int64(1), snapshot.RowCount)
	snapshotChunk, err := restarted.GetSnapshotChunk(ctx, actor, testSchemaGenerationProofForService(restarted), snapshot.SnapshotID, 0, 10, 2048)
	require.NoError(t, err)
	require.Len(t, snapshotChunk.Rows, 1)
	require.JSONEq(t, string(committedBundle.Rows[0].Payload), string(snapshotChunk.Rows[0].Payload))

	_, err = pushRowsOneShot(t, ctx, restarted, Actor{UserID: actor.UserID, SourceID: "conflict-device"}, 1, []PushRequestRow{{
		Schema: schemaName, Table: "items", Key: SyncKey{"id": rowID.String()}, Op: OpUpdate, BaseRowVersion: 0, Payload: payload,
	}})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	require.JSONEq(t, string(committedBundle.Rows[0].Payload), string(conflict.Conflict.ServerRow))

	trustedActor := Actor{UserID: "trusted"}
	mustInitializeEmptyScope(t, ctx, restarted, trustedActor.UserID, "server")
	trustedID := uuid.New()
	err = restarted.WithinSyncBundle(ctx, trustedActor, BundleSource{SourceID: "server", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.items (id,document) VALUES ($1,$2::jsonb)`, schemaIdent), trustedID, canonicalInner)
		return execErr
	})
	require.NoError(t, err)
	require.LessOrEqual(t, loadCommittedBundleForUser(t, ctx, restarted, trustedActor.UserID, 1).ByteCount, int64(logicalLimit))
	require.NoError(t, restarted.Close(ctx))

	require.NoError(t, resetTestSyncSchema(ctx, pool))
	adoptedID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.items (_sync_scope_id,id,document) VALUES ('adopted',$1,$2::jsonb)`, schemaIdent), adoptedID, canonicalInner)
	require.NoError(t, err)
	adoptedService := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	require.LessOrEqual(t, loadCommittedBundleForUser(t, ctx, adoptedService, "adopted", 1).ByteCount, int64(logicalLimit))
	require.NoError(t, adoptedService.Close(ctx))
}
