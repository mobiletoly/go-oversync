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

	bundle, err := pushRowsViaSession(t, ctx, svc, actor, sourceBundleID, []PushRequestRow{{
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

func TestBinaryWire_ProcessPushAcceptedBundleCanonicalizesByteaPayload(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_binary_wire_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-binary-wire-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
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

	var storedData string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT payload->>'data'
		FROM sync.bundle_rows
		WHERE user_id = $1 AND bundle_seq = $2 AND schema_name = $3 AND table_name = 'files'
	`, userID, bundle.BundleSeq, schemaName).Scan(&storedData))
	require.True(t, strings.HasPrefix(storedData, "\\x"), "expected internal bundle_rows payload to remain postgres bytea text, got %q", storedData)
}

func TestBinaryWire_ProcessPullCanonicalizesByteaPayload(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_binary_wire_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "pull-binary-wire-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "pull-binary-wire-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	fileID := uuid.New()
	fileData := []byte("pull binary payload")

	bundle := mustPushFileBundle(t, ctx, svc, writer, schemaName, 1, fileID, "pull.bin", fileData)

	page, err := svc.ProcessPull(ctx, reader, 0, 10, 0)
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

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-binary-wire-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "snapshot-binary-wire-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	fileID := uuid.New()
	fileData := []byte("snapshot binary payload")

	bundle := mustPushFileBundle(t, ctx, svc, writer, schemaName, 1, fileID, "snapshot.bin", fileData)

	session, err := svc.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, bundle.BundleSeq, session.SnapshotBundleSeq)

	chunk, err := svc.GetSnapshotChunk(ctx, reader, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, "files", chunk.Rows[0].Table)
	requirePayloadFieldBase64(t, chunk.Rows[0].Payload, "data", fileData)
}
