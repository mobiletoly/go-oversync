package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestGenerationWire_PostgresExactDomainsAcrossAuthoritativeSurfaces(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "generation_exact_" + suffix
	tableIdent := createGenerationExactDomainTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	registered := []RegisteredTable{{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		AppName:                        "generation-exact-domains",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
		SchemaGenerations:              testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
	}, integrationTestLogger(slog.LevelWarn))

	userID := "generation-exact-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	rowID := "row-one"
	maxDecimal := strings.Repeat("9", 1000)
	maxDepthJSON := strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65)
	requestPayload := generationExactPayload(t, rowID, "null", maxDepthJSON, nil, maxDecimal)

	bundle, err := pushRowsOneShot(t, ctx, service, writer, 1, []PushRequestRow{{
		Schema:  schemaName,
		Table:   "records",
		Key:     SyncKey{"id": rowID},
		Op:      OpInsert,
		Payload: requestPayload,
	}})
	require.NoError(t, err)
	require.Len(t, bundle.Rows, 1)
	requireGenerationExactPayload(t, bundle.Rows[0].Payload, rowID, "null", maxDepthJSON, nil, maxDecimal)

	var jsonIsNull, jsonbIsNull, optionalIsNull bool
	var jsonText, jsonbText, numericText string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT document_json IS NULL, document_json::text,
			document_jsonb IS NULL, document_jsonb::text,
			optional_document IS NULL, amount::text
		FROM %s WHERE _sync_scope_id = $1 AND id = $2
	`, tableIdent), userID, rowID).Scan(&jsonIsNull, &jsonText, &jsonbIsNull, &jsonbText, &optionalIsNull, &numericText))
	require.False(t, jsonIsNull)
	require.Equal(t, "null", jsonText)
	require.False(t, jsonbIsNull)
	require.Equal(t, maxDepthJSON, jsonbText)
	require.True(t, optionalIsNull)
	require.Equal(t, maxDecimal, numericText)

	// A successful one-shot push returns the complete generation-visible
	// authoritative effect; no separate committed-row retrieval exists.
	requireGenerationExactPayload(t, bundle.Rows[0].Payload, rowID, "null", maxDepthJSON, nil, maxDecimal)

	proof := testSchemaGenerationProofForService(service)
	_, err = service.Connect(ctx, reader, proof, &ConnectRequest{})
	require.NoError(t, err)
	pull, err := service.ProcessPull(ctx, reader, proof, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	requireGenerationExactPayload(t, pull.Bundles[0].Rows[0].Payload, rowID, "null", maxDepthJSON, nil, maxDecimal)

	snapshot, err := service.CreateSnapshotSession(ctx, reader, proof)
	require.NoError(t, err)
	chunk, err := service.GetSnapshotChunk(ctx, reader, proof, snapshot.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	requireGenerationExactPayload(t, chunk.Rows[0].Payload, rowID, "null", maxDepthJSON, nil, maxDecimal)

	_, err = pushRowsOneShot(t, ctx, service, Actor{UserID: userID, SourceID: "conflicting-writer"}, 1, []PushRequestRow{{
		Schema:  schemaName,
		Table:   "records",
		Key:     SyncKey{"id": rowID},
		Op:      OpInsert,
		Payload: requestPayload,
	}})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	requireGenerationExactPayload(t, conflict.Conflict.ServerRow, rowID, "null", maxDepthJSON, nil, maxDecimal)

	directID := "row-direct"
	directActor := Actor{UserID: userID, SourceID: "server-app"}
	require.NoError(t, service.WithinSyncBundle(ctx, directActor, BundleSource{SourceID: directActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, document_json, document_jsonb, optional_document, amount)
			VALUES ($1, 'null'::json, 'null'::jsonb, NULL, $2::numeric)
		`, tableIdent), directID, maxDecimal)
		return err
	}))
	var directBundleSeq int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT max(bundle_seq) FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&directBundleSeq))
	directBundle := loadCommittedBundleForUser(t, ctx, service, userID, directBundleSeq)
	require.Len(t, directBundle.Rows, 1)
	requireGenerationExactPayload(t, directBundle.Rows[0].Payload, directID, "null", "null", nil, maxDecimal)
}

func TestGenerationWire_PopulatedAdoptionPreservesJSONNullAndLargeDecimal(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "generation_adopt_" + suffix
	tableIdent := createGenerationExactDomainTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	userID := "generation-adopt-user-" + suffix
	maxDecimal := strings.Repeat("8", 1000)
	maxDepthJSON := strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65)
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, document_json, document_jsonb, optional_document, amount)
		VALUES ($1, 'adopted', 'null'::json, $3::jsonb, NULL, $2::numeric)
	`, tableIdent), userID, maxDecimal, maxDepthJSON)
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		AppName:                        "generation-exact-adoption",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
		SchemaGenerations:              testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
	}, integrationTestLogger(slog.LevelWarn))
	actor := Actor{UserID: userID, SourceID: "reader"}
	_, err = service.Connect(ctx, actor, testSchemaGenerationProofForService(service), &ConnectRequest{})
	require.NoError(t, err)
	pull, err := service.ProcessPull(ctx, actor, testSchemaGenerationProofForService(service), 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	requireGenerationExactPayload(t, pull.Bundles[0].Rows[0].Payload, "adopted", "null", maxDepthJSON, nil, maxDecimal)
}

func TestGenerationWire_HTTPRejectsMalformedCompactKeysBeforePush(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "generation_http_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "records"}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %s;
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id TEXT COLLATE "C" NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent, tableIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	registered := []RegisteredTable{{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		AppName:                        "generation-http-structure",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
		SchemaGenerations:              testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil),
	}, integrationTestLogger(slog.LevelWarn))
	actor := Actor{UserID: "generation-http-user-" + suffix, SourceID: "writer"}
	handlers := NewHTTPSyncHandlers(service, slog.Default())

	malformed := []struct {
		name      string
		keyValues []byte
	}{
		{name: "unpaired high surrogate", keyValues: []byte(`["\ud800"]`)},
		{name: "unpaired low surrogate", keyValues: []byte(`["\udc00"]`)},
		{name: "duplicate target", keyValues: []byte(`["duplicate"]`)},
		{name: "U+0000", keyValues: []byte(`["a\u0000b"]`)},
		{name: "invalid UTF-8", keyValues: []byte{'[', '"', 0xff, '"', ']'}},
	}
	for _, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			body := append([]byte(`{"source_bundle_id":1,"canonical_request_hash":"`+strings.Repeat("0", 64)+`","rows":[[0,0,[["i",`), test.keyValues...)
			body = append(body, []byte(`,0,{"value":"hello"}]]]]}`)...)
			if test.name == "duplicate target" {
				body = []byte(`{"source_bundle_id":1,"canonical_request_hash":"` + strings.Repeat("0", 64) + `","rows":[[0,0,[["i",["duplicate"],0,{"value":"hello"}],["i",["duplicate"],0,{"value":"hello"}]]]]}`)
			}
			req := httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/json")
			req = req.WithContext(ContextWithActor(ctx, actor))
			setTestSchemaGenerationHeaders(req, service)
			recorder := httptest.NewRecorder()
			handlers.HandlePush(recorder, req)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			var records int
			require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, tableIdent)).Scan(&records))
			require.Zero(t, records)
		})
	}
}

func TestGenerationWire_WideJSONTablesAcrossSupportedPostgresVersions(t *testing.T) {
	for _, image := range []string{"postgres:16", "postgres:17.10"} {
		t.Run(image, func(t *testing.T) {
			server, err := startManagedIntegrationPostgres(image)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.close()) })

			for _, jsonColumnCount := range []int{50, 51} {
				t.Run(fmt.Sprintf("columns_%d", jsonColumnCount), func(t *testing.T) {
					testGenerationWideJSONTable(t, server, jsonColumnCount)
				})
			}
		})
	}
}

func testGenerationWideJSONTable(t *testing.T, server *managedIntegrationPostgresServer, jsonColumnCount int) {
	t.Helper()
	ctx := context.Background()
	pool := newIntegrationTestPoolForServer(t, ctx, server)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := fmt.Sprintf("generation_wide_%d_%s", jsonColumnCount, suffix)
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "records"}.Sanitize()

	columnDefinitions := make([]string, 0, jsonColumnCount)
	insertColumns := []string{"_sync_scope_id", "id"}
	insertValues := []string{"$1", "$2"}
	insertArgs := []any{"wide-user-" + suffix, "adopted"}
	generationColumns := []GenerationColumn{{Name: "id", WireType: GenerationWireTypeText}}
	for index := 0; index < jsonColumnCount; index++ {
		columnName := fmt.Sprintf("j%02d", index)
		postgresType := "json"
		if index%2 == 1 {
			postgresType = "jsonb"
		}
		columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s %s", pgx.Identifier{columnName}.Sanitize(), postgresType))
		insertColumns = append(insertColumns, pgx.Identifier{columnName}.Sanitize())
		insertValues = append(insertValues, fmt.Sprintf("$%d::%s", index+3, postgresType))
		generationColumns = append(generationColumns, GenerationColumn{Name: columnName, WireType: GenerationWireTypeJSON, Nullable: true})
		switch index % 3 {
		case 0:
			insertArgs = append(insertArgs, nil)
		case 1:
			insertArgs = append(insertArgs, "null")
		default:
			insertArgs = append(insertArgs, `"null"`)
		}
	}

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %s;
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id TEXT COLLATE "C" NOT NULL,
			%s,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent, tableIdent, strings.Join(columnDefinitions, ",\n")))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (%s) VALUES (%s)
	`, tableIdent, strings.Join(insertColumns, ", "), strings.Join(insertValues, ", ")), insertArgs...)
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		AppName:                        "generation-wide-json",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
		SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{
			Schema: schemaName, Table: "records", Columns: generationColumns,
		}}}},
	}, integrationTestLogger(slog.LevelWarn))
	proof := testSchemaGenerationProofForService(service)
	userID := insertArgs[0].(string)
	reader := Actor{UserID: userID, SourceID: "reader"}
	_, err = service.Connect(ctx, reader, proof, &ConnectRequest{})
	require.NoError(t, err)

	pull, err := service.ProcessPull(ctx, reader, proof, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	requireWideJSONPayload(t, pull.Bundles[0].Rows[0].Payload, jsonColumnCount)

	populatedSnapshot, err := service.CreateSnapshotSession(ctx, reader, proof)
	require.NoError(t, err)
	require.Equal(t, int64(1), populatedSnapshot.RowCount)
	chunk, err := service.GetSnapshotChunk(ctx, reader, proof, populatedSnapshot.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	requireWideJSONPayload(t, chunk.Rows[0].Payload, jsonColumnCount)

	emptyReader := Actor{UserID: "wide-empty-" + suffix, SourceID: "reader"}
	_, err = service.Connect(ctx, emptyReader, proof, &ConnectRequest{})
	require.NoError(t, err)
	emptySnapshot, err := service.CreateSnapshotSession(ctx, emptyReader, proof)
	require.NoError(t, err)
	require.Zero(t, emptySnapshot.RowCount)

	_, err = pushRowsOneShot(t, ctx, service, Actor{UserID: userID, SourceID: "conflicting-writer"}, 1, []PushRequestRow{{
		Schema:  schemaName,
		Table:   "records",
		Key:     SyncKey{"id": "adopted"},
		Op:      OpInsert,
		Payload: json.RawMessage(`{"id":"adopted"}`),
	}})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	require.NotNil(t, conflict.Conflict)
	requireWideJSONPayload(t, conflict.Conflict.ServerRow, jsonColumnCount)

	var committedBundles int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sync.bundle_log AS bundles
		JOIN sync.user_state AS users ON users.user_pk = bundles.user_pk
		WHERE users.user_id = $1
	`, userID).Scan(&committedBundles))
	require.Equal(t, 1, committedBundles, "the conflicting wide-row write must roll back without another bundle")
}

func requireWideJSONPayload(t *testing.T, payload json.RawMessage, jsonColumnCount int) {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &fields))
	require.Len(t, fields, jsonColumnCount+1)
	require.Equal(t, `"adopted"`, string(fields["id"]))
	for index := 0; index < jsonColumnCount; index++ {
		columnName := fmt.Sprintf("j%02d", index)
		switch index % 3 {
		case 0:
			require.Equal(t, "null", string(fields[columnName]), columnName)
		case 1:
			require.Equal(t, `"null"`, string(fields[columnName]), columnName)
		default:
			require.Equal(t, `"\"null\""`, string(fields[columnName]), columnName)
		}
	}
}

func createGenerationExactDomainTable(t *testing.T, ctx context.Context, pool interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, schemaName string) string {
	t.Helper()
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "records"}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %s;
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id TEXT COLLATE "C" NOT NULL,
			document_json JSON NOT NULL,
			document_jsonb JSONB NOT NULL,
			optional_document JSONB,
			amount NUMERIC NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent, tableIdent))
	require.NoError(t, err)
	return tableIdent
}

func generationExactPayload(t *testing.T, id, documentJSON, documentJSONB string, optionalDocument *string, amount string) json.RawMessage {
	t.Helper()
	fields := map[string]any{
		"id":                id,
		"document_json":     documentJSON,
		"document_jsonb":    documentJSONB,
		"optional_document": optionalDocument,
		"amount":            amount,
	}
	payload, err := json.Marshal(fields)
	require.NoError(t, err)
	return payload
}

func requireGenerationExactPayload(t *testing.T, payload json.RawMessage, id, documentJSON, documentJSONB string, optionalDocument *string, amount string) {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &fields))
	require.Equal(t, fmt.Sprintf("%q", id), string(fields["id"]))
	require.Equal(t, mustJSONString(t, documentJSON), string(fields["document_json"]))
	require.Equal(t, mustJSONString(t, documentJSONB), string(fields["document_jsonb"]))
	if optionalDocument == nil {
		require.Equal(t, "null", string(fields["optional_document"]))
	} else {
		require.Equal(t, mustJSONString(t, *optionalDocument), string(fields["optional_document"]))
	}
	require.Equal(t, mustJSONString(t, amount), string(fields["amount"]))
}

func mustJSONString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}
