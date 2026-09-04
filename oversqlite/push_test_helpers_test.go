// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

var mockGenerationByFingerprint sync.Map // map[string]oversync.SchemaGenerationCapabilities

func registerMockGeneration(generation oversync.SchemaGenerationCapabilities) {
	if generation.Fingerprint == "" {
		return
	}
	mockGenerationByFingerprint.Store(generation.Fingerprint, generation)
}

// Engine fixtures already provide an admitted contract. Discovery tests use the
// public generation-only configuration and production network admission path.
func admittedFixtureConfig(schema string, tables []SyncTable, proof oversync.SchemaGenerationProof) *Config {
	config := DefaultConfig(0, tables)
	config.schema, config.generationProof = schema, proof
	return config
}

func newBundleClient(t *testing.T, schema string, tables []SyncTable, ddl ...string) (*Client, *sql.DB) {
	t.Helper()
	config := admittedFixtureConfig(schema, tables, testSchemaGenerationProof())
	config.RetryPolicy = newTestRetryPolicy()
	return newBundleClientWithConfig(t, config, ddl...)
}

func newBundleClientWithConfig(t *testing.T, config *Config, ddl ...string) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}
	config = testConfigWithLocalContract(t, db, config)

	client, err := newAdmittedFixtureClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	attachTestClient(t, client, "bundle-user", "bundle-device")
	return client, db
}

func newTestClient(t *testing.T, db *sql.DB, baseURL string, tok func(context.Context) (string, error), config *Config) (*Client, error) {
	t.Helper()
	if config != nil && config.localContract == nil {
		config = testConfigWithLocalContract(t, db, config)
	}
	return newAdmittedFixtureClient(db, baseURL, tok, config)
}

func newOwnedTestClient(t *testing.T, db *sql.DB, baseURL string, tok func(context.Context) (string, error), config *Config) (*Client, error) {
	t.Helper()
	if config != nil && config.localContract == nil {
		config = testConfigWithLocalContract(t, db, config)
	}
	return newOwnedAdmittedFixtureClient(db, baseURL, tok, config)
}

func testConfigWithLocalContract(t *testing.T, db *sql.DB, config *Config) *Config {
	t.Helper()
	if config == nil || config.localContract != nil {
		return config
	}
	clone := cloneCoordinatorClientConfig(config)
	registered := make([]oversync.RegisteredTable, 0, len(clone.Tables))
	generation := oversync.SchemaGeneration{ID: clone.generationProof.ID}
	localColumns := make([]LocalTableColumns, 0, len(clone.Tables))
	for _, table := range clone.Tables {
		keys, err := normalizedSyncKeyColumns(table)
		require.NoError(t, err)
		registered = append(registered, oversync.RegisteredTable{Schema: clone.schema, Table: table.TableName, SyncKeyColumns: keys})
		foreignKeys := testGenerationForeignKeys(t, db, clone.schema, table.TableName)
		foreignKeyColumns := map[string]struct{}{}
		for _, foreignKey := range foreignKeys {
			for _, column := range foreignKey.Columns {
				foreignKeyColumns[column] = struct{}{}
			}
		}
		rows, err := db.Query(`PRAGMA table_info(` + quoteIdent(table.TableName) + `)`)
		require.NoError(t, err)
		var generationColumns []oversync.GenerationColumn
		var codecs []LocalColumnContract
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, declaredType string
			var defaultValue any
			require.NoError(t, rows.Scan(&cid, &name, &declaredType, &notNull, &defaultValue, &primaryKey))
			_, foreignKeyColumn := foreignKeyColumns[strings.ToLower(name)]
			wire, codec := testDeclaredCodec(declaredType, primaryKey != 0 || foreignKeyColumn)
			generationColumns = append(generationColumns, oversync.GenerationColumn{Name: strings.ToLower(name), WireType: wire, Nullable: notNull == 0})
			codecs = append(codecs, LocalColumnContract{Name: strings.ToLower(name), Codec: codec})
		}
		require.NoError(t, rows.Close())
		generation.Tables = append(generation.Tables, oversync.GenerationTable{Schema: clone.schema, Table: table.TableName, Columns: generationColumns, ForeignKeys: foreignKeys})
		localColumns = append(localColumns, LocalTableColumns{Schema: clone.schema, Table: table.TableName, Columns: codecs})
	}
	managed := make(map[string]struct{}, len(clone.Tables))
	for _, table := range clone.Tables {
		managed[table.TableName] = struct{}{}
	}
	for _, table := range clone.LocalOnlyTables {
		managed[table] = struct{}{}
	}
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	require.NoError(t, err)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		if _, ok := managed[name]; !ok && !strings.HasPrefix(name, "_sync_") {
			clone.LocalOnlyTables = append(clone.LocalOnlyTables, name)
		}
	}
	require.NoError(t, rows.Close())
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	compiled, ok := registry.Generation(generation.ID)
	require.True(t, ok)
	clone.generationProof = oversync.SchemaGenerationProof{ID: generation.ID, Fingerprint: compiled.Fingerprint()}
	contract := LocalGenerationContract{Proof: clone.generationProof, RegisteredTables: registered, Generation: generation, Tables: clone.Tables, Columns: localColumns}
	clone.localContract = &contract
	return clone
}

func testDeclaredCodec(declaredType string, primaryKey bool) (oversync.GenerationWireType, LocalColumnCodec) {
	typeName := strings.ToUpper(declaredType)
	switch {
	case strings.Contains(typeName, "BLOB"):
		if primaryKey {
			return oversync.GenerationWireTypeUUID, LocalCodecUUIDBlob
		}
		return oversync.GenerationWireTypeBytes, LocalCodecBytesBlob
	case strings.Contains(typeName, "INT"):
		return oversync.GenerationWireTypeInt64, LocalCodecInt64Integer
	case strings.Contains(typeName, "REAL"), strings.Contains(typeName, "FLOA"), strings.Contains(typeName, "DOUB"):
		return oversync.GenerationWireTypeFloat64, LocalCodecFloat64Real
	default:
		return oversync.GenerationWireTypeText, LocalCodecText
	}
}

func testGenerationForeignKeys(t *testing.T, db *sql.DB, schema, table string) []oversync.GenerationForeignKey {
	t.Helper()
	rows, err := db.Query(`PRAGMA foreign_key_list(` + quoteIdent(table) + `)`)
	require.NoError(t, err)
	type group struct {
		fk    oversync.GenerationForeignKey
		parts map[int][2]string
	}
	groups := map[int]*group{}
	var order []int
	for rows.Next() {
		var id, seq int
		var parent, from, to, onUpdate, onDelete, match string
		require.NoError(t, rows.Scan(&id, &seq, &parent, &from, &to, &onUpdate, &onDelete, &match))
		g := groups[id]
		if g == nil {
			g = &group{fk: oversync.GenerationForeignKey{ReferencedSchema: schema, ReferencedTable: parent, OnDelete: testFKAction(onDelete), OnUpdate: testFKAction(onUpdate)}, parts: map[int][2]string{}}
			groups[id] = g
			order = append(order, id)
		}
		g.parts[seq] = [2]string{from, to}
	}
	require.NoError(t, rows.Close())
	result := make([]oversync.GenerationForeignKey, 0, len(order))
	for _, id := range order {
		g := groups[id]
		for seq := 0; seq < len(g.parts); seq++ {
			g.fk.Columns = append(g.fk.Columns, g.parts[seq][0])
			g.fk.ReferencedColumns = append(g.fk.ReferencedColumns, g.parts[seq][1])
		}
		result = append(result, g.fk)
	}
	return result
}

func testFKAction(action string) oversync.GenerationForeignKeyAction {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return oversync.GenerationForeignKeyCascade
	case "RESTRICT":
		return oversync.GenerationForeignKeyRestrict
	case "SET NULL":
		return oversync.GenerationForeignKeySetNull
	default:
		return oversync.GenerationForeignKeyNoAction
	}
}

func decodeDirtyPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &payload))
	for name, value := range payload {
		entry, ok := value.(map[string]any)
		require.True(t, ok, "typed capture for %s", name)
		require.Len(t, entry, 2)
		require.Contains(t, entry, "storage")
		require.Contains(t, entry, "value")
		payload[name] = entry["value"]
	}
	return payload
}

func rowKeyJSON(id string) string {
	raw, _ := json.Marshal(map[string]any{"id": id})
	return string(raw)
}

func mustJSONPayload(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return raw
}

func stageCommittedPushBundleRows(t *testing.T, db *sql.DB, bundleSeq int64, rows []oversync.BundleRow) {
	t.Helper()
	for idx, row := range rows {
		idValue, ok := row.Key["id"].(string)
		require.True(t, ok)

		var payload any
		if row.Op != oversync.OpDelete {
			payload = string(row.Payload)
		}
		_, err := db.Exec(`
			INSERT INTO _sync_push_stage (
				bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, bundleSeq, idx, row.Schema, row.Table, rowKeyJSON(idValue), row.Op, row.RowVersion, payload)
		require.NoError(t, err)
	}
}

func readApplyMode(t *testing.T, db *sql.DB, userID string) int {
	t.Helper()
	var applyMode int
	require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1 AND ? IS NOT NULL`, userID).Scan(&applyMode))
	return applyMode
}

func loadDirtyRowForKey(t *testing.T, db *sql.DB, keyJSON string) (string, int64, sql.NullString, bool) {
	t.Helper()

	var (
		op             string
		baseRowVersion int64
		payload        sql.NullString
	)
	err := db.QueryRow(`
		SELECT op, base_row_version, payload
		FROM _sync_dirty_rows
		WHERE table_name = 'users' AND key_json = ?
	`, keyJSON).Scan(&op, &baseRowVersion, &payload)
	if err == sql.ErrNoRows {
		return "", 0, sql.NullString{}, false
	}
	require.NoError(t, err)
	return op, baseRowVersion, payload, true
}

func loadUserForKey(t *testing.T, db *sql.DB, id string) (string, string, bool) {
	t.Helper()

	var name, email string
	err := db.QueryRow(`SELECT name, email FROM users WHERE id = ?`, id).Scan(&name, &email)
	if err == sql.ErrNoRows {
		return "", "", false
	}
	require.NoError(t, err)
	return name, email, true
}

func loadUserRowStateForKey(t *testing.T, db *sql.DB, keyJSON string) (int64, bool, bool) {
	t.Helper()

	var (
		rowVersion int64
		deletedInt int
	)
	err := db.QueryRow(`
		SELECT row_version, server_kind='deleted'
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, keyJSON).Scan(&rowVersion, &deletedInt)
	if err == sql.ErrNoRows {
		return 0, false, false
	}
	require.NoError(t, err)
	return rowVersion, deletedInt == 1, true
}

func seedSyncedUserRow(t *testing.T, client *Client, db *sql.DB, id, name string, rowVersion int64) {
	t.Helper()

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, name, strings.ToLower(name)+"@example.com")
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, client.seedFixtureRowStateInTx(context.Background(), tx, "main", "users", rowKeyJSON(id), rowVersion, false))
	require.NoError(t, tx.Commit())

	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
}

func newFixedVersionPushHook(t *testing.T, server *mockPushServer, rowVersion int64) func(*mockDirectPush) *http.Response {
	t.Helper()

	return func(session *mockDirectPush) *http.Response {
		bundleSeq := rowVersion
		rows := make([]oversync.BundleRow, 0, len(session.Rows))
		for _, row := range session.Rows {
			rows = append(rows, oversync.BundleRow{
				Schema:     row.Schema,
				Table:      row.Table,
				Key:        row.Key,
				Op:         row.Op,
				RowVersion: rowVersion,
				Payload:    row.Payload,
			})
		}
		committed := &mockCommittedBundle{
			BundleSeq:            bundleSeq,
			SourceID:             session.SourceID,
			SourceBundleID:       session.SourceBundleID,
			Rows:                 rows,
			BundleHash:           mustBundleHash(t, rows),
			ByteCount:            mustBundleByteCount(t, rows),
			CanonicalRequestHash: session.CanonicalRequestHash,
			Generation:           session.Generation,
		}
		server.nextBundleSeq = bundleSeq + 1
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponse(committed)
	}
}

type mockDirectPush struct {
	SourceID             string
	SourceBundleID       int64
	RequestRowCount      int64
	CanonicalRequestHash string
	Rows                 []oversync.PushRequestRow
	Generation           oversync.SchemaGenerationCapabilities
}

type mockCommittedBundle struct {
	BundleSeq            int64
	SourceID             string
	SourceBundleID       int64
	PlannedRowCount      int64
	Rows                 []oversync.BundleRow
	BundleHash           string
	ByteCount            int64
	CanonicalRequestHash string
	Generation           oversync.SchemaGenerationCapabilities
}

func (b *mockCommittedBundle) plannedRowCount() int64 {
	if b.PlannedRowCount > 0 {
		return b.PlannedRowCount
	}
	return int64(len(b.Rows))
}

type mockPushServer struct {
	t                 *testing.T
	nextBundleSeq     int64
	pushRequests      []oversync.CompactPushRequest
	committedBySource map[int64]*mockCommittedBundle
	committedBySeq    map[int64]*mockCommittedBundle
	pushHook          func(session *mockDirectPush) *http.Response
}

func newMockPushServer(t *testing.T) *mockPushServer {
	t.Helper()
	return &mockPushServer{
		t:                 t,
		nextBundleSeq:     1,
		committedBySource: make(map[int64]*mockCommittedBundle),
		committedBySeq:    make(map[int64]*mockCommittedBundle),
	}
}

func (s *mockPushServer) RoundTrip(r *http.Request) (*http.Response, error) {
	s.t.Helper()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/sync/push":
		requireAuthenticatedSyncHeaders(s.t, r, "")
		generation, ok := mockGenerationByFingerprint.Load(r.Header.Get(oversync.SchemaGenerationFingerprintHeader))
		require.True(s.t, ok, "registered admitted generation required for compact push mock")
		contract := generation.(oversync.SchemaGenerationCapabilities)
		var req oversync.CompactPushRequest
		require.NoError(s.t, json.NewDecoder(r.Body).Decode(&req))
		s.pushRequests = append(s.pushRequests, req)
		if committed := s.committedBySource[req.SourceBundleID]; committed != nil {
			require.Equal(s.t, committed.CanonicalRequestHash, req.CanonicalRequestHash)
			return s.compactResponse(committed), nil
		}
		rows, err := oversync.ExpandCompactPushRows(contract, req.Rows)
		require.NoError(s.t, err)
		session := &mockDirectPush{
			SourceID:             r.Header.Get(oversync.SourceIDHeader),
			SourceBundleID:       req.SourceBundleID,
			RequestRowCount:      int64(len(rows)),
			CanonicalRequestHash: req.CanonicalRequestHash,
			Rows:                 rows,
			Generation:           contract,
		}
		if s.pushHook != nil {
			return s.pushHook(session), nil
		}
		return s.defaultPushResponse(session), nil
	}

	return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
}

func (s *mockPushServer) defaultPushResponse(session *mockDirectPush) *http.Response {
	rows := make([]oversync.BundleRow, 0, len(session.Rows))
	for _, row := range session.Rows {
		rows = append(rows, oversync.BundleRow{
			Schema:     row.Schema,
			Table:      row.Table,
			Key:        row.Key,
			Op:         row.Op,
			RowVersion: s.nextBundleSeq,
			Payload:    row.Payload,
		})
	}
	committed := &mockCommittedBundle{
		BundleSeq:            s.nextBundleSeq,
		SourceID:             session.SourceID,
		SourceBundleID:       session.SourceBundleID,
		Rows:                 rows,
		BundleHash:           mustBundleHash(s.t, rows),
		ByteCount:            mustBundleByteCount(s.t, rows),
		CanonicalRequestHash: session.CanonicalRequestHash,
		Generation:           session.Generation,
	}
	s.nextBundleSeq++
	s.committedBySource[session.SourceBundleID] = committed
	s.committedBySeq[committed.BundleSeq] = committed
	return s.compactResponseWithGeneration(session.Generation, committed)
}

func (s *mockPushServer) compactResponse(committed *mockCommittedBundle) *http.Response {
	return s.compactResponseWithGeneration(committed.Generation, committed)
}

func (s *mockPushServer) compactResponseWithGeneration(generation oversync.SchemaGenerationCapabilities, committed *mockCommittedBundle) *http.Response {
	groups, err := oversync.CompactGroupsForBundleRows(generation, committed.Rows)
	require.NoError(s.t, err)
	return jsonResponse(oversync.CompactPushResponse{BundleSeq: committed.BundleSeq, BundleHash: committed.BundleHash, Rows: groups})
}

func expandCapturedPushRequest(t *testing.T, client *Client, request oversync.CompactPushRequest) []oversync.PushRequestRow {
	t.Helper()
	require.NotNil(t, client.discovery)
	rows, err := oversync.ExpandCompactPushRows(client.discovery.SchemaGeneration, request.Rows)
	require.NoError(t, err)
	return rows
}

func mustBundleHash(t *testing.T, rows []oversync.BundleRow) string {
	t.Helper()
	logicalRows := make([]protocolhash.BundleRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload})
	}
	hash, _, err := protocolhash.CommittedBundle(logicalRows)
	require.NoError(t, err)
	return hash
}

func mustBundleByteCount(t *testing.T, rows []oversync.BundleRow) int64 {
	t.Helper()
	logicalRows := make([]protocolhash.BundleRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload})
	}
	_, byteCount, err := protocolhash.CommittedBundle(logicalRows)
	require.NoError(t, err)
	return byteCount
}

func committedPushForTest(t *testing.T, client *Client, db *sql.DB, bundleSeq int64, rows []oversync.BundleRow) *committedPushBundle {
	t.Helper()
	outbox := requireOutboxBundle(t, db)
	committed := &committedPushBundle{
		BundleSeq:            bundleSeq,
		SourceID:             outbox.SourceID,
		SourceBundleID:       outbox.SourceBundleID,
		PlannedRowCount:      outbox.RowCount,
		RowCount:             int64(len(rows)),
		ByteCount:            mustBundleByteCount(t, rows),
		BundleHash:           mustBundleHash(t, rows),
		CanonicalRequestHash: outbox.CanonicalRequestHash,
	}
	return committed
}

// Test fixtures explicitly promote their seeded business values to server evidence.
// Production observations always come from validated wire payloads.
func (c *Client) seedFixtureRowStateInTx(ctx context.Context, tx sqliteTransaction, schema, table, key string, version int64, deleted bool) error {
	kind := "deleted"
	var wire []byte
	if !deleted {
		kind = "live"
		pk, _, err := c.decodeDirtyKeyForPush(tx, table, key)
		if err != nil {
			return err
		}
		capture, exists, err := c.serializeExistingRowInTx(ctx, tx, table, pk)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("fixture row missing")
		}
		wire, err = c.processPayloadForUpload(table, string(capture))
		if err != nil {
			return err
		}
	}
	return c.installAuthoritativeBaseline(ctx, tx, schema, table, key, kind, version, wire, baselineObservation{Origin: "pull"})
}
