package oversync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestRegisteredTableCatalog_TargetRevisionAndDeepCopy(t *testing.T) {
	validTables := []RegisteredTable{{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}}}
	for _, revision := range []int64{0, -1} {
		_, err := newTestRuntimeService(nil, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(validTables), RegisteredTables: validTables, RegisteredTableCatalogRevision: revision}, nil)
		var catalogErr *RegisteredTableCatalogError
		require.ErrorAs(t, err, &catalogErr)
		require.Equal(t, RegisteredTableCatalogError{
			Code:  RegisteredTableCatalogInvalidTargetRevision,
			Field: "RegisteredTableCatalogRevision",
			Value: fmt.Sprintf("%d", revision), TargetRevision: revision,
		}, *catalogErr)
	}

	service, err := newTestRuntimeService(nil, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(validTables), RegisteredTables: validTables, RegisteredTableCatalogRevision: math.MaxInt64}, nil)
	require.NoError(t, err)
	validTables[0].Schema = "changed"
	validTables[0].SyncKeyColumns[0] = "changed"
	require.Equal(t, "business", service.config.RegisteredTables[0].Schema)
	require.Equal(t, []string{"id"}, service.config.RegisteredTables[0].SyncKeyColumns)
	require.Empty(t, service.registeredTableInfo)
	require.Empty(t, service.registeredTableByID)
}

func TestRegisteredTableCatalog_UsesSchemaGenerationRegisteredTableValidation(t *testing.T) {
	_, err := newTestRuntimeService(nil, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: "z_invalid", Table: "users", SyncKeyColumns: []string{"id", "other"}},
		{Schema: "A", Table: "users", SyncKeyColumns: []string{"id"}},
	}), RegisteredTableCatalogRevision: 1,
		RegisteredTables: []RegisteredTable{
			{Schema: "z_invalid", Table: "users", SyncKeyColumns: []string{"id", "other"}},
			{Schema: "A", Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, nil)
	var configErr *SchemaGenerationConfigError
	require.ErrorAs(t, err, &configErr)
	require.Equal(t, SchemaGenerationConfigError{
		Code:   SchemaGenerationInvalidRegisteredTable,
		Schema: "A", Table: "users", Field: "Schema", Value: "A",
	}, *configErr)
}

func TestRegisteredTableCatalog_UsesEveryRegisteredTableRuleAndSubphase(t *testing.T) {
	valid := RegisteredTable{Schema: "business", Table: "valid", SyncKeyColumns: []string{"id"}}
	tests := []struct {
		name   string
		tables []RegisteredTable
		want   SchemaGenerationConfigError
	}{
		{name: "empty", want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyRegisteredTables}},
		{name: "schema before table", tables: []RegisteredTable{
			{Schema: "business", Table: "A", SyncKeyColumns: []string{"id"}},
			{Schema: "Business", Table: "valid", SyncKeyColumns: []string{"id"}},
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredTable, Schema: "Business", Table: "valid", Field: "Schema", Value: "Business"}},
		{name: "table before duplicate", tables: []RegisteredTable{
			{Schema: "business", Table: "A", SyncKeyColumns: []string{"id"}},
			valid, valid,
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredTable, Schema: "business", Table: "A", Field: "Table", Value: "A"}},
		{name: "duplicate before key count", tables: []RegisteredTable{
			{Schema: "business", Table: "a", SyncKeyColumns: nil},
			valid, valid,
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateRegisteredTable, Schema: "business", Table: "valid", ObservedCount: 2}},
		{name: "key count before key identifier", tables: []RegisteredTable{
			{Schema: "business", Table: "a", SyncKeyColumns: []string{"ID"}},
			{Schema: "business", Table: "z", SyncKeyColumns: nil},
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKeyCount, Schema: "business", Table: "z", Field: "SyncKeyColumns", ObservedCount: 0}},
		{name: "multiple key columns", tables: []RegisteredTable{
			{Schema: "business", Table: "valid", SyncKeyColumns: []string{"id", "other_id"}},
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKeyCount, Schema: "business", Table: "valid", Field: "SyncKeyColumns", ObservedCount: 2}},
		{name: "key identifier before hidden key", tables: []RegisteredTable{
			{Schema: "business", Table: "a", SyncKeyColumns: []string{syncScopeColumnName}},
			{Schema: "business", Table: "z", SyncKeyColumns: []string{"ID"}},
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKey, Schema: "business", Table: "z", Column: "ID", Field: "SyncKeyColumns", Value: "ID"}},
		{name: "hidden key", tables: []RegisteredTable{
			{Schema: "business", Table: "valid", SyncKeyColumns: []string{syncScopeColumnName}},
		}, want: SchemaGenerationConfigError{Code: SchemaGenerationHiddenRegisteredSyncKey, Schema: "business", Table: "valid", Column: syncScopeColumnName}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newTestRuntimeService(nil, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(test.tables), RegisteredTables: test.tables, RegisteredTableCatalogRevision: 1}, nil)
			var configErr *SchemaGenerationConfigError
			require.ErrorAs(t, err, &configErr)
			require.Equal(t, test.want, *configErr)
		})
	}
}

func TestRegisteredTableCatalog_GoldenVectors(t *testing.T) {
	seedEntries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "files", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
		{tableID: 2, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	seedMetadata := registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 3}
	canonical, err := registeredTableCatalogCanonicalBytes(seedMetadata, seedEntries)
	require.NoError(t, err)
	require.Equal(t,
		`["oversync.registered-table-catalog.v1","1","3",[["1","business","files","id","uuid","active","1",null],["2","business","users","id","uuid","active","1",null]]]`,
		string(canonical),
	)
	fingerprint, err := registeredTableCatalogFingerprint(seedMetadata, seedEntries)
	require.NoError(t, err)
	require.Equal(t, "b87467a1eb7a0cc505642c3415f95f990bb37dd5b8b96c233e98349232bf405b", fingerprint)

	appendEntries := append(append([]registeredTableCatalogEntry(nil), seedEntries...), registeredTableCatalogEntry{
		tableID: 3, schemaName: "business", tableName: "accounts", syncKeyColumn: "id",
		syncKeyKind: 1, stateCode: 1, introducedRevision: 2,
	})
	appendMetadata := registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 4}
	canonical, err = registeredTableCatalogCanonicalBytes(appendMetadata, appendEntries)
	require.NoError(t, err)
	require.Equal(t,
		`["oversync.registered-table-catalog.v1","2","4",[["1","business","files","id","uuid","active","1",null],["2","business","users","id","uuid","active","1",null],["3","business","accounts","id","uuid","active","2",null]]]`,
		string(canonical),
	)
	fingerprint, err = registeredTableCatalogFingerprint(appendMetadata, appendEntries)
	require.NoError(t, err)
	require.Equal(t, "ea4b543c395198a4569bc1abe0c3080673b35d226d03d4c6e6c93df9b121377f", fingerprint)
}

func TestRegisteredTableCatalog_AppendPreservesIDsAcrossConfigOrder(t *testing.T) {
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "files", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
		{tableID: 2, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	metadata := registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 3}
	metadata.catalogFingerprint, _ = registeredTableCatalogFingerprint(metadata, entries)
	physical := map[string]registeredTableRuntimeInfo{
		"business.files":    {syncKeyKind: 1},
		"business.users":    {syncKeyKind: 1},
		"business.accounts": {syncKeyKind: 1},
	}
	configured := []RegisteredTable{
		{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "accounts", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "files", SyncKeyColumns: []string{"id"}},
	}
	plan, err := planExistingRegisteredTableCatalog(configured, 2, metadata, entries, physical)
	require.NoError(t, err)
	require.Equal(t, registeredTableCatalogPlanAppend, plan.mode)
	require.Equal(t, int32(1), plan.entries[0].tableID)
	require.Equal(t, int32(2), plan.entries[1].tableID)
	require.Equal(t, int32(3), plan.entries[2].tableID)
	require.Equal(t, "accounts", plan.appended[0].tableName)
	require.Equal(t, "ea4b543c395198a4569bc1abe0c3080673b35d226d03d4c6e6c93df9b121377f", plan.metadata.catalogFingerprint)
}

func TestRegisteredTableCatalog_PlanningDoesNotMutateInputsAndRetryReconstructs(t *testing.T) {
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	metadata := registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 2}
	metadata.catalogFingerprint, _ = registeredTableCatalogFingerprint(metadata, entries)
	configured := []RegisteredTable{
		{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "files", SyncKeyColumns: []string{"id"}},
	}
	physical := map[string]registeredTableRuntimeInfo{
		"business.users": {syncKeyKind: 1},
		"business.files": {syncKeyKind: 1},
	}
	wantConfigured := cloneRegisteredTables(configured)
	wantEntries := append([]registeredTableCatalogEntry(nil), entries...)
	wantPhysical := map[string]registeredTableRuntimeInfo{
		"business.users": {syncKeyKind: 1},
		"business.files": {syncKeyKind: 1},
	}

	first, err := planExistingRegisteredTableCatalog(configured, 2, metadata, entries, physical)
	require.NoError(t, err)
	require.Equal(t, wantConfigured, configured)
	require.Equal(t, wantEntries, entries)
	require.Equal(t, wantPhysical, physical)

	first.entries[0].tableName = "mutated-result"
	first.appended[0].tableName = "mutated-result"
	second, err := planExistingRegisteredTableCatalog(configured, 2, metadata, entries, physical)
	require.NoError(t, err)
	require.Equal(t, "users", second.entries[0].tableName)
	require.Equal(t, "files", second.appended[0].tableName)
	require.Equal(t, first.metadata, second.metadata)
}

func TestBootstrap_RegisteredTableCatalogAppendPreservesExistingIDs(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_append_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	seedTables := []RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}
	seed, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(seedTables), RegisteredTables: seedTables, RegisteredTableCatalogRevision: 1}, slog.Default())
	require.NoError(t, err)
	require.NoError(t, seed.Bootstrap(ctx))
	require.NoError(t, seed.Close(ctx))

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.accounts (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent))
	require.NoError(t, err)

	appendService, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "accounts", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "accounts", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, slog.Default())
	require.NoError(t, err)
	require.NoError(t, appendService.Bootstrap(ctx))
	t.Cleanup(func() { _ = appendService.Close(context.Background()) })

	rows, err := pool.Query(ctx, `SELECT table_id, table_name FROM sync.table_catalog ORDER BY table_id`)
	require.NoError(t, err)
	defer rows.Close()
	got := make([]string, 0, 3)
	for rows.Next() {
		var id int32
		var table string
		require.NoError(t, rows.Scan(&id, &table))
		got = append(got, fmt.Sprintf("%d:%s", id, table))
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"1:files", "2:users", "3:accounts"}, got)
	var revision, nextID int64
	var fingerprint string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT catalog_revision, next_table_id, catalog_fingerprint FROM sync.meta
	`).Scan(&revision, &nextID, &fingerprint))
	require.Equal(t, int64(2), revision)
	require.Equal(t, int64(4), nextID)
	require.Len(t, fingerprint, 64)
	require.Equal(t, int32(3), appendService.registeredTableInfo[Key(schemaName, "accounts")].tableID)
	require.Equal(t, int32(1), appendService.retainedTableByID[1].tableID)
}

func TestBootstrap_RegisteredTableCatalogMultiAppendPreservesHistory(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	var userPK int64
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO sync.user_state (user_id, next_bundle_seq, retained_bundle_floor)
		VALUES ('catalog-history-user', 2, 0) RETURNING user_pk
	`).Scan(&userPK))
	hash := make([]byte, 32)
	for index := range hash {
		hash[index] = byte(index + 1)
	}
	key := []byte{1, 2, 3}
	_, err := pool.Exec(ctx, `
		INSERT INTO sync.source_state (user_pk, source_id, state, max_committed_source_bundle_id)
		VALUES ($1, 'source', 'active', 5)
	`, userPK)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_pk, bundle_seq, source_id, source_bundle_id, row_count, byte_count,
			bundle_hash, canonical_request_hash
		) VALUES ($1, 1, 'source', 5, 1, 17, $2, $3)
	`, userPK, hash, strings.Repeat("a", 64))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.bundle_rows (
			user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire
		) VALUES ($1, 1, 0, 1, $2, 1, '{"id":"kept"}'::json)
	`, userPK, key)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.row_state (user_pk, table_id, key_bytes, bundle_seq, deleted)
		VALUES ($1, 1, $2, 1, FALSE)
	`, userPK, key)
	require.NoError(t, err)

	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(ctx))
	t.Cleanup(func() { _ = service.Close(context.Background()) })

	rows, err := pool.Query(ctx, `SELECT table_id, table_name FROM sync.table_catalog ORDER BY table_id`)
	require.NoError(t, err)
	var catalog []string
	for rows.Next() {
		var tableID int32
		var tableName string
		require.NoError(t, rows.Scan(&tableID, &tableName))
		catalog = append(catalog, fmt.Sprintf("%d:%s", tableID, tableName))
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, []string{"1:users", "2:files", "3:posts"}, catalog)
	var storedTableID int32
	var storedHash, storedKey []byte
	var rowOrdinal, retainedFloor, watermark int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT rows.table_id, rows.row_ordinal, rows.key_bytes, log.bundle_hash,
		       users.retained_bundle_floor, sources.max_committed_source_bundle_id
		FROM sync.bundle_rows rows
		JOIN sync.bundle_log log USING (user_pk, bundle_seq)
		JOIN sync.user_state users USING (user_pk)
		JOIN sync.source_state sources USING (user_pk, source_id)
		WHERE rows.user_pk=$1 AND rows.bundle_seq=1
	`, userPK).Scan(&storedTableID, &rowOrdinal, &storedKey, &storedHash, &retainedFloor, &watermark))
	require.Equal(t, int32(1), storedTableID)
	require.Zero(t, rowOrdinal)
	require.Equal(t, key, storedKey)
	require.Equal(t, hash, storedHash)
	require.Zero(t, retainedFloor)
	require.Equal(t, int64(5), watermark)
	var newStateCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.row_state WHERE table_id IN (2,3)`).Scan(&newStateCount))
	require.Zero(t, newStateCount)
}

func TestRegisteredTableCatalog_ErrorRemainsDiscoverable(t *testing.T) {
	original := &RegisteredTableCatalogError{Code: RegisteredTableCatalogEmptyAppend, CatalogRevision: 1, TargetRevision: 2}
	wrapped := fmt.Errorf("bootstrap: %w", original)
	var actual *RegisteredTableCatalogError
	require.True(t, errors.As(wrapped, &actual))
	require.Same(t, original, actual)
}

func TestRegisteredTableCatalog_ExportedSurface(t *testing.T) {
	revisionField, ok := reflect.TypeOf(ServiceConfig{}).FieldByName("RegisteredTableCatalogRevision")
	require.True(t, ok)
	require.Equal(t, reflect.TypeOf(int64(0)), revisionField.Type)

	errorType := reflect.TypeOf(RegisteredTableCatalogError{})
	fields := make([]string, 0, errorType.NumField())
	for index := 0; index < errorType.NumField(); index++ {
		fields = append(fields, errorType.Field(index).Name)
	}
	require.Equal(t, []string{
		"Code", "TableID", "Schema", "Table", "Field", "Value", "Expected", "Actual",
		"CatalogRevision", "TargetRevision", "NextTableID", "ObservedCount",
	}, fields)
	require.Equal(t, map[RegisteredTableCatalogErrorCode]string{
		RegisteredTableCatalogInvalidTargetRevision:     "invalid_target_revision",
		RegisteredTableCatalogLayoutReplacementRequired: "layout_replacement_required",
		RegisteredTableCatalogInvalidMetadata:           "invalid_metadata",
		RegisteredTableCatalogInvalidEntry:              "invalid_entry",
		RegisteredTableCatalogFingerprintMismatch:       "fingerprint_mismatch",
		RegisteredTableCatalogRevisionConflict:          "revision_conflict",
		RegisteredTableCatalogActiveTableOmitted:        "active_table_omitted",
		RegisteredTableCatalogRetiredTableConfigured:    "retired_table_configured",
		RegisteredTableCatalogIdentityMismatch:          "identity_mismatch",
		RegisteredTableCatalogEmptyAppend:               "empty_append",
		RegisteredTableCatalogAppendNotDrained:          "append_not_drained",
		RegisteredTableCatalogAppendTableNotEmpty:       "append_table_not_empty",
		RegisteredTableCatalogIDExhausted:               "table_id_exhausted",
	}, map[RegisteredTableCatalogErrorCode]string{
		RegisteredTableCatalogInvalidTargetRevision:     string(RegisteredTableCatalogInvalidTargetRevision),
		RegisteredTableCatalogLayoutReplacementRequired: string(RegisteredTableCatalogLayoutReplacementRequired),
		RegisteredTableCatalogInvalidMetadata:           string(RegisteredTableCatalogInvalidMetadata),
		RegisteredTableCatalogInvalidEntry:              string(RegisteredTableCatalogInvalidEntry),
		RegisteredTableCatalogFingerprintMismatch:       string(RegisteredTableCatalogFingerprintMismatch),
		RegisteredTableCatalogRevisionConflict:          string(RegisteredTableCatalogRevisionConflict),
		RegisteredTableCatalogActiveTableOmitted:        string(RegisteredTableCatalogActiveTableOmitted),
		RegisteredTableCatalogRetiredTableConfigured:    string(RegisteredTableCatalogRetiredTableConfigured),
		RegisteredTableCatalogIdentityMismatch:          string(RegisteredTableCatalogIdentityMismatch),
		RegisteredTableCatalogEmptyAppend:               string(RegisteredTableCatalogEmptyAppend),
		RegisteredTableCatalogAppendNotDrained:          string(RegisteredTableCatalogAppendNotDrained),
		RegisteredTableCatalogAppendTableNotEmpty:       string(RegisteredTableCatalogAppendTableNotEmpty),
		RegisteredTableCatalogIDExhausted:               string(RegisteredTableCatalogIDExhausted),
	})
}

func TestRegisteredTableCatalog_AllocationGapErrorsAreStructured(t *testing.T) {
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "files", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
		{tableID: 2, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	tests := []struct {
		name     string
		metadata registeredTableCatalogMetadata
		entries  []registeredTableCatalogEntry
		want     RegisteredTableCatalogError
	}{
		{
			name: "terminal gap", metadata: registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 4}, entries: entries,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, Field: "table_id", Expected: "3", Actual: "end"},
		},
		{
			name: "middle gap", metadata: registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 4},
			entries: []registeredTableCatalogEntry{entries[0], {
				tableID: 3, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1,
			}},
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 3, Schema: "business", Table: "users", Field: "table_id", Expected: "2", Actual: "3"},
		},
		{
			name: "next id behind", metadata: registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 2}, entries: entries,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidMetadata, Field: "next_table_id", Expected: "3", Actual: "2", CatalogRevision: 1, NextTableID: 2},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRegisteredTableCatalogEntries(test.metadata, test.entries)
			var catalogErr *RegisteredTableCatalogError
			require.ErrorAs(t, err, &catalogErr)
			require.Equal(t, test.want, *catalogErr)
		})
	}
}

func TestRegisteredTableCatalog_EntryFieldErrorsAreStructured(t *testing.T) {
	base := registeredTableCatalogEntry{
		tableID: 1, schemaName: "business", tableName: "users", syncKeyColumn: "id",
		syncKeyKind: 1, stateCode: 1, introducedRevision: 1,
	}
	retiredAt := int64(2)
	tests := []struct {
		name     string
		entries  []registeredTableCatalogEntry
		metadata registeredTableCatalogMetadata
		want     RegisteredTableCatalogError
	}{
		{name: "schema", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.schemaName = "Bad"; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "Bad", Table: "users", Field: "schema_name", Value: "Bad"}},
		{name: "table", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.tableName = "Bad"; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "Bad", Field: "table_name", Value: "Bad"}},
		{name: "sync key", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry {
			value := base
			value.syncKeyColumn = "_sync_scope_id"
			return value
		}()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "users", Field: "sync_key_column", Value: "_sync_scope_id"}},
		{name: "kind", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.syncKeyKind = 3; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "users", Field: "sync_key_kind", Value: "3"}},
		{name: "state", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.stateCode = 3; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "users", Field: "state_code", Value: "3"}},
		{name: "introduced", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.introducedRevision = 3; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "users", Field: "introduced_revision", Value: "3"}},
		{name: "active retirement", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.retiredRevision = &retiredAt; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "users", Field: "retired_revision", Value: "2"}},
		{name: "retired null", entries: []registeredTableCatalogEntry{func() registeredTableCatalogEntry { value := base; value.stateCode = 2; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 1, Schema: "business", Table: "users", Field: "retired_revision", Value: "<null>"}},
		{name: "duplicate identity", entries: []registeredTableCatalogEntry{base, func() registeredTableCatalogEntry { value := base; value.tableID = 2; return value }()}, metadata: registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 3}, want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, TableID: 2, Schema: "business", Table: "users", Field: "schema_table", ObservedCount: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRegisteredTableCatalogEntries(test.metadata, test.entries)
			var catalogErr *RegisteredTableCatalogError
			require.ErrorAs(t, err, &catalogErr)
			require.Equal(t, test.want, *catalogErr)
		})
	}
}

func TestRegisteredTableCatalog_ReconciliationUsesFixedSubphaseOrder(t *testing.T) {
	retiredAt := int64(2)
	metadata := registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 3}
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "accounts", syncKeyColumn: "legacy_id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
		{tableID: 2, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 2, introducedRevision: 1, retiredRevision: &retiredAt},
	}
	configured := []RegisteredTable{
		{Schema: "business", Table: "accounts", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
	}
	_, err := reconcileRegisteredTableCatalog(configured, 2, metadata, entries, nil)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogRetiredTableConfigured, TableID: 2,
		Schema: "business", Table: "users", CatalogRevision: 2,
	}, *catalogErr)

	entries = []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	configured = []RegisteredTable{{Schema: "business", Table: "accounts", SyncKeyColumns: []string{"id"}}}
	_, err = reconcileRegisteredTableCatalog(configured, 2, registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 2}, entries, nil)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogActiveTableOmitted, TableID: 1,
		Schema: "business", Table: "users", CatalogRevision: 1, TargetRevision: 2,
	}, *catalogErr)

	entries = []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "accounts", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
		{tableID: 2, schemaName: "business", tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	configured = []RegisteredTable{
		{Schema: "business", Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
	}
	for _, permutation := range [][]RegisteredTable{configured, {configured[1], configured[0]}} {
		_, err = reconcileRegisteredTableCatalog(
			permutation,
			2,
			registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 3},
			entries,
			map[string]registeredTableRuntimeInfo{"business.users": {syncKeyKind: 2}},
		)
		require.ErrorAs(t, err, &catalogErr)
		require.Equal(t, RegisteredTableCatalogError{
			Code: RegisteredTableCatalogIdentityMismatch, TableID: 2,
			Schema: "business", Table: "users", Field: "sync_key_kind",
			Expected: "1", Actual: "2", CatalogRevision: 1, TargetRevision: 2,
		}, *catalogErr)
	}
}

func TestRegisteredTableCatalog_FingerprintIsStableAcrossNumericRowOrder(t *testing.T) {
	entries := make([]registeredTableCatalogEntry, 10)
	for index := range entries {
		entries[index] = registeredTableCatalogEntry{
			tableID: int32(index + 1), schemaName: "business", tableName: fmt.Sprintf("table_%02d", index+1),
			syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1,
		}
	}
	metadata := registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 11}
	want, err := registeredTableCatalogFingerprint(metadata, entries)
	require.NoError(t, err)
	permuted := []registeredTableCatalogEntry{entries[9], entries[1], entries[0], entries[8], entries[4], entries[3], entries[7], entries[2], entries[6], entries[5]}
	got, err := registeredTableCatalogFingerprint(metadata, permuted)
	require.NoError(t, err)
	require.Equal(t, want, got)
	canonical, err := registeredTableCatalogCanonicalBytes(metadata, permuted)
	require.NoError(t, err)
	require.Less(t, strings.Index(string(canonical), `["2","business"`), strings.Index(string(canonical), `["10","business"`))
}

func TestRegisteredTableCatalog_LifecycleEncodingDistinguishesNullAndRetirement(t *testing.T) {
	retiredAt := int64(2)
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: "business", tableName: "active", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
		{tableID: 2, schemaName: "business", tableName: "retired", syncKeyColumn: "key", syncKeyKind: 2, stateCode: 2, introducedRevision: 1, retiredRevision: &retiredAt},
	}
	canonical, err := registeredTableCatalogCanonicalBytes(registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 3}, entries)
	require.NoError(t, err)
	require.Contains(t, string(canonical), `"uuid","active","1",null`)
	require.Contains(t, string(canonical), `"text","retired","1","2"`)
}

func TestRegisteredTableCatalog_PlannerRejectsEveryReconciliationConflict(t *testing.T) {
	baseEntries := []registeredTableCatalogEntry{{
		tableID: 1, schemaName: "business", tableName: "users", syncKeyColumn: "id",
		syncKeyKind: 1, stateCode: 1, introducedRevision: 1,
	}}
	baseMetadata := registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: 2}
	baseMetadata.catalogFingerprint, _ = registeredTableCatalogFingerprint(baseMetadata, baseEntries)
	users := RegisteredTable{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}}
	accounts := RegisteredTable{Schema: "business", Table: "accounts", SyncKeyColumns: []string{"id"}}
	physical := map[string]registeredTableRuntimeInfo{
		"business.users": {syncKeyKind: 1}, "business.accounts": {syncKeyKind: 1},
	}

	attach, err := planExistingRegisteredTableCatalog([]RegisteredTable{users}, 1, baseMetadata, baseEntries, physical)
	require.NoError(t, err)
	require.Equal(t, registeredTableCatalogPlanAttach, attach.mode)

	tests := []struct {
		name       string
		configured []RegisteredTable
		target     int64
		metadata   registeredTableCatalogMetadata
		entries    []registeredTableCatalogEntry
		physical   map[string]registeredTableRuntimeInfo
		want       RegisteredTableCatalogError
	}{
		{
			name: "fingerprint", configured: []RegisteredTable{users}, target: 1,
			metadata: func() registeredTableCatalogMetadata {
				value := baseMetadata
				value.catalogFingerprint = strings.Repeat("0", 64)
				return value
			}(),
			entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogFingerprintMismatch, CatalogRevision: 1},
		},
		{
			name: "add without advance", configured: []RegisteredTable{accounts, users}, target: 1,
			metadata: baseMetadata, entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogRevisionConflict, CatalogRevision: 1, TargetRevision: 1, Expected: "2", Actual: "1"},
		},
		{
			name: "revision gap", configured: []RegisteredTable{accounts, users}, target: 3,
			metadata: baseMetadata, entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogRevisionConflict, CatalogRevision: 1, TargetRevision: 3, Expected: "2", Actual: "3"},
		},
		{
			name: "revision rollback", configured: []RegisteredTable{users}, target: 1,
			metadata: func() registeredTableCatalogMetadata {
				value := registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 2}
				value.catalogFingerprint, _ = registeredTableCatalogFingerprint(value, baseEntries)
				return value
			}(),
			entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogRevisionConflict, CatalogRevision: 2, TargetRevision: 1, Expected: "3", Actual: "1"},
		},
		{
			name: "empty append", configured: []RegisteredTable{users}, target: 2,
			metadata: baseMetadata, entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogEmptyAppend, CatalogRevision: 1, TargetRevision: 2},
		},
		{
			name: "active omitted", configured: []RegisteredTable{accounts}, target: 2,
			metadata: baseMetadata, entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogActiveTableOmitted, TableID: 1, Schema: "business", Table: "users", CatalogRevision: 1, TargetRevision: 2},
		},
		{
			name: "key mismatch", configured: []RegisteredTable{{Schema: "business", Table: "users", SyncKeyColumns: []string{"other"}}}, target: 1,
			metadata: baseMetadata, entries: baseEntries, physical: physical,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogIdentityMismatch, TableID: 1, Schema: "business", Table: "users", Field: "sync_key_column", Expected: "id", Actual: "other", CatalogRevision: 1, TargetRevision: 1},
		},
		{
			name: "kind mismatch", configured: []RegisteredTable{users}, target: 1,
			metadata: baseMetadata, entries: baseEntries,
			physical: map[string]registeredTableRuntimeInfo{"business.users": {syncKeyKind: 2}},
			want:     RegisteredTableCatalogError{Code: RegisteredTableCatalogIdentityMismatch, TableID: 1, Schema: "business", Table: "users", Field: "sync_key_kind", Expected: "1", Actual: "2", CatalogRevision: 1, TargetRevision: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := planExistingRegisteredTableCatalog(test.configured, test.target, test.metadata, test.entries, test.physical)
			var catalogErr *RegisteredTableCatalogError
			require.ErrorAs(t, err, &catalogErr)
			if test.want.Code == RegisteredTableCatalogFingerprintMismatch {
				require.Equal(t, test.want.Code, catalogErr.Code)
				require.Equal(t, test.want.CatalogRevision, catalogErr.CatalogRevision)
				require.Equal(t, test.metadata.catalogFingerprint, catalogErr.Actual)
				require.NotEmpty(t, catalogErr.Expected)
				return
			}
			require.Equal(t, test.want, *catalogErr)
		})
	}
}

func TestRegisteredTableCatalog_AllocationStopsAtExhaustedSentinel(t *testing.T) {
	candidate := RegisteredTable{Schema: "business", Table: "later", SyncKeyColumns: []string{"id"}}
	_, err := allocateRegisteredTableCatalogAppend(
		registeredTableCatalogMetadata{catalogRevision: 7, nextTableID: registeredTableCatalogExhaustedID},
		nil,
		[]RegisteredTable{candidate},
		map[string]registeredTableRuntimeInfo{"business.later": {syncKeyKind: 1}},
		8,
	)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogIDExhausted, Schema: "business", Table: "later", NextTableID: registeredTableCatalogExhaustedID,
	}, *catalogErr)
}

func TestRegisteredTableCatalog_MetadataErrorsRetainReadableRevision(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	_, err := pool.Exec(ctx, `
		CREATE SCHEMA sync;
		CREATE TABLE sync.meta (
			singleton_key BOOLEAN,
			protocol_label TEXT,
			layout_name TEXT,
			catalog_revision BIGINT,
			next_table_id BIGINT,
			catalog_fingerprint TEXT
		);
		CREATE TABLE sync.table_catalog (
			table_id INTEGER, schema_name TEXT, table_name TEXT, sync_key_column TEXT,
			sync_key_kind SMALLINT, state_code SMALLINT, introduced_revision BIGINT,
			retired_revision BIGINT
		);
		INSERT INTO sync.meta VALUES (FALSE, 'v1', 'server_postgres_sync_catalog_v1', 7, 1,
			'0000000000000000000000000000000000000000000000000000000000000000')
	`)
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "singleton_key",
		Expected: "true", Actual: "false", CatalogRevision: 7,
	}, *catalogErr)

	_, err = pool.Exec(ctx, `UPDATE sync.meta SET singleton_key = TRUE, protocol_label = 'wrong', catalog_revision = -4`)
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "protocol_label",
		Expected: "v1", Actual: "wrong", CatalogRevision: -4,
	}, *catalogErr)

	_, err = pool.Exec(ctx, `UPDATE sync.meta SET protocol_label = 'v1'`)
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "catalog_revision",
		Expected: ">=1", Actual: "-4", CatalogRevision: -4,
	}, *catalogErr)

	_, err = pool.Exec(ctx, `UPDATE sync.meta SET catalog_revision = 7, next_table_id = 0`)
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "next_table_id",
		Expected: "1..2147483648", Actual: "0", CatalogRevision: 7,
	}, *catalogErr)

	_, err = pool.Exec(ctx, `UPDATE sync.meta SET next_table_id = 1, catalog_fingerprint = NULL`)
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "catalog_fingerprint",
		Expected: "64 lowercase hexadecimal characters", Actual: "<null>", CatalogRevision: 7,
	}, *catalogErr)

	_, err = pool.Exec(ctx, `UPDATE sync.meta SET singleton_key = NULL, catalog_fingerprint = $1`, strings.Repeat("0", 64))
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "singleton_key",
		Expected: "true", Actual: "<null>", CatalogRevision: 7,
	}, *catalogErr)

	_, err = pool.Exec(ctx, `UPDATE sync.meta SET singleton_key = TRUE, catalog_revision = NULL`)
	require.NoError(t, err)
	_, _, err = loadRegisteredTableCatalog(ctx, pool)
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: "catalog_revision",
		Expected: ">=1", Actual: "<null>", CatalogRevision: 0,
	}, *catalogErr)
}

func TestBootstrap_RegisteredTableCatalogRejectsSchemaOnlyObjects(t *testing.T) {
	objects := []struct {
		name string
		sql  string
	}{
		{name: "enum", sql: `CREATE TYPE sync.only_object AS ENUM ('one')`},
		{name: "domain", sql: `CREATE DOMAIN sync.only_object AS TEXT`},
		{name: "collation", sql: `CREATE COLLATION sync.only_object FROM "C"`},
	}
	for _, object := range objects {
		t.Run(object.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_object_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			_, err := pool.Exec(ctx, `CREATE SCHEMA sync`)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, object.sql)
			require.NoError(t, err)
			service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
				RegisteredTableCatalogRevision: 1,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			err = service.Bootstrap(ctx)
			var catalogErr *RegisteredTableCatalogError
			require.ErrorAs(t, err, &catalogErr)
			require.Equal(t, RegisteredTableCatalogError{
				Code:     RegisteredTableCatalogLayoutReplacementRequired,
				Expected: syncSchemaLayoutName, Actual: "<missing>",
			}, *catalogErr)
			var metaExists bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT to_regclass('sync.meta') IS NOT NULL`).Scan(&metaExists))
			require.False(t, metaExists)
		})
	}
}

func TestBootstrap_RegisteredTableCatalogRejectsPopulatedAppendWithoutMutation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_populated_append_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NotNil(t, seed)
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.files (_sync_scope_id, id, name, data) VALUES ('scope', $1, 'file', '\x01')`, pgx.Identifier{schemaName}.Sanitize()), uuid.New())
	require.NoError(t, err)
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = service.Bootstrap(ctx)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogError{
		Code: RegisteredTableCatalogAppendTableNotEmpty, Schema: schemaName, Table: "files", ObservedCount: 1,
	}, *catalogErr)
	require.Empty(t, service.registeredTableByID)
	var revision, nextID, catalogCount, triggerCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision, next_table_id FROM sync.meta`).Scan(&revision, &nextID))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&catalogCount))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=$1 AND c.relname='files' AND t.tgname = ANY($2::text[])
	`, schemaName, []string{registeredTableOwnerGuardTrigger, registeredTableCaptureTriggerName, registeredTableTruncateGuardTrigger}).Scan(&triggerCount))
	require.Equal(t, []int64{1, 2, 1, 0}, []int64{revision, nextID, catalogCount, triggerCount})

	status, err := service.GetStatus(ctx)
	require.NoError(t, err)
	require.False(t, status.AcceptingOperations)
	require.NoError(t, service.Close(context.Background()))
}

func TestBootstrap_RegisteredTableCatalogValidatesAppendRelationshipsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_bad_fk_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.bad_children (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			user_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			FOREIGN KEY (_sync_scope_id, user_id) REFERENCES %s.users (_sync_scope_id, id)
		)
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "bad_children", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "bad_children", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = service.Bootstrap(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-deferrable")
	requireCatalogAppendRollback(t, ctx, pool, service, schemaName, "bad_children")
}

func TestBootstrap_RegisteredTableCatalogRejectsReservedAppendTriggerBeforeMutation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_reserved_trigger_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON %s.files
		FOR EACH ROW EXECUTE FUNCTION sync.enforce_registered_row_owner()
	`, registeredTableCaptureTriggerName, pgx.Identifier{schemaName}.Sanitize()))
	require.NoError(t, err)
	service := newCatalogAppendTestService(t, pool, schemaName)
	err = service.Bootstrap(ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.ErrorContains(t, err, registeredTableCaptureTriggerName)
	var revision, nextID, catalogCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision, next_table_id FROM sync.meta`).Scan(&revision, &nextID))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&catalogCount))
	require.Equal(t, []int64{1, 2, 1}, []int64{revision, nextID, catalogCount})
	require.Empty(t, service.registeredTableByID)
}

func TestBootstrap_RegisteredTableCatalogAppendsPartitionedRootAndGuardsDescendants(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_partition_append_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		) PARTITION BY HASH (_sync_scope_id);
		CREATE TABLE %s.records_p0 PARTITION OF %s.records FOR VALUES WITH (MODULUS 2, REMAINDER 0);
		CREATE TABLE %s.records_p1 PARTITION OF %s.records FOR VALUES WITH (MODULUS 2, REMAINDER 1)
	`, schemaIdent, schemaIdent, schemaIdent, schemaIdent, schemaIdent))
	require.NoError(t, err)
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(ctx))
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.Equal(t, int32(2), service.registeredTableInfo[Key(schemaName, "records")].tableID)
	for _, tableName := range []string{"records", "records_p0", "records_p1"} {
		var count int64
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_trigger trigger
			JOIN pg_class relation ON relation.oid=trigger.tgrelid
			JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
			WHERE namespace.nspname=$1 AND relation.relname=$2
			  AND trigger.tgname=$3 AND NOT trigger.tgisinternal
		`, schemaName, tableName, registeredTableTruncateGuardTrigger).Scan(&count))
		require.Equal(t, int64(1), count)
	}
}

func TestBootstrap_RegisteredTableCatalogAppendDrainUsesFixedOrder(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_drain_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	proof := testSchemaGenerationProofForService(seed)
	var userPK int64
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO sync.user_state (user_id) VALUES ('catalog-drain-user') RETURNING user_pk`).Scan(&userPK))
	_, err := pool.Exec(ctx, `
		INSERT INTO sync.source_state (user_pk, source_id, state, max_committed_source_bundle_id)
		VALUES ($1, 'source', 'active', 0)
	`, userPK)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.source_generation_bindings (
			user_pk, source_id, schema_generation_id, schema_generation_fingerprint
		) VALUES ($1, 'source', $2, $3)
	`, userPK, proof.ID, proof.Fingerprint)
	require.NoError(t, err)
	snapshotID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.snapshot_sessions (
			snapshot_id, user_pk, source_id, schema_generation_id, schema_generation_fingerprint,
			snapshot_bundle_seq, expires_at
		) VALUES ($1, $2, 'source', $3, $4, 0, now() + interval '1 hour')
	`, snapshotID, userPK, proof.ID, proof.Fingerprint)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.bundle_capture_stage (
			txid, user_pk, table_id, op_code, key_bytes, payload_db
		) VALUES (1, $1, 1, 1, '\x01', '{}'::jsonb)
	`, userPK)
	require.NoError(t, err)

	for _, expected := range []struct {
		relation string
		cleanup  string
	}{
		{relation: "snapshot_sessions", cleanup: `DELETE FROM sync.snapshot_sessions`},
		{relation: "bundle_capture_stage", cleanup: `DELETE FROM sync.bundle_capture_stage`},
	} {
		service := newCatalogAppendTestService(t, pool, schemaName)
		err := service.Bootstrap(ctx)
		var catalogErr *RegisteredTableCatalogError
		require.ErrorAs(t, err, &catalogErr)
		require.Equal(t, RegisteredTableCatalogError{
			Code: RegisteredTableCatalogAppendNotDrained, Field: "sync." + expected.relation, ObservedCount: 1,
		}, *catalogErr)
		requireCatalogAppendRollback(t, ctx, pool, service, schemaName, "files")
		_, err = pool.Exec(ctx, expected.cleanup)
		require.NoError(t, err)
	}
}

func TestBootstrap_RegisteredTableCatalogAppendMutationFailuresRollback(t *testing.T) {
	t.Parallel()
	phases := []string{
		"before_metadata_lock",
		"after_metadata_lock",
		"after_append_table_locks",
		"after_plan_validation",
		"after_catalog_insert",
		"after_truncate_trigger",
		"after_owner_trigger",
		"after_capture_trigger",
		"before_metadata_update",
		"after_metadata_update",
		"before_final_validation",
		"during_final_managed_validation",
		"before_commit_known_rollback",
	}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_fault_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
				RegisteredTableCatalogRevision: 1,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, seed.Close(ctx))
			service := newCatalogAppendTestService(t, pool, schemaName)
			injected := errors.New("catalog mutation fault: " + phase)
			service.catalogHooks = &registeredTableCatalogTestHooks{afterMutation: func(actual string, _ int) error {
				if actual == phase {
					return injected
				}
				return nil
			}}
			err := service.Bootstrap(ctx)
			require.ErrorIs(t, err, injected)
			requireCatalogAppendRollback(t, ctx, pool, service, schemaName, "files")
			requireCatalogBootstrapResourcesReleased(t, ctx, pool)

			service.catalogHooks = nil
			require.NoError(t, service.Bootstrap(ctx))
			require.Equal(t, int32(2), service.registeredTableInfo[Key(schemaName, "files")].tableID)
			require.NoError(t, service.Close(ctx))
			requireCatalogBootstrapResourcesReleased(t, ctx, pool)

			later := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
			}), RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
			},
				RegisteredTableCatalogRevision: 3,
			}, integrationTestLogger(slog.LevelWarn))
			require.Equal(t, int32(3), later.registeredTableInfo[Key(schemaName, "posts")].tableID)
			var nextTableID int64
			require.NoError(t, pool.QueryRow(ctx, `SELECT next_table_id FROM sync.meta`).Scan(&nextTableID))
			require.Equal(t, int64(4), nextTableID)
		})
	}
}

func TestBootstrap_RegisteredTableCatalogMutationUsesReadCommitted(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_isolation_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	service := newCatalogAppendTestService(t, pool, schemaName)
	var isolation string
	service.catalogHooks = &registeredTableCatalogTestHooks{inspectTransaction: func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SHOW transaction_isolation`).Scan(&isolation)
	}}
	require.NoError(t, service.Bootstrap(ctx))
	require.Equal(t, "read committed", isolation)
	require.Equal(t, pgx.ReadCommitted, syncMutationTxOptions().IsoLevel)
}

func TestBootstrap_RegisteredTableCatalogEachCandidateInsertFailureRollsBack(t *testing.T) {
	t.Parallel()
	for _, failIndex := range []int{0, 1} {
		t.Run(fmt.Sprintf("candidate_%d", failIndex), func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_insert_fault_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
				RegisteredTableCatalogRevision: 1,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, seed.Close(ctx))
			service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
			}), RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
			},
				RegisteredTableCatalogRevision: 2,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = service.Close(context.Background()) })
			injected := errors.New("candidate insert fault")
			service.catalogHooks = &registeredTableCatalogTestHooks{afterMutation: func(phase string, index int) error {
				if phase == "after_catalog_insert" && index == failIndex {
					return injected
				}
				return nil
			}}
			err = service.Bootstrap(ctx)
			require.ErrorIs(t, err, injected)
			requireCatalogAppendRollback(t, ctx, pool, service, schemaName, "files")
			requireCatalogAppendRollback(t, ctx, pool, service, schemaName, "posts")
			requireCatalogBootstrapResourcesReleased(t, ctx, pool)

			service.catalogHooks = nil
			require.NoError(t, service.Bootstrap(ctx))
			require.Equal(t, int32(2), service.registeredTableInfo[Key(schemaName, "files")].tableID)
			require.Equal(t, int32(3), service.registeredTableInfo[Key(schemaName, "posts")].tableID)
			require.NoError(t, service.Close(ctx))
			requireCatalogBootstrapResourcesReleased(t, ctx, pool)

			later := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "file_reviews", SyncKeyColumns: []string{"id"}},
			}), RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "file_reviews", SyncKeyColumns: []string{"id"}},
			},
				RegisteredTableCatalogRevision: 3,
			}, integrationTestLogger(slog.LevelWarn))
			require.Equal(t, int32(4), later.registeredTableInfo[Key(schemaName, "file_reviews")].tableID)
			var nextTableID int64
			require.NoError(t, pool.QueryRow(ctx, `SELECT next_table_id FROM sync.meta`).Scan(&nextTableID))
			require.Equal(t, int64(5), nextTableID)
		})
	}
}

func TestBootstrap_RegisteredTableCatalogFinalReloadRejectsCompetingInsert(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_competing_insert_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	service := newCatalogAppendTestService(t, pool, schemaName)
	service.catalogHooks = &registeredTableCatalogTestHooks{afterMutation: func(phase string, _ int) error {
		if phase != "before_final_validation" {
			return nil
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO sync.table_catalog (
				table_id, schema_name, table_name, sync_key_column, sync_key_kind,
				state_code, introduced_revision
			) VALUES (3, 'intruder', 'table', 'id', 1, 1, 2)
		`)
		return err
	}}
	err := service.Bootstrap(ctx)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogInvalidMetadata, catalogErr.Code)
	require.Equal(t, "next_table_id", catalogErr.Field)
	require.Empty(t, service.registeredTableByID)
	var ownCandidateCount, competingCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog WHERE table_id = 2`).Scan(&ownCandidateCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog WHERE table_id = 3`).Scan(&competingCount))
	require.Zero(t, ownCandidateCount)
	require.Equal(t, int64(1), competingCount)
}

func TestBootstrap_RegisteredTableCatalogAmbiguousCommitStaysUnreadyAndRetryAttaches(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_ambiguous_commit_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	service := newCatalogAppendTestService(t, pool, schemaName)
	ambiguous := errors.New("simulated ambiguous commit outcome")
	service.catalogHooks = &registeredTableCatalogTestHooks{afterMutation: func(phase string, _ int) error {
		if phase == "after_commit_ambiguous" {
			return ambiguous
		}
		return nil
	}}
	err := service.Bootstrap(ctx)
	require.ErrorIs(t, err, ambiguous)
	require.Empty(t, service.registeredTableByID)
	status, err := service.GetStatus(ctx)
	require.NoError(t, err)
	require.False(t, status.AcceptingOperations)
	var revision, count int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision FROM sync.meta`).Scan(&revision))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&count))
	require.Equal(t, []int64{2, 2}, []int64{revision, count})
	requireCatalogBootstrapResourcesReleased(t, ctx, pool)
	service.catalogHooks = nil
	require.NoError(t, service.Bootstrap(ctx))
	require.Equal(t, int32(2), service.registeredTableInfo[Key(schemaName, "files")].tableID)
	require.NoError(t, service.Close(ctx))
	requireCatalogBootstrapResourcesReleased(t, ctx, pool)

	later := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 3,
	}, integrationTestLogger(slog.LevelWarn))
	require.Equal(t, int32(3), later.registeredTableInfo[Key(schemaName, "posts")].tableID)
	var nextTableID int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT next_table_id FROM sync.meta`).Scan(&nextTableID))
	require.Equal(t, int64(4), nextTableID)
}

func TestBootstrap_RegisteredTableCatalogStaticDriftPrecedesBusinessValidation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_precedence_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	_, err := pool.Exec(ctx, `DROP INDEX sync.bcs_tx_user_ordinal_idx`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE %s.users CASCADE`, pgx.Identifier{schemaName}.Sanitize()))
	require.NoError(t, err)
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = service.Bootstrap(ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.ErrorContains(t, err, "bcs_tx_user_ordinal_idx")
	require.NotContains(t, err.Error(), "registered table")
}

func TestBootstrap_RegisteredTableCatalogLayoutClassificationMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		createMeta string
		markers    []any
		revision   int64
		want       RegisteredTableCatalogError
	}{
		{
			name: "sole unknown marker", createMeta: `CREATE TABLE sync.meta (layout_name TEXT)`, markers: []any{"unknown_layout"}, revision: 1,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogLayoutReplacementRequired, Expected: syncSchemaLayoutName, Actual: "unknown_layout"},
		},
		{
			name: "null marker", createMeta: `CREATE TABLE sync.meta (layout_name TEXT)`, markers: []any{nil}, revision: 1,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogLayoutReplacementRequired, Expected: syncSchemaLayoutName, Actual: "<missing>"},
		},
		{
			name: "ambiguous marker", createMeta: `CREATE TABLE sync.meta (layout_name TEXT)`, markers: []any{"alpha", "beta"}, revision: 1,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogLayoutReplacementRequired, Expected: syncSchemaLayoutName, Actual: "<ambiguous>"},
		},
		{
			name: "invalid marker shape", createMeta: `CREATE TABLE sync.meta (layout_name TEXT[])`, revision: 1,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogLayoutReplacementRequired, Expected: syncSchemaLayoutName, Actual: "<invalid-shape>"},
		},
		{
			name: "multiple rows including target", createMeta: `CREATE TABLE sync.meta (layout_name TEXT)`, markers: []any{syncSchemaLayoutName, "other"}, revision: 1,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidMetadata, Field: "row_count", Expected: "1", Actual: "2"},
		},
		{
			name: "fresh revision two", revision: 2,
			want: RegisteredTableCatalogError{Code: RegisteredTableCatalogRevisionConflict, Field: "RegisteredTableCatalogRevision", Expected: "1", Actual: "2", TargetRevision: 2},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_layout_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			if test.createMeta != "" {
				_, err := pool.Exec(ctx, `CREATE SCHEMA sync`)
				require.NoError(t, err)
				_, err = pool.Exec(ctx, test.createMeta)
				require.NoError(t, err)
				for _, marker := range test.markers {
					_, err = pool.Exec(ctx, `INSERT INTO sync.meta (layout_name) VALUES ($1)`, marker)
					require.NoError(t, err)
				}
			}
			service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
				RegisteredTableCatalogRevision: test.revision,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			err = service.Bootstrap(ctx)
			var catalogErr *RegisteredTableCatalogError
			require.ErrorAs(t, err, &catalogErr)
			require.Equal(t, test.want, *catalogErr)
		})
	}
}

func TestBootstrap_RegisteredTableCatalogRetainedIdentityIsNotActive(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_retained_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	retiredAt := int64(2)
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: schemaName, tableName: "files", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 2, introducedRevision: 1, retiredRevision: &retiredAt},
		{tableID: 2, schemaName: schemaName, tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	metadata := registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 3}
	metadata.catalogFingerprint, _ = registeredTableCatalogFingerprint(metadata, entries)
	_, err := pool.Exec(ctx, `UPDATE sync.table_catalog SET state_code=2, retired_revision=2 WHERE table_id=1`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE sync.meta SET catalog_revision=2, catalog_fingerprint=$1`, metadata.catalogFingerprint)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE %s.files CASCADE`, pgx.Identifier{schemaName}.Sanitize()))
	require.NoError(t, err)

	reuse, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = reuse.Bootstrap(ctx)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogRetiredTableConfigured, catalogErr.Code)
	require.Equal(t, int32(1), catalogErr.TableID)

	active := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.False(t, active.IsTableRegistered(schemaName, "files"))
	require.True(t, active.IsTableRegistered(schemaName, "users"))
	require.Len(t, active.registeredTableByID, 1)
	require.Len(t, active.retainedTableByID, 2)
	retained, err := active.tableInfoForID(1)
	require.NoError(t, err)
	require.Equal(t, "files", retained.tableName)
	_, err = pool.Exec(ctx, `DELETE FROM sync.table_catalog WHERE table_id=1`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be deleted")
	_, err = pool.Exec(ctx, `UPDATE sync.table_catalog SET schema_name='other' WHERE table_id=2`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "identity is immutable")

	active.config.RetainedBundlesPerUser = 1
	var userPK int64
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO sync.user_state (user_id, next_bundle_seq, retained_bundle_floor)
		VALUES ('retired-catalog-pruning', 4, 0)
		RETURNING user_pk
	`).Scan(&userPK))
	var fingerprintBefore string
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_fingerprint FROM sync.meta`).Scan(&fingerprintBefore))
	require.NoError(t, pgx.BeginTxFunc(ctx, pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		return active.applyRetentionPolicyForUser(ctx, tx, userPK)
	}))
	var retainedFloor, nextIDAfterPrune int64
	var fingerprintAfter string
	var retiredState int16
	var retiredRevision int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT retained_bundle_floor FROM sync.user_state WHERE user_pk=$1`, userPK).Scan(&retainedFloor))
	require.NoError(t, pool.QueryRow(ctx, `SELECT next_table_id, catalog_fingerprint FROM sync.meta`).Scan(&nextIDAfterPrune, &fingerprintAfter))
	require.NoError(t, pool.QueryRow(ctx, `SELECT state_code, retired_revision FROM sync.table_catalog WHERE table_id=1`).Scan(&retiredState, &retiredRevision))
	require.Equal(t, int64(2), retainedFloor)
	require.Equal(t, int64(3), nextIDAfterPrune)
	require.Equal(t, fingerprintBefore, fingerprintAfter)
	require.Equal(t, int16(2), retiredState)
	require.Equal(t, int64(2), retiredRevision)
	require.NoError(t, active.Close(ctx))

	appended := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 3,
	}, integrationTestLogger(slog.LevelWarn))
	require.Equal(t, int32(3), appended.registeredTableInfo[Key(schemaName, "posts")].tableID)
	var nextTableID int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT next_table_id FROM sync.meta`).Scan(&nextTableID))
	require.Equal(t, int64(4), nextTableID)
}

func TestRegisteredTableCatalog_RetiredIdentityDecodesHistoryButIsExcludedFromCurrentSnapshot(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_retired_decode_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "retired-decode-user"
	userRowID := uuid.New()
	fileRowID := uuid.New()
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Active User', 'active@example.com')
	`, schemaIdent), userID, userRowID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.files (_sync_scope_id, id, name, data)
		VALUES ($1, $2, 'Retired File', '\x01')
	`, schemaIdent), userID, fileRowID)
	require.NoError(t, err)
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))

	retiredAt := int64(2)
	entries := []registeredTableCatalogEntry{
		{tableID: 1, schemaName: schemaName, tableName: "files", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 2, introducedRevision: 1, retiredRevision: &retiredAt},
		{tableID: 2, schemaName: schemaName, tableName: "users", syncKeyColumn: "id", syncKeyKind: 1, stateCode: 1, introducedRevision: 1},
	}
	metadata := registeredTableCatalogMetadata{catalogRevision: 2, nextTableID: 3}
	metadata.catalogFingerprint, err = registeredTableCatalogFingerprint(metadata, entries)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE sync.table_catalog SET state_code=2, retired_revision=2 WHERE table_id=1`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE sync.meta SET catalog_revision=2, catalog_fingerprint=$1`, metadata.catalogFingerprint)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE %s.files CASCADE`, schemaIdent))
	require.NoError(t, err)

	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.Len(t, service.registeredTableByID, 1)
	require.Len(t, service.retainedTableByID, 2)

	actor := Actor{UserID: userID, SourceID: "retired-reader"}
	proof := testSchemaGenerationProofForService(service)
	_, err = service.Connect(ctx, actor, proof, &ConnectRequest{})
	require.NoError(t, err)
	session, err := service.CreateSnapshotSession(ctx, actor, testSchemaGenerationProofForService(service))
	require.NoError(t, err)
	require.Equal(t, int64(1), session.RowCount)
	snapshotRows, _ := collectSnapshotChunkRows(t, ctx, service, actor, session.SnapshotID, 10)
	require.Len(t, snapshotRows, 1)
	require.Equal(t, "users", snapshotRows[0].Table)

	var userPK, bundleSeq int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_pk, next_bundle_seq-1 FROM sync.user_state WHERE user_id=$1`, userID).Scan(&userPK, &bundleSeq))
	pull, err := service.ProcessPull(ctx, actor, proof, 0, 10, bundleSeq)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.Equal(t, "users", pull.Bundles[0].Rows[0].Table)

	fileKey := append([]byte(nil), fileRowID[:]...)
	historicalSnapshotID := uuid.New()
	historicalPayload := fmt.Sprintf(`{"id":%q,"name":"Retired File"}`, fileRowID.String())
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.snapshot_sessions (
			snapshot_id, user_pk, source_id, schema_generation_id, schema_generation_fingerprint,
			snapshot_bundle_seq, row_count, byte_count, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, now()+interval '1 hour')
	`, historicalSnapshotID, userPK, actor.SourceID, proof.ID, proof.Fingerprint, bundleSeq, len(historicalPayload))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.snapshot_session_rows (
			snapshot_id, row_ordinal, table_id, key_bytes, bundle_seq, payload_wire, wire_byte_count
		) VALUES ($1, 1, 1, $2, $3, $4::json, $5)
	`, historicalSnapshotID, fileKey, bundleSeq, historicalPayload, len(historicalPayload))
	require.NoError(t, err)
	historicalChunk, err := service.GetSnapshotChunk(ctx, actor, testSchemaGenerationProofForService(service), historicalSnapshotID.String(), 0, 10, defaultBytesPerSnapshotChunk)
	require.Nil(t, historicalChunk)
	var projectionErr *GenerationProjectionError
	require.ErrorAs(t, err, &projectionErr)
	require.Equal(t, GenerationProjectionIntegrityError, projectionErr.Code)

	retiredPushActor := Actor{UserID: userID, SourceID: "retired-push"}
	_, err = service.Connect(ctx, retiredPushActor, proof, &ConnectRequest{})
	require.NoError(t, err)
	retiredPushRows := []PushRequestRow{{
		Schema: schemaName, Table: "files", Key: SyncKey{"id": fileRowID.String()}, Op: OpDelete, BaseRowVersion: bundleSeq,
	}}
	requestHash, err := computeGenerationPinnedPushRequestHash(proof, retiredPushRows)
	require.NoError(t, err)
	_, err = service.PushOneShot(ctx, retiredPushActor, proof, &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, Rows: retiredPushRows,
	})
	var invalidPushErr *PushValidationError
	require.ErrorAs(t, err, &invalidPushErr)
	require.Contains(t, invalidPushErr.Error(), "not present in schema generation")

	var currentRows []adoptionCurrentRowState
	require.NoError(t, pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var loadErr error
		currentRows, loadErr = service.loadAdoptionCurrentRowState(ctx, tx, userID, adoptionScopeState{
			userPK: userPK, rowStateCount: 2, liveRowStateCount: 2,
		}, bundleSeq, nil)
		return loadErr
	}))
	require.Len(t, currentRows, 2)
}

func TestBootstrap_RegisteredTableCatalogCorruptionMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(context.Context, *pgxpool.Pool) error
		code   RegisteredTableCatalogErrorCode
		field  string
	}{
		{
			name: "fingerprint drift",
			mutate: func(ctx context.Context, pool *pgxpool.Pool) error {
				_, err := pool.Exec(ctx, `UPDATE sync.meta SET catalog_fingerprint=$1`, strings.Repeat("0", 64))
				return err
			},
			code: RegisteredTableCatalogFingerprintMismatch,
		},
		{
			name: "next id drift",
			mutate: func(ctx context.Context, pool *pgxpool.Pool) error {
				_, err := pool.Exec(ctx, `UPDATE sync.meta SET next_table_id=4`)
				return err
			},
			code: RegisteredTableCatalogInvalidEntry, field: "table_id",
		},
		{
			name: "state revision drift",
			mutate: func(ctx context.Context, pool *pgxpool.Pool) error {
				_, err := pool.Exec(ctx, `UPDATE sync.table_catalog SET state_code=2, retired_revision=2 WHERE table_id=1`)
				return err
			},
			code: RegisteredTableCatalogInvalidEntry, field: "retired_revision",
		},
		{
			name: "missing retained row",
			mutate: func(ctx context.Context, pool *pgxpool.Pool) error {
				if _, err := pool.Exec(ctx, `ALTER TABLE sync.table_catalog DISABLE TRIGGER table_catalog_identity_guard`); err != nil {
					return err
				}
				if _, err := pool.Exec(ctx, `DELETE FROM sync.table_catalog WHERE table_id=1`); err != nil {
					return err
				}
				_, err := pool.Exec(ctx, `ALTER TABLE sync.table_catalog ENABLE TRIGGER table_catalog_identity_guard`)
				return err
			},
			code: RegisteredTableCatalogInvalidEntry, field: "table_id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_corruption_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			}), RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			},
				RegisteredTableCatalogRevision: 1,
			}
			seed := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, seed.Close(ctx))
			require.NoError(t, test.mutate(ctx, pool))
			service, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			err = service.Bootstrap(ctx)
			var catalogErr *RegisteredTableCatalogError
			require.ErrorAs(t, err, &catalogErr)
			require.Equal(t, test.code, catalogErr.Code)
			require.Equal(t, test.field, catalogErr.Field)
			require.Empty(t, service.registeredTableByID)
		})
	}
}

func TestBootstrap_RegisteredTableCatalogUsesOneConnectionForFreshAttachAndAppend(t *testing.T) {
	t.Parallel()
	registered := func(schemaName string, names ...string) []RegisteredTable {
		tables := make([]RegisteredTable, 0, len(names))
		for _, name := range names {
			tables = append(tables, RegisteredTable{Schema: schemaName, Table: name, SyncKeyColumns: []string{"id"}})
		}
		return tables
	}

	t.Run("fresh relationship discovery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		control := newIntegrationTestPool(t, ctx)
		schemaName := "catalog_one_conn_fresh_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		require.NoError(t, resetTestBusinessSchema(ctx, control, schemaName))
		t.Cleanup(func() { _ = dropTestSchema(context.Background(), control, schemaName) })
		pool := newCatalogSingleConnectionPool(t, ctx, control, "fresh")
		service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered(schemaName, "users", "posts")), RegisteredTables: registered(schemaName, "users", "posts"), RegisteredTableCatalogRevision: 1}, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		require.NoError(t, service.Bootstrap(ctx))
		require.Contains(t, service.discoveredSchema.Dependencies[Key(schemaName, "posts")], Key(schemaName, "users"))
		require.NoError(t, service.Close(ctx))
		requireCatalogPoolReleased(t, pool)
	})

	t.Run("exact attach relationship discovery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		control := newIntegrationTestPool(t, ctx)
		schemaName := "catalog_one_conn_attach_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		require.NoError(t, resetTestBusinessSchema(ctx, control, schemaName))
		t.Cleanup(func() { _ = dropTestSchema(context.Background(), control, schemaName) })
		config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered(schemaName, "users", "posts")), RegisteredTables: registered(schemaName, "users", "posts"), RegisteredTableCatalogRevision: 1}
		seed := newBootstrappedIntegrationService(t, ctx, control, config, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, seed.Close(ctx))
		pool := newCatalogSingleConnectionPool(t, ctx, control, "attach")
		service, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		require.NoError(t, service.Bootstrap(ctx))
		require.Contains(t, service.discoveredSchema.Dependencies[Key(schemaName, "posts")], Key(schemaName, "users"))
		require.NoError(t, service.Close(ctx))
		requireCatalogPoolReleased(t, pool)
	})

	t.Run("append revalidates relationships while business locks are held", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		control := newIntegrationTestPool(t, ctx)
		schemaName := "catalog_one_conn_append_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		require.NoError(t, resetTestBusinessSchema(ctx, control, schemaName))
		t.Cleanup(func() { _ = dropTestSchema(context.Background(), control, schemaName) })
		seed := newBootstrappedIntegrationService(t, ctx, control, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered(schemaName, "users")), RegisteredTables: registered(schemaName, "users"), RegisteredTableCatalogRevision: 1}, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, seed.Close(ctx))

		pool := newCatalogSingleConnectionPool(t, ctx, control, "append")
		service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered(schemaName, "users", "posts")), RegisteredTables: registered(schemaName, "users", "posts"), RegisteredTableCatalogRevision: 2}, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		observedLockedRevalidation := false
		service.catalogHooks = &registeredTableCatalogTestHooks{afterMutation: func(phase string, _ int) error {
			if phase != "after_plan_validation" {
				return nil
			}
			var lockCount int64
			err := control.QueryRow(ctx, `
				SELECT count(*)
				FROM pg_locks AS locks
				JOIN pg_class AS relation ON relation.oid=locks.relation
				JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
				WHERE namespace.nspname=$1 AND relation.relname='posts'
				  AND locks.mode='ShareRowExclusiveLock' AND locks.granted
			`, schemaName).Scan(&lockCount)
			observedLockedRevalidation = err == nil && lockCount == 1
			return err
		}}
		require.NoError(t, service.Bootstrap(ctx))
		require.True(t, observedLockedRevalidation)
		require.Contains(t, service.discoveredSchema.Dependencies[Key(schemaName, "posts")], Key(schemaName, "users"))
		require.Equal(t, int32(2), service.registeredTableInfo[Key(schemaName, "posts")].tableID)
		require.NoError(t, service.Close(ctx))
		requireCatalogPoolReleased(t, pool)
	})
}

func TestBootstrap_RegisteredTableCatalogActiveTriggerDriftPrecedesRevisionAndCandidateDefects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		targetRevision int64
		dropCandidate  bool
	}{
		{name: "revision conflict", targetRevision: 1},
		{name: "candidate physical defect", targetRevision: 2, dropCandidate: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_active_precedence_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
				RegisteredTableCatalogRevision: 1,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, seed.Close(ctx))
			schemaIdent := pgx.Identifier{schemaName}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`DROP TRIGGER %s ON %s.users`,
				pgx.Identifier{registeredTableCaptureTriggerName}.Sanitize(), schemaIdent))
			require.NoError(t, err)
			if test.dropCandidate {
				_, err = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE %s.files CASCADE`, schemaIdent))
				require.NoError(t, err)
			}
			service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
			}), RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
				{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
			},
				RegisteredTableCatalogRevision: test.targetRevision,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			err = service.Bootstrap(ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr)
			require.ErrorContains(t, err, registeredTableCaptureTriggerName)
			require.Empty(t, service.registeredTableByID)
			var revision, nextID, catalogCount int64
			require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision, next_table_id FROM sync.meta`).Scan(&revision, &nextID))
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&catalogCount))
			require.Equal(t, []int64{1, 2, 1}, []int64{revision, nextID, catalogCount})
		})
	}
}

func TestBootstrap_RegisteredTableCatalogConcurrentIdenticalAppendConverges(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_converge_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	first := newCatalogAppendTestService(t, pool, schemaName)
	second, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	services := []*SyncService{first, second}
	start := make(chan struct{})
	results := make(chan error, len(services))
	for _, service := range services {
		go func(service *SyncService) {
			<-start
			results <- service.Bootstrap(ctx)
		}(service)
	}
	close(start)
	for range services {
		require.NoError(t, <-results)
	}
	for _, service := range services {
		require.Equal(t, int32(2), service.registeredTableInfo[Key(schemaName, "files")].tableID)
	}
	var revision, count int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision FROM sync.meta`).Scan(&revision))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&count))
	require.Equal(t, []int64{2, 2}, []int64{revision, count})
}

func TestBootstrap_RegisteredTableCatalogAppendWinnerWaiterCancellationAndRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_waiter_retry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))

	winner := newCatalogAppendTestService(t, pool, schemaName)
	waiter := newCatalogAppendTestService(t, pool, schemaName)
	winnerAtLockedValidation := make(chan struct{})
	releaseWinner := make(chan struct{})
	winner.catalogHooks = &registeredTableCatalogTestHooks{afterMutation: func(phase string, _ int) error {
		if phase == "after_plan_validation" {
			close(winnerAtLockedValidation)
			<-releaseWinner
		}
		return nil
	}}
	winnerResult := make(chan error, 1)
	go func() { winnerResult <- winner.Bootstrap(ctx) }()
	select {
	case <-winnerAtLockedValidation:
	case <-ctx.Done():
		t.Fatal("append winner did not reach locked relationship validation")
	}

	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	waiterResult := make(chan error, 1)
	go func() { waiterResult <- waiter.Bootstrap(waiterCtx) }()
	waitForCatalogBootstrapAdvisoryWaiters(t, ctx, pool, 1)
	cancelWaiter()
	require.ErrorIs(t, <-waiterResult, context.Canceled)
	require.Empty(t, waiter.registeredTableByID)

	close(releaseWinner)
	require.NoError(t, <-winnerResult)
	require.Equal(t, int32(2), winner.registeredTableInfo[Key(schemaName, "files")].tableID)

	require.NoError(t, waiter.Bootstrap(ctx))
	require.Equal(t, int32(2), waiter.registeredTableInfo[Key(schemaName, "files")].tableID)
	var revision, nextID, count int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision, next_table_id FROM sync.meta`).Scan(&revision, &nextID))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&count))
	require.Equal(t, []int64{2, 3, 2}, []int64{revision, nextID, count})
}

func TestBootstrap_RegisteredTableCatalogConcurrentCompetingAppendRejectsLoser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_compete_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, seed.Close(ctx))
	configs := [][]RegisteredTable{
		{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}, {Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}}},
		{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}, {Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}}},
	}
	services := make([]*SyncService, 2)
	for index := range services {
		var err error
		services[index], err = newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(configs[index]), RegisteredTables: configs[index], RegisteredTableCatalogRevision: 2}, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		t.Cleanup(func() { _ = services[index].Close(context.Background()) })
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, service := range services {
		go func(service *SyncService) { <-start; results <- service.Bootstrap(ctx) }(service)
	}
	close(start)
	errorsSeen := []error{<-results, <-results}
	successes := 0
	failures := 0
	for _, err := range errorsSeen {
		if err == nil {
			successes++
			continue
		}
		failures++
		var catalogErr *RegisteredTableCatalogError
		require.ErrorAs(t, err, &catalogErr)
		require.Equal(t, RegisteredTableCatalogActiveTableOmitted, catalogErr.Code)
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, failures)
	var revision, count int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision FROM sync.meta`).Scan(&revision))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&count))
	require.Equal(t, []int64{2, 2}, []int64{revision, count})
}

func TestBootstrap_RegisteredTableCatalogConcurrentFreshCreationConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_fresh_race_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	services := make([]*SyncService, 2)
	for index := range services {
		var err error
		services[index], err = newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
			RegisteredTableCatalogRevision: 1,
		}, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		t.Cleanup(func() { _ = services[index].Close(context.Background()) })
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, service := range services {
		go func(service *SyncService) { <-start; results <- service.Bootstrap(ctx) }(service)
	}
	close(start)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	for _, service := range services {
		require.Equal(t, int32(1), service.registeredTableInfo[Key(schemaName, "users")].tableID)
	}
}

func TestBootstrap_RegisteredTableCatalogAppendLockCancellationRollsBack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		hold func(context.Context, *pgxpool.Pool, string) func()
	}{
		{
			name: "advisory lock",
			hold: func(ctx context.Context, pool *pgxpool.Pool, _ string) func() {
				conn, err := pool.Acquire(ctx)
				require.NoError(t, err)
				_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, syncBootstrapLockKey)
				require.NoError(t, err)
				return func() {
					var unlocked bool
					require.NoError(t, conn.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey).Scan(&unlocked))
					require.True(t, unlocked)
					conn.Release()
				}
			},
		},
		{
			name: "business table lock",
			hold: func(ctx context.Context, pool *pgxpool.Pool, schemaName string) func() {
				tx, err := pool.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s.files IN ACCESS EXCLUSIVE MODE`, pgx.Identifier{schemaName}.Sanitize()))
				require.NoError(t, err)
				return func() { require.NoError(t, tx.Rollback(context.Background())) }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			schemaName := "catalog_lock_cancel_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
			t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
			seed := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
				RegisteredTableCatalogRevision: 1,
			}, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, seed.Close(ctx))
			release := test.hold(ctx, pool, schemaName)
			service := newCatalogAppendTestService(t, pool, schemaName)
			waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
			err := service.Bootstrap(waitCtx)
			cancel()
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			release()
			requireCatalogAppendRollback(t, ctx, pool, service, schemaName, "files")
		})
	}
}

type catalogCountingRows struct {
	pgx.Rows
	readCount *int
}

func (r *catalogCountingRows) Next() bool {
	ok := r.Rows.Next()
	if ok {
		*r.readCount++
	}
	return ok
}

type catalogReservationCountingQuerier struct {
	syncCatalogQuerier
	readCount int
}

func (q *catalogReservationCountingQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := q.syncCatalogQuerier.Query(ctx, sql, args...)
	if err != nil || !strings.Contains(sql, "FROM sync.server_source_reservations") {
		return rows, err
	}
	return &catalogCountingRows{Rows: rows, readCount: &q.readCount}, nil
}

func TestRegisteredTableCatalog_ReservationValidationConsumesExpectedPlusOne(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "catalog_reservation_bound_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), RegisteredTables: []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
		ReservedServerSourceIDs:        []string{"expected"},
	}, integrationTestLogger(slog.LevelWarn))
	for index := 0; index < 50; index++ {
		_, err := pool.Exec(ctx, `INSERT INTO sync.server_source_reservations (source_id) VALUES ($1)`, fmt.Sprintf("extra-%02d", index))
		require.NoError(t, err)
	}
	querier := &catalogReservationCountingQuerier{syncCatalogQuerier: pool}
	err := service.validateServerSourceReservations(ctx, querier)
	require.Error(t, err)
	require.Equal(t, len(service.config.ReservedServerSourceIDs)+1, querier.readCount)
}

func newCatalogAppendTestService(t *testing.T, pool *pgxpool.Pool, schemaName string) *SyncService {
	t.Helper()
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	},
		RegisteredTableCatalogRevision: 2,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}

func requireCatalogAppendRollback(t *testing.T, ctx context.Context, pool *pgxpool.Pool, service *SyncService, schemaName, tableName string) {
	t.Helper()
	var revision, nextID, catalogCount, triggerCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision, next_table_id FROM sync.meta`).Scan(&revision, &nextID))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog`).Scan(&catalogCount))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=$1 AND c.relname=$2 AND t.tgname = ANY($3::text[])
	`, schemaName, tableName, []string{registeredTableOwnerGuardTrigger, registeredTableCaptureTriggerName, registeredTableTruncateGuardTrigger}).Scan(&triggerCount))
	require.Equal(t, []int64{1, 2, 1, 0}, []int64{revision, nextID, catalogCount, triggerCount})
	require.Empty(t, service.registeredTableByID)
	status, err := service.GetStatus(ctx)
	require.NoError(t, err)
	require.False(t, status.AcceptingOperations)
}

func newCatalogSingleConnectionPool(
	t *testing.T,
	ctx context.Context,
	control *pgxpool.Pool,
	scenario string,
) *pgxpool.Pool {
	t.Helper()
	config := control.Config()
	config.MaxConns = 1
	config.MinConns = 0
	config.ConnConfig.RuntimeParams["application_name"] = "catalog-one-connection-" + scenario
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))
	return pool
}

func requireCatalogPoolReleased(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "catalog Bootstrap connection was not released")
}

func requireCatalogBootstrapResourcesReleased(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	requireCatalogPoolReleased(t, pool)
	var lockCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_locks
		WHERE locktype='advisory'
		  AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		  AND classid::bigint=($1::bigint >> 32)
		  AND objid::bigint=($1::bigint & 4294967295::bigint)
		  AND objsubid=1
	`, syncBootstrapLockKey).Scan(&lockCount))
	require.Zero(t, lockCount, "catalog Bootstrap advisory lock was not released")
}

func waitForCatalogBootstrapAdvisoryWaiters(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	want int64,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		var count int64
		err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_locks
			WHERE locktype='advisory'
			  AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
			  AND classid::bigint=($1::bigint >> 32)
			  AND objid::bigint=($1::bigint & 4294967295::bigint)
			  AND objsubid=1
			  AND NOT granted
		`, syncBootstrapLockKey).Scan(&count)
		return err == nil && count == want
	}, 5*time.Second, 10*time.Millisecond, "expected %d catalog Bootstrap advisory-lock waiters", want)
}
