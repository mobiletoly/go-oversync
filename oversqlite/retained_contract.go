package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Discovery metadata has exact keys and no optional/null fields. Validate its
// shape before decoding: encoding/json alone accepts duplicate and folded keys.
func decodeDiscoveredContract(raw []byte, selected oversync.SchemaGenerationID) (*oversync.GenerationDiscoveryResponse, error) {
	if err := jcs.ValidateStructure(raw); err != nil {
		return nil, err
	}
	root, err := requiredContractFields(raw, "protocol_version", "schema_generation", "limits", "watch_enabled")
	if err != nil {
		return nil, err
	}
	descriptor, err := requiredContractFields(root["schema_generation"], "id", "fingerprint", "tables")
	if err != nil {
		return nil, err
	}
	if _, err := requiredContractFields(root["limits"],
		"max_push_rows", "max_push_decoded_bytes", "max_pull_bundles", "max_pull_decoded_bytes",
		"max_snapshot_rows_per_chunk", "max_snapshot_decoded_bytes_per_chunk", "max_snapshot_rows",
		"max_snapshot_logical_bytes", "max_row_decoded_bytes"); err != nil {
		return nil, err
	}
	var tables []json.RawMessage
	if err := json.Unmarshal(descriptor["tables"], &tables); err != nil {
		return nil, err
	}
	for _, rawTable := range tables {
		table, err := requiredContractFields(rawTable, "schema", "table", "sync_key_columns", "columns", "foreign_keys")
		if err != nil {
			return nil, err
		}
		if err := validateContractArrayFields(table["columns"], "name", "wire_type", "nullable"); err != nil {
			return nil, err
		}
		if err := validateContractArrayFields(table["foreign_keys"], "columns", "referenced_schema", "referenced_table", "referenced_columns", "on_delete", "on_update"); err != nil {
			return nil, err
		}
	}
	var response oversync.GenerationDiscoveryResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	if err := requireSupportedProtocolVersion(response.ProtocolVersion); err != nil {
		return nil, err
	}
	if response.SchemaGeneration.ID != strconv.FormatInt(int64(selected), 10) {
		return nil, fmt.Errorf("discovered generation does not match selected ID")
	}
	for _, value := range []int64{
		int64(response.Limits.MaxPushRows), response.Limits.MaxPushDecodedBytes, int64(response.Limits.MaxPullBundles),
		response.Limits.MaxPullDecodedBytes, int64(response.Limits.MaxSnapshotRowsPerChunk),
		response.Limits.MaxSnapshotDecodedBytesPerChunk, response.Limits.MaxSnapshotRows,
		response.Limits.MaxSnapshotLogicalBytes, response.Limits.MaxRowDecodedBytes,
	} {
		if value < 1 {
			return nil, fmt.Errorf("discovery limits must be positive")
		}
	}
	return &response, nil
}

func requiredContractFields(raw []byte, names ...string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	if len(object) != len(names) {
		return nil, fmt.Errorf("generation descriptor has unexpected or missing fields")
	}
	for _, name := range names {
		value, ok := object[name]
		if !ok || string(value) == "null" {
			return nil, fmt.Errorf("generation descriptor requires %s", name)
		}
	}
	return object, nil
}

func validateContractArrayFields(raw []byte, names ...string) error {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := requiredContractFields(entry, names...); err != nil {
			return err
		}
	}
	return nil
}

// Convert the wire descriptor once for the existing registry validator.
func localContractFromDescriptor(selected oversync.SchemaGenerationID, descriptor oversync.SchemaGenerationCapabilities) LocalGenerationContract {
	local := LocalGenerationContract{Proof: oversync.SchemaGenerationProof{ID: selected, Fingerprint: descriptor.Fingerprint}, Generation: oversync.SchemaGeneration{ID: selected}}
	for _, table := range descriptor.Tables {
		local.RegisteredTables = append(local.RegisteredTables, oversync.RegisteredTable{Schema: table.Schema, Table: table.Table, SyncKeyColumns: table.SyncKeyColumns})
		generationTable := oversync.GenerationTable{Schema: table.Schema, Table: table.Table}
		for _, column := range table.Columns {
			generationTable.Columns = append(generationTable.Columns, oversync.GenerationColumn{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable})
		}
		for _, fk := range table.ForeignKeys {
			generationTable.ForeignKeys = append(generationTable.ForeignKeys, oversync.GenerationForeignKey{Columns: fk.Columns, ReferencedSchema: fk.ReferencedSchema, ReferencedTable: fk.ReferencedTable, ReferencedColumns: fk.ReferencedColumns, OnDelete: fk.OnDelete, OnUpdate: fk.OnUpdate})
		}
		local.Generation.Tables = append(local.Generation.Tables, generationTable)

	}
	return local
}

// deriveLocalContract adapts server-owned metadata to the existing validation
// and apply engine. The application supplies only its table/key registration.
func deriveLocalContract(ctx context.Context, q upgradeQueryer, selected oversync.SchemaGenerationID, tables []SyncTable, descriptor oversync.SchemaGenerationCapabilities) (LocalGenerationContract, error) {
	local := localContractFromDescriptor(selected, descriptor)
	local.Tables = tables
	if descriptor.ID != strconv.FormatInt(int64(selected), 10) || len(tables) != len(descriptor.Tables) {
		return local, fmt.Errorf("selected generation must match the complete local table set")
	}
	seen := make(map[string]bool)
	for _, table := range descriptor.Tables {
		if seen[table.Table] {
			return local, fmt.Errorf("server tables collide at local name %s", table.Table)
		}
		seen[table.Table] = true
		affinities, err := inspectLocalAffinities(ctx, q, table.Table)
		if err != nil {
			return local, err
		}
		columns := LocalTableColumns{Schema: table.Schema, Table: table.Table}
		for _, column := range table.Columns {
			codec, err := automaticLocalCodec(column.WireType, affinities[column.Name])
			if err != nil {
				return local, fmt.Errorf("column %s.%s: %w", table.Table, column.Name, err)
			}
			columns.Columns = append(columns.Columns, LocalColumnContract{Name: column.Name, Codec: codec})
		}
		local.Columns = append(local.Columns, columns)
	}
	compiled, err := compileCompleteLocalContract("discovered", local)
	if err != nil {
		return local, err
	}
	for _, table := range compiled.localTables {
		if err := validatePhysicalLocalTable(ctx, q, table, compiled.codecs[table.Schema+"."+table.Table]); err != nil {
			return local, err
		}
	}
	return local, nil
}

func inspectLocalAffinities(ctx context.Context, q upgradeQueryer, table string) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, "PRAGMA table_xinfo("+quoteIdent(table)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var cid, notNull, pk, hidden int
		var name, declared string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &declared, &notNull, &defaultValue, &pk, &hidden); err != nil {
			return nil, err
		}
		result[name] = sqliteDeclaredAffinity(declared)
	}
	return result, rows.Err()
}

func automaticLocalCodec(wire oversync.GenerationWireType, affinity string) (LocalColumnCodec, error) {
	codecs := map[oversync.GenerationWireType]LocalColumnCodec{
		oversync.GenerationWireTypeBoolean: LocalCodecBooleanInteger,
		oversync.GenerationWireTypeInt64:   LocalCodecInt64Integer,
		oversync.GenerationWireTypeFloat64: LocalCodecFloat64Real,
		oversync.GenerationWireTypeDecimal: LocalCodecDecimalText,
		oversync.GenerationWireTypeText:    LocalCodecText,
		oversync.GenerationWireTypeBytes:   LocalCodecBytesBlob,
		oversync.GenerationWireTypeJSON:    LocalCodecJSONText,
		oversync.GenerationWireTypeUUID:    LocalCodecUUIDText,
	}
	codec := codecs[wire]
	if wire == oversync.GenerationWireTypeUUID && affinity == "BLOB" {
		codec = LocalCodecUUIDBlob
	}
	if !codecMatchesAffinity(codec, affinity) {
		return "", fmt.Errorf("SQLite %s cannot represent server %s", affinity, wire)
	}
	return codec, nil
}

func (c *Client) validateAndUnwrapCapturedPayload(table string, payload map[string]any) error {
	if c.localGeneration == nil {
		return fmt.Errorf("server contract is unconfirmed; local intent cannot become a wire request")
	}
	expected := c.localGeneration.codecs[c.schemaForTable(table)+"."+strings.ToLower(table)]
	if len(payload) != len(expected) {
		return fmt.Errorf("typed local capture must contain every admitted column")
	}
	// Persisted captures use exact admitted names. A case alias must not stand
	// in for a missing column through declaredColumn's case-insensitive lookup.
	for column := range expected {
		if _, present := payload[column]; !present {
			return fmt.Errorf("typed local capture is missing admitted column %s.%s", table, column)
		}
	}
	for column, captured := range payload {
		codec, declared, ok := c.declaredColumn(table, column)
		if !ok {
			return fmt.Errorf("column %s.%s is absent from the admitted contract", table, column)
		}
		entry, ok := captured.(map[string]any)
		if !ok || len(entry) != 2 {
			return fmt.Errorf("column %s.%s has invalid typed local capture", table, column)
		}
		storage, ok := entry["storage"].(string)
		value, present := entry["value"]
		if !ok || !present {
			return fmt.Errorf("column %s.%s has incomplete typed local capture", table, column)
		}
		if storage == "null" {
			if value != nil || !declared.Nullable {
				return fmt.Errorf("column %s.%s must not be null", table, column)
			}
			payload[column] = nil
			continue
		}
		if _, ok := value.(string); !ok || !codecMatchesAffinity(codec, strings.ToUpper(storage)) {
			return fmt.Errorf("column %s.%s has incompatible SQLite storage %s", table, column, storage)
		}
		payload[column] = value
	}
	return nil
}
