package oversync

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/wirevalue"
)

const (
	// This records the exponent-only expansion of the smallest negative
	// binary64 value. It is intentionally not the JSONB physical ceiling:
	// PostgreSQL can retain substantially more decimal scale for the same finite
	// value.
	maxPostgresBinary64ExpandedNumberBytes int64 = 327

	// This former exponent-expansion factor remains useful as a regression
	// boundary. It is not a sufficient physical ceiling for JSONB because
	// PostgreSQL also retains arbitrary decimal scale.
	maxPostgresPayloadBytesPerLogicalByte = maxPostgresBinary64ExpandedNumberBytes + 1

	// PostgreSQL varlena values, including json/jsonb and their text output,
	// cannot exceed the one-gigabyte datum boundary. Plain json preserves
	// insignificant whitespace, so it has no ratio to its canonical JCS size;
	// the database datum boundary is its finite physical ceiling.
	maxPostgresPayloadDatumBytes int64 = (1 << 30) - 1

	maxPostgresOrdinaryPayloadBytesPerLogicalByte int64 = 2
)

// physicalDatabasePayloadByteLimitForColumnTypes bounds the temporary
// PostgreSQL-facing JSON representation independently of the exact logical Q39
// and committed-JCS limits. The derivation covers the worst supported JSONB
// numeric rendering, PostgreSQL JSON formatting, bytea hexadecimal text, and
// all smaller scalar codecs. Exact logical accounting remains authoritative
// after canonicalization.
func physicalDatabasePayloadByteLimitForColumnTypes(logicalLimit int64, columnTypes map[string]string) (int64, error) {
	baseLimit, err := checkedPhysicalPayloadByteLimit(logicalLimit, maxPostgresOrdinaryPayloadBytesPerLogicalByte)
	if err != nil {
		return 0, err
	}
	var floatingColumns int64
	for _, columnType := range columnTypes {
		switch columnType {
		case "json", "jsonb", "numeric":
			// PostgreSQL json preserves insignificant whitespace and jsonb
			// and numeric preserve decimal scale. These representations therefore
			// have no finite ratio to canonical JCS bytes. The database datum
			// boundary is their smallest sufficient representation ceiling.
			return maxPostgresPayloadDatumBytes, nil
		case "float4", "float8":
			floatingColumns++
		}
	}
	// A PostgreSQL binary floating scalar can render as at most 327 decimal
	// bytes while the wire codec canonicalizes it to a short finite string.
	// Charge that full maximum once per registered floating column in addition
	// to the ordinary object/string/bytea allowance. Arrays and composites are
	// unsupported, so one row cannot contain more such scalar values than the
	// registered floating-column count.
	if floatingColumns > (maxPostgresPayloadDatumBytes-baseLimit)/maxPostgresBinary64ExpandedNumberBytes {
		return maxPostgresPayloadDatumBytes, nil
	}
	return baseLimit + floatingColumns*maxPostgresBinary64ExpandedNumberBytes, nil
}

func physicalStoredPushPayloadByteLimit(logicalLimit int64) (int64, error) {
	// Retained apply payloads preserve accepted inner JSON as an outer JSON
	// string. They do not render a business jsonb value, so bytea hexadecimal
	// is their largest codec expansion.
	return checkedPhysicalPayloadByteLimit(logicalLimit, maxPostgresOrdinaryPayloadBytesPerLogicalByte)
}

// physicalStoredWirePayloadByteLimit bounds retained payload_wire values before
// PostgreSQL transfers them to Go. Service-owned payload_wire is generated from
// the supported generation wire types: nested JSON and numeric values are wire
// strings, while PostgreSQL's outer-object rendering can add at most one space
// per delimiter. Twice the canonical ceiling therefore covers that formatting
// expansion. Callers still enforce the exact logical limit after transfer.
func physicalStoredWirePayloadByteLimit(logicalLimit int64) (int64, error) {
	return checkedPhysicalPayloadByteLimit(logicalLimit, maxPostgresOrdinaryPayloadBytesPerLogicalByte)
}

func checkedPhysicalPayloadByteLimit(logicalLimit, factor int64) (int64, error) {
	if logicalLimit <= 0 || factor <= 0 || logicalLimit > math.MaxInt64/factor {
		return 0, fmt.Errorf("logical bundle byte limit %d cannot produce a finite physical payload limit", logicalLimit)
	}
	limit := logicalLimit * factor
	if limit > maxPostgresPayloadDatumBytes {
		return maxPostgresPayloadDatumBytes, nil
	}
	return limit, nil
}

func (s *SyncService) normalizePushPayloadFields(schemaName, tableName string, payloadObject map[string]any) error {
	columnTypes := s.columnTypesForTable(schemaName, tableName)
	if len(columnTypes) == 0 {
		return nil
	}

	for columnName, rawValue := range payloadObject {
		columnType := columnTypes[strings.ToLower(columnName)]
		if rawValue == nil {
			continue
		}
		switch columnType {
		case "bytea":
			encodedValue, ok := rawValue.(string)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload binary field %s.%s.%s must be a base64 string", schemaName, tableName, columnName)}
			}
			decodedValue, err := base64.StdEncoding.DecodeString(encodedValue)
			if err != nil || base64.StdEncoding.EncodeToString(decodedValue) != encodedValue {
				return &PushValidationError{Message: fmt.Sprintf("payload binary field %s.%s.%s must use canonical standard base64", schemaName, tableName, columnName)}
			}
			payloadObject[columnName] = "\\x" + hex.EncodeToString(decodedValue)
		case "int2", "int4", "int8":
			raw, ok := rawValue.(string)
			if !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical signed integer JSON string", "legacy_json_number")
			}
			value, err := wirevalue.ParseInt64(raw)
			if err != nil {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical signed integer JSON string", "invalid_integer")
			}
			if !integerFitsPostgresType(value, columnType) {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical signed integer JSON string", "out_of_range")
			}
		case "numeric", "decimal":
			raw, ok := rawValue.(string)
			if !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "finite decimal JSON string", "legacy_json_number")
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return numericPushValidationError(schemaName, tableName, columnName, "finite decimal JSON string", "invalid_decimal")
			}
		case "float4", "float8":
			raw, ok := rawValue.(string)
			if !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical finite binary64 JSON string", "legacy_json_number")
			}
			value, err := wirevalue.ParseFloat64(raw)
			if err != nil {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical finite binary64 JSON string", "invalid_float")
			}
			if columnType == "float4" && math.IsInf(float64(float32(value)), 0) {
				return numericPushValidationError(schemaName, tableName, columnName, "finite PostgreSQL float4 value", "destination_type_invalid")
			}
		case "bool":
			if _, ok := rawValue.(bool); !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "JSON Boolean", "invalid_boolean")
			}
		case "uuid":
			raw, ok := rawValue.(string)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload field %s.%s.%s must be a canonical lowercase dashed UUID", schemaName, tableName, columnName)}
			}
			parsed, err := uuid.Parse(raw)
			if err != nil || parsed.String() != raw || len(raw) != 36 {
				return &PushValidationError{Message: fmt.Sprintf("payload field %s.%s.%s must be a canonical lowercase dashed UUID", schemaName, tableName, columnName)}
			}
		case "json", "jsonb":
			raw, ok := rawValue.(string)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload field %s.%s.%s must carry canonical inner JCS bytes in a JSON string", schemaName, tableName, columnName)}
			}
			canonical, err := jcs.Canonicalize([]byte(raw))
			if err != nil || string(canonical) != raw {
				return &PushValidationError{Message: fmt.Sprintf("payload field %s.%s.%s must carry canonical inner JCS bytes in a JSON string", schemaName, tableName, columnName)}
			}
		}
	}

	return nil
}

func (s *SyncService) canonicalizeWirePayload(schemaName, tableName string, payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return payload, nil
	}

	columnTypes := s.columnTypesForTable(schemaName, tableName)
	if len(columnTypes) == 0 {
		return append(json.RawMessage(nil), payload...), nil
	}

	payloadObject, err := jcs.DecodeObjectLossless(payload)
	if err != nil {
		return nil, fmt.Errorf("decode payload for %s.%s: %w", schemaName, tableName, err)
	}

	changed := false
	if stripHiddenOwnerColumn(payloadObject) {
		changed = true
	}
	for columnName, rawValue := range payloadObject {
		if rawValue == nil {
			continue
		}
		columnType := columnTypes[strings.ToLower(columnName)]
		switch columnType {
		case "bytea":
			canonicalValue, ok, err := canonicalizeWireBinaryValue(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize binary payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if ok && canonicalValue != rawValue {
				payloadObject[columnName] = canonicalValue
				changed = true
			}
		case "int2", "int4", "int8":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize integer payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			value, err := wirevalue.ParseInt64(raw)
			if err != nil {
				return nil, fmt.Errorf("canonicalize integer payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if !integerFitsPostgresType(value, columnType) {
				return nil, fmt.Errorf("canonicalize integer payload for %s.%s.%s: value is outside PostgreSQL %s range", schemaName, tableName, columnName, columnType)
			}
			payloadObject[columnName] = raw
			changed = true
		case "numeric", "decimal":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize exact decimal payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return nil, fmt.Errorf("canonicalize exact decimal payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = raw
			changed = true
		case "float4", "float8":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize floating payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			value, err := parseDatabaseFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("canonicalize floating payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			var canonical string
			if columnType == "float4" {
				canonical, err = wirevalue.RenderFloat32(float32(value))
			} else {
				canonical, err = wirevalue.RenderFloat64(value)
			}
			if err != nil {
				return nil, fmt.Errorf("canonicalize floating payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = canonical
			changed = true
		case "bool":
			if _, ok := rawValue.(bool); !ok {
				return nil, fmt.Errorf("canonicalize Boolean payload for %s.%s.%s: expected database Boolean, got %T", schemaName, tableName, columnName, rawValue)
			}
		case "uuid":
			raw, ok := rawValue.(string)
			if !ok {
				return nil, fmt.Errorf("canonicalize UUID payload for %s.%s.%s: expected string, got %T", schemaName, tableName, columnName, rawValue)
			}
			parsed, err := uuid.Parse(raw)
			if err != nil || parsed.String() != raw || len(raw) != 36 {
				return nil, fmt.Errorf("canonicalize UUID payload for %s.%s.%s: database value is not canonical", schemaName, tableName, columnName)
			}
		case "text", "varchar":
			raw, ok := rawValue.(string)
			if !ok || !utf8.ValidString(raw) || strings.IndexByte(raw, 0) >= 0 {
				return nil, fmt.Errorf("canonicalize text payload for %s.%s.%s: database value is outside the PostgreSQL-compatible Unicode scalar domain", schemaName, tableName, columnName)
			}
		case "json", "jsonb":
			raw, ok := rawValue.(string)
			if !ok {
				return nil, fmt.Errorf("canonicalize JSON payload for %s.%s.%s: expected schema-aware database JSON text, got %T", schemaName, tableName, columnName, rawValue)
			}
			inner, err := jcs.Canonicalize([]byte(raw))
			if err != nil {
				return nil, fmt.Errorf("canonicalize JSON payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = string(inner)
			changed = true
		}
	}

	if !changed {
		return append(json.RawMessage(nil), payload...), nil
	}

	canonicalPayload, err := jcs.Marshal(payloadObject)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical payload for %s.%s: %w", schemaName, tableName, err)
	}
	canonicalPayload, err = canonicalJSON(canonicalPayload)
	if err != nil {
		return nil, fmt.Errorf("canonicalize canonical payload for %s.%s: %w", schemaName, tableName, err)
	}
	return canonicalPayload, nil
}

// databaseWirePayloadExpression returns a PostgreSQL expression that keeps SQL
// NULL distinct from a json/jsonb document whose value is JSON null. JSON
// columns are represented as their exact PostgreSQL text inside an outer JSON
// string; canonicalizeWirePayload validates and canonicalizes each inner
// document independently.
func databaseWirePayloadExpression(rowAlias string, columnTypes map[string]string) string {
	expression := fmt.Sprintf("to_jsonb(%s) - %s", rowAlias, quoteSQLLiteral(syncScopeColumnName))
	jsonColumns := databaseJSONColumnNames(columnTypes)
	if len(jsonColumns) == 0 {
		return expression
	}

	// PostgreSQL limits function calls to 100 arguments. Each JSON column
	// contributes one key and one value, so build at most 50 columns per object
	// and concatenate the bounded objects. PostgreSQL tables may contain far
	// more than 50 JSON columns, and the generation contract does not impose a
	// lower protocol-specific column limit.
	const maxJSONBuildObjectPairs = 50
	parts := []string{expression}
	for start := 0; start < len(jsonColumns); start += maxJSONBuildObjectPairs {
		end := min(start+maxJSONBuildObjectPairs, len(jsonColumns))
		arguments := make([]string, 0, (end-start)*2)
		for _, columnName := range jsonColumns[start:end] {
			columnIdent := pgx.Identifier{columnName}.Sanitize()
			arguments = append(arguments,
				quoteSQLLiteral(columnName),
				fmt.Sprintf("CASE WHEN %s.%s IS NULL THEN 'null'::jsonb ELSE to_jsonb((%s.%s)::text) END", rowAlias, columnIdent, rowAlias, columnIdent),
			)
		}
		parts = append(parts, fmt.Sprintf("jsonb_build_object(%s)", strings.Join(arguments, ", ")))
	}
	return fmt.Sprintf("(%s)", strings.Join(parts, " || "))
}

// databaseVisibleWirePayloadExpression selects only generation-visible columns
// in PostgreSQL, before any potentially large hidden value reaches Go.
func databaseVisibleWirePayloadExpression(rowAlias string, columns []GenerationColumn, columnTypes map[string]string) string {
	const maxPairs = 50
	parts := make([]string, 0, (len(columns)+maxPairs-1)/maxPairs)
	for start := 0; start < len(columns); start += maxPairs {
		end := min(start+maxPairs, len(columns))
		args := make([]string, 0, (end-start)*2)
		for _, column := range columns[start:end] {
			ident := pgx.Identifier{column.Name}.Sanitize()
			value := fmt.Sprintf("to_jsonb(%s.%s)", rowAlias, ident)
			if typ := columnTypes[strings.ToLower(column.Name)]; typ == "json" || typ == "jsonb" {
				value = fmt.Sprintf("CASE WHEN %s.%s IS NULL THEN 'null'::jsonb ELSE to_jsonb((%s.%s)::text) END", rowAlias, ident, rowAlias, ident)
			}
			args = append(args, quoteSQLLiteral(column.Name), value)
		}
		parts = append(parts, fmt.Sprintf("jsonb_build_object(%s)", strings.Join(args, ", ")))
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, " || ") + ")"
}

func databaseJSONColumnNames(columnTypes map[string]string) []string {
	columns := make([]string, 0)
	for columnName, columnType := range columnTypes {
		if columnType == "json" || columnType == "jsonb" {
			columns = append(columns, columnName)
		}
	}
	sort.Strings(columns)
	return columns
}

func numericPushValidationError(schemaName, tableName, columnName, expected, category string) error {
	return &PushValidationError{Message: fmt.Sprintf(
		"payload field %s.%s.%s must use %s (category=%s)",
		schemaName,
		tableName,
		columnName,
		expected,
		category,
	)}
}

func integerFitsPostgresType(value int64, columnType string) bool {
	switch columnType {
	case "int2":
		return value >= -32768 && value <= 32767
	case "int4":
		return value >= -2147483648 && value <= 2147483647
	case "int8":
		return true
	default:
		return false
	}
}

func parseDatabaseFloat(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, fmt.Errorf("database value must be finite")
	}
	return value, nil
}

func exactNumberText(value any) (string, error) {
	switch typed := value.(type) {
	case json.Number:
		return typed.String(), nil
	case string:
		return typed, nil
	default:
		return "", fmt.Errorf("expected exact numeric JSON string or database number, got %T", value)
	}
}

func (s *SyncService) columnTypesForTable(schemaName, tableName string) map[string]string {
	if s == nil {
		return nil
	}
	return s.columnTypesByTable[Key(schemaName, tableName)]
}

func canonicalizeWireBinaryValue(value any) (string, bool, error) {
	if value == nil {
		return "", false, nil
	}

	rawValue, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("expected JSON string, got %T", value)
	}

	decoded, err := decodeStoredBinaryValue(rawValue)
	if err != nil {
		return "", false, err
	}
	return base64.StdEncoding.EncodeToString(decoded), true, nil
}

func decodeStoredBinaryValue(rawValue string) ([]byte, error) {
	if strings.HasPrefix(rawValue, "\\x") || strings.HasPrefix(rawValue, "\\X") {
		decoded, err := hex.DecodeString(rawValue[2:])
		if err != nil {
			return nil, fmt.Errorf("decode postgres bytea hex: %w", err)
		}
		return decoded, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(rawValue)
	if err != nil {
		return nil, fmt.Errorf("decode base64: %w", err)
	}
	return decoded, nil
}
