package oversync

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

func (s *SyncService) normalizePushPayloadBinaryFields(schemaName, tableName string, payloadObject map[string]any) error {
	columnTypes := s.columnTypesForTable(schemaName, tableName)
	if len(columnTypes) == 0 {
		return nil
	}

	for columnName, rawValue := range payloadObject {
		if columnTypes[strings.ToLower(columnName)] != "bytea" || rawValue == nil {
			continue
		}

		encodedValue, ok := rawValue.(string)
		if !ok {
			return &PushValidationError{Message: fmt.Sprintf("payload binary field %s.%s.%s must be a base64 string", schemaName, tableName, columnName)}
		}

		decodedValue, err := base64.StdEncoding.DecodeString(encodedValue)
		if err != nil {
			return &PushValidationError{Message: fmt.Sprintf("payload binary field %s.%s.%s must be valid base64", schemaName, tableName, columnName)}
		}
		payloadObject[columnName] = "\\x" + hex.EncodeToString(decodedValue)
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

	var payloadObject map[string]any
	if err := json.Unmarshal(payload, &payloadObject); err != nil {
		return nil, fmt.Errorf("decode payload for %s.%s: %w", schemaName, tableName, err)
	}

	changed := false
	for columnName, rawValue := range payloadObject {
		if columnTypes[strings.ToLower(columnName)] != "bytea" {
			continue
		}
		canonicalValue, ok, err := canonicalizeWireBinaryValue(rawValue)
		if err != nil {
			return nil, fmt.Errorf("canonicalize binary payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
		}
		if ok && canonicalValue != rawValue {
			payloadObject[columnName] = canonicalValue
			changed = true
		}
	}

	if !changed {
		return append(json.RawMessage(nil), payload...), nil
	}

	canonicalPayload, err := json.Marshal(payloadObject)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical payload for %s.%s: %w", schemaName, tableName, err)
	}
	canonicalPayload, err = canonicalJSON(canonicalPayload)
	if err != nil {
		return nil, fmt.Errorf("canonicalize canonical payload for %s.%s: %w", schemaName, tableName, err)
	}
	return canonicalPayload, nil
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
