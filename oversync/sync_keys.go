package oversync

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type normalizedVisibleSyncKey struct {
	column    string
	keyType   string
	keyString string
	keyJSON   string
	dbValue   any
}

func (s *SyncService) syncKeyInfoForTable(schemaName, tableName string) (registeredTableRuntimeInfo, error) {
	if s == nil {
		return registeredTableRuntimeInfo{}, &PushValidationError{Message: fmt.Sprintf("table %s.%s is not configured for sync", schemaName, tableName)}
	}
	info, ok := s.registeredTableInfo[Key(schemaName, tableName)]
	if !ok {
		return registeredTableRuntimeInfo{}, &PushValidationError{Message: fmt.Sprintf("table %s.%s is not configured for sync", schemaName, tableName)}
	}
	return info, nil
}

func (s *SyncService) syncKeyColumnForTable(schemaName, tableName string) (string, error) {
	info, err := s.syncKeyInfoForTable(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return info.syncKeyColumn, nil
}

func (s *SyncService) syncKeyTypeForTable(schemaName, tableName string) (string, error) {
	info, err := s.syncKeyInfoForTable(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return info.syncKeyType, nil
}

func (s *SyncService) normalizeVisibleSyncKey(schemaName, tableName string, rawKey SyncKey) (normalizedVisibleSyncKey, error) {
	info, err := s.syncKeyInfoForTable(schemaName, tableName)
	if err != nil {
		return normalizedVisibleSyncKey{}, err
	}

	rawValue, ok := rawKey[info.syncKeyColumn]
	if !ok {
		return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("row key for %s.%s must include %q", schemaName, tableName, info.syncKeyColumn)}
	}
	keyString, ok := rawValue.(string)
	if !ok {
		return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("row key %s.%s.%s must be a string", schemaName, tableName, info.syncKeyColumn)}
	}

	normalized := normalizedVisibleSyncKey{
		column:    info.syncKeyColumn,
		keyType:   info.syncKeyType,
		keyString: keyString,
		dbValue:   keyString,
	}
	switch info.syncKeyType {
	case syncKeyTypeUUID:
		parsed, err := uuid.Parse(keyString)
		if err != nil {
			return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("row key %s.%s.%s must be a UUID", schemaName, tableName, info.syncKeyColumn)}
		}
		canonical := parsed.String()
		if keyString != canonical {
			return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("row key %s.%s.%s must use canonical dashed lowercase UUID text", schemaName, tableName, info.syncKeyColumn)}
		}
		normalized.keyString = canonical
		normalized.dbValue = parsed
	case syncKeyTypeText:
	default:
		return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("table %s.%s uses unsupported sync key type %q", schemaName, tableName, info.syncKeyType)}
	}

	keyRaw, err := json.Marshal(map[string]any{info.syncKeyColumn: normalized.keyString})
	if err != nil {
		return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("marshal row key for %s.%s: %v", schemaName, tableName, err)}
	}
	keyJSONBytes, err := canonicalJSON(keyRaw)
	if err != nil {
		return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("canonicalize row key for %s.%s: %v", schemaName, tableName, err)}
	}
	normalized.keyJSON = string(keyJSONBytes)
	return normalized, nil
}

func normalizePayloadVisibleSyncKey(payloadObj map[string]any, key normalizedVisibleSyncKey, schemaName, tableName string) error {
	if existingKey, ok := payloadObj[key.column]; ok {
		existingKeyString, ok := existingKey.(string)
		if !ok || existingKeyString != key.keyString {
			return &PushValidationError{Message: fmt.Sprintf("payload key %s.%s.%s must match request key", schemaName, tableName, key.column)}
		}
		return nil
	}
	payloadObj[key.column] = key.keyString
	return nil
}

func injectOwnerUserIDPayload(payload []byte, ownerUserID string) ([]byte, error) {
	var payloadObj map[string]any
	if err := json.Unmarshal(payload, &payloadObj); err != nil {
		return nil, fmt.Errorf("decode payload for owner injection: %w", err)
	}
	payloadObj[syncScopeColumnName] = ownerUserID
	payloadRaw, err := json.Marshal(payloadObj)
	if err != nil {
		return nil, fmt.Errorf("marshal owner-injected payload: %w", err)
	}
	return canonicalJSON(payloadRaw)
}

func decodeNormalizedStoredSyncKey(schemaName, tableName string, info registeredTableRuntimeInfo, keyJSON string) (normalizedVisibleSyncKey, error) {
	key, err := decodeSyncKeyJSON(keyJSON)
	if err != nil {
		return normalizedVisibleSyncKey{}, fmt.Errorf("decode sync key for %s.%s: %w", schemaName, tableName, err)
	}
	rawValue, ok := key[info.syncKeyColumn]
	if !ok {
		return normalizedVisibleSyncKey{}, fmt.Errorf("push session key missing %s for %s.%s", info.syncKeyColumn, schemaName, tableName)
	}
	keyString, ok := rawValue.(string)
	if !ok {
		return normalizedVisibleSyncKey{}, fmt.Errorf("push session key %s.%s.%s must be string", schemaName, tableName, info.syncKeyColumn)
	}
	normalized := normalizedVisibleSyncKey{
		column:    info.syncKeyColumn,
		keyType:   info.syncKeyType,
		keyString: keyString,
		dbValue:   keyString,
		keyJSON:   keyJSON,
	}
	switch info.syncKeyType {
	case syncKeyTypeUUID:
		parsed, err := uuid.Parse(keyString)
		if err != nil {
			return normalizedVisibleSyncKey{}, fmt.Errorf("push session key %s.%s.%s must be UUID: %w", schemaName, tableName, info.syncKeyColumn, err)
		}
		if keyString != parsed.String() {
			return normalizedVisibleSyncKey{}, fmt.Errorf("push session key %s.%s.%s must use canonical dashed lowercase UUID text", schemaName, tableName, info.syncKeyColumn)
		}
		normalized.dbValue = parsed
	case syncKeyTypeText:
	default:
		return normalizedVisibleSyncKey{}, fmt.Errorf("push session key %s.%s uses unsupported sync key type %q", schemaName, tableName, info.syncKeyType)
	}
	return normalized, nil
}

func syncKeyArray(rows []pushPreparedRow) any {
	if len(rows) == 0 {
		return nil
	}
	if rows[0].keyType == syncKeyTypeUUID {
		values := make([]uuid.UUID, len(rows))
		for i, row := range rows {
			values[i] = row.keyValue.(uuid.UUID)
		}
		return values
	}
	values := make([]string, len(rows))
	for i, row := range rows {
		values[i] = row.keyValue.(string)
	}
	return values
}

func stripHiddenOwnerColumn(payloadObject map[string]any) bool {
	for columnName := range payloadObject {
		if strings.EqualFold(columnName, syncScopeColumnName) {
			delete(payloadObject, columnName)
			return true
		}
	}
	return false
}
