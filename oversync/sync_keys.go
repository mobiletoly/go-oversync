package oversync

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type normalizedVisibleSyncKey struct {
	column    string
	keyType   string
	keyString string
	keyBytes  []byte
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

func (s *SyncService) tableIDForTable(schemaName, tableName string) (int32, error) {
	info, err := s.syncKeyInfoForTable(schemaName, tableName)
	if err != nil {
		return 0, err
	}
	return info.tableID, nil
}

func (s *SyncService) tableInfoForID(tableID int32) (registeredTableRuntimeInfo, error) {
	if s == nil {
		return registeredTableRuntimeInfo{}, fmt.Errorf("table_id %d is not configured for sync", tableID)
	}
	info, ok := s.registeredTableByID[tableID]
	if !ok {
		return registeredTableRuntimeInfo{}, fmt.Errorf("table_id %d is not configured for sync", tableID)
	}
	return info, nil
}

func encodeKeyBytes(keyType, keyString string) ([]byte, any, error) {
	switch keyType {
	case syncKeyTypeUUID:
		parsed, err := uuid.Parse(keyString)
		if err != nil {
			return nil, nil, err
		}
		canonical := parsed.String()
		if keyString != canonical {
			return nil, nil, fmt.Errorf("key must use canonical dashed lowercase UUID text")
		}
		raw := make([]byte, 16)
		copy(raw, parsed[:])
		return raw, parsed, nil
	case syncKeyTypeText:
		return []byte(keyString), keyString, nil
	default:
		return nil, nil, fmt.Errorf("unsupported sync key type %q", keyType)
	}
}

func decodeKeyBytes(keyType string, keyBytes []byte) (string, any, error) {
	switch keyType {
	case syncKeyTypeUUID:
		if len(keyBytes) != 16 {
			return "", nil, fmt.Errorf("uuid key_bytes length must be 16, got %d", len(keyBytes))
		}
		var id uuid.UUID
		copy(id[:], keyBytes)
		return id.String(), id, nil
	case syncKeyTypeText:
		return string(keyBytes), string(keyBytes), nil
	default:
		return "", nil, fmt.Errorf("unsupported sync key type %q", keyType)
	}
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
	}
	keyBytes, dbValue, err := encodeKeyBytes(info.syncKeyType, keyString)
	if err != nil {
		if info.syncKeyType == syncKeyTypeUUID {
			return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("row key %s.%s.%s must use canonical dashed lowercase UUID text", schemaName, tableName, info.syncKeyColumn)}
		}
		return normalizedVisibleSyncKey{}, &PushValidationError{Message: fmt.Sprintf("table %s.%s uses unsupported sync key type %q", schemaName, tableName, info.syncKeyType)}
	}
	normalized.keyBytes = keyBytes
	normalized.dbValue = dbValue
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

func decodeNormalizedStoredSyncKeyBytes(schemaName, tableName string, info registeredTableRuntimeInfo, keyBytes []byte) (normalizedVisibleSyncKey, error) {
	keyString, dbValue, err := decodeKeyBytes(info.syncKeyType, keyBytes)
	if err != nil {
		return normalizedVisibleSyncKey{}, fmt.Errorf("decode sync key for %s.%s: %w", schemaName, tableName, err)
	}
	normalized := normalizedVisibleSyncKey{
		column:    info.syncKeyColumn,
		keyType:   info.syncKeyType,
		keyString: keyString,
		keyBytes:  append([]byte(nil), keyBytes...),
		dbValue:   dbValue,
	}
	return normalized, nil
}

func wireSyncKeyFromBytes(info registeredTableRuntimeInfo, keyBytes []byte) (SyncKey, error) {
	keyString, _, err := decodeKeyBytes(info.syncKeyType, keyBytes)
	if err != nil {
		return nil, err
	}
	return SyncKey{info.syncKeyColumn: keyString}, nil
}

func keyBytesEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}

func appendInt32BigEndian(dst []byte, value int32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return append(dst, buf[:]...)
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
