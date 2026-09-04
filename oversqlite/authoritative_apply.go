// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) setBundleApplyModeInTx(ctx context.Context, tx execContexter, enabled bool) error {
	if err := setApplyMode(ctx, tx, enabled); err != nil {
		return fmt.Errorf("failed to update _sync_apply_state apply_mode: %w", err)
	}
	return nil
}

func (c *Client) decodeDirtyKeyForPush(tx tableInfoQueryer, tableName, keyJSON string) (string, oversync.SyncKey, error) {
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil {
		return "", nil, err
	}
	if len(keyColumns) != 1 {
		return "", nil, fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(keyJSON), &raw); err != nil {
		return "", nil, fmt.Errorf("failed to decode dirty key for %s: %w", tableName, err)
	}
	rawValue, ok := raw[strings.ToLower(keyColumns[0])]
	if !ok {
		rawValue, ok = raw[keyColumns[0]]
	}
	if !ok {
		return "", nil, fmt.Errorf("dirty key for %s is missing %s", tableName, keyColumns[0])
	}
	pkValue, ok := rawValue.(string)
	if !ok {
		return "", nil, fmt.Errorf("dirty key %s.%s must be a string", tableName, keyColumns[0])
	}
	wirePK, err := c.normalizePKForServerInTx(tx, tableName, pkValue)
	if err != nil {
		return "", nil, fmt.Errorf("failed to normalize key for push table %s column %s: %w", tableName, keyColumns[0], err)
	}
	return pkValue, oversync.SyncKey{strings.ToLower(keyColumns[0]): wirePK}, nil
}

func (c *Client) bundleRowKeyToLocalKey(tx tableInfoQueryer, tableName string, key oversync.SyncKey) (string, string, error) {
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil {
		return "", "", err
	}
	if len(keyColumns) != 1 {
		return "", "", fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
	}

	value, ok := key[strings.ToLower(keyColumns[0])]
	if !ok {
		value, ok = key[keyColumns[0]]
	}
	if !ok {
		return "", "", fmt.Errorf("bundle row key for %s is missing %s", tableName, keyColumns[0])
	}
	wirePK, ok := value.(string)
	if !ok {
		return "", "", fmt.Errorf("bundle row key for %s must be a string", tableName)
	}
	localPK := wirePK
	isBlobPK, err := c.isPrimaryKeyBlobInTx(tx, tableName)
	if err != nil {
		return "", "", fmt.Errorf("failed to inspect bundle row key for %s: %w", tableName, err)
	}
	if isBlobPK {
		uuidBytes, err := decodeCanonicalWireUUIDBytes(wirePK)
		if err != nil {
			return "", "", fmt.Errorf("failed to decode canonical wire bundle row key for %s: %w", tableName, err)
		}
		localPK = hex.EncodeToString(uuidBytes)
	}
	keyJSONBytes, err := json.Marshal(map[string]any{strings.ToLower(keyColumns[0]): localPK})
	if err != nil {
		return "", "", fmt.Errorf("failed to encode local key json for %s: %w", tableName, err)
	}
	return string(keyJSONBytes), localPK, nil
}

type structuredRowState struct {
	Exists     bool
	RowVersion int64
	Deleted    bool
}

func (c *Client) loadStructuredRowStateInTx(ctx context.Context, tx sqliteTransaction, schemaName, tableName, keyJSON string) (*structuredRowState, error) {
	state := &structuredRowState{}
	var deletedInt int
	err := tx.QueryRowContext(ctx, `
			SELECT row_version, server_kind='deleted'
			FROM _sync_row_state
			WHERE schema_name = ? AND table_name = ? AND key_json = ?
		`, schemaName, tableName, keyJSON).Scan(&state.RowVersion, &deletedInt)
	if err == sql.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load structured row state for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	state.Exists = true
	state.Deleted = deletedInt == 1
	return state, nil
}

func (c *Client) applyBundleRowAuthoritativelyInTx(ctx context.Context, tx sqliteTransaction, row *oversync.BundleRow, localPK string) error {
	return c.applyBundleRowAuthoritativelyInTxUsing(ctx, tx, tx, row, localPK, baselineObservation{Origin: "explicit_conflict"})
}

func (c *Client) applyBundleRowAuthoritativelyInTxUsing(ctx context.Context, execer execContexter, tx sqliteTransaction, row *oversync.BundleRow, localPK string, observation baselineObservation) error {
	switch row.Op {
	case oversync.OpInsert, oversync.OpUpdate:
		payload, err := jcs.DecodeObject(row.Payload)
		if err != nil {
			return fmt.Errorf("failed to decode bundle row payload for %s.%s: %w", row.Schema, row.Table, err)
		}
		if err := c.upsertRowInTxUsing(ctx, execer, tx, row.Table, payload); err != nil {
			return fmt.Errorf("failed to apply authoritative upsert for %s.%s: %w", row.Schema, row.Table, err)
		}
	case oversync.OpDelete:
		pkColumn, err := c.primaryKeyColumnForTable(row.Table)
		if err != nil {
			return err
		}
		pkValue, err := c.convertPKForQueryInTx(tx, row.Table, localPK)
		if err != nil {
			return fmt.Errorf("failed to convert local key for delete: %w", err)
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(row.Table), quoteIdent(pkColumn))
		if _, err := execer.ExecContext(ctx, query, pkValue); err != nil {
			return fmt.Errorf("failed to apply authoritative delete for %s.%s: %w", row.Schema, row.Table, err)
		}
	default:
		return fmt.Errorf("unsupported bundle row op %q", row.Op)
	}

	keyJSON, err := c.syncKeyJSONForPKInTx(tx, row.Table, localPK)
	if err != nil {
		return err
	}
	kind := "live"
	if row.Op == oversync.OpDelete {
		kind = "deleted"
	}
	return c.installAuthoritativeBaseline(ctx, tx, row.Schema, row.Table, keyJSON, kind, row.RowVersion, row.Payload, observation)
}

// normalizeWirePayloadForSQLiteRoundTrip converts generation-visible Boolean
// values to the INTEGER representation used by SQLite before comparing an
// installed row with its authoritative wire payload. Other numeric fields use
// the same normalization as ordinary upload reconstruction, so verification
// observes the supported local storage round trip instead of mistaking SQLite
// affinity for trigger corruption.
func (c *Client) normalizeWirePayloadForSQLiteRoundTrip(tableName, payload string) (json.RawMessage, error) {
	payloadData, err := jcs.DecodeObject([]byte(payload))
	if err != nil {
		return nil, err
	}
	for columnName, value := range payloadData {
		codec, declared, ok := c.declaredColumn(tableName, columnName)
		if !ok {
			return nil, fmt.Errorf("column %s.%s is absent from the local generation contract", tableName, columnName)
		}
		if value == nil {
			if !declared.Nullable {
				return nil, fmt.Errorf("column %s.%s must not be null", tableName, columnName)
			}
			continue
		}
		sqliteValue, err := c.sqliteValueWithDeclaredCodec(tableName, ColumnInfo{Name: columnName}, value)
		if err != nil {
			return nil, err
		}
		wireValue, err := wireValueFromSQLiteCodec(codec, sqliteValue)
		if err != nil {
			return nil, err
		}
		payloadData[columnName] = wireValue
	}
	normalized, err := jcs.Marshal(payloadData)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}
