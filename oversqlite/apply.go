// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

// DefaultResolver provides a simple conflict resolution strategy
type DefaultResolver struct{}

func (r *DefaultResolver) Merge(table string, pk string, server json.RawMessage, local json.RawMessage) (merged json.RawMessage, keepLocal bool, err error) {
	// Simple strategy: server wins
	return server, false, nil
}

// applyServerChange applies a change received from the server to the local database
func (c *Client) applyServerChange(ctx context.Context, change *oversync.ChangeDownloadResponse) error {
	// Normalize PK from wire/server format to local metadata format.
	// For BLOB UUID PK tables, server sends UUID string, while local meta stores hex.
	if pk, err := c.normalizePKForMeta(change.TableName, change.PK); err == nil {
		change.PK = pk
	} else {
		return fmt.Errorf("failed to normalize PK for apply: %w", err)
	}

	switch change.Op {
	case "INSERT":
		return c.applyInsert(ctx, change)
	case "UPDATE":
		return c.applyUpdate(ctx, change)
	case "DELETE":
		return c.applyDelete(ctx, change)
	default:
		return fmt.Errorf("unknown operation: %s", change.Op)
	}
}

// applyServerChangeInTx applies a change received from the server to the local database within a transaction
func (c *Client) applyServerChangeInTx(ctx context.Context, tx *sql.Tx, change *oversync.ChangeDownloadResponse) error {
	// Normalize PK from wire/server format to local metadata format.
	// Use tx-aware lookup to avoid deadlocks when MaxOpenConns=1 and a tx is held.
	if pk, err := c.normalizePKForMetaInTx(tx, change.TableName, change.PK); err == nil {
		change.PK = pk
	} else {
		return fmt.Errorf("failed to normalize PK for apply: %w", err)
	}

	switch change.Op {
	case "INSERT":
		return c.applyInsertInTx(ctx, tx, change)
	case "UPDATE":
		return c.applyUpdateInTx(ctx, tx, change)
	case "DELETE":
		return c.applyDeleteInTx(ctx, tx, change)
	default:
		return fmt.Errorf("unknown operation: %s", change.Op)
	}
}

// applyUpsertOperation applies an INSERT or UPDATE operation from the server
func (c *Client) applyUpsertOperation(ctx context.Context, change *oversync.ChangeDownloadResponse, opType string) error {
	if change.Payload == nil {
		return fmt.Errorf("%s operation requires payload", opType)
	}

	// Local DELETE wins logic
	// Check if there's a local DELETE pending
	var hasLocalDelete bool
	err := c.DB.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM _sync_pending WHERE table_name=? AND pk_uuid=? AND op='DELETE')
	`, change.TableName, change.PK).Scan(&hasLocalDelete)
	if err != nil {
		return fmt.Errorf("failed to check local DELETE: %w", err)
	}

	// Check if we recently uploaded a DELETE (row meta shows deleted=true)
	// But only if the server version is not newer than our deletion
	var recentlyDeleted bool
	var deletionServerVersion int64
	if !hasLocalDelete {
		err = c.DB.QueryRowContext(ctx, `
			SELECT deleted, server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?
		`, change.TableName, change.PK).Scan(&recentlyDeleted, &deletionServerVersion)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to check recent DELETE: %w", err)
		}
		// If the incoming server version is newer than or equal to our deletion, allow the operation
		if recentlyDeleted && change.ServerVersion >= deletionServerVersion {
			recentlyDeleted = false
		}
	}

	if hasLocalDelete || recentlyDeleted {
		// Local DELETE wins - don't apply server operation
		// Update meta to track that we've seen this server version but didn't apply it
		return c.updateRowMeta(ctx, change.PK, change.TableName, change.ServerVersion, true)
	}

	// Version guard: skip if incoming server_version is significantly older
	var currSv int64
	_ = c.DB.QueryRowContext(ctx, `SELECT server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?`, change.TableName, change.PK).Scan(&currSv)
	if currSv > 0 && change.ServerVersion < currSv {
		return nil
	}

	// Parse payload
	var payload map[string]interface{}
	if err := json.Unmarshal(change.Payload, &payload); err != nil {
		return fmt.Errorf("failed to parse payload: %w", err)
	}

	// Apply the change using UPSERT
	err = c.upsertRow(ctx, change.TableName, change.PK, payload)
	if err != nil {
		return fmt.Errorf("failed to upsert row: %w", err)
	}

	// Update row metadata
	return c.updateRowMeta(ctx, change.PK, change.TableName, change.ServerVersion, false)
}

// applyInsert applies an INSERT operation from the server
func (c *Client) applyInsert(ctx context.Context, change *oversync.ChangeDownloadResponse) error {
	return c.applyUpsertOperation(ctx, change, "INSERT")
}

// applyUpdate applies an UPDATE operation from the server
func (c *Client) applyUpdate(ctx context.Context, change *oversync.ChangeDownloadResponse) error {
	return c.applyUpsertOperation(ctx, change, "UPDATE")
}

// applyDelete applies a DELETE operation from the server
func (c *Client) applyDelete(ctx context.Context, change *oversync.ChangeDownloadResponse) error {
	// If there is a local pending INSERT for this PK, do not apply an older server DELETE.
	// This avoids resurrect/flip-flop issues during pre-upload window downloads.
	var pendingInsertCount int
	_ = c.DB.QueryRowContext(ctx, `SELECT COUNT(1) FROM _sync_pending WHERE table_name = ? AND pk_uuid = ? AND op = 'INSERT'`, change.TableName, change.PK).Scan(&pendingInsertCount)
	if pendingInsertCount > 0 {
		// Skip applying server DELETE; local intent is to (re)create the row.
		return nil
	}

	pkColumn := c.getPrimaryKeyColumn(change.TableName)

	// Convert PK value for query (handles BLOB primary keys)
	pkValue, err := c.convertPKForQuery(change.TableName, change.PK)
	if err != nil {
		return fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	// Delete from business table
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", change.TableName, pkColumn)
	_, err = c.DB.ExecContext(ctx, query, pkValue)
	if err != nil {
		return fmt.Errorf("failed to execute DELETE: %w", err)
	}

	// Update row metadata to mark as deleted
	return c.updateRowMeta(ctx, change.PK, change.TableName, change.ServerVersion, true)
}

// upsertRow performs an INSERT OR REPLACE operation
func (c *Client) upsertRow(ctx context.Context, table, pk string, payload map[string]interface{}) error {
	// Convert encoded BLOB payload fields (base64/hex/uuid string) back to bytes for SQLite.
	blobCols := map[string]struct{}(nil)
	if tableInfo, err := GetTableInfo(c.DB, strings.ToLower(table)); err == nil {
		blobCols = make(map[string]struct{})
		for _, col := range tableInfo.Columns {
			if col.IsBlob() {
				blobCols[strings.ToLower(col.Name)] = struct{}{}
			}
		}
	}

	// Build column list and values
	columns := make([]string, 0, len(payload))
	placeholders := make([]string, 0, len(payload))
	values := make([]interface{}, 0, len(payload))

	for col, val := range payload {
		if blobCols != nil {
			if _, isBlob := blobCols[strings.ToLower(col)]; isBlob && val != nil {
				if s, ok := val.(string); ok {
					decoded, err := decodeBlobBytesFromString(s)
					if err != nil {
						return fmt.Errorf("failed to decode blob payload field %s: %w", col, err)
					}
					val = decoded
				}
			}
		}
		columns = append(columns, col)
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf(
		"INSERT OR REPLACE INTO %s (%s) VALUES (%s)",

		table,
		fmt.Sprintf("%s", columns),
		fmt.Sprintf("%s", placeholders),
	)

	// Fix the column and placeholder formatting
	colStr := ""
	phStr := ""
	for i, col := range columns {
		if i > 0 {
			colStr += ", "
			phStr += ", "
		}
		colStr += col
		phStr += "?"
	}

	query = fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)", table, colStr, phStr)

	_, err := c.DB.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute upsert: %w", err)
	}

	return nil
}

// getPrimaryKeyColumn returns the primary key column name for a given table
func (c *Client) getPrimaryKeyColumn(tableName string) string {
	for _, syncTable := range c.config.Tables {
		if strings.EqualFold(syncTable.TableName, tableName) {
			if syncTable.SyncKeyColumnName == "" {
				return "id" // Default to "id"
			}
			return syncTable.SyncKeyColumnName
		}
	}
	return "id" // Default fallback
}

// convertPKForQuery converts a primary key value for use in database queries
// For BLOB primary keys, this converts hex strings back to binary data
func (c *Client) convertPKForQuery(tableName, pkValue string) (interface{}, error) {
	// Get table information to check if primary key is BLOB
	tableInfo, err := GetTableInfo(c.DB, strings.ToLower(tableName))
	if err != nil {
		return pkValue, nil // Fallback to string if we can't get table info
	}

	pkColumn := c.getPrimaryKeyColumn(tableName)

	// Check if the primary key column is BLOB
	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, pkColumn) && col.IsBlob() {
			// Convert encoded PK back to bytes (accept UUID string, base64, or hex).
			binaryData, err := decodeBlobBytesFromString(pkValue)
			if err != nil {
				return nil, fmt.Errorf("failed to decode primary key %s: %w", pkValue, err)
			}
			return binaryData, nil
		}
	}

	// Not a BLOB primary key, return as string
	return pkValue, nil
}

func (c *Client) convertPKForQueryInTx(tx *sql.Tx, tableName, pkValue string) (interface{}, error) {
	tableInfo, err := GetTableInfoTx(tx, strings.ToLower(tableName))
	if err != nil {
		return pkValue, nil // Fallback to string if we can't get table info
	}

	pkColumn := c.getPrimaryKeyColumn(tableName)

	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, pkColumn) && col.IsBlob() {
			binaryData, err := decodeBlobBytesFromString(pkValue)
			if err != nil {
				return nil, fmt.Errorf("failed to decode primary key %s: %w", pkValue, err)
			}
			return binaryData, nil
		}
	}

	return pkValue, nil
}

// updateRowMeta updates the row metadata for a record
func (c *Client) updateRowMeta(ctx context.Context, pk, tableName string, serverVersion int64, deleted bool) error {
	deletedInt := 0
	if deleted {
		deletedInt = 1
	}

	_, err := c.DB.ExecContext(ctx, `
		INSERT OR REPLACE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted, updated_at)
		VALUES(?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	`, tableName, pk, serverVersion, deletedInt)

	if err != nil {

		return fmt.Errorf("failed to update row metadata: %w", err)
	}

	return nil
}

// GetTableRowCount returns the number of rows in a business table
func (c *Client) GetTableRowCount(tableName string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := c.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in %s: %w", tableName, err)
	}
	return count, nil
}

// GetTableRows returns all rows from a business table (for debugging/inspection)
func (c *Client) GetTableRows(tableName string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := c.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", tableName, err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		// Create slice of interface{} for scanning
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// Transaction-aware versions of apply functions

// applyUpsertOperationInTx applies an INSERT or UPDATE operation from the server within a transaction
func (c *Client) applyUpsertOperationInTx(ctx context.Context, tx *sql.Tx, change *oversync.ChangeDownloadResponse, opType string) error {
	if change.Payload == nil {
		return fmt.Errorf("%s operation requires payload", opType)
	}

	// Check if there's a server DELETE with higher server version that should prevent this operation
	var maxDeleteVersion int64
	err := tx.QueryRowContext(ctx, `
		SELECT server_version FROM _sync_row_meta
		WHERE table_name=? AND pk_uuid=? AND deleted=1
	`, change.TableName, change.PK).Scan(&maxDeleteVersion)

	if err == nil && change.ServerVersion <= maxDeleteVersion {
		// Server DELETE wins - don't apply older or equal operation
		return c.updateRowMetaInTx(ctx, tx, change.PK, change.TableName, change.ServerVersion, true)
	}

	// Parse payload
	var payload map[string]interface{}
	if err := json.Unmarshal(change.Payload, &payload); err != nil {
		return fmt.Errorf("failed to parse payload: %w", err)
	}

	// Apply the change using UPSERT
	err = c.upsertRowInTx(ctx, tx, change.TableName, change.PK, payload)
	if err != nil {
		return fmt.Errorf("failed to upsert row: %w", err)
	}

	// Update row metadata
	return c.updateRowMetaInTx(ctx, tx, change.PK, change.TableName, change.ServerVersion, false)
}

// applyInsertInTx applies an INSERT operation from the server within a transaction
func (c *Client) applyInsertInTx(ctx context.Context, tx *sql.Tx, change *oversync.ChangeDownloadResponse) error {
	return c.applyUpsertOperationInTx(ctx, tx, change, "INSERT")
}

// applyUpdateInTx applies an UPDATE operation from the server within a transaction
func (c *Client) applyUpdateInTx(ctx context.Context, tx *sql.Tx, change *oversync.ChangeDownloadResponse) error {
	return c.applyUpsertOperationInTx(ctx, tx, change, "UPDATE")
}

// applyDeleteInTx applies a DELETE operation from the server within a transaction
func (c *Client) applyDeleteInTx(ctx context.Context, tx *sql.Tx, change *oversync.ChangeDownloadResponse) error {
	// If there is a local pending INSERT for this PK, do not apply an older server DELETE
	var pendingInsertCount int
	_ = tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM _sync_pending WHERE table_name = ? AND pk_uuid = ? AND op = 'INSERT'`, change.TableName, change.PK).Scan(&pendingInsertCount)
	if pendingInsertCount > 0 {
		return nil
	}

	pkColumn := c.getPrimaryKeyColumn(change.TableName)

	// Convert PK value for query (handles BLOB primary keys)
	pkValue, err := c.convertPKForQueryInTx(tx, change.TableName, change.PK)
	if err != nil {
		return fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	// Delete from business table
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", change.TableName, pkColumn)
	_, err = tx.ExecContext(ctx, query, pkValue)
	if err != nil {
		return fmt.Errorf("failed to execute DELETE: %w", err)
	}

	// Update row metadata to mark as deleted
	return c.updateRowMetaInTx(ctx, tx, change.PK, change.TableName, change.ServerVersion, true)
}

// upsertRowInTx performs an INSERT OR REPLACE operation within a transaction
func (c *Client) upsertRowInTx(ctx context.Context, tx *sql.Tx, table, pk string, payload map[string]interface{}) error {
	// Convert encoded BLOB payload fields (base64/hex/uuid string) back to bytes for SQLite.
	blobCols := map[string]struct{}(nil)
	if tableInfo, err := GetTableInfoTx(tx, strings.ToLower(table)); err == nil {
		blobCols = make(map[string]struct{})
		for _, col := range tableInfo.Columns {
			if col.IsBlob() {
				blobCols[strings.ToLower(col.Name)] = struct{}{}
			}
		}
	}

	// Build column list and values
	columns := make([]string, 0, len(payload))
	values := make([]interface{}, 0, len(payload))

	for col, val := range payload {
		if blobCols != nil {
			if _, isBlob := blobCols[strings.ToLower(col)]; isBlob && val != nil {
				if s, ok := val.(string); ok {
					decoded, err := decodeBlobBytesFromString(s)
					if err != nil {
						return fmt.Errorf("failed to decode blob payload field %s: %w", col, err)
					}
					val = decoded
				}
			}
		}
		columns = append(columns, col)
		values = append(values, val)
	}

	// Build query
	colStr := ""
	phStr := ""
	for i, col := range columns {
		if i > 0 {
			colStr += ", "
			phStr += ", "
		}
		colStr += col
		phStr += "?"
	}

	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)", table, colStr, phStr)

	_, err := tx.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute upsert: %w", err)
	}

	return nil
}

// updateRowMetaInTx updates the row metadata for a record within a transaction
func (c *Client) updateRowMetaInTx(ctx context.Context, tx *sql.Tx, pk, tableName string, serverVersion int64, deleted bool) error {
	deletedInt := 0
	if deleted {
		deletedInt = 1
	}

	_, err := tx.ExecContext(ctx, `
		INSERT OR REPLACE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted, updated_at)
		VALUES(?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	`, tableName, pk, serverVersion, deletedInt)

	if err != nil {
		return fmt.Errorf("failed to update row metadata: %w", err)
	}

	return nil
}
