// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (c *Client) upsertRowInTx(ctx context.Context, tx sqliteTransaction, table string, payload map[string]interface{}) error {
	return c.upsertRowInTxUsing(ctx, tx, tx, table, payload)
}

func (c *Client) upsertRowInTxUsing(ctx context.Context, execer execContexter, tx tableInfoQueryer, table string, payload map[string]interface{}) error {
	return c.upsertPayloadInTxUsing(ctx, execer, tx, table, payload, false)
}

func (c *Client) upsertLocalRowInTxUsing(ctx context.Context, execer execContexter, tx tableInfoQueryer, table string, payload map[string]interface{}) error {
	return c.upsertPayloadInTxUsing(ctx, execer, tx, table, payload, true)
}

func (c *Client) upsertPayloadInTxUsing(ctx context.Context, execer execContexter, tx tableInfoQueryer, table string, payload map[string]interface{}, storedLocal bool) error {
	tableInfo, err := c.getTableInfoTx(tx, strings.ToLower(table))
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	pkColumn, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return err
	}

	bindings, err := c.bindUpsertPayload(table, payload, storedLocal, tableInfo, pkColumn)
	if err != nil {
		return err
	}
	columns, values, updateAssignments := bindings.columns, bindings.values, bindings.updateAssignments

	if handled, err := c.reusePreparedUniqueUpsert(ctx, tx, table, tableInfo, pkColumn, values); err != nil || handled {
		return err
	}

	if _, ok := payload[strings.ToLower(pkColumn)]; !ok {
		return fmt.Errorf("payload for %s is missing primary key column %s", table, pkColumn)
	}
	if rowid, ok := ctx.Value(applyRowIDKey{}).(sql.NullInt64); ok && rowid.Valid {
		alias, err := c.observableRowIDColumn(ctx, tx, table)
		if err != nil {
			return err
		}
		if alias != "" {
			columns = append(columns, quoteIdent(alias))
			values = append(values, rowid.Int64)
		}
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if len(updateAssignments) == 0 {
		query += fmt.Sprintf(" ON CONFLICT(%s) DO NOTHING", quoteIdent(pkColumn))
	} else {
		query += fmt.Sprintf(" ON CONFLICT(%s) DO UPDATE SET %s", quoteIdent(pkColumn), strings.Join(updateAssignments, ", "))
	}

	if _, err := execer.ExecContext(ctx, query, values...); err != nil {
		return fmt.Errorf("failed to execute upsert: %w", err)
	}
	return nil
}

func (c *Client) syncKeyJSONForPKInTx(tx tableInfoQueryer, tableName, pk string) (string, error) {
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil {
		return "", err
	}
	if len(keyColumns) != 1 {
		return "", fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
	}
	keyJSON, err := localKeyJSON(tx, strings.ToLower(keyColumns[0]), pk)
	if err != nil {
		return "", fmt.Errorf("failed to encode sync key for table %s column %s: %w", tableName, keyColumns[0], err)
	}
	return keyJSON, nil
}

// GetTableRowCount returns the number of rows in a business table
func (c *Client) GetTableRowCount(tableName string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(tableName))
	err := c.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in %s: %w", tableName, err)
	}
	return count, nil
}

// GetTableRows returns all rows from a business table (for debugging/inspection)
func (c *Client) GetTableRows(tableName string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", quoteIdent(tableName))
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

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

type upsertBindings struct {
	columns           []string
	values            []interface{}
	updateAssignments []string
}

// Validate the complete image and decode physical bindings before attempting
// either a prepared final-image reuse or the original upsert statement.
func (c *Client) bindUpsertPayload(table string, payload map[string]interface{}, storedLocal bool, tableInfo *TableInfo, pkColumn string) (upsertBindings, error) {
	var err error
	if storedLocal {
		if err := c.validateAndUnwrapCapturedPayload(table, payload); err != nil {
			return upsertBindings{}, err
		}
	}

	columnByLower := make(map[string]ColumnInfo, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columnByLower[strings.ToLower(col.Name)] = col
	}
	if len(payload) != len(columnByLower) {
		return upsertBindings{}, fmt.Errorf("payload for %s must contain every table column", table)
	}

	columns := make([]string, 0, len(tableInfo.Columns))
	values := make([]interface{}, 0, len(tableInfo.Columns))
	updateAssignments := make([]string, 0, len(tableInfo.Columns))

	for _, col := range tableInfo.Columns {
		key := strings.ToLower(col.Name)
		val, ok := payload[key]
		if !ok {
			return upsertBindings{}, fmt.Errorf("payload for %s is missing column %s", table, col.Name)
		}

		if storedLocal {
			codec, declared, ok := c.declaredColumn(table, col.Name)
			if !ok {
				return upsertBindings{}, fmt.Errorf("payload for %s column %s is absent from the local generation contract", table, col.Name)
			}
			if val == nil && !declared.Nullable {
				return upsertBindings{}, fmt.Errorf("payload for %s column %s is unexpectedly null", table, col.Name)
			}
			val, err = sqliteValueFromLocalCodec(codec, val)
		} else {
			val, err = c.sqliteValueWithDeclaredCodec(table, col, val)
		}
		if err != nil {
			return upsertBindings{}, fmt.Errorf("payload for %s column %s: %w", table, col.Name, err)
		}

		columns = append(columns, quoteIdent(col.Name))
		values = append(values, val)
		if !strings.EqualFold(col.Name, pkColumn) {
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s = excluded.%s", quoteIdent(col.Name), quoteIdent(col.Name)))
		}
	}

	return upsertBindings{columns, values, updateAssignments}, nil
}
