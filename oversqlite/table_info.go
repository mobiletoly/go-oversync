// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type tableInfoQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// ColumnInfo holds information about a table column
type ColumnInfo struct {
	Name         string
	DeclaredType string
	IsPrimaryKey bool
	NotNull      bool
	DefaultValue *string
}

// IsBlob returns true if this column should be treated as BLOB data
func (c *ColumnInfo) IsBlob() bool {
	return strings.Contains(strings.ToLower(c.DeclaredType), "blob")
}

func (t *TableInfo) IsBlobReferenceColumn(columnName string) bool {
	if t == nil {
		return false
	}
	return t.ForeignKeyColumnsLower[strings.ToLower(columnName)]
}

// TableInfo holds cached information about a table's structure
type TableInfo struct {
	Table                  string
	Columns                []ColumnInfo
	ColumnNamesLower       []string
	TypesByNameLower       map[string]string
	PrimaryKey             *ColumnInfo
	PrimaryKeyIsBlob       bool
	ForeignKeyColumnsLower map[string]bool
}

// TableInfoProvider manages cached table information
type TableInfoProvider struct {
	cache map[tableInfoCacheKey]*TableInfo
	mutex sync.RWMutex
}

type tableInfoCacheKey struct {
	scope uintptr
	table string
}

// NewTableInfoProvider creates a new TableInfoProvider
func NewTableInfoProvider() *TableInfoProvider {
	return &TableInfoProvider{
		cache: make(map[tableInfoCacheKey]*TableInfo),
	}
}

// Get retrieves table information, using cache when available
func (p *TableInfoProvider) Get(queryer tableInfoQueryer, tableName string) (*TableInfo, error) {
	key := tableInfoCacheKey{
		scope: reflect.ValueOf(queryer).Pointer(),
		table: strings.ToLower(tableName),
	}

	// Check cache first
	p.mutex.RLock()
	if info, exists := p.cache[key]; exists {
		p.mutex.RUnlock()
		return info, nil
	}
	p.mutex.RUnlock()

	// Not in cache, fetch from database
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Double-check in case another goroutine populated it
	if info, exists := p.cache[key]; exists {
		return info, nil
	}

	// Query table info using PRAGMA table_info
	rows, err := queryer.Query(fmt.Sprintf("PRAGMA table_info(%s)", key.table))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	var columnNamesLower []string
	typesByNameLower := make(map[string]string)
	var primaryKey *ColumnInfo

	for rows.Next() {
		var cid int
		var name, declaredType string
		var notNull, pk int
		var defaultValue sql.NullString

		err := rows.Scan(&cid, &name, &declaredType, &notNull, &defaultValue, &pk)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		var defaultVal *string
		if defaultValue.Valid {
			defaultVal = &defaultValue.String
		}

		isPrimaryKey := pk == 1
		column := ColumnInfo{
			Name:         name,
			DeclaredType: declaredType,
			IsPrimaryKey: isPrimaryKey,
			NotNull:      notNull == 1,
			DefaultValue: defaultVal,
		}

		columns = append(columns, column)
		columnNamesLower = append(columnNamesLower, strings.ToLower(name))
		typesByNameLower[strings.ToLower(name)] = declaredType

		if isPrimaryKey {
			primaryKey = &column
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Determine if primary key is BLOB
	primaryKeyIsBlob := false
	if primaryKey != nil {
		primaryKeyIsBlob = primaryKey.IsBlob()
	}

	foreignKeyColumnsLower := make(map[string]bool)
	fkRows, err := queryer.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", key.table))
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys for %s: %w", tableName, err)
	}
	defer fkRows.Close()

	for fkRows.Next() {
		var (
			id       int
			seq      int
			refTable string
			fromCol  string
			toCol    string
			onUpdate string
			onDelete string
			match    string
		)
		if err := fkRows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpdate, &onDelete, &match); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key info for %s: %w", tableName, err)
		}
		foreignKeyColumnsLower[strings.ToLower(fromCol)] = true
	}
	if err := fkRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating foreign keys for %s: %w", tableName, err)
	}

	tableInfo := &TableInfo{
		Table:                  key.table,
		Columns:                columns,
		ColumnNamesLower:       columnNamesLower,
		TypesByNameLower:       typesByNameLower,
		PrimaryKey:             primaryKey,
		PrimaryKeyIsBlob:       primaryKeyIsBlob,
		ForeignKeyColumnsLower: foreignKeyColumnsLower,
	}

	// Cache the result
	p.cache[key] = tableInfo
	return tableInfo, nil
}

// ClearCache clears the table info cache
func (p *TableInfoProvider) ClearCache() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cache = make(map[tableInfoCacheKey]*TableInfo)
}

// GetTableInfo is a convenience function for one-off schema lookups.
// It intentionally avoids process-wide caching so metadata cannot leak across
// unrelated databases or test harnesses.
func GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error) {
	return NewTableInfoProvider().Get(db, tableName)
}

// GetTableInfoTx is a convenience function for one-off schema lookups inside a
// transaction. Use this when running under a single-connection DB
// (MaxOpenConns=1) and holding an open tx.
func GetTableInfoTx(tx *sql.Tx, tableName string) (*TableInfo, error) {
	return NewTableInfoProvider().Get(tx, tableName)
}

// ClearGlobalTableInfoCache is a no-op.
// Convenience lookups no longer use process-wide caching.
func ClearGlobalTableInfoCache() {
}
