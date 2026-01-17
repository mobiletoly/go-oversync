// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"database/sql"
	"fmt"
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

// TableInfo holds cached information about a table's structure
type TableInfo struct {
	Table            string
	Columns          []ColumnInfo
	ColumnNamesLower []string
	TypesByNameLower map[string]string
	PrimaryKey       *ColumnInfo
	PrimaryKeyIsBlob bool
}

// TableInfoProvider manages cached table information
type TableInfoProvider struct {
	cache map[string]*TableInfo
	mutex sync.RWMutex
}

// NewTableInfoProvider creates a new TableInfoProvider
func NewTableInfoProvider() *TableInfoProvider {
	return &TableInfoProvider{
		cache: make(map[string]*TableInfo),
	}
}

// Get retrieves table information, using cache when available
func (p *TableInfoProvider) Get(queryer tableInfoQueryer, tableName string) (*TableInfo, error) {
	key := strings.ToLower(tableName)

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
	rows, err := queryer.Query(fmt.Sprintf("PRAGMA table_info(%s)", key))
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

	tableInfo := &TableInfo{
		Table:            key,
		Columns:          columns,
		ColumnNamesLower: columnNamesLower,
		TypesByNameLower: typesByNameLower,
		PrimaryKey:       primaryKey,
		PrimaryKeyIsBlob: primaryKeyIsBlob,
	}

	// Cache the result
	p.cache[key] = tableInfo
	return tableInfo, nil
}

// ClearCache clears the table info cache
func (p *TableInfoProvider) ClearCache() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cache = make(map[string]*TableInfo)
}

// Global table info provider instance
var globalTableInfoProvider = NewTableInfoProvider()

// GetTableInfo is a convenience function that uses the global provider
func GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error) {
	return globalTableInfoProvider.Get(db, tableName)
}

// GetTableInfoTx is a convenience function that uses the global provider via a transaction.
// Use this when running under a single-connection DB (MaxOpenConns=1) and holding an open tx.
func GetTableInfoTx(tx *sql.Tx, tableName string) (*TableInfo, error) {
	return globalTableInfoProvider.Get(tx, tableName)
}

// ClearGlobalTableInfoCache clears the global table info cache
// This should be called when initializing a new database connection to prevent
// cached data from previous databases from being used
func ClearGlobalTableInfoCache() {
	globalTableInfoProvider.ClearCache()
}
