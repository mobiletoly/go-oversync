// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func (c *Client) isPrimaryKeyBlob(tableName string) (bool, error) {
	tableInfo, err := c.getTableInfo(strings.ToLower(tableName))
	if err != nil {
		return false, err
	}

	pkColumn, err := c.primaryKeyColumnForTable(tableName)
	if err != nil {
		return false, err
	}
	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, pkColumn) {
			return col.IsBlob(), nil
		}
	}
	return false, nil
}

func (c *Client) isPrimaryKeyBlobInTx(tx *sql.Tx, tableName string) (bool, error) {
	tableInfo, err := c.getTableInfoTx(tx, strings.ToLower(tableName))
	if err != nil {
		return false, err
	}

	pkColumn, err := c.primaryKeyColumnForTable(tableName)
	if err != nil {
		return false, err
	}
	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, pkColumn) {
			return col.IsBlob(), nil
		}
	}
	return false, nil
}

func (c *Client) normalizePKForMeta(tableName, pk string) (string, error) {
	isBlobPK, err := c.isPrimaryKeyBlob(tableName)
	if err != nil {
		return "", err
	}
	if !isBlobPK {
		return pk, nil
	}

	b, err := decodeUUIDBytesFromString(pk)
	if err != nil {
		return "", fmt.Errorf("invalid blob UUID pk %q: %w", pk, err)
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) normalizePKForMetaInTx(tx *sql.Tx, tableName, pk string) (string, error) {
	isBlobPK, err := c.isPrimaryKeyBlobInTx(tx, tableName)
	if err != nil {
		return "", err
	}
	if !isBlobPK {
		return pk, nil
	}

	b, err := decodeUUIDBytesFromString(pk)
	if err != nil {
		return "", fmt.Errorf("invalid blob UUID pk %q: %w", pk, err)
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) normalizePKForServer(tableName, pk string) (string, error) {
	isBlobPK, err := c.isPrimaryKeyBlob(tableName)
	if err != nil {
		return "", err
	}
	if !isBlobPK {
		return pk, nil
	}

	b, err := decodeUUIDBytesFromString(pk)
	if err != nil {
		return "", fmt.Errorf("invalid blob UUID pk %q: %w", pk, err)
	}
	id, err := uuid.FromBytes(b)
	if err != nil {
		return "", fmt.Errorf("invalid UUID bytes: %w", err)
	}
	return id.String(), nil
}

func (c *Client) normalizePKForServerInTx(tx *sql.Tx, tableName, pk string) (string, error) {
	isBlobPK, err := c.isPrimaryKeyBlobInTx(tx, tableName)
	if err != nil {
		return "", err
	}
	if !isBlobPK {
		return pk, nil
	}

	b, err := decodeUUIDBytesFromString(pk)
	if err != nil {
		return "", fmt.Errorf("invalid blob UUID pk %q: %w", pk, err)
	}
	id, err := uuid.FromBytes(b)
	if err != nil {
		return "", fmt.Errorf("invalid UUID bytes: %w", err)
	}
	return id.String(), nil
}

// convertPKForQuery converts a primary key value for use in database queries.
// For BLOB primary keys, this converts string encodings back to binary data.
func (c *Client) convertPKForQuery(tableName, pkValue string) (interface{}, error) {
	tableInfo, err := c.getTableInfo(strings.ToLower(tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", tableName, err)
	}

	pkColumn, err := c.primaryKeyColumnForTable(tableName)
	if err != nil {
		return nil, err
	}

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

func (c *Client) convertPKForQueryInTx(tx *sql.Tx, tableName, pkValue string) (interface{}, error) {
	tableInfo, err := c.getTableInfoTx(tx, strings.ToLower(tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", tableName, err)
	}

	pkColumn, err := c.primaryKeyColumnForTable(tableName)
	if err != nil {
		return nil, err
	}

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
