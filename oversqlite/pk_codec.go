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
	tableInfo, err := GetTableInfo(c.DB, strings.ToLower(tableName))
	if err != nil {
		return false, err
	}

	pkColumn := c.getPrimaryKeyColumn(tableName)
	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, pkColumn) {
			return col.IsBlob(), nil
		}
	}
	return false, nil
}

func (c *Client) isPrimaryKeyBlobInTx(tx *sql.Tx, tableName string) (bool, error) {
	tableInfo, err := GetTableInfoTx(tx, strings.ToLower(tableName))
	if err != nil {
		return false, err
	}

	pkColumn := c.getPrimaryKeyColumn(tableName)
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
