// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

// isValidSchemaName checks if schema name matches ^[a-z0-9_]+$
func isValidSchemaName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// isValidTableName checks if table name matches ^[a-z0-9_]+$
func isValidTableName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// isValidColumnName checks if column name matches ^[a-z0-9_]+$
func isValidColumnName(name string) bool {
	return isValidTableName(name)
}
