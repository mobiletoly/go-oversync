// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"errors"
	"fmt"
)

var errUnsupportedSchema = errors.New("registered schema falls outside the supported sync contract")

// UnsupportedSchemaError reports a bootstrap-time schema shape that the current
// production-ready contract intentionally does not support.
type UnsupportedSchemaError struct {
	Message      string
	Code         UnsupportedSchemaErrorCode
	GenerationID SchemaGenerationID
	Schema       string
	Table        string
	Column       string
	Field        string
	Expected     string
	Actual       string
}

type UnsupportedSchemaErrorCode string

const (
	UnsupportedSchemaGenerationColumnMissing             UnsupportedSchemaErrorCode = "generation_column_missing"
	UnsupportedSchemaGenerationColumnTypeMismatch        UnsupportedSchemaErrorCode = "generation_column_type_mismatch"
	UnsupportedSchemaGenerationColumnNullabilityMismatch UnsupportedSchemaErrorCode = "generation_column_nullability_mismatch"
	UnsupportedSchemaGenerationColumnNotWritable         UnsupportedSchemaErrorCode = "generation_column_not_writable"
	UnsupportedSchemaGenerationForeignKeyMissing         UnsupportedSchemaErrorCode = "generation_foreign_key_missing"
	UnsupportedSchemaGenerationForeignKeyUnexpected      UnsupportedSchemaErrorCode = "generation_foreign_key_unexpected"
	UnsupportedSchemaGenerationForeignKeyMismatch        UnsupportedSchemaErrorCode = "generation_foreign_key_mismatch"
)

func (e *UnsupportedSchemaError) Error() string {
	return e.Message
}

func (e *UnsupportedSchemaError) Is(target error) bool {
	return target == errUnsupportedSchema
}

func unsupportedSchemaf(format string, args ...any) error {
	return &UnsupportedSchemaError{
		Message: fmt.Sprintf(format, args...),
	}
}
