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
	Message string
}

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
