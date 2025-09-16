// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

// Operation constants for change operations
const (
	OpInsert = "INSERT"
	OpUpdate = "UPDATE"
	OpDelete = "DELETE"
)

// Status constants for change upload statuses
const (
	StApplied          = "applied"
	StConflict         = "conflict"
	StInvalid          = "invalid"
	StMaterializeError = "materialize_error"
)

// Invalid reason constants
const (
	ReasonFKMissing         = "fk_missing"
	ReasonBadPayload        = "bad_payload"
	ReasonPrecheckError     = "precheck_error"
	ReasonInternalError     = "internal_error"
	ReasonUnregisteredTable = "unregistered_table"
)
