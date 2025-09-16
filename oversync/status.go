// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
)

// statusApplied creates a status for successfully applied changes with new server version
func statusApplied(scid int64, newVer int64) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID:   scid,
		Status:           StApplied,
		NewServerVersion: &newVer,
	}
}

// statusAppliedIdempotent creates a status for idempotent changes (already processed)
func statusAppliedIdempotent(scid int64) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID: scid,
		Status:         StApplied,
	}
}

// statusConflict creates a status for version conflicts with current server state
func statusConflict(scid int64, serverRow json.RawMessage) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID: scid,
		Status:         StConflict,
		ServerRow:      serverRow,
	}
}

// statusInvalidFKMissing creates a status for foreign key validation failures
func statusInvalidFKMissing(scid int64, miss []string) ChangeUploadStatus {
	det := map[string]any{}
	if len(miss) > 0 {
		det["missing"] = miss
	}
	return ChangeUploadStatus{
		SourceChangeID: scid,
		Status:         StInvalid,
		Invalid: map[string]any{
			"reason":  ReasonFKMissing,
			"details": det,
		},
	}
}

// statusInvalidOther creates a status for other validation failures
func statusInvalidOther(scid int64, reason string, err error) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID: scid,
		Status:         StInvalid,
		Message:        err.Error(),
		Invalid: map[string]any{
			"reason":  reason,
			"details": map[string]any{"error": err.Error()},
		},
	}
}

// statusInvalidUnregisteredTable creates a status when a table isn't registered for sync
func statusInvalidUnregisteredTable(scid int64, schema, table string) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID: scid,
		Status:         StInvalid,
		Invalid: map[string]any{
			"reason":  ReasonUnregisteredTable,
			"details": map[string]any{"schema": schema, "table": table},
		},
		Message: "table not registered " + schema + "." + table,
	}
}

// statusMaterializeError creates a status for business table materialization errors
func statusMaterializeError(scid int64, newVer int64, err error) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID:   scid,
		Status:           StMaterializeError,
		NewServerVersion: &newVer,
		Message:          err.Error(),
	}
}

// statusInternalError creates a status for internal errors with proper reason
func statusInternalError(scid int64, err error) ChangeUploadStatus {
	return ChangeUploadStatus{
		SourceChangeID: scid,
		Status:         StInvalid,
		Invalid: map[string]any{
			"reason":  ReasonInternalError,
			"details": map[string]any{"error": err.Error()},
		},
	}
}
