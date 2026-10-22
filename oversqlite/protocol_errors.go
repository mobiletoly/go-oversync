// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

// SnapshotSessionLimitExceededError reports that the server could not
// materialize a snapshot within one of its configured limits.
type SnapshotSessionLimitExceededError struct {
	Dimension string
	Actual    int64
	Limit     int64
}

// PushReplayMismatchError is terminal for the frozen outbox: the server has
// already committed the source tuple for different canonical content.
type PushReplayMismatchError struct{}

func (*PushReplayMismatchError) Error() string {
	return "committed source tuple belongs to a different canonical push request"
}

// BundleProjectionTooLargeError reports a rejected push whose authoritative
// result or future indivisible pull representation cannot fit the admitted
// contract. The frozen request is preserved for explicit correction.
type BundleProjectionTooLargeError struct{}

func (*BundleProjectionTooLargeError) Error() string {
	return "push result cannot be represented within the admitted contract"
}

// CommittedBundleUnrepresentableError is terminal corruption reported while
// pulling retained history. Automatic retry cannot repair it.
type CommittedBundleUnrepresentableError struct{}

func (*CommittedBundleUnrepresentableError) Error() string {
	return "committed bundle cannot be represented within the admitted contract"
}

// AdmittedRowTooLargeError reports a server-originated logical row that
// violates the generation discovery contract. It is terminal because retrying
// the same representation cannot make the row smaller.
type AdmittedRowTooLargeError struct {
	Operation string
	Limit     int64
}

func (e *AdmittedRowTooLargeError) Error() string {
	return fmt.Sprintf("%s row exceeds admitted max_row_decoded_bytes=%d", e.Operation, e.Limit)
}

func (c *Client) validateAdmittedResponseRows(operation string, rows []oversync.PushRequestRow) error {
	if c == nil || c.discovery == nil || c.discovery.Limits.MaxRowDecodedBytes < 1 {
		return &InvalidAdmittedContractError{Message: "max_row_decoded_bytes is unavailable"}
	}
	for _, row := range rows {
		rowBytes, err := oversync.ExpandedRowDecodedBytes(row)
		if err != nil || rowBytes > c.discovery.Limits.MaxRowDecodedBytes {
			return &AdmittedRowTooLargeError{Operation: operation, Limit: c.discovery.Limits.MaxRowDecodedBytes}
		}
	}
	return nil
}

func isTerminalProtocolRepresentationError(err error) bool {
	var replayMismatch *PushReplayMismatchError
	var projectionTooLarge *BundleProjectionTooLargeError
	var committedUnrepresentable *CommittedBundleUnrepresentableError
	var rowTooLarge *PushRowExceedsAdmittedLimitError
	var admittedRowTooLarge *AdmittedRowTooLargeError
	var invalidContract *InvalidAdmittedContractError
	return errors.As(err, &replayMismatch) || errors.As(err, &projectionTooLarge) ||
		errors.As(err, &committedUnrepresentable) || errors.As(err, &rowTooLarge) ||
		errors.As(err, &admittedRowTooLarge) || errors.As(err, &invalidContract)
}

// Error implements error without retaining the server-provided message.
func (e *SnapshotSessionLimitExceededError) Error() string {
	return fmt.Sprintf(
		"snapshot session exceeds server %s limit: actual=%d limit=%d",
		e.Dimension,
		e.Actual,
		e.Limit,
	)
}

func decodeServerErrorBody(body []byte) string {
	if len(body) == 0 {
		return "invalid_error_response"
	}
	resp, ok := decodeServerErrorResponse(body)
	if !ok {
		return "invalid_error_response"
	}
	if !isSafeServerErrorCode(resp.Error) {
		return "invalid_error_response"
	}
	return resp.Error
}

func decodeSnapshotSessionLimitExceededError(body []byte) error {
	errorResp, ok := decodeServerErrorResponse(body)
	if !ok || errorResp.Error != "snapshot_session_limit_exceeded" {
		return nil
	}

	var response oversync.SnapshotSessionLimitResponse
	if err := json.Unmarshal(body, &response); err != nil ||
		response.Error != "snapshot_session_limit_exceeded" ||
		!isSnapshotSessionLimitDimension(response.Dimension) ||
		response.Limit <= 0 || response.Actual <= response.Limit {
		return errors.New("invalid snapshot_session_limit_exceeded response")
	}
	return &SnapshotSessionLimitExceededError{
		Dimension: response.Dimension,
		Actual:    response.Actual,
		Limit:     response.Limit,
	}
}

func isSnapshotSessionLimitDimension(dimension string) bool {
	switch dimension {
	case "row_count", "logical_byte_count", "row_decoded_body_bytes":
		return true
	default:
		return false
	}
}

func isSafeServerErrorCode(code string) bool {
	switch code {
	case "bundle_change_watch_forbidden", "checkpoint_ahead",
		"recovery_floor_ahead", "push_outcome_pending",
		"history_pruned", "initialization_expired", "initialization_stale", "invalid_request",
		"push_conflict", "push_invalid", "push_replay_mismatch", "bundle_projection_too_large", "committed_bundle_unrepresentable",
		"scope_initializing", "scope_uninitialized", "service_unavailable", "snapshot_build_capacity",
		"snapshot_chunk_capacity", "snapshot_chunk_invalid", "snapshot_chunk_too_small",
		"snapshot_session_expired", "snapshot_session_forbidden", "snapshot_session_invalid",
		"snapshot_session_limit_exceeded", "snapshot_session_not_found", "source_replacement_invalid", "source_retired",
		"source_sequence_changed", "source_sequence_out_of_order":
		return true
	default:
		return false
	}
}

func decodeServerErrorResponse(body []byte) (oversync.ErrorResponse, bool) {
	if len(body) == 0 {
		return oversync.ErrorResponse{}, false
	}
	var resp oversync.ErrorResponse
	if err := json.Unmarshal(body, &resp); err == nil && strings.TrimSpace(resp.Error) != "" {
		resp.Message = ""
		return resp, true
	}
	return oversync.ErrorResponse{}, false
}

func decodeSourceRetiredResponse(body []byte, expectedSourceID string) (oversync.SourceRetiredResponse, bool) {
	if len(body) == 0 {
		return oversync.SourceRetiredResponse{}, false
	}
	var resp oversync.SourceRetiredResponse
	if err := json.Unmarshal(body, &resp); err == nil && resp.Error == "source_retired" {
		if validateSourceID(expectedSourceID) != nil || resp.SourceID != expectedSourceID {
			return oversync.SourceRetiredResponse{}, false
		}
		resp.Message = ""
		return resp, true
	}
	return oversync.SourceRetiredResponse{}, false
}
