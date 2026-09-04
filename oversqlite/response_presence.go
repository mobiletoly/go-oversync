// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

func requireJSONObjectFields(body []byte, operation string, fields ...string) (map[string]json.RawMessage, error) {
	return requireJSONObjectFieldsWithOptional(body, operation, fields, nil)
}

func requireJSONObjectFieldsWithOptional(body []byte, operation string, required, optional []string) (map[string]json.RawMessage, error) {
	if err := jcs.ValidateStructure(body); err != nil {
		return nil, fmt.Errorf("failed to decode %s response: %w", operation, err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil {
		return nil, fmt.Errorf("failed to decode %s response: %w", operation, err)
	}
	if object == nil {
		return nil, fmt.Errorf("%s response must be a JSON object", operation)
	}
	allowed := make(map[string]struct{}, len(required)+len(optional))
	for _, field := range required {
		allowed[field] = struct{}{}
		value, ok := object[field]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%s response missing required %s", operation, field)
		}
	}
	for _, field := range optional {
		allowed[field] = struct{}{}
		if value, ok := object[field]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%s response optional %s must be omitted or non-null", operation, field)
		}
	}
	for field := range object {
		if _, ok := allowed[field]; !ok {
			return nil, fmt.Errorf("%s response contains unknown field %s", operation, field)
		}
	}
	return object, nil
}

func validatePullResponseFieldPresence(body []byte) error {
	object, err := requireJSONObjectFields(body, "pull", "stable_bundle_seq", "bundles", "has_more")
	if err != nil {
		return err
	}
	var bundles []json.RawMessage
	if err := json.Unmarshal(object["bundles"], &bundles); err != nil {
		return fmt.Errorf("failed to decode pull response bundles: %w", err)
	}
	if bundles == nil {
		return fmt.Errorf("pull response bundles must be an array")
	}
	for index, bundle := range bundles {
		fields, err := requireJSONObjectFields(bundle, fmt.Sprintf("pull bundle %d", index),
			"bundle_seq", "source_id", "source_bundle_id", "bundle_hash", "rows")
		if err != nil {
			return err
		}
		for _, retired := range []string{"canonical_request_hash", "row_count", "byte_count"} {
			if _, present := fields[retired]; present {
				return fmt.Errorf("pull bundle %d must omit %s", index, retired)
			}
		}
	}
	return nil
}

func validateSnapshotSessionResponseFieldPresence(body []byte) error {
	return requireSnapshotResponseFields(body, "snapshot session",
		"snapshot_id", "snapshot_bundle_seq", "row_count", "logical_byte_count", "expires_at")
}

func validateSnapshotChunkResponseFieldPresence(body []byte) error {
	return requireSnapshotResponseFields(body, "snapshot chunk",
		"rows", "next_row_ordinal", "has_more", "logical_byte_count")
}

func requireSnapshotResponseFields(body []byte, operation string, fields ...string) error {
	_, err := requireJSONObjectFields(body, operation, fields...)
	return err
}
