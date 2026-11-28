package oversqlite

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

var errMalformedConflictEvidence = errors.New("malformed or mismatched authoritative conflict evidence")

func completeConflictResponse(body []byte) (*oversync.PushConflictResponse, error) {
	_, err := jcs.Canonicalize(body)
	if err != nil {
		return nil, err
	}
	// Keep integer protocol versions exact; canonicalization here is only a duplicate-key/syntax check.
	raw := body
	var envelope map[string]json.RawMessage
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if !conflictFieldsPresent(envelope, []string{"error", "message", "conflict"}, "") {
		return nil, errMalformedConflictEvidence
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(envelope["conflict"], &fields); err != nil {
		return nil, err
	}
	if !conflictFieldsPresent(fields, []string{"schema", "table", "key", "op", "base_row_version", "server_row_version", "server_row_deleted", "server_row"}, "server_row") {
		return nil, errMalformedConflictEvidence
	}
	var response oversync.PushConflictResponse
	if err = json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	if response.Error != "push_conflict" || !coherentConflict(response.Conflict) {
		return nil, errMalformedConflictEvidence
	}
	return &response, nil
}

func conflictFieldsPresent(fields map[string]json.RawMessage, required []string, nullable string) bool {
	for _, name := range required {
		value, ok := fields[name]
		if !ok || (name != nullable && bytes.Equal(bytes.TrimSpace(value), []byte("null"))) {
			return false
		}
	}
	return true
}

func coherentConflict(c *oversync.PushConflictDetails) bool {
	if c == nil || c.Schema == "" || c.Table == "" || len(c.Key) == 0 || c.BaseRowVersion < 0 || c.ServerRowVersion < 0 {
		return false
	}
	if c.Op != "INSERT" && c.Op != "UPDATE" && c.Op != "DELETE" {
		return false
	}
	raw := bytes.TrimSpace(c.ServerRow)
	if len(raw) == 0 {
		return false
	}
	absent := bytes.Equal(raw, []byte("null"))
	if c.ServerRowVersion == 0 {
		return absent && !c.ServerRowDeleted
	}
	if c.ServerRowDeleted {
		return absent
	}
	var row map[string]json.RawMessage
	return !absent && json.Unmarshal(raw, &row) == nil && row != nil
}
