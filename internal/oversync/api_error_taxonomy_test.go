package oversync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// API Error Taxonomy: verifies standardized invalid reasons and messages
func TestAT01_BadPayloadReservedKey(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	noteID := h.MakeUUID("at01-note")
	payload := map[string]any{
		"id":             noteID.String(),
		"title":          "Title",
		"content":        "C",
		"updated_at":     time.Now().Format(time.RFC3339),
		"server_version": 99, // reserved key
	}
	b, _ := json.Marshal(payload)

	resp, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        b,
	}}})

	// Bad payload should not fail the whole batch contract; accepted remains true
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	st := resp.Statuses[0]
	require.Equal(t, oversync.StInvalid, st.Status)
	// Invalid reason is bad_payload for reserved key
	require.Equal(t, oversync.ReasonBadPayload, st.Invalid["reason"])
	require.Contains(t, st.Message, "server_version")
}

func TestAT02_InvalidOperation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	noteID := h.MakeUUID("at02-note")
	payload := h.MakeNotePayload(noteID, "T", "C", time.Now())

	resp, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INVALID_OP",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        payload,
	}}})

	// Invalid operation should not fail entire batch; accepted stays true
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	st := resp.Statuses[0]
	require.Equal(t, oversync.StInvalid, st.Status)
	// Reason is bad_payload for invalid op
	require.Equal(t, oversync.ReasonBadPayload, st.Invalid["reason"])
	require.Contains(t, st.Message, "invalid operation")
}

func TestAT03_InvalidUUID(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	payload := h.MakeNotePayload(h.MakeUUID("at03-note"), "T", "C", time.Now())

	resp, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             "not-a-uuid",
		ServerVersion:  0,
		Payload:        payload,
	}}})

	// Invalid UUID should not fail entire batch; accepted stays true
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	st := resp.Statuses[0]
	require.Equal(t, oversync.StInvalid, st.Status)
	require.Equal(t, oversync.ReasonBadPayload, st.Invalid["reason"])
	require.Contains(t, st.Message, "invalid UUID format")
}

func TestAT04_UnregisteredTable(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	noteID := h.MakeUUID("at04-note")
	payload := h.MakeNotePayload(noteID, "T", "C", time.Now())

	// Use an unregistered table name
	resp, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "not_registered",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        payload,
	}}})

	// Batch should not be accepted when targeting unregistered tables
	require.False(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	st := resp.Statuses[0]
	require.Equal(t, oversync.StInvalid, st.Status)
	// Invalid reason should be explicit for unregistered table
	require.Equal(t, oversync.ReasonUnregisteredTable, st.Invalid["reason"])
	require.Contains(t, st.Message, "table not registered")
}
