package oversync

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPL01_BatchSizeLimit(t *testing.T) {
	h := NewSimpleTestHarnessWithConfig(t, func(cfg *oversync.ServiceConfig) {
		cfg.MaxUploadBatchSize = 2
	})
	defer h.Cleanup()
	h.Reset()

	// Build 3 changes (exceeds limit of 2)
	c1 := oversync.ChangeUpload{SourceChangeID: 1, Table: "note", Op: "INSERT", PK: h.MakeUUID("pl01-1").String(), ServerVersion: 0, Payload: h.MakeNotePayload(h.MakeUUID("pl01-1"), "t1", "c1", time.Now())}
	c2 := oversync.ChangeUpload{SourceChangeID: 2, Table: "note", Op: "INSERT", PK: h.MakeUUID("pl01-2").String(), ServerVersion: 0, Payload: h.MakeNotePayload(h.MakeUUID("pl01-2"), "t2", "c2", time.Now())}
	c3 := oversync.ChangeUpload{SourceChangeID: 3, Table: "note", Op: "INSERT", PK: h.MakeUUID("pl01-3").String(), ServerVersion: 0, Payload: h.MakeNotePayload(h.MakeUUID("pl01-3"), "t3", "c3", time.Now())}

	resp, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{c1, c2, c3}})
	require.False(t, resp.Accepted)
	require.Len(t, resp.Statuses, 3)
	for _, st := range resp.Statuses {
		require.Equal(t, oversync.StInvalid, st.Status)
		require.Equal(t, oversync.ReasonBatchTooLarge, st.Invalid["reason"])
	}

	// At boundary (limit=2) should succeed
	respOK, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{c1, c2}})
	require.True(t, respOK.Accepted)
	require.Len(t, respOK.Statuses, 2)
}

func TestPL02_PayloadSizeLimit(t *testing.T) {
	h := NewSimpleTestHarnessWithConfig(t, func(cfg *oversync.ServiceConfig) {
		cfg.MaxPayloadBytes = 128 // small limit for test
	})
	defer h.Cleanup()
	h.Reset()

	// Create a large string to exceed 128 bytes in JSON
	big := strings.Repeat("x", 256)
	id := h.MakeUUID("pl02")
	payload := map[string]any{"id": id.String(), "title": big, "content": "c", "updated_at": time.Now().Format(time.RFC3339)}
	b, _ := json.Marshal(payload)
	require.Greater(t, len(b), 128)

	resp, _ := h.DoUpload(h.client1Token, &oversync.UploadRequest{Changes: []oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             id.String(),
		ServerVersion:  0,
		Payload:        b,
	}}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	st := resp.Statuses[0]
	require.Equal(t, oversync.StInvalid, st.Status)
	require.Equal(t, oversync.ReasonBadPayload, st.Invalid["reason"])
}
