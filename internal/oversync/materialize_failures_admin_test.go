package oversync

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestMF_AdminListAndRetry(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	userID := h.ExtractUserIDFromToken(h.client1Token)
	noteID := h.MakeUUID("mfadmin-note")

	// Create a failing upload (force_fail=true)
	payload := map[string]any{
		"id":         noteID.String(),
		"title":      "Failing Note",
		"content":    "test",
		"updated_at": time.Now().Format(time.RFC3339),
		"force_fail": true,
	}
	b, _ := json.Marshal(payload)

	_ = h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        b,
	}})

	// List failures via HTTP
	req := h.createHTTPRequest("GET", "/admin/materialize-failures?limit=10", nil)
	req.Header.Set("Authorization", "Bearer "+h.client1Token)
	rec := h.executeHTTPRequest(req)
	require.Equal(t, 200, rec.Code)
	var failures []oversync.MaterializeFailure
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &failures))
	require.Len(t, failures, 1)
	f := failures[0]
	require.Equal(t, userID, f.UserID)
	require.Equal(t, "public", f.SchemaName)
	require.Equal(t, "note", f.TableName)
	require.Equal(t, noteID.String(), f.PK)
	require.Equal(t, int64(1), f.AttemptedVersion)
	require.Equal(t, "INSERT", f.Op)
	require.True(t, f.RetryCount >= 0)

	// Update stored payload to remove force_fail so retry can succeed
	var newPayload map[string]any
	_ = json.Unmarshal(b, &newPayload)
	delete(newPayload, "force_fail")
	fixed, _ := json.Marshal(newPayload)
	// Best-effort; ignore RETURNING scan error on some drivers
	_, _ = h.service.Pool().Exec(h.ctx, `UPDATE sync.materialize_failures SET payload=$1 WHERE id=$2`, fixed, f.ID)

	// Retry via HTTP
	retryReq := h.createHTTPRequest("POST", fmt.Sprintf("/admin/materialize-failures/retry?id=%d", f.ID), nil)
	retryReq.Header.Set("Authorization", "Bearer "+h.client1Token)
	retryRec := h.executeHTTPRequest(retryReq)
	require.Equal(t, 200, retryRec.Code)
	var status oversync.ChangeUploadStatus
	require.NoError(t, json.Unmarshal(retryRec.Body.Bytes(), &status))
	require.Equal(t, "applied", status.Status)

	// Verify failure row deleted
	var cnt int
	require.NoError(t, h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.materialize_failures WHERE id=$1`, f.ID).Scan(&cnt))
	require.Equal(t, 0, cnt)

	// Verify meta exists with version 1
	meta, err2 := h.GetSyncRowMeta("public", "note", noteID)
	require.NoError(t, err2)
	require.Equal(t, int64(1), meta.ServerVersion)
}
