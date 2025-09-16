package oversync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Materializer failure persistence and retry tests

func TestMF01_RecordFailureOnMaterializerError(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	userID := h.ExtractUserIDFromToken(h.client1Token)

	noteID := h.MakeUUID("mf01-note")
	// Upload an INSERT that forces the table handler to fail
	payload := map[string]any{
		"id":         noteID.String(),
		"title":      "Should Fail",
		"content":    "payload",
		"updated_at": time.Now().Format(time.RFC3339),
		"force_fail": true,
	}
	b, _ := json.Marshal(payload)

	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        b,
	}})

	// Verify materialize_error status and attempted version present
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "materialize_error", resp.Statuses[0].Status)
	require.NotNil(t, resp.Statuses[0].NewServerVersion)
	require.Equal(t, int64(1), *resp.Statuses[0].NewServerVersion)

	// Verify sidecar not advanced (no meta row)
	_, err := h.GetSyncRowMeta("public", "note", noteID)
	require.Error(t, err)

	// Verify failure persisted
	var count int
	err = h.service.Pool().QueryRow(h.ctx, `
        SELECT COUNT(*) FROM sync.materialize_failures
        WHERE user_id=$1 AND schema_name='public' AND table_name='note'
          AND pk_uuid=$2 AND attempted_version=1`, userID, noteID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestMF02_RetryIncrementsRetryCount(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	userID := h.ExtractUserIDFromToken(h.client1Token)

	noteID := h.MakeUUID("mf02-note")
	payload := map[string]any{
		"id":         noteID.String(),
		"title":      "Should Fail Again",
		"content":    "payload",
		"updated_at": time.Now().Format(time.RFC3339),
		"force_fail": true,
	}
	b, _ := json.Marshal(payload)

	// First failing attempt
	_ = h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        b,
	}})

	// Second failing attempt (same version 0 → attempted version 1 again)
	_ = h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        b,
	}})

	// Verify a single row exists with retry_count incremented to 1
	var retryCount int
	err := h.service.Pool().QueryRow(h.ctx, `
        SELECT retry_count FROM sync.materialize_failures
        WHERE user_id=$1 AND schema_name='public' AND table_name='note'
          AND pk_uuid=$2 AND attempted_version=1`, userID, noteID).Scan(&retryCount)
	require.NoError(t, err)
	require.Equal(t, 1, retryCount)
}
