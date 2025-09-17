package oversync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Idempotency Tests (I01-I05) from spec section 3

func TestI01_DuplicateUpload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// I01 – Duplicate upload
	// Upload same change twice → second upload is idempotent.
	noteID := h.MakeUUID("000000000001")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Test Note",
					"content": "This is a test note",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// First upload
	response1, httpResp1 := h.DoUpload(h.client1Token, uploadReq)
	require.Equal(t, http.StatusOK, httpResp1.StatusCode)
	require.True(t, response1.Accepted)
	require.Equal(t, "applied", response1.Statuses[0].Status)
	require.Equal(t, int64(1), response1.HighestServerSeq)

	// Second upload (identical request)
	response2, httpResp2 := h.DoUpload(h.client1Token, uploadReq)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, response2.Accepted)

	// Should be idempotent - same result, no new server_id
	require.Equal(t, int64(1), response2.HighestServerSeq) // No new changes
	require.Len(t, response2.Statuses, 1)

	// The duplicate should be handled gracefully (either "applied" or "duplicate")
	status := response2.Statuses[0]
	require.Equal(t, int64(1), status.SourceChangeID)
	// Status could be "applied" (idempotent) or "duplicate" depending on implementation
	require.Contains(t, []string{"applied", "duplicate"}, status.Status)

	// Verify only one change in change log
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 1) // Only one change should exist

	t.Logf("✅ I01 Duplicate Upload test passed - idempotency maintained")
}

func TestI02_PartialDuplicateUpload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// I02 – Partial duplicate upload
	// Upload batch with some new and some duplicate changes.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")

	// First upload with one change
	uploadReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "First Note",
					"content": "First note content",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	response1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, response1.Accepted)
	require.Equal(t, int64(1), response1.HighestServerSeq)

	// Second upload with duplicate + new change
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1, // Duplicate
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "First Note",
					"content": "First note content",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 2, // New
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Second Note",
					"content": "Second note content",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	response2, httpResp2 := h.DoUpload(h.client1Token, uploadReq2)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, response2.Accepted)
	require.Len(t, response2.Statuses, 2)

	// Note: highest_server_seq may have gaps due to PostgreSQL sequence behavior with conflicts
	// This is normal - duplicate changes consume sequence numbers but don't create rows
	require.Greater(t, response2.HighestServerSeq, response1.HighestServerSeq) // Should increase

	// First change should be duplicate/idempotent
	status1 := response2.Statuses[0]
	require.Equal(t, int64(1), status1.SourceChangeID)
	require.Contains(t, []string{"applied", "duplicate"}, status1.Status)

	// Second change should be applied
	status2 := response2.Statuses[1]
	require.Equal(t, int64(2), status2.SourceChangeID)
	require.Equal(t, "applied", status2.Status)

	// Verify total changes in database
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 2) // Two distinct changes

	t.Logf("✅ I02 Partial Duplicate Upload test passed - mixed batch handled correctly")
}

func TestI03_ReorderedDuplicateUpload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// I03 – Reordered duplicate upload
	// Upload changes in different order → idempotency maintained.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")

	// First upload with changes in order 1, 2
	uploadReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "First Note",
					"content": "First note content",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Second Note",
					"content": "Second note content",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	response1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, response1.Accepted)
	require.Equal(t, int64(2), response1.HighestServerSeq)

	// Second upload with changes in reverse order 2, 1
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2, // Reordered
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Second Note",
					"content": "Second note content",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 1, // Reordered
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "First Note",
					"content": "First note content",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	response2, httpResp2 := h.DoUpload(h.client1Token, uploadReq2)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, response2.Accepted)
	require.Equal(t, int64(2), response2.HighestServerSeq) // No new changes
	require.Len(t, response2.Statuses, 2)

	// Both changes should be handled as duplicates/idempotent
	for _, status := range response2.Statuses {
		require.Contains(t, []string{"applied", "duplicate"}, status.Status)
	}

	// Verify no additional changes in database
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 2) // Still only two changes

	t.Logf("✅ I03 Reordered Duplicate Upload test passed - order independence maintained")
}

func TestI04_CrossClientIdempotency(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// I04 – Cross-client idempotency
	// Different clients can't duplicate each other's source_change_ids.
	noteID := h.MakeUUID("000000000001")

	// Client 1 uploads change with source_change_id=1
	uploadReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Client 1 Note",
					"content": "From client 1",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	response1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, response1.Accepted)
	require.Equal(t, int64(1), response1.HighestServerSeq)

	// Client 2 uploads different change with same source_change_id=1 (different source_id)
	noteID2 := h.MakeUUID("000000000002")
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1, // Same source_change_id but different source_id
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Client 2 Note",
					"content": "From client 2",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	response2, httpResp2 := h.DoUpload(h.client2Token, uploadReq2)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, response2.Accepted)
	require.Equal(t, int64(2), response2.HighestServerSeq) // New change created
	require.Equal(t, "applied", response2.Statuses[0].Status)

	// Client 1 sees only Client 2's change (excludes own changes)
	downloadResp, _ := h.DoDownload(h.client1Token, 0, 100)
	require.Len(t, downloadResp.Changes, 1)

	// Verify Client 1 sees Client 2's change
	require.Equal(t, h.client2ID, downloadResp.Changes[0].SourceID)
	require.Equal(t, int64(1), downloadResp.Changes[0].SourceChangeID)

	// Verify Client 2 sees Client 1's change
	downloadResp2, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp2.Changes, 1)
	require.Equal(t, h.client1ID, downloadResp2.Changes[0].SourceID)
	require.Equal(t, int64(1), downloadResp2.Changes[0].SourceChangeID)

	t.Logf("✅ I04 Cross-Client Idempotency test passed - different clients can use same source_change_ids")
}

func TestI05_IdempotencyWithConflicts(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// I05 – Idempotency with conflicts
	// Duplicate upload of conflicting change → same conflict result.
	noteID := h.MakeUUID("000000000001")

	// First, create a note
	insertReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Original Note",
					"content": "Original content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)

	// Another client updates the note (increments server_version to 2)
	updateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated by Client 2",
					"content": "Updated content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	updateResp, _ := h.DoUpload(h.client2Token, updateReq)
	require.True(t, updateResp.Accepted)
	require.Equal(t, int64(2), *updateResp.Statuses[0].NewServerVersion)

	// Now client 1 tries to update with stale version (will conflict)
	conflictReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1, // Stale version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Conflicting Update",
					"content": "This will conflict",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// First conflicting upload
	response1, _ := h.DoUpload(h.client1Token, conflictReq)
	require.True(t, response1.Accepted)
	require.Equal(t, "conflict", response1.Statuses[0].Status)
	require.NotNil(t, response1.Statuses[0].ServerRow)

	// Duplicate conflicting upload (same request)
	response2, httpResp2 := h.DoUpload(h.client1Token, conflictReq)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, response2.Accepted)

	// For duplicate conflicting uploads, the server re-returns the same conflict result.
	// We do not log conflicts in the idempotency log, so the duplicate is re-evaluated and remains a conflict.
	require.Equal(t, "conflict", response2.Statuses[0].Status)
	// Ensure no extra changes were added to the change stream.
	// Note: download excludes caller's own changes (include_self=false),
	// so client2 sees only client1's original INSERT → exactly 1 change.
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 1)
	require.Equal(t, h.client1ID, downloadResp.Changes[0].SourceID)

	// Symmetry: client1 should see client2's UPDATE as 1 change as well.
	downloadResp2, _ := h.DoDownload(h.client1Token, 0, 100)
	require.Len(t, downloadResp2.Changes, 1)
	require.Equal(t, h.client2ID, downloadResp2.Changes[0].SourceID)

	// The key point is that the duplicate upload doesn't create a new server_change_log entry
	// and returns a consistent result

	t.Logf("✅ I05 Idempotency with Conflicts test passed - conflict results are idempotent")
}
