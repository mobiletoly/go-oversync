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

// Multi-client Tests (M01-M05) from spec section 4

func TestM01_TwoClientSync(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// M01 – Two-client sync
	// C1 uploads, C2 downloads → C2 sees C1's changes.
	noteID := h.MakeUUID("000000000001")

	// Client 1 uploads a change
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
					"title": "From Client 1",
					"content": "This note was created by client 1",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, int64(1), uploadResp.HighestServerSeq)

	// Client 2 downloads changes
	downloadResp, httpResp := h.DoDownload(h.client2Token, 0, 100)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, downloadResp.Changes, 1)

	// Verify Client 2 sees Client 1's change
	change := downloadResp.Changes[0]
	require.Equal(t, int64(1), change.ServerID)
	require.Equal(t, "note", change.TableName)
	require.Equal(t, "INSERT", change.Op)
	require.Equal(t, noteID.String(), change.PK)
	require.Equal(t, h.client1ID, change.SourceID)
	require.Equal(t, int64(1), change.SourceChangeID)

	// Verify payload content
	var payload map[string]interface{}
	err := json.Unmarshal(change.Payload, &payload)
	require.NoError(t, err)
	require.Equal(t, "From Client 1", payload["title"])
	require.Equal(t, "This note was created by client 1", payload["content"])

	t.Logf("✅ M01 Two-Client Sync test passed - Client 2 sees Client 1's changes")
}

func TestM02_BidirectionalSync(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// M02 – Bidirectional sync
	// C1 uploads, C2 uploads, both download → both see each other's changes.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")

	// Client 1 uploads a change
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
					"title": "From Client 1",
					"content": "Client 1's note",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, uploadResp1.Accepted)
	require.Equal(t, int64(1), uploadResp1.HighestServerSeq)

	// Client 2 uploads a change
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "From Client 2",
					"content": "Client 2's note",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp2, _ := h.DoUpload(h.client2Token, uploadReq2)
	require.True(t, uploadResp2.Accepted)
	require.Equal(t, int64(2), uploadResp2.HighestServerSeq)

	// Sidecar v2: Client 1 downloads changes (excludes own changes)
	downloadResp1, _ := h.DoDownload(h.client1Token, 0, 100)
	require.Len(t, downloadResp1.Changes, 1) // Only sees Client 2's change

	// Verify Client 1 sees only Client 2's change
	require.Equal(t, h.client2ID, downloadResp1.Changes[0].SourceID) // Client 2's change

	// Sidecar v2: Client 2 downloads changes (excludes own changes)
	downloadResp2, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp2.Changes, 1) // Only sees Client 1's change

	// Verify Client 2 sees only Client 1's change
	require.Equal(t, h.client1ID, downloadResp2.Changes[0].SourceID) // Client 1's change

	t.Logf("✅ M02 Bidirectional Sync test passed - both clients see each other's changes")
}

func TestM03_ConflictBetweenClients(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// M03 – Conflict between clients
	// C1 uploads note, C2 updates same note with stale version → conflict.
	noteID := h.MakeUUID("000000000001")

	// Client 1 creates a note
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
					"title": "Original Note",
					"content": "Original content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, uploadResp1.Accepted)
	require.Equal(t, int64(1), *uploadResp1.Statuses[0].NewServerVersion)

	// Client 1 updates the note (increments server_version to 2)
	uploadReq1Update := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated by Client 1",
					"content": "Client 1 updated this",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp1Update, _ := h.DoUpload(h.client1Token, uploadReq1Update)
	require.True(t, uploadResp1Update.Accepted)
	require.Equal(t, int64(2), *uploadResp1Update.Statuses[0].NewServerVersion)

	// Client 2 tries to update the same note with stale version (expects version 1, but server has version 2)
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 1, // Client 2 hasn't seen the update from Client 1
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1, // Stale version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated by Client 2",
					"content": "Client 2 tried to update this",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp2, httpResp2 := h.DoUpload(h.client2Token, uploadReq2)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, uploadResp2.Accepted)
	require.Len(t, uploadResp2.Statuses, 1)

	// Verify conflict detected
	status := uploadResp2.Statuses[0]
	require.Equal(t, int64(1), status.SourceChangeID)
	require.Equal(t, "conflict", status.Status)
	require.Nil(t, status.NewServerVersion)
	require.NotNil(t, status.ServerRow)

	// Parse and verify server row shows Client 1's update (sidecar v2 format)
	var serverState map[string]interface{}
	err := json.Unmarshal(status.ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, "note", serverState["table_name"])
	require.Equal(t, float64(2), serverState["server_version"])
	require.Equal(t, false, serverState["deleted"])

	// Extract payload and verify business data
	payload, ok := serverState["payload"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Updated by Client 1", payload["title"])
	require.Equal(t, "Client 1 updated this", payload["content"])

	t.Logf("✅ M03 Conflict Between Clients test passed - conflict detected and server state returned")
}

func TestM04_IncrementalSyncBetweenClients(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// M04 – Incremental sync between clients
	// Multiple uploads and downloads with proper watermarking.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")
	noteID3 := h.MakeUUID("000000000003")

	// Phase 1: Client 1 uploads first note
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
					"title": "Note 1",
					"content": "First note",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, uploadResp1.Accepted)
	require.Equal(t, int64(1), uploadResp1.HighestServerSeq)

	// Phase 2: Client 2 downloads and sees first note
	downloadResp2Phase1, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp2Phase1.Changes, 1)
	require.Equal(t, int64(1), downloadResp2Phase1.NextAfter)

	// Phase 3: Client 2 uploads second note
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: downloadResp2Phase1.NextAfter,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Note 2",
					"content": "Second note",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp2, _ := h.DoUpload(h.client2Token, uploadReq2)
	require.True(t, uploadResp2.Accepted)
	require.Equal(t, int64(2), uploadResp2.HighestServerSeq)

	// Phase 4: Client 1 downloads incrementally (only new changes since last download)
	downloadResp1Incremental, _ := h.DoDownload(h.client1Token, uploadResp1.HighestServerSeq, 100)
	require.Len(t, downloadResp1Incremental.Changes, 1) // Only Client 2's new note
	require.Equal(t, h.client2ID, downloadResp1Incremental.Changes[0].SourceID)
	require.Equal(t, int64(2), downloadResp1Incremental.NextAfter)

	// Phase 5: Client 1 uploads third note
	uploadReq1Phase2 := &oversync.UploadRequest{
		LastServerSeqSeen: downloadResp1Incremental.NextAfter,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID3.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Note 3",
					"content": "Third note",
					"updated_at": %d
				}`, noteID3.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp1Phase2, _ := h.DoUpload(h.client1Token, uploadReq1Phase2)
	require.True(t, uploadResp1Phase2.Accepted)
	require.Equal(t, int64(3), uploadResp1Phase2.HighestServerSeq)

	// Phase 6: Client 2 downloads incrementally (only new changes since last download)
	downloadResp2Incremental, _ := h.DoDownload(h.client2Token, uploadResp2.HighestServerSeq, 100)
	require.Len(t, downloadResp2Incremental.Changes, 1) // Only Client 1's third note
	require.Equal(t, h.client1ID, downloadResp2Incremental.Changes[0].SourceID)
	require.Equal(t, int64(3), downloadResp2Incremental.NextAfter)

	// Final verification: Sidecar v2 - each client sees only other's changes
	downloadRespFinal1, _ := h.DoDownload(h.client1Token, 0, 100)
	downloadRespFinal2, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadRespFinal1.Changes, 1) // Client 1 sees only Client 2's change
	require.Len(t, downloadRespFinal2.Changes, 2) // Client 2 sees Client 1's 2 changes

	t.Logf("✅ M04 Incremental Sync Between Clients test passed - watermarking works correctly")
}

func TestM05_ConcurrentUploads(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// M05 – Concurrent uploads
	// Both clients upload simultaneously → both succeed with different server_ids.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")

	// Prepare upload requests for both clients
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
					"title": "Concurrent Note 1",
					"content": "From client 1",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Concurrent Note 2",
					"content": "From client 2",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	// Upload from both clients (simulating concurrent uploads)
	uploadResp1, httpResp1 := h.DoUpload(h.client1Token, uploadReq1)
	uploadResp2, httpResp2 := h.DoUpload(h.client2Token, uploadReq2)

	// Both uploads should succeed
	require.Equal(t, http.StatusOK, httpResp1.StatusCode)
	require.Equal(t, http.StatusOK, httpResp2.StatusCode)
	require.True(t, uploadResp1.Accepted)
	require.True(t, uploadResp2.Accepted)

	// Both should get "applied" status
	require.Equal(t, "applied", uploadResp1.Statuses[0].Status)
	require.Equal(t, "applied", uploadResp2.Statuses[0].Status)

	// Both should have different server_ids
	require.NotEqual(t, uploadResp1.HighestServerSeq, uploadResp2.HighestServerSeq)

	// The final highest_server_seq should be 2 (both changes processed)
	finalHighest := max(uploadResp1.HighestServerSeq, uploadResp2.HighestServerSeq)
	require.Equal(t, int64(2), finalHighest)

	// Sidecar v2: Verify each client sees only the other's changes
	downloadResp1, _ := h.DoDownload(h.client1Token, 0, 100)
	downloadResp2, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp1.Changes, 1) // Client 1 sees only Client 2's change
	require.Len(t, downloadResp2.Changes, 1) // Client 2 sees only Client 1's change

	// Verify Client 1 sees Client 2's change
	require.Equal(t, h.client2ID, downloadResp1.Changes[0].SourceID)
	require.Equal(t, int64(2), downloadResp1.Changes[0].ServerID) // Client 2's change has server_id=2

	// Verify Client 2 sees Client 1's change
	require.Equal(t, h.client1ID, downloadResp2.Changes[0].SourceID)
	require.Equal(t, int64(1), downloadResp2.Changes[0].ServerID) // Client 1's change has server_id=1

	t.Logf("✅ M05 Concurrent Uploads test passed - both clients succeeded with different server_ids")
}

// Helper function for max (Go 1.21+ has this built-in)
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
