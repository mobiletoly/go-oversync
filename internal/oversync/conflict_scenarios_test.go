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

// Conflict Scenarios Tests (C01-C04) from spec section 7

func TestC01_UpdateVsUpdate(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// C01 – Update vs Update (C1 vs C2)
	// • Seed server note(A) SVID=5.
	// • C1 updates with SVID=5 → accepted (SVID=6).
	// • C2 updates with SVID=5 → conflict.
	// Assert: C2 receives server_row (SVID=6).

	noteID := h.MakeUUID("000000000001")

	// Create note and update it to get SVID=5
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

	// Update 4 times to get server_version=5
	for i := 2; i <= 5; i++ {
		updateReq := &oversync.UploadRequest{
			LastServerSeqSeen: int64(i - 1),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(i),
					Table:          "note",
					Op:             "UPDATE",
					PK:             noteID.String(),
					ServerVersion:  int64(i - 1),
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Update %d",
						"content": "Content %d",
						"updated_at": %d
					}`, noteID.String(), i, i, time.Now().Unix())),
				},
			},
		}

		updateResp, _ := h.DoUpload(h.client1Token, updateReq)
		require.True(t, updateResp.Accepted)
		require.Equal(t, int64(i), *updateResp.Statuses[0].NewServerVersion)
	}

	// Verify server_version is 5
	note, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.NotNil(t, note)
	require.Equal(t, int64(5), note.ServerVersion)

	// C1 updates with SVID=5 → accepted (SVID=6)
	c1UpdateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 6,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  5, // Current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "C1 Update",
					"content": "Updated by Client 1",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	c1UpdateResp, _ := h.DoUpload(h.client1Token, c1UpdateReq)
	require.True(t, c1UpdateResp.Accepted)
	require.Equal(t, "applied", c1UpdateResp.Statuses[0].Status)
	require.Equal(t, int64(6), *c1UpdateResp.Statuses[0].NewServerVersion)

	// C2 updates with SVID=5 → conflict (server now has SVID=6)
	c2UpdateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5, // C2 hasn't seen C1's update
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  5, // Stale version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "C2 Update",
					"content": "Updated by Client 2",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	c2UpdateResp, httpResp := h.DoUpload(h.client2Token, c2UpdateReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, c2UpdateResp.Accepted)
	require.Len(t, c2UpdateResp.Statuses, 1)

	// Assert: C2 receives conflict with server_row (SVID=6)
	status := c2UpdateResp.Statuses[0]
	require.Equal(t, int64(1), status.SourceChangeID)
	require.Equal(t, "conflict", status.Status)
	require.Nil(t, status.NewServerVersion)
	require.NotNil(t, status.ServerRow)

	// Parse and verify server row shows C1's update (SVID=6) - sidecar
	var serverState map[string]interface{}
	err = json.Unmarshal(status.ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, "note", serverState["table_name"])
	require.Equal(t, float64(6), serverState["server_version"])
	require.Equal(t, false, serverState["deleted"])

	// Extract payload and verify business data
	payload, ok := serverState["payload"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "C1 Update", payload["title"])
	require.Equal(t, "Updated by Client 1", payload["content"])

	t.Logf("✅ C01 Update vs Update test passed - C2 received conflict with server_row SVID=6")
}

func TestC02_UpdateVsDelete(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// C02 – Update vs Delete
	// • Seed SVID=5.
	// • C1 updates first → SVID=6.
	// • C2 deletes with SVID=5 → conflict; C2 retries with SVID=6 → accepted.

	noteID := h.MakeUUID("000000000002")

	// Create note and update to SVID=5 (same as C01 setup)
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
					"title": "Note for Update vs Delete",
					"content": "Will be updated then deleted",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)

	// Update 4 times to get server_version=5
	for i := 2; i <= 5; i++ {
		updateReq := &oversync.UploadRequest{
			LastServerSeqSeen: int64(i - 1),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(i),
					Table:          "note",
					Op:             "UPDATE",
					PK:             noteID.String(),
					ServerVersion:  int64(i - 1),
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Update %d",
						"content": "Content %d",
						"updated_at": %d
					}`, noteID.String(), i, i, time.Now().Unix())),
				},
			},
		}

		updateResp, _ := h.DoUpload(h.client1Token, updateReq)
		require.True(t, updateResp.Accepted)
	}

	// C1 updates first → SVID=6
	c1UpdateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 6,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  5,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Final Update",
					"content": "Updated before delete attempt",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	c1UpdateResp, _ := h.DoUpload(h.client1Token, c1UpdateReq)
	require.True(t, c1UpdateResp.Accepted)
	require.Equal(t, int64(6), *c1UpdateResp.Statuses[0].NewServerVersion)

	// C2 deletes with SVID=5 → conflict
	c2DeleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  5, // Stale version
				Payload:        nil,
			},
		},
	}

	c2DeleteResp, _ := h.DoUpload(h.client2Token, c2DeleteReq)
	require.True(t, c2DeleteResp.Accepted)
	require.Equal(t, "conflict", c2DeleteResp.Statuses[0].Status)

	// Verify server row shows SVID=6
	var serverState map[string]interface{}
	err := json.Unmarshal(c2DeleteResp.Statuses[0].ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, float64(6), serverState["server_version"])

	// C2 retries with SVID=6 → accepted
	c2DeleteRetryReq := &oversync.UploadRequest{
		LastServerSeqSeen: 6,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2, // New source_change_id
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  6, // Correct version
				Payload:        nil,
			},
		},
	}

	c2DeleteRetryResp, _ := h.DoUpload(h.client2Token, c2DeleteRetryReq)
	require.True(t, c2DeleteRetryResp.Accepted)
	require.Equal(t, "applied", c2DeleteRetryResp.Statuses[0].Status)

	// Verify note is deleted
	note, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.Nil(t, note)

	t.Logf("✅ C02 Update vs Delete test passed - conflict then successful retry")
}

func TestC03_InsertVsInsertSamePK(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// C03 – Insert vs Insert same PK (duplicate UUID)
	// • Both clients insert same UUID A offline.
	// • First insert wins (SVID=1).
	// • Second insert with SVID=0 conflicts; server returns existing row; client must generate new ID.

	noteID := h.MakeUUID("000000000003") // Same UUID for both clients

	// C1 inserts first
	c1InsertReq := &oversync.UploadRequest{
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
					"title": "C1 Note",
					"content": "Created by Client 1",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	c1InsertResp, _ := h.DoUpload(h.client1Token, c1InsertReq)
	require.True(t, c1InsertResp.Accepted)
	require.Equal(t, "applied", c1InsertResp.Statuses[0].Status)
	require.Equal(t, int64(1), *c1InsertResp.Statuses[0].NewServerVersion)

	// C2 inserts same UUID → conflict
	c2InsertReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0, // C2 hasn't seen C1's insert
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(), // Same UUID
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "C2 Note",
					"content": "Created by Client 2",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	c2InsertResp, _ := h.DoUpload(h.client2Token, c2InsertReq)
	require.True(t, c2InsertResp.Accepted)
	require.Equal(t, "conflict", c2InsertResp.Statuses[0].Status)

	// Server returns existing row (C1's insert) - sidecar format
	var serverState map[string]interface{}
	err := json.Unmarshal(c2InsertResp.Statuses[0].ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, "note", serverState["table_name"])
	require.Equal(t, float64(1), serverState["server_version"])
	require.Equal(t, false, serverState["deleted"])

	// Extract payload and verify business data
	payload, ok := serverState["payload"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "C1 Note", payload["title"])
	require.Equal(t, "Created by Client 1", payload["content"])

	// Verify only C1's note exists in database
	note, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.NotNil(t, note)
	require.Equal(t, "C1 Note", note.Title)
	require.Equal(t, "Created by Client 1", note.Content)

	// In practice, C2 would generate a new UUID and retry
	newNoteID := h.MakeUUID("c2retry")
	c2RetryReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2, // New source_change_id
				Table:          "note",
				Op:             "INSERT",
				PK:             newNoteID.String(), // New UUID
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "C2 Note Retry",
					"content": "Created by Client 2 with new ID",
					"updated_at": %d
				}`, newNoteID.String(), time.Now().Unix())),
			},
		},
	}

	c2RetryResp, _ := h.DoUpload(h.client2Token, c2RetryReq)
	require.True(t, c2RetryResp.Accepted)
	require.Equal(t, "applied", c2RetryResp.Statuses[0].Status)

	t.Logf("✅ C03 Insert vs Insert Same PK test passed - first insert wins, second conflicts and retries with new ID")
}

func TestC04_OutOfOrderUploadsFromSameClient(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// C04 – Out-of-order uploads from same client
	// • Client sends SCID=3 before SCID=2.
	// Assert: Version checks ensure correctness; eventual log order stable; client can retry SCID=2.

	noteID := h.MakeUUID("000000000004")

	// First, create a note (SCID=1)
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
	require.Equal(t, int64(1), *insertResp.Statuses[0].NewServerVersion)

	// Client sends SCID=3 before SCID=2 (out of order)
	// This simulates network reordering or client retry logic
	scid3Req := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3, // SCID=3 sent first
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Update 3",
					"content": "Third update sent first",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	scid3Resp, _ := h.DoUpload(h.client1Token, scid3Req)
	require.True(t, scid3Resp.Accepted)
	require.Equal(t, "applied", scid3Resp.Statuses[0].Status)
	require.Equal(t, int64(2), *scid3Resp.Statuses[0].NewServerVersion) // Server version increments

	// Now client sends SCID=2 (late arrival)
	scid2Req := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2, // SCID=2 sent after SCID=3
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1, // Based on original state, but server now has version 2
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Update 2",
					"content": "Second update sent late",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	scid2Resp, _ := h.DoUpload(h.client1Token, scid2Req)
	require.True(t, scid2Resp.Accepted)

	// This should conflict because server_version is now 2, but client expects 1
	require.Equal(t, "conflict", scid2Resp.Statuses[0].Status)

	// Verify server state shows the result of SCID=3 (sidecar format)
	var serverState map[string]interface{}
	err := json.Unmarshal(scid2Resp.Statuses[0].ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, "note", serverState["table_name"])
	require.Equal(t, float64(2), serverState["server_version"])
	require.Equal(t, false, serverState["deleted"])

	// Extract payload and verify business data
	payload, ok := serverState["payload"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Update 3", payload["title"])

	// Verify server_change_log has both entries in the order they were processed
	changeLog3, err := h.GetChangeLogEntry(h.client1ID, 3)
	require.NoError(t, err)
	require.NotNil(t, changeLog3)

	// SCID=2 should NOT have created a server_change_log entry because it conflicted
	// The new behavior only logs successful applications, not conflicts
	changeLog2, err := h.GetChangeLogEntry(h.client1ID, 2)
	require.NoError(t, err)
	require.Nil(t, changeLog2) // Entry should not exist for conflicted changes

	// Client can retry SCID=2 with correct server_version
	scid2RetryReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 4, // New SCID for retry
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  2, // Correct current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Update 2 Retry",
					"content": "Second update retried with correct version",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	scid2RetryResp, _ := h.DoUpload(h.client1Token, scid2RetryReq)
	require.True(t, scid2RetryResp.Accepted)
	require.Equal(t, "applied", scid2RetryResp.Statuses[0].Status)

	t.Logf("✅ C04 Out-of-Order Uploads test passed - version checks ensure correctness, client can retry")
}
