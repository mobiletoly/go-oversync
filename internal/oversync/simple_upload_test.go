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

func TestSimpleU01_BasicInsert(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U01 – Basic insert
	// C1 uploads note(A) with SCID=1, SVID=0 → applied → SVID=1.
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

	// Upload the change
	response, httpResp := h.DoUpload(h.client1Token, uploadReq)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert upload accepted
	require.True(t, response.Accepted)
	require.Equal(t, int64(1), response.HighestServerSeq)
	require.Len(t, response.Statuses, 1)

	// Assert change applied with new server version
	status := response.Statuses[0]
	require.Equal(t, int64(1), status.SourceChangeID)
	require.Equal(t, "applied", status.Status)
	require.NotNil(t, status.NewServerVersion)
	require.Equal(t, int64(1), *status.NewServerVersion)

	t.Logf("✅ U01 Basic Insert test passed - note created with server_version=1")
}

func TestSimpleU04_ConflictOnUpdate(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U04 – Conflict on update
	// First, insert a note to establish baseline
	noteID := h.MakeUUID("000000000004")

	// Insert the note first
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
					"title": "Original Title",
					"content": "Original Content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)
	require.Equal(t, "applied", insertResp.Statuses[0].Status)

	// Now simulate another client updating the note (this would increment server_version to 2)
	updateReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1, // Expect version 1
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated by Client 2",
					"content": "Content updated by client 2",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	updateResp1, _ := h.DoUpload(h.client2Token, updateReq1)
	require.True(t, updateResp1.Accepted)
	require.Equal(t, "applied", updateResp1.Statuses[0].Status)
	require.Equal(t, int64(2), *updateResp1.Statuses[0].NewServerVersion)

	// Now client 1 tries to update with stale version (expects version 1, but server has version 2)
	conflictReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1, // Client 1 hasn't seen the update from client 2
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1, // Expect version 1, but server has version 2
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Conflicting Update",
					"content": "This should conflict",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// Upload the conflicting change
	response, httpResp := h.DoUpload(h.client1Token, conflictReq)

	// Assert HTTP 200 (conflicts are not HTTP errors)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert upload accepted but with conflict
	require.True(t, response.Accepted)
	require.Len(t, response.Statuses, 1)

	// Assert conflict detected
	status := response.Statuses[0]
	require.Equal(t, int64(2), status.SourceChangeID)
	require.Equal(t, "conflict", status.Status)
	require.Nil(t, status.NewServerVersion) // No new version on conflict
	require.NotNil(t, status.ServerRow)     // Server state returned

	// Parse and verify server row (sidecar v2 format)
	var serverState map[string]interface{}
	err := json.Unmarshal(status.ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, "note", serverState["table_name"])
	require.Equal(t, float64(2), serverState["server_version"]) // JSON numbers are float64
	require.Equal(t, false, serverState["deleted"])

	// Extract payload and verify business data
	payload, ok := serverState["payload"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Updated by Client 2", payload["title"])
	require.Equal(t, "Content updated by client 2", payload["content"])

	t.Logf("✅ U04 Conflict on Update test passed - conflict detected and server state returned")
}

func TestSimpleU02_BasicUpdate(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U02 – Basic update
	// Seed note(A) at SVID=1, then C1 updates at SVID=1 → applied → SVID=2.
	noteID := h.MakeUUID("000000000002")

	// First insert the note
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
					"title": "Original Title",
					"content": "Original Content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)
	require.Equal(t, "applied", insertResp.Statuses[0].Status)
	require.Equal(t, int64(1), *insertResp.Statuses[0].NewServerVersion)

	// Now update the note with correct server_version
	updateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1, // Expect current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated Title",
					"content": "Updated Content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// Upload the update
	response, httpResp := h.DoUpload(h.client1Token, updateReq)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert upload accepted
	require.True(t, response.Accepted)
	require.Len(t, response.Statuses, 1)

	// Assert change applied with incremented server version
	status := response.Statuses[0]
	require.Equal(t, int64(2), status.SourceChangeID)
	require.Equal(t, "applied", status.Status)
	require.NotNil(t, status.NewServerVersion)
	require.Equal(t, int64(2), *status.NewServerVersion)

	t.Logf("✅ U02 Basic Update test passed - note updated with server_version=2")
}

func TestSimpleU03_BasicDelete(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U03 – Basic delete
	// Seed note(A) at SVID=1, then C1 deletes at SVID=1 → applied → row removed.
	noteID := h.MakeUUID("000000000003")

	// First insert the note
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
					"title": "To Be Deleted",
					"content": "This will be deleted",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)
	require.Equal(t, "applied", insertResp.Statuses[0].Status)

	// Now delete the note
	deleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  1,   // Expect current version
				Payload:        nil, // DELETE has no payload
			},
		},
	}

	// Upload the delete
	response, httpResp := h.DoUpload(h.client1Token, deleteReq)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert upload accepted
	require.True(t, response.Accepted)
	require.Len(t, response.Statuses, 1)

	// Assert change applied
	status := response.Statuses[0]
	require.Equal(t, int64(2), status.SourceChangeID)
	require.Equal(t, "applied", status.Status)
	require.NotNil(t, status.NewServerVersion) // Sidecar v2: DELETE does return new version
	require.Equal(t, int64(2), *status.NewServerVersion)

	t.Logf("✅ U03 Basic Delete test passed - note deleted successfully")
}

func TestSimpleU05_ConflictOnDelete(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U05 – Conflict on delete
	// Seed note(A) at SVID=3, then C1 deletes expecting SVID=1 → conflict.
	noteID := h.MakeUUID("000000000005")

	// Insert and update the note to get server_version=2
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
					"title": "Protected Note",
					"content": "Cannot delete with wrong version",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)

	// Update to increment server_version to 2
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
					"title": "Updated Protected Note",
					"content": "Still cannot delete with wrong version",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	updateResp, _ := h.DoUpload(h.client2Token, updateReq)
	require.True(t, updateResp.Accepted)
	require.Equal(t, int64(2), *updateResp.Statuses[0].NewServerVersion)

	// Now client 1 tries to delete with stale version (expects version 1, but server has version 2)
	deleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1, // Client 1 hasn't seen the update
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  1, // Expect version 1, but server has version 2
				Payload:        nil,
			},
		},
	}

	// Upload the conflicting delete
	response, httpResp := h.DoUpload(h.client1Token, deleteReq)

	// Assert HTTP 200 (conflicts are not HTTP errors)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert upload accepted but with conflict
	require.True(t, response.Accepted)
	require.Len(t, response.Statuses, 1)

	// Assert conflict detected
	status := response.Statuses[0]
	require.Equal(t, int64(2), status.SourceChangeID)
	require.Equal(t, "conflict", status.Status)
	require.Nil(t, status.NewServerVersion) // No new version on conflict
	require.NotNil(t, status.ServerRow)     // Server state returned

	// Parse and verify server row (sidecar v2 format)
	var serverState map[string]interface{}
	err := json.Unmarshal(status.ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, "note", serverState["table_name"])
	require.Equal(t, float64(2), serverState["server_version"]) // JSON numbers are float64
	require.Equal(t, false, serverState["deleted"])

	// Extract payload and verify business data
	payload, ok := serverState["payload"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Updated Protected Note", payload["title"])
	require.Equal(t, "Still cannot delete with wrong version", payload["content"])

	t.Logf("✅ U05 Conflict on Delete test passed - conflict detected and server state returned")
}
