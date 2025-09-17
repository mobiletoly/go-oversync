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

// Download Tests (D01-D05) from spec section 2

func TestD01_EmptyDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// D01 – Empty download
	// No changes in database → empty response.
	response, httpResp := h.DoDownload(h.client1Token, 0, 100)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert empty response
	require.Len(t, response.Changes, 0)
	require.False(t, response.HasMore)
	require.Equal(t, int64(0), response.NextAfter)

	t.Logf("✅ D01 Empty Download test passed - no changes returned")
}

func TestD02_SingleChangeDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// D02 – Single change download
	// Upload one change, then download → returns that change.
	noteID := h.MakeUUID("000000000001")

	// Upload a change first
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

	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, int64(1), uploadResp.HighestServerSeq)

	// Now download changes
	response, httpResp := h.DoDownload(h.client2Token, 0, 100) // Different client downloads

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert single change returned
	require.Len(t, response.Changes, 1)
	require.False(t, response.HasMore)
	require.Equal(t, int64(1), response.NextAfter)

	// Verify change details
	change := response.Changes[0]
	require.Equal(t, int64(1), change.ServerID)
	require.Equal(t, "note", change.TableName)
	require.Equal(t, "INSERT", change.Op)
	require.Equal(t, noteID.String(), change.PK)
	require.Equal(t, h.client1ID, change.SourceID)
	require.Equal(t, int64(1), change.SourceChangeID)
	require.NotNil(t, change.Payload)

	// Verify payload content
	var payload map[string]interface{}
	err := json.Unmarshal(change.Payload, &payload)
	require.NoError(t, err)
	require.Equal(t, "Test Note", payload["title"])
	require.Equal(t, "This is a test note", payload["content"])

	t.Logf("✅ D02 Single Change Download test passed - change downloaded correctly")
}

func TestD03_MultipleChangesDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// D03 – Multiple changes download
	// Upload multiple changes, then download → returns all changes in order.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")

	// Upload multiple changes
	uploadReq := &oversync.UploadRequest{
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

	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, int64(2), uploadResp.HighestServerSeq)

	// Now download changes
	response, httpResp := h.DoDownload(h.client2Token, 0, 100)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert multiple changes returned in order
	require.Len(t, response.Changes, 2)
	require.False(t, response.HasMore)
	require.Equal(t, int64(2), response.NextAfter)

	// Verify changes are in server_id order
	require.Equal(t, int64(1), response.Changes[0].ServerID)
	require.Equal(t, int64(2), response.Changes[1].ServerID)

	// Verify first change
	change1 := response.Changes[0]
	require.Equal(t, "note", change1.TableName)
	require.Equal(t, "INSERT", change1.Op)
	require.Equal(t, noteID1.String(), change1.PK)
	require.Equal(t, int64(1), change1.SourceChangeID)

	// Verify second change
	change2 := response.Changes[1]
	require.Equal(t, "note", change2.TableName)
	require.Equal(t, "INSERT", change2.Op)
	require.Equal(t, noteID2.String(), change2.PK)
	require.Equal(t, int64(2), change2.SourceChangeID)

	t.Logf("✅ D03 Multiple Changes Download test passed - all changes downloaded in order")
}

func TestD04_IncrementalDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// D04 – Incremental download
	// Upload changes, download some, upload more, download incrementally.
	noteID1 := h.MakeUUID("000000000001")
	noteID2 := h.MakeUUID("000000000002")

	// Upload first batch of changes
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
					"content": "First batch",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, uploadResp1.Accepted)
	require.Equal(t, int64(1), uploadResp1.HighestServerSeq)

	// Download first batch
	response1, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, response1.Changes, 1)
	require.Equal(t, int64(1), response1.NextAfter)

	// Upload second batch of changes
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Second Note",
					"content": "Second batch",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp2, _ := h.DoUpload(h.client1Token, uploadReq2)
	require.True(t, uploadResp2.Accepted)
	require.Equal(t, int64(2), uploadResp2.HighestServerSeq)

	// Download incrementally (only new changes)
	response2, httpResp := h.DoDownload(h.client2Token, response1.NextAfter, 100)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert only new change returned
	require.Len(t, response2.Changes, 1)
	require.False(t, response2.HasMore)
	require.Equal(t, int64(2), response2.NextAfter)

	// Verify it's the second change
	change := response2.Changes[0]
	require.Equal(t, int64(2), change.ServerID)
	require.Equal(t, noteID2.String(), change.PK)
	require.Equal(t, int64(2), change.SourceChangeID)

	t.Logf("✅ D04 Incremental Download test passed - only new changes downloaded")
}

func TestD05_LimitedDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// D05 – Limited download
	// Upload many changes, download with limit → returns limited set with has_more=true.

	// Upload 5 changes
	changes := make([]oversync.ChangeUpload, 5)
	for i := 0; i < 5; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("00000000000%d", i+1))
		changes[i] = oversync.ChangeUpload{
			SourceChangeID: int64(i + 1),
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID.String(),
			ServerVersion:  0,
			Payload: json.RawMessage(fmt.Sprintf(`{
				"id": "%s",
				"title": "Note %d",
				"content": "Content %d",
				"updated_at": %d
			}`, noteID.String(), i+1, i+1, time.Now().Unix())),
		}
	}

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           changes,
	}

	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, int64(5), uploadResp.HighestServerSeq)

	// Download with limit=3
	response, httpResp := h.DoDownload(h.client2Token, 0, 3)

	// Assert HTTP 200
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	// Assert limited results with has_more=true
	require.Len(t, response.Changes, 3)
	require.True(t, response.HasMore)
	require.Equal(t, int64(3), response.NextAfter)

	// Verify changes are in order
	for i, change := range response.Changes {
		require.Equal(t, int64(i+1), change.ServerID)
		require.Equal(t, int64(i+1), change.SourceChangeID)
	}

	// Download remaining changes
	response2, _ := h.DoDownload(h.client2Token, response.NextAfter, 3)
	require.Len(t, response2.Changes, 2) // Remaining 2 changes
	require.False(t, response2.HasMore)
	require.Equal(t, int64(5), response2.NextAfter)

	t.Logf("✅ D05 Limited Download test passed - pagination working correctly")
}

func TestD06_UnknownTableForwardCompatibility(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// D06 – Unknown table forward compatibility
	// Given: Server has changes for tables the client doesn't recognize.
	// When: Client downloads.
	// Then: Unknown table changes included; client can skip/store for future compatibility.

	noteID := h.MakeUUID("note001")

	// Phase 1: Create a known table change
	knownReq := &oversync.UploadRequest{
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
					"title": "Known Table Note",
					"content": "This is a known table",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	knownResp, _ := h.DoUpload(h.client1Token, knownReq)
	require.True(t, knownResp.Accepted)

	// Phase 2: Simulate a future table change by directly inserting into server_change_log
	// This simulates what would happen if a newer server version supported additional tables
	futureTableID := h.MakeUUID("future001")
	futurePayload := json.RawMessage(fmt.Sprintf(`{
		"id": "%s",
		"name": "Future Entity",
		"type": "unknown",
		"created_at": %d
	}`, futureTableID.String(), time.Now().Unix()))

	// Insert directly into sync.server_change_log to simulate unknown table
	// Use a different source_id so Client 2 can see this change
	// Extract user_id from the test harness token
	testUserID := h.ExtractUserIDFromToken(h.client1Token)
	_, err := h.service.Pool().Exec(h.ctx, `
		INSERT INTO sync.server_change_log (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
		VALUES ($1, $2, $3, $4, $5::uuid, $6, $7, $8, $9)`,
		testUserID, "public", "future_table", "INSERT", futureTableID.String(), futurePayload, "future-server", 1, int64(1))
	require.NoError(t, err)

	// Phase 3: Add another known table change after the unknown one
	taskID := h.MakeUUID("task001")
	afterUnknownReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2, // After the unknown table change
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "task",
				Op:             "INSERT",
				PK:             taskID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "After Unknown",
					"done": false,
					"updated_at": %d
				}`, taskID.String(), time.Now().Unix())),
			},
		},
	}

	afterUnknownResp, _ := h.DoUpload(h.client1Token, afterUnknownReq)
	require.True(t, afterUnknownResp.Accepted)

	// Phase 4: Client 2 downloads all changes (excludes own changes)
	downloadResp, httpResp := h.DoDownload(h.client2Token, 0, 100)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, downloadResp.Changes, 3) // Client 1's note + future_table + Client 1's task
	require.False(t, downloadResp.HasMore)

	changes := downloadResp.Changes

	// Verify first change (known table)
	require.Equal(t, int64(1), changes[0].ServerID)
	require.Equal(t, "note", changes[0].TableName)
	require.Equal(t, "INSERT", changes[0].Op)
	require.Equal(t, noteID.String(), changes[0].PK)

	// Verify second change (unknown table) - should be included
	require.Equal(t, int64(2), changes[1].ServerID)
	require.Equal(t, "future_table", changes[1].TableName) // Unknown table name
	require.Equal(t, "INSERT", changes[1].Op)
	require.Equal(t, futureTableID.String(), changes[1].PK)
	require.Equal(t, "future-server", changes[1].SourceID)

	// Verify the unknown table payload is preserved
	var unknownPayload map[string]interface{}
	err = json.Unmarshal(changes[1].Payload, &unknownPayload)
	require.NoError(t, err)
	require.Equal(t, "Future Entity", unknownPayload["name"])
	require.Equal(t, "unknown", unknownPayload["type"])

	// Verify third change (known table after unknown)
	require.Equal(t, int64(3), changes[2].ServerID)
	require.Equal(t, "task", changes[2].TableName)
	require.Equal(t, "INSERT", changes[2].Op)
	require.Equal(t, taskID.String(), changes[2].PK)

	// Phase 5: Verify client can handle unknown tables gracefully
	// In a real client implementation, unknown tables would be:
	// 1. Stored for future compatibility
	// 2. Skipped during local application
	// 3. Preserved for re-upload to other clients

	// For this test, we verify the server provides all information needed
	unknownChange := changes[1]
	require.NotEmpty(t, unknownChange.TableName)
	require.NotEmpty(t, unknownChange.Op)
	require.NotEmpty(t, unknownChange.PK)
	require.NotEmpty(t, unknownChange.Payload)
	require.NotEmpty(t, unknownChange.SourceID)
	require.Greater(t, unknownChange.SourceChangeID, int64(0))
	require.NotZero(t, unknownChange.Timestamp)

	t.Logf("✅ D06 Unknown Table Forward Compatibility test passed - unknown table '%s' included in download", unknownChange.TableName)
}
