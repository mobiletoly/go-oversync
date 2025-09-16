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

// Advanced Upload Tests (U06-U10) from spec section 4

func TestU06_RetryAfterConflictRebase(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U06 – Retry after conflict (rebase) succeeds
	// Given: From U05, client updates local copy to SVID=5, issues DELETE again (new SCID).
	// When: Upload.
	// Then: Applied; row removed; server_version increments or delete accepted per server policy.

	noteID := h.MakeUUID("000000000001")

	// Create note and update to SVID=5
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
					"title": "Note for Rebase Test",
					"content": "Will be updated then deleted",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)

	// Update to SVID=5
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

	// Another client updates the note (increments to SVID=6)
	c2UpdateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  5,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "C2 Update",
					"content": "Updated by Client 2",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	c2UpdateResp, _ := h.DoUpload(h.client2Token, c2UpdateReq)
	require.True(t, c2UpdateResp.Accepted)
	require.Equal(t, int64(6), *c2UpdateResp.Statuses[0].NewServerVersion)

	// Client 1 tries to delete with stale SVID=5 → conflict
	deleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 6,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  5, // Stale version
				Payload:        nil,
			},
		},
	}

	deleteResp, _ := h.DoUpload(h.client1Token, deleteReq)
	require.True(t, deleteResp.Accepted)
	require.Equal(t, "conflict", deleteResp.Statuses[0].Status)

	// Client 1 rebases: updates local copy to SVID=6, issues DELETE again (new SCID)
	deleteRetryReq := &oversync.UploadRequest{
		LastServerSeqSeen: 6,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 7, // New SCID
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  6, // Correct version after rebase
				Payload:        nil,
			},
		},
	}

	deleteRetryResp, httpResp := h.DoUpload(h.client1Token, deleteRetryReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, deleteRetryResp.Accepted)
	require.Equal(t, "applied", deleteRetryResp.Statuses[0].Status)

	// Verify row is removed
	note, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.Nil(t, note)

	t.Logf("✅ U06 Retry After Conflict (Rebase) test passed - conflict resolved with rebase")
}

func TestU07_IdempotentReUpload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U07 – Idempotent re-upload (duplicate SCID)
	// Given: U01 already performed. C1 retries the same payload with (source_id, SCID=1).
	// When: Upload.
	// Then: HTTP: accepted (noop/applied); no duplicate in server_change_log; DB unchanged.

	noteID := h.MakeUUID("000000000001")

	// Original upload (U01 equivalent)
	originalReq := &oversync.UploadRequest{
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

	originalResp, _ := h.DoUpload(h.client1Token, originalReq)
	require.True(t, originalResp.Accepted)
	require.Equal(t, "applied", originalResp.Statuses[0].Status)
	require.Equal(t, int64(1), originalResp.HighestServerSeq)

	// Get initial state
	note1, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.NotNil(t, note1)
	initialTitle := note1.Title
	initialServerVersion := note1.ServerVersion

	// Count initial server_change_log entries
	var initialLogCount int
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.server_change_log`).Scan(&initialLogCount)
	require.NoError(t, err)

	// Retry the same payload (duplicate SCID)
	retryReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1, // Same SCID
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

	retryResp, httpResp := h.DoUpload(h.client1Token, retryReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, retryResp.Accepted)
	require.Equal(t, "applied", retryResp.Statuses[0].Status) // Idempotent
	require.Equal(t, int64(1), retryResp.HighestServerSeq)    // No new server_id

	// Verify no duplicate in server_change_log
	var finalLogCount int
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.server_change_log`).Scan(&finalLogCount)
	require.NoError(t, err)
	require.Equal(t, initialLogCount, finalLogCount) // No new entries

	// Verify DB unchanged
	note2, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.NotNil(t, note2)
	require.Equal(t, initialTitle, note2.Title)
	require.Equal(t, initialServerVersion, note2.ServerVersion)

	t.Logf("✅ U07 Idempotent Re-upload test passed - no duplicates, DB unchanged")
}

func TestU08_MixedBatchAcrossTables(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U08 – Mixed batch (insert+update+delete across tables)
	// Given: C1 batch contains note INSERT, task UPDATE, note DELETE.
	// When: Upload.
	// Then: Each change follows version rules; per-change statuses reflect result; highest_server_seq equals max SID in txn.

	noteID1 := h.MakeUUID("newnote1")
	noteID2 := h.MakeUUID("oldnote2")
	taskID1 := h.MakeUUID("oldtask1")

	// First, create a note and task to enable UPDATE and DELETE
	setupReq := &oversync.UploadRequest{
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
					"title": "Note to Delete",
					"content": "Will be deleted",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 2,
				Table:          "task",
				Op:             "INSERT",
				PK:             taskID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Task to Update",
					"done": false,
					"updated_at": %d
				}`, taskID1.String(), time.Now().Unix())),
			},
		},
	}

	setupResp, _ := h.DoUpload(h.client1Token, setupReq)
	require.True(t, setupResp.Accepted)
	require.Equal(t, int64(2), setupResp.HighestServerSeq)

	// Check actual server_versions after setup
	task, err := h.GetServerTask(taskID1)
	require.NoError(t, err)
	require.NotNil(t, task)
	taskServerVersion := task.ServerVersion

	note2, err := h.GetServerNote(noteID2)
	require.NoError(t, err)
	require.NotNil(t, note2)
	note2ServerVersion := note2.ServerVersion

	// Mixed batch: note INSERT, task UPDATE, note DELETE
	mixedReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "note",
				Op:             "INSERT", // New note
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "New Note",
					"content": "Inserted in mixed batch",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 4,
				Table:          "task",
				Op:             "UPDATE", // Update existing task
				PK:             taskID1.String(),
				ServerVersion:  taskServerVersion, // Use actual current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated Task",
					"done": true,
					"updated_at": %d
				}`, taskID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 5,
				Table:          "note",
				Op:             "DELETE", // Delete existing note
				PK:             noteID2.String(),
				ServerVersion:  note2ServerVersion, // Use actual current version
				Payload:        nil,
			},
		},
	}

	mixedResp, httpResp := h.DoUpload(h.client1Token, mixedReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, mixedResp.Accepted)
	require.Len(t, mixedResp.Statuses, 3)

	// Verify each change follows version rules
	// Note INSERT should be applied
	require.Equal(t, int64(3), mixedResp.Statuses[0].SourceChangeID)
	require.Equal(t, "applied", mixedResp.Statuses[0].Status)
	require.NotNil(t, mixedResp.Statuses[0].NewServerVersion)

	// Task UPDATE should be applied
	require.Equal(t, int64(4), mixedResp.Statuses[1].SourceChangeID)
	require.Equal(t, "applied", mixedResp.Statuses[1].Status)
	require.NotNil(t, mixedResp.Statuses[1].NewServerVersion)

	// Note DELETE should be applied
	require.Equal(t, int64(5), mixedResp.Statuses[2].SourceChangeID)
	require.Equal(t, "applied", mixedResp.Statuses[2].Status)
	require.NotNil(t, mixedResp.Statuses[2].NewServerVersion) // Sidecar v2: DELETE does return version

	// Verify highest_server_seq equals max SID in transaction
	require.Equal(t, int64(5), mixedResp.HighestServerSeq)

	// Verify actual database state
	// New note should exist
	newNote, err := h.GetServerNote(noteID1)
	require.NoError(t, err)
	require.NotNil(t, newNote)
	require.Equal(t, "New Note", newNote.Title)

	// Deleted note should not exist
	deletedNote, err := h.GetServerNote(noteID2)
	require.NoError(t, err)
	require.Nil(t, deletedNote)

	// Updated task should exist with new values
	updatedTask, err := h.GetServerTask(taskID1)
	require.NoError(t, err)
	require.NotNil(t, updatedTask)
	require.Equal(t, "Updated Task", updatedTask.Title)
	require.True(t, updatedTask.Done)

	t.Logf("✅ U08 Mixed Batch Across Tables test passed - all operations applied correctly")
}

func TestU09_LargeBatchAndOrdering(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U09 – Large batch & ordering preserved
	// Given: 500+ changes interleaved across tables, SCID increasing.
	// When: Upload.
	// Then: Server applies in order; server_change_log.server_id monotonic; statuses align; txn atomic.

	// For testing purposes, use a smaller batch (50 changes) to keep test reasonable
	batchSize := 50
	changes := make([]oversync.ChangeUpload, batchSize)

	// Create interleaved changes across note and task tables
	for i := 0; i < batchSize; i++ {
		scid := int64(i + 1)

		if i%2 == 0 {
			// Even indices: note operations
			noteID := h.MakeUUID(fmt.Sprintf("note%d", i))
			changes[i] = oversync.ChangeUpload{
				SourceChangeID: scid,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Note %d",
					"content": "Content for note %d",
					"updated_at": %d
				}`, noteID.String(), i, i, time.Now().Unix())),
			}
		} else {
			// Odd indices: task operations
			taskID := h.MakeUUID(fmt.Sprintf("task%d", i))
			changes[i] = oversync.ChangeUpload{
				SourceChangeID: scid,
				Table:          "task",
				Op:             "INSERT",
				PK:             taskID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Task %d",
					"done": %t,
					"updated_at": %d
				}`, taskID.String(), i, i%4 == 1, time.Now().Unix())),
			}
		}
	}

	largeBatchReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           changes,
	}

	largeBatchResp, httpResp := h.DoUpload(h.client1Token, largeBatchReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, largeBatchResp.Accepted)
	require.Len(t, largeBatchResp.Statuses, batchSize)

	// Verify all changes applied
	for i, status := range largeBatchResp.Statuses {
		require.Equal(t, int64(i+1), status.SourceChangeID)
		require.Equal(t, "applied", status.Status)
	}

	// Verify highest_server_seq equals batch size
	require.Equal(t, int64(batchSize), largeBatchResp.HighestServerSeq)

	// Verify server_change_log.server_id is monotonic
	rows, err := h.service.Pool().Query(h.ctx, `
		SELECT server_id, source_change_id, table_name 
		FROM sync.server_change_log 
		WHERE source_id = $1 
		ORDER BY server_id`, h.client1ID)
	require.NoError(t, err)
	defer rows.Close()

	var prevServerID int64 = 0
	var logEntries []struct {
		ServerID       int64
		SourceChangeID int64
		TableName      string
	}

	for rows.Next() {
		var entry struct {
			ServerID       int64
			SourceChangeID int64
			TableName      string
		}
		err := rows.Scan(&entry.ServerID, &entry.SourceChangeID, &entry.TableName)
		require.NoError(t, err)

		// Verify monotonic server_id
		require.Greater(t, entry.ServerID, prevServerID)
		prevServerID = entry.ServerID

		logEntries = append(logEntries, entry)
	}

	// Verify we have all entries
	require.Len(t, logEntries, batchSize)

	// Verify ordering: source_change_id should match the order
	for i, entry := range logEntries {
		require.Equal(t, int64(i+1), entry.SourceChangeID)

		// Verify table alternation
		if i%2 == 0 {
			require.Equal(t, "note", entry.TableName)
		} else {
			require.Equal(t, "task", entry.TableName)
		}
	}

	// Verify transaction atomicity by checking all records exist
	var noteCount, taskCount int
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM note`).Scan(&noteCount)
	require.NoError(t, err)
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM task`).Scan(&taskCount)
	require.NoError(t, err)

	expectedNotes := (batchSize + 1) / 2 // Ceiling division for even indices
	expectedTasks := batchSize / 2       // Floor division for odd indices
	require.Equal(t, expectedNotes, noteCount)
	require.Equal(t, expectedTasks, taskCount)

	t.Logf("✅ U09 Large Batch & Ordering test passed - %d changes processed in order atomically", batchSize)
}

func TestU10_MalformedPayloadValidation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// U10 – Malformed payload & validation
	// • Missing required fields (table/op/pk/server_version).
	// • Unknown table.
	// • Non-UUID pk_uuid.
	// Then: 400; no partial side effects; no log insertion.

	// Test missing required fields
	malformedReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				// Missing Table field
				Op:            "INSERT",
				PK:            h.MakeUUID("000000000001").String(),
				ServerVersion: 0,
				Payload:       json.RawMessage(`{"title": "Test"}`),
			},
		},
	}

	// This should be handled by the server validation
	// Upload malformed request - should be rejected (sidecar v2: returns HTTP 200 with "invalid" status)
	resp, httpResp := h.DoUpload(h.client1Token, malformedReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "invalid", resp.Statuses[0].Status)

	// Verify no side effects - no entries in server_change_log
	var logCount int
	err := h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.server_change_log`).Scan(&logCount)
	require.NoError(t, err)
	require.Equal(t, 0, logCount)

	t.Logf("✅ U10 Malformed Payload Validation test passed - validation prevents side effects")
}
